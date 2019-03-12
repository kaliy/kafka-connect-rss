# Development notes

This project was started to understand internals of Kafka Connect framework and here I will share some observations about it.

All code snippets and line numbers are based on 2.1.1 version of kafka.

## Offsets and message sending

Kafka Connect doesn't allow you to store objects (even Serializable ones) or collections in offsets (`OffsetUtils#49`) but it allows you to store huge strings and byte arrays there. So objects can be stored as a byte[] or ByteBuffer representation or as a base64 string.

Producer has following configuration by default (`Worker#135` and then `Worker#501`):
~~~~
        // These settings are designed to ensure there is no data loss. They *may* be overridden via configs passed to the
        // worker, but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
~~~~

Offsets are being committed:
- by `SourceTaskOffsetsCommitter` every 1 minute (default value, can be changed with `offset.flush.interval.ms` property) - started by `Worker#470` 
- by `WorkerSourceTask` class on processing message.

Second way is really interesting. `SourceTask.poll()` provides a list of messages and they are processed sequentially. `KafkaProducer.send()` method is asynchronous but because the configuration has only 1 in-flight request, messages can't be sent out of order and `outstandingMessages`/`outstandingMessagesBacklog` is ordered and messages there would be removed in order.
On committing offsets, WorkerSourceTask will wait until `outstandingMessages` is empty (all pending messages have been sent) but there may be a situation when flushing is started in the middle of processing. For example offsets for first 7 messages out of 10 can be committed. That's why it's important to keep offsets in order.

It's good to know that although `KafkaProducer.send()` returns Future, only the actual sending is done asynchronously. Obtaining metadata is done synchronously and it will block forever (check the configuration above) until it will be obtained or exception will be thrown. In case of RetriableException, kafka connect will retry sending messages starting from the failed after 100ms (`WorkerSourceTask.SEND_FAILED_BACKOFF_MS`, not configurable). 
In case of retry, no poll will be done and kafka connect will continue to send remaining records from the old batch. It won't try to send again messages that were sent successfully (which means `KafkaProducer.send()` method didn't produce any exception).

## Waiting for a next poll

Thread.sleep() is an awful way to achieve this, I need to investigate further. WorkerSourceTask.execute() has following code:

~~~~
            while (!isStopping()) {
                if (shouldPause()) {
                    onPause();
                    if (awaitUnpause()) {
                        onResume();
                    }
                    continue;
                }
                ...
            }
~~~~ 
shouldPause() is checking for `this.targetState == TargetState.PAUSED`, maybe we can utilize it somehow if that's not internal kafka connect functionality.
