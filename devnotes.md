# Development notes

This project was started to understand internals of Kafka Connect framework and here I will share some observations about it.

All code snippets and line numbers are based on 2.1.1 version of kafka.

## Offsets and message sending

Kafka Connect doesn't allow you to store objects (even Serializable ones) or collections in offsets (`OffsetUtils#49`) but it allows you to store huge strings and byte arrays there. So objects can be stored as a byte[] or ByteBuffer representation or as a base64 string.

Producer has following configuration by default (`Worker#135` and then `Worker#501`):
```java
        // These settings are designed to ensure there is no data loss. They *may* be overridden via configs passed to the
        // worker, but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
```

Offsets are being committed:
- by `SourceTaskOffsetsCommitter` every 1 minute (default value, can be changed with `offset.flush.interval.ms` property) - started by `Worker#470` 
- by `WorkerSourceTask` class on processing message.

Second way is really interesting. `SourceTask.poll()` provides a list of messages and they are processed sequentially. `KafkaProducer.send()` method is asynchronous but because the configuration has only 1 in-flight request, messages can't be sent out of order and `outstandingMessages`/`outstandingMessagesBacklog` is ordered and messages there would be removed in order.
On committing offsets, WorkerSourceTask will wait until `outstandingMessages` is empty (all pending messages have been sent) but there may be a situation when flushing is started in the middle of processing. For example offsets for first 7 messages out of 10 can be committed. That's why it's important to keep offsets in order.

It's good to know that although `KafkaProducer.send()` returns Future, only the actual sending is done asynchronously. Obtaining metadata is done synchronously and it will block forever (check the configuration above) until it will be obtained or exception will be thrown. In case of RetriableException, kafka connect will retry sending messages starting from the failed after 100ms (`WorkerSourceTask.SEND_FAILED_BACKOFF_MS`, not configurable). 
In case of retry, no poll will be done and kafka connect will continue to send remaining records from the old batch. It won't try to send again messages that were sent successfully (which means `KafkaProducer.send()` method didn't produce any exception).

## Waiting for a next poll

Thread.sleep() is an awful way to achieve this because it blocks the task and it won't kafka connect to stop it as it will wait for poll() to finish. 
It waits for 5 seconds by default (can be changed by `task.shutdown.graceful.timeout.ms` parameter) and then just just continue to stop Worker (`Worker#196` and then `Worker#585`) or StandaloneHerder (`StandaloneHerder#107` and then `StandaloneHerder#314`) depending on the kafka connect mode.  

It is definitely a problem for a standalone mode but I'm not really sure it's a problem in a distributed mode, I need to check it.

I've decided that having CountDownLatch and count it down in the stop() method should be good enough. Although I can look further for better a way to pause the thread. WorkerSourceTask.execute() has following code:

```java
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
``` 
shouldPause() is checking for `this.targetState == TargetState.PAUSED`, maybe I can utilize it somehow if that's not internal kafka connect functionality.

I've checked some public projects for their ways of waiting:

- kafka-connect-jdbc uses `org.apache.kafka.common.utils.Time` which uses Thread.sleep() underneath but wraps the InterruptedException to interrupt the current thread (`SystemTime#36`)
- kafka-connect-github-source uses Thread.sleep()
- kafka-connect-irc has no waiting whatsoever :)

## Preventing reprocessing of the same item in feed

When we read a feed again, some or all of the items there may be already processed by previous poll. To prevent them from being sent twice, we need to store information about previously sent items in the offsets. We may implement it with following approaches:
- serialize the whole item into base64 string (`SyndFeedImpl.toString()` -> base64 or `serialize(SyndFeed)` -> base64) and store last sent items in offs
  - \+ allows you to resend messages that were updated but has the same ID
  - \- depends on the implementation and may break and resend messages after updating ROME library
  - \- will resend the message even if some minor field that is not present in output message has been changed
  - \- offsets may be huge (that's not really a problem though - see Offsets and message sending section of this document)

- store list of IDs (which is id in Atom and _(it's complicated as no field is mandatory in RSS)_ in RSS)
  - \+ offsets are compact
  - \+/- it's tricky to extract ID from RSS feeds as no element in `<item>` is mandatory.
  - \- if the item will be updated, we won't send a new message

- serialize the output `Struct` objects:
  - \+ we can send new messages with updates and we won't send duplicates if some of the fields than are not present in schema were updated
  - \+ offsets are much more compact comparing to option #1 and still not that big to store
  - \- we don't need to store schema and the only easy way to get the `Struct.values` is to call `Struct.toString()` which is not guaranteed to be the same in future versions of kafka connect
  - \- in case of changing the schema all messages will be resent
  - \- some users may not want to get updates

- serialize RssData object
  - \+/- the same pros and cons as in option #3 but we don't need to rely on toString implementation and write our own serialization
  - \+ if we want, we can even support schema upgrades and we won't resend messages that were already sent but with previous schema

I think the best way would be to implement option #4 and then later implement option #2 as well giving users a possibility to change the behaviour in the configuration.
