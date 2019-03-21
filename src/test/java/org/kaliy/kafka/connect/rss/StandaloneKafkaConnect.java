package org.kaliy.kafka.connect.rss;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderProvider;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * This class is based on the kafka-connect-standalone CLI utility code
 *
 * @see org.apache.kafka.connect.cli.ConnectStandalone
 */
public class StandaloneKafkaConnect {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneKafkaConnect.class);

    private Thread connectThread;
    private CountDownLatch stopLatch;

    private final String offsetsFilename;
    private final int maxTasks;
    private final String urls;

    public StandaloneKafkaConnect(String offsetsFilename, int maxTasks, String urls) {
        this.offsetsFilename = offsetsFilename;
        this.maxTasks = maxTasks;
        this.urls = urls;
    }

    public void start() {
        stopLatch = new CountDownLatch(1);
        connectThread = new Thread(() -> {
            Map<String, String> workerProps = workerProps();

            Plugins plugins = new Plugins(workerProps);
            plugins.compareAndSwapWithDelegatingLoader();
            StandaloneConfig config = new StandaloneConfig(workerProps);

            String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);

            RestServer rest = new RestServer(config);
            HerderProvider provider = new HerderProvider();
            rest.start(provider, plugins);

            URI advertisedUrl = rest.advertisedUrl();
            String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

            Worker worker = new Worker(workerId, Time.SYSTEM, plugins, config, new FileOffsetBackingStore());

            Herder herder = new StandaloneHerder(worker, kafkaClusterId);
            final Connect connect = new Connect(herder, rest);

            logger.info("Kafka Connect standalone worker has been initialized");

            try {
                connect.start();
                provider.setHerder(herder);
                Map<String, String> connectorProps = connectorProps();
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                    if (error != null) {
                        logger.error("Failed to create job", error);
                    } else {
                        logger.info("Created connector {}", info.result().name());
                    }
                });
                herder.putConnectorConfig(
                        connectorProps.get(ConnectorConfig.NAME_CONFIG),
                        connectorProps, false, cb);
                cb.get();
            } catch (Throwable t) {
                logger.error("Stopping after connector error", t);
                connect.stop();
                connect.awaitStop();
            }
            try {
                stopLatch.await();
            } catch (InterruptedException e) {
                logger.info("Connect thread has been interrupted, stopping.");
                connect.stop();
                connect.awaitStop();
            }
            connect.stop();
            connect.awaitStop();
        });

        connectThread.start();
    }

    public void stop() {
        stopLatch.countDown();
        while (connectThread.isAlive()) {
            logger.info("Waiting for the kafka connect to stop");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteOffsetsFile() {
        new File(offsetsFilename).delete();
    }

    private Map<String, String> workerProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", "localhost:9092");
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "true");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "true");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "true");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "true");
        workerProps.put("rest.port", "8086");
        workerProps.put("rest.host.name", "127.0.0.1");
        workerProps.put("offset.storage.file.filename", offsetsFilename);
        workerProps.put("offset.flush.interval.ms", "10000");
        return workerProps;
    }

    private Map<String, String> connectorProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("name", "RssSourceConnectorDemo");
        workerProps.put("tasks.max", Integer.toString(maxTasks));
        workerProps.put("sleep.seconds", "2");
        workerProps.put("connector.class", "org.kaliy.kafka.connect.rss.RssSourceConnector");
        workerProps.put("rss.urls", urls);
        workerProps.put("topic", "test_topic");
        return workerProps;
    }
}
