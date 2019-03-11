package org.kaliy.kafka.connect.rss;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.awaitility.Awaitility.await;

class RssSourceConnectorIntegrationTest {
    private static Thread connectorThread;

    @BeforeAll
    static void beforeAll() {
        connectorThread = new Thread(() -> {
            try {
                ConnectStandalone.main(new String[] {
                        "config/worker.properties", "config/RssSourceConnectorExample.properties"
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        connectorThread.start();
    }

    @Test
    void producesStaticMessage() throws IOException {
        Consumer<String, String> consumer = createConsumer();
        ConsumerRecords<String, String> records =
                await().atMost(org.awaitility.Duration.ONE_MINUTE)
                        .until(() -> consumer.poll(Duration.ofSeconds(1)), recs -> recs.count() > 0);
        assertThatJson(records.iterator().next().value()).isEqualTo(read("sample-output.json"));
    }

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test_topic"));
        return consumer;
    }

    private static String read(String file) throws IOException {
        URL url = Resources.getResource(file);
        return Resources.toString(url, Charsets.UTF_8);
    }
}
