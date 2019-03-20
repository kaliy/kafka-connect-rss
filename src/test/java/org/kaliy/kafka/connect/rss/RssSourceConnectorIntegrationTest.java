package org.kaliy.kafka.connect.rss;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RssSourceConnectorIntegrationTest {
    private static WireMockServer wireMockServer;

    private StandaloneKafkaConnect standaloneKafkaConnect = new StandaloneKafkaConnect();

    @BeforeAll
    static void beforeAll() {
        wireMockServer = new WireMockServer(8888);
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        standaloneKafkaConnect.stop();
        wireMockServer.resetAll();
    }

    @Test
    void pollsMessagesMultipleTimes() throws IOException {
        //TODO: split this test into few
        wireMockServer.stubFor(get("/feed.atom").willReturn(aResponse().withBody(read("integration-test/multiple-requests/input-1.atom"))));
        wireMockServer.stubFor(get("/feed.rss").willReturn(aResponse().withBody(read("integration-test/multiple-requests/input-1.rss"))));

        standaloneKafkaConnect.start();

        Consumer<String, String> consumer = createConsumer();
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(org.awaitility.Duration.ONE_MINUTE)
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });
        consumerRecords.sort(Comparator.comparing(c -> ((String) c.value())));
        assertThatJson(consumerRecords.get(0).value()).isEqualTo(read("integration-test/multiple-requests/output-1-1.json"));
        assertThatJson(consumerRecords.get(1).value()).isEqualTo(read("integration-test/multiple-requests/output-1-2.json"));
        assertThatJson(consumerRecords.get(2).value()).isEqualTo(read("integration-test/multiple-requests/output-1-3.json"));
        assertThatJson(consumerRecords.get(3).value()).isEqualTo(read("integration-test/multiple-requests/output-1-4.json"));

        IntStream.rangeClosed(0, 5).forEach((i) -> assertThat(consumer.poll(Duration.ofSeconds(1)).iterator()).isExhausted());

        consumerRecords.clear();
        wireMockServer.stubFor(get("/feed.atom").willReturn(aResponse().withBody(read("integration-test/multiple-requests/input-2.atom"))));
        wireMockServer.stubFor(get("/feed.rss").willReturn(aResponse().withBody(read("integration-test/multiple-requests/input-2.rss"))));

        await().atMost(org.awaitility.Duration.ONE_MINUTE)
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 2;
                });
        consumerRecords.sort(Comparator.comparing(c -> ((String) c.value())));
        assertThatJson(consumerRecords.get(0).value()).isEqualTo(read("integration-test/multiple-requests/output-2-1.json"));
        assertThatJson(consumerRecords.get(1).value()).isEqualTo(read("integration-test/multiple-requests/output-2-2.json"));
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
