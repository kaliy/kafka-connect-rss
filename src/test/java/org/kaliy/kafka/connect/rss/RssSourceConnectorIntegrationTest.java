package org.kaliy.kafka.connect.rss;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class RssSourceConnectorIntegrationTest {

    @Container
    private static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    private static WireMockServer wireMockServer;

    private static final String ATOM = "http://localhost:8888/feed.atom";
    private static final String RSS = "http://localhost:8888/feed.rss";
    private static final String URLS = ATOM + " " + RSS;
    private StandaloneKafkaConnect standaloneKafkaConnect;

    @BeforeAll
    static void beforeAll() {
        wireMockServer = new WireMockServer(8888);
        wireMockServer.start();
    }

    @BeforeEach
    void setUp() throws Exception {
        standaloneKafkaConnect = new StandaloneKafkaConnect("target/" + UUID.randomUUID(), 2, URLS, kafkaContainer.getBootstrapServers());
        wireMockServer.stubFor(get("/feed.atom").willReturn(aResponse().withBody(read("integration-test/input-1.atom"))));
        wireMockServer.stubFor(get("/feed.rss").willReturn(aResponse().withBody(read("integration-test/input-1.rss"))));
    }

    @AfterEach
    void tearDown() {
        standaloneKafkaConnect.stop();
        standaloneKafkaConnect.deleteOffsetsFile();
        wireMockServer.resetAll();
    }

    @Test
    void pollsFeedsAndSendsMessages() throws IOException {
        Consumer<String, String> consumer = createConsumer();

        standaloneKafkaConnect.start();

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-1.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-2.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-3.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-4.json"))
        );
    }

    @Test
    void pollsMultipleUrlsFromASingleTask() throws IOException {
        Consumer<String, String> consumer = createConsumer();

        String offsetsFilename = "target/" + UUID.randomUUID();
        standaloneKafkaConnect = new StandaloneKafkaConnect(offsetsFilename, 1, URLS, kafkaContainer.getBootstrapServers());
        standaloneKafkaConnect.start();

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-1.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-2.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-3.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-1-4.json"))
        );
    }

    @Test
    void doesNotSendSameMessagesAfterMultiplePolling() throws Exception {
        Consumer<String, String> consumer = createConsumer();

        standaloneKafkaConnect.start();

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });

        IntStream.rangeClosed(0, 5).forEach((i) -> assertThat(consumer.poll(Duration.ofSeconds(1)).iterator()).isExhausted());

        standaloneKafkaConnect.stop();

        IntStream.rangeClosed(0, 5).forEach((i) -> assertThat(consumer.poll(Duration.ofSeconds(1)).iterator()).isExhausted());
    }

    @Test
    void sendsOnlyNewMessagesOnNewPolls() throws Exception {
        Consumer<String, String> consumer = createConsumer();

        standaloneKafkaConnect.start();

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });
        consumerRecords.clear();

        wireMockServer.stubFor(get("/feed.atom").willReturn(aResponse().withBody(read("integration-test/input-2.atom"))));
        wireMockServer.stubFor(get("/feed.rss").willReturn(aResponse().withBody(read("integration-test/input-2.rss"))));

        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 2;
                });
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-2-1.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-2-2.json"))
        );
    }

    @Test
    void storesOffsetsAndUsesThemToGetInformationAboutPreviouslyPolledMessages() throws Exception {
        Consumer<String, String> consumer = createConsumer();

        String offsetsFilename = "target/" + UUID.randomUUID();
        standaloneKafkaConnect = new StandaloneKafkaConnect(offsetsFilename, 2, URLS, kafkaContainer.getBootstrapServers());
        standaloneKafkaConnect.start();

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 4;
                });
        consumerRecords.clear();
        standaloneKafkaConnect.stop();
        assertThat(new File(offsetsFilename)).exists();

        wireMockServer.stubFor(get("/feed.atom").willReturn(aResponse().withBody(read("integration-test/input-2.atom"))));
        wireMockServer.stubFor(get("/feed.rss").willReturn(aResponse().withBody(read("integration-test/input-2.rss"))));

        standaloneKafkaConnect.start();

        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 2;
                });
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-2-1.json"))
        );
        assertThat(consumerRecords).anySatisfy(record ->
                assertThatJson(record.value()).isEqualTo(read("integration-test/output-2-2.json"))
        );
    }

    @Test
    void pollsMultipleTimesToGetNewMessages() {
        wireMockServer.resetAll();

        wireMockServer.stubFor(get("/feed.atom")
                .inScenario("Multiple polls for atom")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withBody(read("integration-test/input-1.atom")))
                .willSetStateTo("First poll succeeded"));
        wireMockServer.stubFor(get("/feed.atom")
                .inScenario("Multiple polls for atom")
                .whenScenarioStateIs("First poll succeeded")
                .willReturn(aResponse().withBody(read("integration-test/input-2.atom")))
                .willSetStateTo("First poll succeeded"));
        wireMockServer.stubFor(get("/feed.rss")
                .inScenario("Multiple polls for rss")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withBody(read("integration-test/input-1.rss")))
                .willSetStateTo("First poll succeeded"));
        wireMockServer.stubFor(get("/feed.rss")
                .inScenario("Multiple polls for rss")
                .whenScenarioStateIs("First poll succeeded")
                .willReturn(aResponse().withBody(read("integration-test/input-2.rss")))
                .willSetStateTo("First poll succeeded"));

        standaloneKafkaConnect.start();

        Consumer<String, String> consumer = createConsumer();
        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(1)).iterator().forEachRemaining(consumerRecords::add);
                    return consumerRecords.size() == 6;
                });
    }

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test_topic"));
        await().until(() -> consumer.poll(Duration.ofSeconds(1)).isEmpty());
        return consumer;
    }

    private static String read(String file) {
        try {
            Path path = Paths.get(RssSourceConnectorIntegrationTest.class.getClassLoader().getResource(file).toURI());
            return new String(Files.readAllBytes(path));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
