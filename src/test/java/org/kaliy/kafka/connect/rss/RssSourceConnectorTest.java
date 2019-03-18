package org.kaliy.kafka.connect.rss;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class RssSourceConnectorTest {

    private RssSourceConnector rssSourceConnector = new RssSourceConnector();
    private Map<String, String> properties = new HashMap<>();

    @BeforeEach
    void setUp() {
        properties.put("name", "RssSourceConnectorDemo");
        properties.put("tasks.max", "1");
        properties.put("connector.class", "org.kaliy.kafka.connect.rss.RssSourceConnector");
        properties.put("rss.urls", "http://url.one");
        properties.put("topic", "test_topic");
        rssSourceConnector.start(properties);
    }

    @Test
    void hasRssSourceTaskClass() {
        assertThat(rssSourceConnector.taskClass()).isEqualTo(RssSourceTask.class);
    }

    @Test
    void hasVersion() {
        assertThat(rssSourceConnector.version()).isEqualTo("0.1.0");
    }

    @Test
    void createsConfigurationWithAllUrlsForASingleTask() {
        properties.put("rss.urls", "http://one.com http://two.com http://three.com");
        rssSourceConnector = new RssSourceConnector();
        rssSourceConnector.start(properties);

        List<Map<String, String>> taskConfigs = rssSourceConnector.taskConfigs(1);

        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("rss.urls", "http://one.com http://two.com http://three.com");
    }

    @Test
    void splitsUrlsAcrossMultipleTasks() {
        properties.put("rss.urls", "http://one.com http://two.com http://three.com http://four.com http://five.com http://six.com http://seven.com");
        rssSourceConnector = new RssSourceConnector();
        rssSourceConnector.start(properties);

        List<Map<String, String>> taskConfigs = rssSourceConnector.taskConfigs(3);

        assertThat(taskConfigs).hasSize(3);
        assertSoftly(softly -> {
            softly.assertThat(taskConfigs.get(0)).containsEntry("rss.urls", "http://one.com http://four.com http://seven.com");
            softly.assertThat(taskConfigs.get(1)).containsEntry("rss.urls", "http://two.com http://five.com");
            softly.assertThat(taskConfigs.get(2)).containsEntry("rss.urls", "http://three.com http://six.com");
        });
    }

    @Test
    void createsOneTaskPerOneUrlIfMaxTasksIsHigherThanUrlsNumber() {
        properties.put("rss.urls", "http://one.com http://two.com http://three.com");
        rssSourceConnector = new RssSourceConnector();
        rssSourceConnector.start(properties);

        List<Map<String, String>> taskConfigs = rssSourceConnector.taskConfigs(666);

        assertThat(taskConfigs).hasSize(3);
        assertSoftly(softly -> {
            softly.assertThat(taskConfigs.get(0)).containsEntry("rss.urls", "http://one.com");
            softly.assertThat(taskConfigs.get(1)).containsEntry("rss.urls", "http://two.com");
            softly.assertThat(taskConfigs.get(2)).containsEntry("rss.urls", "http://three.com");
        });
    }

    @Test
    void setsValuesFromConfigurationToEachTaskConfiguration() {
        List<Map<String, String>> config = rssSourceConnector.taskConfigs(1);

        assertThat(config).hasSize(1);
        assertSoftly(softly -> {
            softly.assertThat(config.get(0)).containsEntry("name", "RssSourceConnectorDemo");
            softly.assertThat(config.get(0)).containsEntry("tasks.max", "1");
            softly.assertThat(config.get(0)).containsEntry("connector.class", "org.kaliy.kafka.connect.rss.RssSourceConnector");
            softly.assertThat(config.get(0)).containsEntry("rss.urls", "http://url.one");
            softly.assertThat(config.get(0)).containsEntry("topic", "test_topic");
        });
    }
}
