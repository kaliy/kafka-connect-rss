package org.kaliy.kafka.connect.rss.config;

import org.apache.kafka.common.config.ConfigException;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class RssSourceConnectorConfigTest {
    private static final String RSS_URL = "rss.urls";
    private static final String TOPIC = "topic";
    private static final String SLEEP_SECONDS = "sleep.seconds";
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        config = new HashMap<>();
        config.put(RSS_URL, "http://rss.com/feed.atom");
        config.put(TOPIC, "test_topic");
        config.put(SLEEP_SECONDS, "666");
    }

    @Test
    void outputsDocumentation() {
        String doc = RssSourceConnectorConfig.config().toRst();
        assertThat(doc).isNotEmpty();
        System.out.println(doc);
    }

    @Test
    void createsConfigurationWithProperValues() {
        RssSourceConnectorConfig rssConfig = new RssSourceConnectorConfig(config);
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(rssConfig.getUrls()).containsOnly("http://rss.com/feed.atom");
            softly.assertThat(rssConfig.getTopic()).isEqualTo("test_topic");
            softly.assertThat(rssConfig.getSleepInSeconds()).isEqualTo(666);
        });
    }

    @Test
    void setsDefaultSleepConfigurationSetting() {
        config.remove(SLEEP_SECONDS);
        RssSourceConnectorConfig rssConfig = new RssSourceConnectorConfig(config);
        assertThat(rssConfig.getSleepInSeconds()).isEqualTo(60);
    }

    @Test
    void validatesUrls() {
        config.put(RSS_URL, "http:/invalid");
        assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new RssSourceConnectorConfig(config));
        assertThat(RssSourceConnectorConfig.config().configKeys().get(RSS_URL).validator.getClass()).isEqualTo(UrlListValidator.class);
    }

    @Test
    void validatesSleepSeconds() {
        config.put(SLEEP_SECONDS, "-60");
        assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new RssSourceConnectorConfig(config));
        assertThat(RssSourceConnectorConfig.config().configKeys().get(SLEEP_SECONDS).validator.getClass()).isEqualTo(PositiveIntegerValidator.class);
    }

    @Test
    void rssUrlIsMandatory() {
        config.remove(RSS_URL);
        assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new RssSourceConnectorConfig(config));
    }

    @Test
    void topicIsMandatory() {
        config.remove(TOPIC);
        assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new RssSourceConnectorConfig(config));
    }
}
