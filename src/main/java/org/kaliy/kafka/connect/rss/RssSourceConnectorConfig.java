package org.kaliy.kafka.connect.rss;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RssSourceConnectorConfig extends AbstractConfig {

    public static final String RSS_URL_CONFIG = "rss.url";
    private static final String RSS_URL_DOC = "RSS or Atom feed URL";

    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Topic to write to";

    private final String url;
    private final String topic;

    public RssSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.url = this.getString(RSS_URL_CONFIG);
        this.topic = getString(TOPIC_CONFIG);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(RSS_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, RSS_URL_DOC)
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC);
    }

    public String getUrl() {
        return url;
    }

    public String getTopic() {
        return topic;
    }
}
