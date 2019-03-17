package org.kaliy.kafka.connect.rss;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RssSourceConnector extends SourceConnector {
    public static final String VERSION = "0.1.0";
    private RssSourceConnectorConfig rssSourceConnectorConfig;

    public void start(Map<String, String> props) {
        rssSourceConnectorConfig = new RssSourceConnectorConfig(props);
    }

    public Class<? extends Task> taskClass() {
        return RssSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return Collections.singletonList(rssSourceConnectorConfig.originalsStrings());
    }

    public void stop() {
    }

    public ConfigDef config() {
        return RssSourceConnectorConfig.config();
    }

    public String version() {
        return VERSION;
    }
}
