package org.kaliy.kafka.connect.rss;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.RSS_URL_CONFIG;
import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.RSS_URL_SEPARATOR;

public class RssSourceConnector extends SourceConnector {
    public static final String VERSION = "0.1.0";
    private RssSourceConnectorConfig rssSourceConnectorConfig;
    private Logger logger = LoggerFactory.getLogger(RssSourceConnector.class);

    @Override
    public void start(Map<String, String> props) {
        rssSourceConnectorConfig = new RssSourceConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RssSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> urls = rssSourceConnectorConfig.getUrls();
        if (maxTasks > urls.size()) {
            logger.info("URLs number defined in the configuration file is lower than maximum task number, only {} tasks will be created", urls.size());
        }
        AtomicInteger i = new AtomicInteger(0);
        return urls.stream()
                .collect(Collectors.groupingBy(it -> i.getAndIncrement() % maxTasks))
                .values()
                .stream()
                .map(partitionedUrls -> {
                    Map<String, String> config = rssSourceConnectorConfig.originalsStrings();
                    config.put(RSS_URL_CONFIG, String.join(RSS_URL_SEPARATOR, partitionedUrls));
                    return config;
                }).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        // No resources to release, nothing to do here
    }

    @Override
    public ConfigDef config() {
        return RssSourceConnectorConfig.config();
    }

    @Override
    public String version() {
        return VERSION;
    }
}
