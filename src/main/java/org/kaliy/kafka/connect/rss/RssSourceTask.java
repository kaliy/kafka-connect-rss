package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig;
import org.kaliy.kafka.connect.rss.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.kaliy.kafka.connect.rss.RssSchemas.VALUE_SCHEMA;
import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.OFFSET_DELIMITER_REGEX;
import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.OFFSET_KEY;

public class RssSourceTask extends SourceTask {
    private static Logger logger = LoggerFactory.getLogger(RssSourceTask.class);

    private RssSourceConnectorConfig config;
    private Map<String, FeedProvider> feedProviders;
    // Task is executed in a single thread so no synchronization is needed
    private boolean shouldWait = false;
    private Function<String, FeedProvider> feedProviderFactory = FeedProvider::new;
    private Function<Map<String, String>, RssSourceConnectorConfig> configFactory = RssSourceConnectorConfig::new;
    private CountDownLatch stopLatch = new CountDownLatch(1);
    private Map<String, Set<String>> previouslySentItems = new HashMap<>();

    @Override
    public String version() {
        return RssSourceConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = configFactory.apply(props);
        logger.info("Starting task with properties {}", props);
        config.getUrls().forEach(url -> {
            Map<String, ?> offsets = context.offsetStorageReader().offset(sourcePartition(url));
            String maybeOffsets = null == offsets ? null : (String) offsets.get(OFFSET_KEY);
            Set<String> sentItems = null == maybeOffsets
                    ? Collections.emptySet()
                    : new HashSet<>(Arrays.asList(maybeOffsets.split(OFFSET_DELIMITER_REGEX)));
            previouslySentItems.put(url, sentItems);
        });
        feedProviders = config.getUrls().stream()
                .collect(Collectors.toMap(Function.identity(), feedProviderFactory));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        boolean shouldStop = false;
        if (shouldWait) {
            logger.debug("Waiting for {} seconds for the next poll", config.getSleepInSeconds());
            shouldStop = stopLatch.await(config.getSleepInSeconds(), TimeUnit.SECONDS);
        }
        if (!shouldStop) {
            logger.debug("Started new polling");
            List<SourceRecord> records = feedProviders.entrySet().stream()
                    .flatMap(entry -> poll(entry.getKey(), entry.getValue()).stream())
                    .collect(Collectors.toList());
            shouldWait = true;
            return records.isEmpty() ? null : records;
        } else {
            logger.debug("Received signal to stop, didn't poll anything");
            return null;
        }
    }

    private List<SourceRecord> poll(String url, FeedProvider feedProvider) {
        logger.info("Polling for new messages from {}", url);
        List<Item> newItems = feedProvider.getNewEvents(previouslySentItems.get(url));
        logger.info("Got {} new items from {}", newItems.size(), url);
        if (!newItems.isEmpty()) {
            previouslySentItems.put(url, new HashSet<>(Arrays.asList(
                    newItems.get(newItems.size() - 1).getOffset().split(OFFSET_DELIMITER_REGEX)))
            );
        }
        return newItems.stream()
                .map(item -> item.toStruct().map(struct -> new Entry(struct, item.getOffset())))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .map(entry ->
                        new SourceRecord(
                                sourcePartition(url),
                                entry.offset,
                                config.getTopic(),
                                null, // partition will be inferred by the framework
                                null,
                                null,
                                VALUE_SCHEMA,
                                entry.struct)
                ).collect(Collectors.toList());
    }

    private Map<String, String> sourcePartition(String url) {
        return Collections.singletonMap(RssSchemas.FEED_URL_FIELD, url);
    }

    @Override
    public void stop() {
        stopLatch.countDown();
    }

    public void setFeedProviderFactory(Function<String, FeedProvider> feedProviderFactory) {
        this.feedProviderFactory = feedProviderFactory;
    }

    public void setConfigFactory(Function<Map<String, String>, RssSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    private static class Entry {
        private final Struct struct;
        private final Map<String, String> offset;

        public Entry(Struct struct, String offset) {
            this.struct = struct;
            this.offset = Collections.singletonMap(OFFSET_KEY, offset);
        }
    }
}
