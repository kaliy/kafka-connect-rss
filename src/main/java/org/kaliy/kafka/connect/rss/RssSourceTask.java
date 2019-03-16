package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.kaliy.kafka.connect.rss.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.kaliy.kafka.connect.rss.RssSchemas.VALUE_SCHEMA;

public class RssSourceTask extends SourceTask {
    private static Logger logger = LoggerFactory.getLogger(RssSourceTask.class);

    private RssSourceConnectorConfig config;
    private FeedProvider feedProvider;

    @Override
    public String version() {
        return RssSourceConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new RssSourceConnectorConfig(props);
        try {
            feedProvider = new FeedProvider(new URL(config.getUrl()));
        } catch (MalformedURLException e) {
            System.exit(1);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(10000);
        Map<String, ?> offsets = context.offsetStorageReader().offset(sourcePartition());
        Set<String> sentItems = null == offsets
                ? Collections.emptySet()
                : new HashSet<>(Arrays.asList(((String) offsets.get("sent_items")).split("|")));

        List<Item> newItems = feedProvider.getNewEvents(sentItems);
        if (newItems.isEmpty()) {
            return null;
        }

        List<SourceRecord> records = newItems.stream()
                .map(item -> item.toStruct()
                        .map(struct -> new Entry(struct, item.getOffset()))
                ).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .map(entry ->
                        new SourceRecord(
                                sourcePartition(),
                                entry.offset,
                                config.getTopic(),
                                null, // partition will be inferred by the framework
                                null,
                                null,
                                VALUE_SCHEMA,
                                entry.struct)
                ).collect(Collectors.toList());

        return records.isEmpty() ? null : records;
    }

    private Map<String, String> sourcePartition() {
        return Collections.singletonMap(RssSchemas.FEED_URL_FIELD, config.getUrl());
    }

    @Override
    public void stop() {
    }

    private static class Entry {
        private final Struct struct;
        private final Map<String, String> offset;

        public Entry(Struct struct, String offset) {
            this.struct = struct;
            this.offset = Collections.singletonMap("sent_items", offset);
        }
    }
}
