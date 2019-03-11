package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_SCHEMA;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_URL_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_FEED_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.VALUE_SCHEMA;

public class RssSourceTask extends SourceTask {
    private RssSourceConnectorConfig config;

    public String version() {
        return RssSourceConnector.VERSION;
    }

    public void start(Map<String, String> props) {
        config = new RssSourceConnectorConfig(props);
    }

    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000);
        return Collections.singletonList(
                new SourceRecord(
                        sourcePartition(),
                        sourceOffset(),
                        config.getTopic(),
                        null, // partition will be inferred by the framework
                        null,
                        null,
                        VALUE_SCHEMA,
                        buildRecordValue())
        );
    }

    private Struct buildRecordValue() {
        Struct feedStruct = new Struct(FEED_SCHEMA)
                .put(FEED_URL_FIELD, config.getUrl())
                .put(FEED_TITLE_FIELD, "title field");
        return new Struct(VALUE_SCHEMA)
                .put(ITEM_FEED_FIELD, feedStruct)
                .put(ITEM_TITLE_FIELD, "item_title");
    }

    private Map<String, String> sourcePartition() {
        return Collections.singletonMap(RssSchemas.FEED_URL_FIELD, config.getUrl());
    }

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();
        //TODO
        return map;
    }

    public void stop() {

    }
}
