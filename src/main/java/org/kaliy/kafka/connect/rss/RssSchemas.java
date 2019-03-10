package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class RssSchemas {
    public static final String SCHEMA_VALUE_FEED = "org.kaliy.kafka.rss.Feed";
    public static final String SCHEMA_VALUE_ITEM = "org.kaliy.kafka.rss.Item";

    public static final String FEED_TITLE_FIELD = "title";
    public static final String FEED_URL_FIELD = "url";

    public static final String ITEM_TITLE_FIELD = "title";
    public static final String ITEM_FEED_FIELD = "feed";

    public static final Schema FEED_SCHEMA = SchemaBuilder.struct().name(SCHEMA_VALUE_FEED)
            .version(1)
            .field(FEED_TITLE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FEED_URL_FIELD, Schema.STRING_SCHEMA)
            .build();

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name(SCHEMA_VALUE_ITEM)
            .version(1)
            .field(ITEM_FEED_FIELD, FEED_SCHEMA)
            .field(ITEM_TITLE_FIELD, Schema.STRING_SCHEMA)
            .build();
}
