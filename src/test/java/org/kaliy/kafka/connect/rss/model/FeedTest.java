package org.kaliy.kafka.connect.rss.model;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_URL_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_AUTHOR_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_CONTENT_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_DATE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_FEED_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_ID_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_LINK_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_TITLE_FIELD;

class FeedTest {

    private static final String TITLE = "title";
    private static final String LINK = "link";
    private static final String ID = "id";
    private static final String CONTENT = "content";
    private static final String AUTHOR = "author";
    private static final Instant DATE = Instant.ofEpochMilli(11000);
    private static final String FEED_TITLE = "feed_title";
    private static final String FEED_URL = "feed_url";
    private static final String OFFSET = "offset";

    @Test
    void serializesFeedToStructWithAllFields() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).hasSize(1);
        Struct struct = structs.get(0);

        assertSoftly(softly -> {
            softly.assertThat(struct.getStruct(ITEM_FEED_FIELD)).isNotNull().satisfies(feedStruct -> {
                softly.assertThat(feedStruct.getString(FEED_TITLE_FIELD)).isEqualTo(FEED_TITLE);
                softly.assertThat(feedStruct.getString(FEED_URL_FIELD)).isEqualTo(FEED_URL);
            });
            softly.assertThat(struct.getString(ITEM_TITLE_FIELD)).isEqualTo(TITLE);
            softly.assertThat(struct.getString(ITEM_AUTHOR_FIELD)).isEqualTo(AUTHOR);
            softly.assertThat(struct.getString(ITEM_CONTENT_FIELD)).isEqualTo(CONTENT);
            softly.assertThat(struct.getString(ITEM_DATE_FIELD)).isEqualTo("1970-01-01T00:00:11Z");
            softly.assertThat(struct.getString(ITEM_ID_FIELD)).isEqualTo(ID);
            softly.assertThat(struct.getString(ITEM_LINK_FIELD)).isEqualTo(LINK);
        });
    }

    @Test
    void serializesMultipleItemsIntoMultipleStructs() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Arrays.asList(item, item, item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).hasSize(3);
    }

    @Test
    void skipsInvalidItems() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Item invalidItem = new Item(null, null, null, null, null, null, null);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Arrays.asList(invalidItem, item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).hasSize(1);
    }

    @Test
    void toStructDoesNotIncludeFeedWithoutUrl() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(null, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isEmpty();
    }

    @Test
    void toStructIncludesFeedWithoutTitle() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, null, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isNotEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemsWithoutTitle() {
        Item item = new Item(null, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemWithoutLink() {
        Item item = new Item(TITLE, null, ID, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemsWithoutId() {
        Item item = new Item(TITLE, LINK, null, CONTENT, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutContent() {
        Item item = new Item(TITLE, LINK, ID, null, AUTHOR, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isNotEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutAuthor() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, null, DATE, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isNotEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutDate() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, null, OFFSET);
        Feed feed = new Feed(FEED_URL, FEED_TITLE, Collections.singletonList(item));

        List<Struct> structs = feed.toStruct();

        assertThat(structs).isNotEmpty();
    }
}
