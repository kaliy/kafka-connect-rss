package org.kaliy.kafka.connect.rss.model;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;

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
import static org.kaliy.kafka.connect.rss.model.Feed.Builder.aFeed;

class ItemTest {

    private static final String TITLE = "title";
    private static final String LINK = "link";
    private static final String ID = "id";
    private static final String CONTENT = "content";
    private static final String AUTHOR = "author";
    private static final String OFFSET = "offset";
    private static final Instant DATE = Instant.ofEpochMilli(11000);
    private static final String FEED_TITLE = "feed_title";
    private static final String FEED_URL = "feed_url";
    private static final Feed FEED = aFeed().withTitle(FEED_TITLE).withUrl(FEED_URL).build();

    @Test
    void convertsItemToBase64StringWithAllRequiredFields() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, Instant.EPOCH, OFFSET, FEED);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64));
        assertThat(decoded).isEqualTo("%s|%s|%s|%s|%s", TITLE, LINK, ID, CONTENT, AUTHOR);
    }

    @Test
    void addsNullFieldsOnConvertingToBase64() {
        Item item = new Item(null, null, null, null, null, null, null, null);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64));
        assertThat(decoded).isEqualTo("null|null|null|null|null");
    }

    @Test
    void supportsUnicodeOnConvertingItemToBase64String() {
        Item item = new Item("название", "ウェブリンク", "መለያ", "يحتوى", "ผู้เขียน", Instant.EPOCH, OFFSET, FEED);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64.getBytes(StandardCharsets.UTF_8)));
        assertThat(decoded).isEqualTo("название|ウェブリンク|መለያ|يحتوى|ผู้เขียน");
    }

    @Test
    void serializesFeedToStructWithAllFields() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET, FEED);

        Optional<Struct> maybeStruct = item.toStruct();

        assertThat(maybeStruct).isNotEmpty();
        Struct struct = maybeStruct.get();

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
    void toStructDoesNotIncludeFeedWithoutUrl() {
        Feed feed = new Feed(null, FEED_TITLE);
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET, feed);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isEmpty();
    }

    @Test
    void toStructIncludesFeedWithoutTitle() {
        Feed feed = new Feed(FEED_URL, null);
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET, feed);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isNotEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemsWithoutTitle() {
        Item item = new Item(null, LINK, ID, CONTENT, AUTHOR, DATE, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemWithoutLink() {
        Item item = new Item(TITLE, null, ID, CONTENT, AUTHOR, DATE, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isEmpty();
    }

    @Test
    void toStructDoesNotIncludeItemsWithoutId() {
        Item item = new Item(TITLE, LINK, null, CONTENT, AUTHOR, DATE, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutContent() {
        Item item = new Item(TITLE, LINK, ID, null, AUTHOR, DATE, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isNotEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutAuthor() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, null, DATE, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isNotEmpty();
    }

    @Test
    void toStructIncludesItemsWithoutDate() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, null, OFFSET, FEED);

        Optional<Struct> struct = item.toStruct();

        assertThat(struct).isNotEmpty();
    }


}
