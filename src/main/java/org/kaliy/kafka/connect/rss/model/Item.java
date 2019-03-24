package org.kaliy.kafka.connect.rss.model;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.StringJoiner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_SCHEMA;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_URL_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_AUTHOR_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_CONTENT_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_DATE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_FEED_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_ID_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_LINK_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.VALUE_SCHEMA;

public class Item {

    private final static Logger logger = LoggerFactory.getLogger(Item.class);
    private static final String BASE64_FIELD_DELIMITER = "|";

    private final String title;
    private final String link;
    private final String id;
    private final String content;
    private final String author;
    private final Instant date;
    private final String offset;
    private final Feed feed;

    public Item(String title, String link, String id, String content, String author, Instant date, String offset, Feed feed) {
        this.title = title;
        this.link = link;
        this.id = id;
        this.content = content;
        this.author = author;
        this.date = date;
        this.offset = offset;
        this.feed = feed;
    }

    public Optional<Struct> toStruct() {
        try {
            Struct feedStruct = new Struct(FEED_SCHEMA)
                    .put(FEED_URL_FIELD, feed.getUrl());
            feed.getTitle().ifPresent(title -> feedStruct.put(FEED_TITLE_FIELD, title));

            Struct struct = new Struct(VALUE_SCHEMA)
                    .put(ITEM_FEED_FIELD, feedStruct)
                    .put(ITEM_LINK_FIELD, link)
                    .put(ITEM_TITLE_FIELD, title)
                    .put(ITEM_ID_FIELD, id);
            getContent().ifPresent(content -> struct.put(ITEM_CONTENT_FIELD, content));
            getAuthor().ifPresent(author -> struct.put(ITEM_AUTHOR_FIELD, author));
            getDate().ifPresent(instant -> struct.put(ITEM_DATE_FIELD, instant.toString()));
            return Optional.of(struct);
        } catch (Exception e) {
            logger.info("Unable to create struct for a feed", e);
            return Optional.empty();
        }
    }


    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public String getId() {
        return id;
    }

    public Optional<String> getContent() {
        return Optional.ofNullable(content);
    }

    public Optional<String> getAuthor() {
        return Optional.ofNullable(author);
    }

    public Optional<Instant> getDate() {
        return Optional.ofNullable(date);
    }

    public Feed getFeed() {
        return feed;
    }

    public String getOffset() {
        return offset;
    }

    public String toBase64() {
        return Base64.getEncoder().encodeToString(
                new StringJoiner(BASE64_FIELD_DELIMITER)
                        .add(title).add(link).add(id).add(content).add(author).toString().getBytes(UTF_8)
        );
    }

    public static class Builder {
        private String title;
        private String link;
        private String id;
        private String content;
        private String author;
        private Instant date;
        private String offset;
        private Feed feed;

        public static Builder anItem() {
            return new Builder();
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withLink(String link) {
            this.link = link;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withContent(String content) {
            this.content = content;
            return this;
        }

        public Builder withAuthor(String author) {
            this.author = author;
            return this;
        }

        public Builder withDate(Instant date) {
            this.date = date;
            return this;
        }

        public Builder withOffset(String offset) {
            this.offset = offset;
            return this;
        }

        public Builder withFeed(Feed feed) {
            this.feed = feed;
            return this;
        }

        public Builder withItem(Item item) {
            title = item.title;
            link = item.link;
            id = item.id;
            content = item.content;
            author = item.author;
            date = item.date;
            offset = item.offset;
            feed = item.feed;
            return this;
        }

        public Item build() {
            return new Item(title, link, id, content, author, date, offset, feed);
        }
    }
}
