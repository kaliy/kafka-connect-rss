package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

public class Feed {

    private static final Logger logger = LoggerFactory.getLogger(Feed.class);

    private final String url;
    private final String title;
    private final List<Item> items;

    public Feed(String url, String title, List<Item> items) {
        this.url = url;
        this.title = title;
        this.items = items;
    }

    public static class Item {
        private final String title;
        private final String link;
        private final String id;
        private final String content;
        private final String author;
        private final Instant date;

        public Item(String title, String link, String id, String content, String author, Instant date) {
            this.title = title;
            this.link = link;
            this.id = id;
            this.content = content;
            this.author = author;
            this.date = date;
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
    }

    public Optional<String> getTitle() {
        return Optional.ofNullable(title);
    }

    public String getUrl() {
        return url;
    }

    public List<Item> getItems() {
        return items;
    }

    public List<Struct> toStruct() {
        Struct feedStruct;
        try {
            feedStruct = new Struct(FEED_SCHEMA)
                    .put(FEED_URL_FIELD, url);
            getTitle().ifPresent(title -> feedStruct.put(FEED_TITLE_FIELD, title));
        } catch (Exception e) {
            logger.info("Unable to create struct for a feed", e);
            return Collections.emptyList();
        }

        return items.stream().flatMap(item -> {
            try {
                Struct struct = new Struct(VALUE_SCHEMA)
                        .put(ITEM_FEED_FIELD, feedStruct)
                        .put(ITEM_LINK_FIELD, item.getLink())
                        .put(ITEM_TITLE_FIELD, item.getTitle())
                        .put(ITEM_ID_FIELD, item.getId());
                item.getContent().ifPresent(content -> struct.put(ITEM_CONTENT_FIELD, content));
                item.getAuthor().ifPresent(author -> struct.put(ITEM_AUTHOR_FIELD, author));
                item.getDate().ifPresent(instant -> struct.put(ITEM_DATE_FIELD, instant.toString()));
                return Stream.of(struct);
            } catch (Exception e) {
                logger.info("Unable to create struct for an item", e);
                return Stream.empty();
            }
        }).collect(Collectors.toList());
    }
}
