package org.kaliy.kafka.connect.rss.model;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final class Builder {
        private String url;
        private String title;
        private List<Item> items;

        private Builder() {
        }

        public static Builder aFeed() {
            return new Builder();
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withItems(List<Item> items) {
            this.items = items;
            return this;
        }

        public Feed build() {
            return new Feed(url, title, items);
        }
    }
}
