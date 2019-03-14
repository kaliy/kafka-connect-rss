package org.kaliy.kafka.connect.rss;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

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
}
