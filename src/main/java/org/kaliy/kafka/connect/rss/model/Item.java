package org.kaliy.kafka.connect.rss.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.StringJoiner;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Item {

    public static final String OFFSET_SEPARATOR = "|";

    private final String title;
    private final String link;
    private final String id;
    private final String content;
    private final String author;
    private final Instant date;
    private final String offset;

    public Item(String title, String link, String id, String content, String author, Instant date, String offset) {
        this.title = title;
        this.link = link;
        this.id = id;
        this.content = content;
        this.author = author;
        this.date = date;
        this.offset = offset;
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

    public String getOffset() {
        return offset;
    }

    public String toBase64() {
        return Base64.getEncoder().encodeToString(
                new StringJoiner("|")
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

        public Builder() {
        }

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

        public Builder withItem(Item item) {
            title = item.title;
            link = item.link;
            id = item.id;
            content = item.content;
            author = item.author;
            date = item.date;
            offset = item.offset;
            return this;
        }

        public Item build() {
            return new Item(title, link, id, content, author, date, offset);
        }
    }
}
