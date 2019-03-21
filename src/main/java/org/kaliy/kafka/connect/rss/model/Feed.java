package org.kaliy.kafka.connect.rss.model;

import java.util.Optional;

public class Feed {

    private final String url;
    private final String title;

    public Feed(String url, String title) {
        this.url = url;
        this.title = title;
    }

    public Optional<String> getTitle() {
        return Optional.ofNullable(title);
    }

    public String getUrl() {
        return url;
    }

    public static final class Builder {
        private String url;
        private String title;

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

        public Feed build() {
            return new Feed(url, title);
        }
    }
}
