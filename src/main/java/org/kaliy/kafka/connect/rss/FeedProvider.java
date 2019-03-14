package org.kaliy.kafka.connect.rss;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndPerson;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import com.rometools.utils.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(FeedProvider.class);

    private final URL url;

    public FeedProvider(URL url) {
        this.url = url;
    }

    public Optional<Feed> getNewEvents(Collection<String> sentItems) {
        SyndFeed feed;
        try {
            SyndFeedInput input = new SyndFeedInput();
            feed = input.build(new XmlReader(url));
        } catch (Exception e) {
            logger.warn("Unable to process feed from {}", url, e);
            return Optional.empty();
        }

        String feedTitle = trim(feed.getTitle());
        String feedUrl = url.toString();

        List<Feed.Item> items = feed.getEntries().stream().map(entry -> new Feed.Item(
                trim(entry.getTitle()),
                link(entry),
                trim(entry.getUri()),
                null != entry.getDescription() ? trim(entry.getDescription().getValue()) : null,
                author(entry, feed),
                date(entry)
        )).collect(Collectors.toList());

        return Optional.of(new Feed(feedUrl, feedTitle, items));
    }

    private String link(SyndEntry entry) {
        String entryLink = entry.getLink();
        if (!Strings.isBlank(entryLink)) {
            return entryLink.trim();
        }
        return entry.getLinks().stream() // although documentation says this method returns null, it actually returns empty list
                .filter(link -> !Strings.isBlank(link.getHref()))
                .findFirst()
                .map(link -> link.getHref().trim())
                .orElse(null);
    }

    private String author(SyndEntry entry, SyndFeed feed) {
        if (!entry.getAuthors().isEmpty()) {
            return authors(entry.getAuthors());
        } else if (!Strings.isBlank(entry.getAuthor())) {
            return entry.getAuthor().trim();
        } else if (!feed.getAuthors().isEmpty()) {
            return authors(feed.getAuthors());
        } else if (!Strings.isBlank(feed.getAuthor())) {
            return feed.getAuthor().trim();
        }
        return null;
    }

    private String authors(List<SyndPerson> authors) {
        return authors.stream().map(SyndPerson::getName).collect(Collectors.joining(", "));
    }

    private Instant date(SyndEntry entry) {
        if (null != entry.getUpdatedDate()) {
            return entry.getUpdatedDate().toInstant();
        } else if (null != entry.getPublishedDate()) {
            return entry.getPublishedDate().toInstant();
        }
        return null;
    }

    private String trim(String string) {
        return null == string ? null : string.trim();
    }
}
