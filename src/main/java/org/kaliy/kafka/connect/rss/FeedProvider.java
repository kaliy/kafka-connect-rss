package org.kaliy.kafka.connect.rss;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndPerson;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import com.rometools.utils.Strings;

import org.kaliy.kafka.connect.rss.model.Feed;
import org.kaliy.kafka.connect.rss.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.OFFSET_DELIMITER;

public class FeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(FeedProvider.class);

    private final String url;
    private final Function<String, Optional<SyndFeed>> feedFetcher;
    private final Supplier<Feed.Builder> feedBuilderFactory;
    private final Supplier<Item.Builder> itemBuilderFactory;

    public FeedProvider(String url) {
        this(url, feedFetcher(url), Feed.Builder::aFeed, Item.Builder::anItem);
    }

    public FeedProvider(String url,
                        Function<String, Optional<SyndFeed>> feedFetcher,
                        Supplier<Feed.Builder> feedBuilderFactory,
                        Supplier<Item.Builder> itemBuilderFactory) {
        this.url = url;
        this.feedFetcher = feedFetcher;
        this.feedBuilderFactory = feedBuilderFactory;
        this.itemBuilderFactory = itemBuilderFactory;
    }

    public List<Item> getNewEvents(Collection<String> sentItems) {
        Optional<SyndFeed> maybeFeed = feedFetcher.apply(url);
        if (!maybeFeed.isPresent()) {
            return Collections.emptyList();
        }
        SyndFeed syndFeed = maybeFeed.get();

        String feedTitle = trim(syndFeed.getTitle());
        String feedUrl = url;

        Feed feed = feedBuilderFactory.get().withUrl(feedUrl).withTitle(feedTitle).build();

        List<Item> allItems = syndFeed.getEntries().stream().map(entry ->
                itemBuilderFactory.get()
                        .withTitle(trim(entry.getTitle()))
                        .withLink(link(entry))
                        .withId(trim(entry.getUri()))
                        .withContent(null != entry.getDescription() ? trim(entry.getDescription().getValue()) : null)
                        .withAuthor(author(entry, syndFeed))
                        .withDate(date(entry))
                        .withFeed(feed)
                        .build()
        ).collect(Collectors.toList());

        Set<String> stillLeftItems = allItems.stream().map(Item::toBase64)
                .filter(sentItems::contains).collect(Collectors.toSet());
        StringJoiner joiner = new StringJoiner(OFFSET_DELIMITER);
        stillLeftItems.forEach(joiner::add);

        return allItems.stream()
                .filter(item -> !sentItems.contains(item.toBase64()))
                .map(item -> itemBuilderFactory.get()
                                .withItem(item)
                                .withOffset(joiner.add(item.toBase64()).toString())
                                .build()
                ).collect(Collectors.toList());
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
        return authors.stream().map(SyndPerson::getName).map(String::trim).collect(Collectors.joining(", "));
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

    private static Function<String, Optional<SyndFeed>> feedFetcher(String url) {
        return (u) -> {
            try {
                SyndFeedInput input = new SyndFeedInput();
                return Optional.of(input.build(new XmlReader(new URL(url))));
            } catch (Exception e) {
                logger.warn("Unable to process feed from {}", url, e);
                return Optional.empty();
            }
        };
    }
}
