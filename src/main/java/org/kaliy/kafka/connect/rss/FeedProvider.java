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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class FeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(FeedProvider.class);

    private final URL url;
    private final Function<URL, Optional<SyndFeed>> feedFetcher;
    private final Supplier<Feed.Builder> feedBuilderFactory;
    private final Supplier<Item.Builder> itemBuilderFactory;

    public FeedProvider(URL url) {
        this(url, feedFetcher(url), Feed.Builder::aFeed, Item.Builder::anItem);
    }

    public FeedProvider(URL url,
                        Function<URL, Optional<SyndFeed>> feedFetcher,
                        Supplier<Feed.Builder> feedBuilderFactory,
                        Supplier<Item.Builder> itemBuilderFactory) {
        this.url = url;
        this.feedFetcher = feedFetcher;
        this.feedBuilderFactory = feedBuilderFactory;
        this.itemBuilderFactory = itemBuilderFactory;
    }

    public Optional<Feed> getNewEvents(Collection<String> sentItems) {
        Optional<SyndFeed> maybeFeed = feedFetcher.apply(url);
        if (!maybeFeed.isPresent()) {
            return Optional.empty();
        }
        SyndFeed feed = maybeFeed.get();

        String feedTitle = trim(feed.getTitle());
        String feedUrl = url.toString();

        List<Item> allItems = feed.getEntries().stream().map(entry ->
                itemBuilderFactory.get()
                        .withTitle(trim(entry.getTitle()))
                        .withLink(link(entry))
                        .withId(trim(entry.getUri()))
                        .withContent(null != entry.getDescription() ? trim(entry.getDescription().getValue()) : null)
                        .withAuthor(author(entry, feed))
                        .withDate(date(entry))
                        .build()
        ).collect(Collectors.toList());

        Set<String> stillLeftItems = allItems.stream().map(Item::toBase64)
                .filter(sentItems::contains).collect(Collectors.toSet());
        StringJoiner joiner = new StringJoiner("|");
        stillLeftItems.forEach(joiner::add);

        List<Item> nonSentItems = allItems.stream()
                .filter(item -> !sentItems.contains(item.toBase64()))
                .map(item -> itemBuilderFactory.get()
                                .withItem(item)
                                .withOffset(joiner.add(item.toBase64()).toString())
                                .build()
                ).collect(Collectors.toList());

        return Optional.of(feedBuilderFactory.get().withUrl(feedUrl).withTitle(feedTitle).withItems(nonSentItems).build());
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

    private static Function<URL, Optional<SyndFeed>> feedFetcher(URL url) {
        return (u) -> {
            try {
                SyndFeedInput input = new SyndFeedInput();
                return Optional.of(input.build(new XmlReader(url)));
            } catch (Exception e) {
                logger.warn("Unable to process feed from {}", url, e);
                return Optional.empty();
            }
        };
    }
}
