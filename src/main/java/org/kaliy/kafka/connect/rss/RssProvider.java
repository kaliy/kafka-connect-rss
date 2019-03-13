package org.kaliy.kafka.connect.rss;

import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import com.rometools.utils.Strings;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_SCHEMA;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.FEED_URL_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_CONTENT_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_FEED_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_ID_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.ITEM_TITLE_FIELD;
import static org.kaliy.kafka.connect.rss.RssSchemas.VALUE_SCHEMA;

public class RssProvider {

    private static final Logger logger = LoggerFactory.getLogger(RssProvider.class);

    private final URL url;

    public RssProvider(URL url) {
        this.url = url;
    }

    public List<RssData> getNewEvents(Collection<String> sentItems) {
        SyndFeed feed;
        try {
            SyndFeedInput input = new SyndFeedInput();
            feed = input.build(new XmlReader(url));
        } catch (Exception e) {
            logger.warn("Unable to process feed from {}", url, e);
            return Collections.emptyList();
        }

        Struct feedStruct = new Struct(FEED_SCHEMA)
                .put(FEED_URL_FIELD, url.toString());
        enrichWithTitle(feedStruct, feed);

        return feed.getEntries().stream().map(entry -> {
            Struct struct = new Struct(VALUE_SCHEMA)
                    .put(ITEM_FEED_FIELD, feedStruct)
                    .put(ITEM_TITLE_FIELD, trim(entry.getTitle()))
                    .put(ITEM_ID_FIELD, trim(entry.getUri()));
            return enrichWithContent(struct, entry);
        }).map(struct -> new RssData(struct, offset()))
                .collect(Collectors.toList());
    }

    private Struct enrichWithContent(Struct struct, SyndEntry entry) {
        SyndContent content = entry.getDescription();
        if (null != content) {
            return struct.put(ITEM_CONTENT_FIELD, trim(content.getValue()));
        }
        return struct;
    }

    private Struct enrichWithTitle(Struct feedStruct, SyndFeed feed) {
        String title = feed.getTitle();
        if (!Strings.isBlank(title)) {
            feedStruct.put(FEED_TITLE_FIELD, trim(title));
        }
        return feedStruct;
    }

    private String trim(String string) {
        return null == string ? null : string.trim();
    }

    private String offset() {
        return "offset";
    }
}
