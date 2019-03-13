package org.kaliy.kafka.connect.rss;

import org.assertj.core.api.JUnitJupiterBDDSoftAssertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.kaliy.kafka.connect.rss.RssSchemas.*;

class RssProviderTest {

    @Test
    void parsesAtom_0_3() {
        RssProvider provider = new RssProvider(url("atom/0.3.atom"));
        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(1);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("atom/0.3.atom");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("The name of your data feed");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("Red wool sweater");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("Comfortable and soft, this sweater will keep you warm on those cold winter nights.");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("tag:google.com,2005-10-15:/support/products");
        });
    }

    @Test
    void parsesAtom_1_0() {
        RssProvider provider = new RssProvider(url("atom/1.0.atom"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(2);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("atom/1.0.atom");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("Общая по всем разделам");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("Истории академии Ядзикита / Yajikita Gakuen Douchuuki / Tales of Yajikita College [OVA] [2 из 2] [Без хардсаба] [JAP+SUB] [1989, приключения, боевые искусства, DVDRip] [924 MB]");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("Some sample content which is not present in a real feed but added here for testing purposes");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("tag:rto.feed,2019-03-09:/t/5701301");
        });
    }

    @Test
    void parsesRss_0_91() {
        RssProvider provider = new RssProvider(url("rss/0.91.rss"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(3);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("rss/0.91.rss");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("RSS0.91 Example");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("The First Item");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("This is the first item.");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("http://www.oreilly.com/example/001.html");
        });
    }

    @Test
    void parsesRss_0_92() {
        RssProvider provider = new RssProvider(url("rss/0.92.rss"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(2);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("rss/0.92.rss");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("RSS0.92 Example");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("The First Item");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("This is the first item.");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("http://www.oreilly.com/example/001.html");
        });
    }

    @Test
    void parsesRss_1_0() {
        RssProvider provider = new RssProvider(url("rss/1.0.rss"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(1);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("rss/1.0.rss");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("Meerkat");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("XML: A Disruptive Technology");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("Some sample description");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("http://c.moreover.com/click/here.pl?r123");
        });
    }

    @Test
    void parsesRss_2_0() {
        RssProvider provider = new RssProvider(url("rss/2.0.rss"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).hasSize(2);
        RssData rss = data.get(0);
        assertThat(rss.getData()).isNotNull();
        assertSoftly(softly -> {
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_URL_FIELD)).endsWith("rss/2.0.rss");
            softly.assertThat(rss.getData().getStruct(ITEM_FEED_FIELD).getString(FEED_TITLE_FIELD)).isEqualTo("All Torrents :: morethan.tv");
            softly.assertThat(rss.getData().getString(ITEM_TITLE_FIELD)).isEqualTo("Best.House.on.the.Block.S01E06.From.Windows.to.Wallpaper.1080p.WEB.x264-CAFFEiNE  - x264 / 1080p / Web-DL");
            softly.assertThat(rss.getData().getString(ITEM_CONTENT_FIELD)).isEqualTo("This tag was empty in the original feed, it was added only for unit testing purposes");
            softly.assertThat(rss.getData().getString(ITEM_ID_FIELD)).isEqualTo("https://www.morethan.tv/torrents.php?action=download&id=437840");
        });
    }

    @Test
    void returnsEmptyCollectionIfXmlIsInvalid() {
        RssProvider provider = new RssProvider(url("special_cases/invalid.xml"));

        List<RssData> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isEmpty();
    }

    private URL url(String file) {
        return RssProviderTest.class.getClassLoader().getResource(file);
    }
}
