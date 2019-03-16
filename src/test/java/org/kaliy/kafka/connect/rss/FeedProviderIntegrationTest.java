package org.kaliy.kafka.connect.rss;

import org.junit.jupiter.api.Test;
import org.kaliy.kafka.connect.rss.model.Feed;
import org.kaliy.kafka.connect.rss.model.Item;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class FeedProviderIntegrationTest {
    @Test
    void parsesAtom_0_3() {
        FeedProvider provider = new FeedProvider(url("atom/0.3.atom"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(1);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("atom/0.3.atom");
            softly.assertThat(rss.getTitle()).hasValue("The name of your data feed");
            softly.assertThat(item.getTitle()).isEqualTo("Red wool sweater");
            softly.assertThat(item.getContent()).hasValue("Comfortable and soft, this sweater will keep you warm on those cold winter nights.");
            softly.assertThat(item.getId()).isEqualTo("tag:google.com,2005-10-15:/support/products");
            softly.assertThat(item.getLink()).isEqualTo("http://www.example.com/item1-info-page.html");
            softly.assertThat(item.getAuthor()).hasValue("Google");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2005-10-13T18:30:02Z"));
        });
    }

    @Test
    void parsesAtom_1_0() {
        FeedProvider provider = new FeedProvider(url("atom/1.0.atom"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(2);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("atom/1.0.atom");
            softly.assertThat(rss.getTitle()).hasValue("Общая по всем разделам");
            softly.assertThat(item.getTitle()).isEqualTo("Истории академии Ядзикита / Yajikita Gakuen Douchuuki / Tales of Yajikita College [OVA] [2 из 2] [Без хардсаба] [JAP+SUB] [1989, приключения, боевые искусства, DVDRip] [924 MB]");
            softly.assertThat(item.getContent()).hasValue("Some sample content which is not present in a real feed but added here for testing purposes");
            softly.assertThat(item.getId()).isEqualTo("tag:rto.feed,2019-03-09:/t/5701301");
            softly.assertThat(item.getLink()).isEqualTo("https://rutracker.org/forum/viewtopic.php?t=5701301");
            softly.assertThat(item.getAuthor()).hasValue("sergeandr");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2019-03-09T10:42:36Z"));
        });
    }

    @Test
    void parsesRss_0_91() {
        FeedProvider provider = new FeedProvider(url("rss/0.91.rss"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(3);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("rss/0.91.rss");
            softly.assertThat(rss.getTitle()).hasValue("RSS0.91 Example");
            softly.assertThat(item.getTitle()).isEqualTo("The First Item");
            softly.assertThat(item.getContent()).hasValue("This is the first item.");
            softly.assertThat(item.getId()).isEqualTo("http://www.oreilly.com/example/001.html");
            softly.assertThat(item.getLink()).isEqualTo("http://www.oreilly.com/example/001.html");
            softly.assertThat(item.getAuthor()).hasValue("editor@oreilly.com");
            softly.assertThat(item.getDate()).isEmpty();
        });
    }

    @Test
    void parsesRss_0_92() {
        FeedProvider provider = new FeedProvider(url("rss/0.92.rss"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(2);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("rss/0.92.rss");
            softly.assertThat(rss.getTitle()).hasValue("RSS0.92 Example");
            softly.assertThat(item.getTitle()).isEqualTo("The First Item");
            softly.assertThat(item.getContent()).hasValue("This is the first item.");
            softly.assertThat(item.getId()).isEqualTo("http://www.oreilly.com/example/001.html");
            softly.assertThat(item.getLink()).isEqualTo("http://www.oreilly.com/example/001.html");
            softly.assertThat(item.getAuthor()).hasValue("editor@oreilly.com");
            softly.assertThat(item.getDate()).isEmpty();
        });
    }

    @Test
    void parsesRss_1_0() {
        FeedProvider provider = new FeedProvider(url("rss/1.0.rss"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(1);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("rss/1.0.rss");
            softly.assertThat(rss.getTitle()).hasValue("Meerkat");
            softly.assertThat(item.getTitle()).isEqualTo("XML: A Disruptive Technology");
            softly.assertThat(item.getContent()).hasValue("Some sample description");
            softly.assertThat(item.getId()).isEqualTo("http://c.moreover.com/click/here.pl?r123");
            softly.assertThat(item.getLink()).isEqualTo("http://c.moreover.com/click/here.pl?r123");
            softly.assertThat(item.getAuthor()).isEmpty();
            softly.assertThat(item.getDate()).isEmpty();
        });
    }

    @Test
    void parsesRss_2_0() {
        FeedProvider provider = new FeedProvider(url("rss/2.0.rss"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isPresent();
        Feed rss = data.get();
        assertThat(rss.getItems()).hasSize(2);
        Item item = rss.getItems().get(0);
        assertSoftly(softly -> {
            softly.assertThat(rss.getUrl()).endsWith("rss/2.0.rss");
            softly.assertThat(rss.getTitle()).hasValue("All Torrents :: morethan.tv");
            softly.assertThat(item.getTitle()).isEqualTo("Best.House.on.the.Block.S01E06.From.Windows.to.Wallpaper.1080p.WEB.x264-CAFFEiNE  - x264 / 1080p / Web-DL");
            softly.assertThat(item.getContent()).hasValue("This tag was empty in the original feed, it was added only for unit testing purposes");
            softly.assertThat(item.getId()).isEqualTo("https://www.morethan.tv/torrents.php?action=download&id=437840");
            softly.assertThat(item.getLink()).isEqualTo("https://www.morethan.tv/torrents.php?action=download&id=437840");
            softly.assertThat(item.getAuthor()).hasValue("TheShadow");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2019-03-08T20:01:45Z"));
        });
    }

    @Test
    void returnsEmptyResultIfXmlIsInvalid() {
        FeedProvider provider = new FeedProvider(url("special_cases/invalid.xml"));

        Optional<Feed> data = provider.getNewEvents(Collections.emptyList());

        assertThat(data).isEmpty();
    }

    private URL url(String file) {
        return FeedProviderTest.class.getClassLoader().getResource(file);
    }

}
