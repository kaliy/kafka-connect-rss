package org.kaliy.kafka.connect.rss;

import org.junit.jupiter.api.Test;
import org.kaliy.kafka.connect.rss.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

class FeedProviderIntegrationTest {

    private final Logger logger = LoggerFactory.getLogger(FeedProviderIntegrationTest.class);

    @Test
    void parsesAtom_0_3() {
        FeedProvider provider = new FeedProvider(url("atom/0.3.atom"));

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(1);
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("atom/0.3.atom");
            softly.assertThat(item.getFeed().getTitle()).hasValue("The name of your data feed");
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

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        items.forEach(item -> logger.info("Item: {}", item));
        assertThat(items).hasSize(2);
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("atom/1.0.atom");
            softly.assertThat(item.getFeed().getTitle()).hasValue("Общая по всем разделам");
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

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(3);
        items.forEach(item -> logger.info("Item: {}", item));
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/0.91.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("RSS0.91 Example");
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

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(2);
        items.forEach(item -> logger.info("Item: {}", item));
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/0.92.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("RSS0.92 Example");
            softly.assertThat(item.getTitle()).isEqualTo("The First Item");
            softly.assertThat(item.getContent()).hasValue("This is the first item.");
            softly.assertThat(item.getId()).isEqualTo("http://www.oreilly.com/example/001.html");
            softly.assertThat(item.getLink()).isEqualTo("http://www.oreilly.com/001.mp3");
            softly.assertThat(item.getAuthor()).hasValue("editor@oreilly.com");
            softly.assertThat(item.getDate()).isEmpty();
        });
    }

    @Test
    void parsesRss_1_0() {
        FeedProvider provider = new FeedProvider(url("rss/1.0.rss"));

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(1);
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/1.0.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("Meerkat");
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

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(2);
        items.forEach(item -> logger.info("Item: {}", item));
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/2.0.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("All Torrents :: morethan.tv");
            softly.assertThat(item.getTitle()).isEqualTo("Best.House.on.the.Block.S01E06.From.Windows.to.Wallpaper.1080p.WEB.x264-CAFFEiNE  - x264 / 1080p / Web-DL");
            softly.assertThat(item.getContent()).hasValue("This tag was empty in the original feed, it was added only for unit testing purposes");
            softly.assertThat(item.getId()).isEqualTo("https://www.morethan.tv/torrents.php?action=download&id=437840");
            softly.assertThat(item.getLink()).isEqualTo("https://www.morethan.tv/torrents.php?action=download&id=437840");
            softly.assertThat(item.getAuthor()).hasValue("TheShadow");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2019-03-08T20:01:45Z"));
        });
    }

    @Test
    void parsesRssWithItunesLinks() {
        FeedProvider provider = new FeedProvider(url("rss/itunes.rss"));

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(3);
        items.forEach(item -> logger.info("Item: {}", item));
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/itunes.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("The Apology Line");
            softly.assertThat(item.getTitle()).isEqualTo("Amazon Music Presents COLD: The Search for Sheree");
            softly.assertThat(item.getContent()).hasValue("<p>Sheree Warren left her job in Salt Lake City on a mild October evening in 1985. She told a coworker she was headed to meet her estranged husband, Charles Warren, at a car dealership. But she never made it, Sheree vanished.</p><p>When a young mother disappears under unexplained circumstances, police always turn suspicious eyes towards the husband. And although there was distrust around Charles Warren, he wasn’t the only suspect when Sheree went missing. She also had a boyfriend, a former cop named Cary Hartmann, who lived a sinister double life.</p><p>Season three follows two suspects– men who both raised suspicion for investigators. But with two strong persons of interest with competing facts and evidence, it muddied the murder investigation. This season, host Dave Cawley, digs into the lives of these two men, the details of the case and examines the intersections between domestic abuse and sexual violence. The COLD team seeks to answer the question: what really happened to Sheree Warren?</p><p>Listen to Cold—exclusively on Amazon Music—included with Prime. Download the Amazon Music app now: <a href=\"http://www.amazon.com/COLD_us_hfd_wiaa_110122\" rel=\"noopener noreferrer\" target=\"_blank\">www.amazon.com/COLD_us_hfd_wiaa_110122</a>.</p><p>See Privacy Policy at <a href=\"https://art19.com/privacy\" rel=\"noopener noreferrer\" target=\"_blank\">https://art19.com/privacy</a> and California Privacy Notice at <a href=\"https://art19.com/privacy#do-not-sell-my-info\" rel=\"noopener noreferrer\" target=\"_blank\">https://art19.com/privacy#do-not-sell-my-info</a>.</p>");
            softly.assertThat(item.getId()).isEqualTo("gid://art19-episode-locator/V0/8ukgqs1KtI5loaLXaXcvQhpN-lNwyIMdDQm-Avs1EFg");
            softly.assertThat(item.getLink()).isEqualTo("https://dts.podtrac.com/redirect.mp3/chrt.fm/track/9EE2G/pdst.fm/e/rss.art19.com/episodes/4d65f69a-a639-4032-a924-b0fa11e15c0f.mp3?rss_browser=BAhJIgtDaHJvbWUGOgZFVA%3D%3D--d05363d83ce333c74f32188013892b2863ad051c");
            softly.assertThat(item.getAuthor()).hasValue("iwonder@wondery.com (Wondery)");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2022-11-14T09:00:00Z"));
        });
    }

    @Test
    void parsesRssWithItunesLinksThatContainsRegularLink() {
        FeedProvider provider = new FeedProvider(url("rss/itunes-with-link.rss"));

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).hasSize(2);
        items.forEach(item -> logger.info("Item: {}", item));
        Item item = items.get(0);
        assertSoftly(softly -> {
            softly.assertThat(item.getFeed().getUrl()).endsWith("rss/itunes-with-link.rss");
            softly.assertThat(item.getFeed().getTitle()).hasValue("The Daily");
            softly.assertThat(item.getTitle()).isEqualTo("Qatar’s Big Bet on the World Cup");
            softly.assertThat(item.getContent()).hasValue("<p>The World Cup, the biggest single sporting event on the planet, began earlier this month. By the time the tournament finishes, half the global population is expected to have watched. </p><p>The 2022 World Cup has also been the focus of over a decade of controversy because of its unlikely host: the tiny, energy-rich country of Qatar. </p><p>How did such a small nation come to host the tournament, and at what cost?</p><p>Guest: <a href=\"https://www.nytimes.com/by/tariq-panja?smid=pc-thedaily\">Tariq Panja</a>, a sports business reporter for The New York Times.</p><p>Background reading: </p><ul><li>The decision to take the World Cup to Qatar has upturned a small nation, battered the reputation of global soccer’s governing body and <a href=\"https://www.nytimes.com/2022/11/19/sports/soccer/world-cup-qatar-2022.html?smid=pc-thedaily\">altered the fabric of the sport</a>.</li><li>Many in Qatar say the barrage of criticism about its human rights record and the exploitation of migrant workers is <a href=\"https://www.nytimes.com/2022/11/25/world/middleeast/qatar-world-cup-criticism.html?smid=pc-thedaily\">laced with discrimination and hypocrisy</a>.</li></ul><p>For more information on today’s episode, visit <a href=\"http://nytimes.com/thedaily?smid=pc-thedaily\">nytimes.com/thedaily</a>. Transcripts of each episode will be made available by the next workday. </p>");
            softly.assertThat(item.getId()).isEqualTo("dfc1e70e-22d0-4c34-8cd4-5170a4f2667a");
            softly.assertThat(item.getLink()).isEqualTo("https://dts.podtrac.com/redirect.mp3/chrt.fm/track/8DB4DB/pdst.fm/e/nyt.simplecastaudio.com/03d8b493-87fc-4bd1-931f-8a8e9b945d8a/episodes/2fb91503-02a8-4336-a2e1-0bee7756b9b7/audio/128/default.mp3?aid=rss_feed&awCollectionId=03d8b493-87fc-4bd1-931f-8a8e9b945d8a&awEpisodeId=2fb91503-02a8-4336-a2e1-0bee7756b9b7&feed=54nAGcIl");
            softly.assertThat(item.getAuthor()).hasValue("thedaily@nytimes.com (The New York Times)");
            softly.assertThat(item.getDate()).isPresent().hasValueSatisfying(d -> assertThat(d).isEqualTo("2022-11-28T10:45:00Z"));
        });
    }

    @Test
    void returnsEmptyResultIfXmlIsInvalid() {
        FeedProvider provider = new FeedProvider(url("special_cases/invalid.xml"));

        List<Item> items = provider.getNewEvents(Collections.emptyList());

        assertThat(items).isEmpty();
    }

    private String url(String file) {
        return FeedProviderTest.class.getClassLoader().getResource(file).toString();
    }

}
