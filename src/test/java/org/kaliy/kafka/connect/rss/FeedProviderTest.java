package org.kaliy.kafka.connect.rss;

import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndFeedImpl;
import com.rometools.rome.feed.synd.SyndLinkImpl;
import com.rometools.rome.feed.synd.SyndPersonImpl;

import org.junit.jupiter.api.Test;
import org.kaliy.kafka.connect.rss.model.Feed;
import org.kaliy.kafka.connect.rss.model.Item;
import org.mockito.ArgumentCaptor;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FeedProviderTest {

    private SyndEntryImpl syndEntry = syndEntry();
    private SyndFeedImpl syndFeed = syndFeed(Collections.singletonList(syndEntry));
    private Item.Builder spyItemBuilder = spy(Item.Builder.class);

    @Test
    void returnsEmptyOptionalIfFetchIsUnsuccessful() throws Exception {
        FeedProvider feedProvider = new FeedProvider("http://random.host",
                (url) -> Optional.empty(),
                Feed.Builder::aFeed,
                () -> spyItemBuilder
        );

        assertThat(feedProvider.getNewEvents(Collections.emptySet())).isEmpty();
    }

    @Test
    void trimsFeedTitle() throws Exception {
        syndFeed.setTitle("     Pikachu      ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getFeed().getTitle()).hasValue("Pikachu"));
    }

    @Test
    void returnsEmptyTitleIfItIsNotPresentInFeed() throws Exception {
        syndFeed.setTitle(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getFeed().getTitle()).isEmpty());
    }

    @Test
    void usesUrlFromTheConstructorAsFeedUrl() throws Exception {
        FeedProvider feedProvider = new FeedProvider("http://naruto.uzumaki",
                (url) -> Optional.of(syndFeed),
                Feed.Builder::aFeed,
                Item.Builder::anItem
        );

        List<Item> items = feedProvider.getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getFeed().getUrl()).isEqualTo("http://naruto.uzumaki"));
    }

    @Test
    void trimsItemTitle() throws Exception {
        syndEntry.setTitle("     Sasuke      ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getTitle()).isEqualTo("Sasuke"));
    }

    @Test
    void returnsNullAsItemTitleIfItIsNotPresentInItem() throws Exception {
        syndEntry.setTitle(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getTitle()).isEqualTo(null));
    }

    @Test
    void trimsAndReturnsItemLinkIfItIsPresent() throws Exception {
        syndEntry.setLink("    Raichu FTW!   ");
        syndEntry.setLinks(Collections.singletonList(syndLink("sqiurtle")));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getLink()).isEqualTo("Raichu FTW!"));
    }

    @Test
    void returnsNullLinkIfItIsNotPresentInEntry() throws Exception {
        syndEntry.setLink(null);
        syndEntry.setLinks(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getLink()).isEqualTo(null));
    }

    @Test
    void trimsAndReturnsFirstLinkInItemIfItIsPresent() throws Exception {
        syndEntry.setLink(null);
        syndEntry.setLinks(Arrays.asList(syndLink("   Leonardo   "), syndLink("Donatello"), syndLink("Raphael"), syndLink("Michelangelo")));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getLink()).isEqualTo("Leonardo"));
    }

    @Test
    void skipsLinkWithNullHrefAndReturnsFirstNonNull() throws Exception {
        syndEntry.setLink(null);
        syndEntry.setLinks(Arrays.asList(syndLink(null), syndLink(null), syndLink("Raphael"), syndLink("Michelangelo")));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getLink()).isEqualTo("Raphael"));
    }

    @Test
    void trimsItemUriAndUsesItAsAnId() throws Exception {
        syndEntry.setUri("     Junpei      ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getId()).isEqualTo("Junpei"));
    }

    @Test
    void returnsNullAsItemIdIfUriIsNotPresent() throws Exception {
        syndEntry.setUri(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getId()).isNull());
    }

    @Test
    void trimsItemDescriptionAndUsesItAsAContent() throws Exception {
        syndEntry.setDescription(syndContent("   I like to move it move it!    "));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getContent()).hasValue("I like to move it move it!"));
    }

    @Test
    void returnsEmptyContentIfItemDescriptionIsNotPresent() throws Exception {
        syndEntry.setDescription(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getContent()).isEmpty());
    }

    @Test
    void combinesAllAvailableAuthorsInItem() throws Exception {
        syndEntry.setAuthors(Arrays.asList(syndPerson(" Seven "), syndPerson(" Junpei ")));
        syndEntry.setAuthor(" Snake ");
        syndFeed.setAuthors(Arrays.asList(syndPerson(" Santa "), syndPerson(" Lotus ")));
        syndFeed.setAuthor(" Clover ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getAuthor()).hasValue("Seven, Junpei"));
    }

    @Test
    void usesItemAuthorIfItemAuthorsListIsNotAvailable() throws Exception {
        syndEntry.setAuthors(null);
        syndEntry.setAuthor(" Snake ");
        syndFeed.setAuthors(Arrays.asList(syndPerson(" Santa "), syndPerson(" Lotus ")));
        syndFeed.setAuthor(" Clover ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getAuthor()).hasValue("Snake"));
    }

    @Test
    void usesFeedAuthorsIfItemDoesNotHaveAuthors() throws Exception {
        syndEntry.setAuthors(null);
        syndEntry.setAuthor(null);
        syndFeed.setAuthors(Arrays.asList(syndPerson(" Santa "), syndPerson(" Lotus ")));
        syndFeed.setAuthor(" Clover ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getAuthor()).hasValue("Santa, Lotus"));
    }

    @Test
    void usesFeedAuthorIfItemDoesNotHaveAuthorsAndFeedAuthorsListIsNotPresent() throws Exception {
        syndEntry.setAuthors(null);
        syndEntry.setAuthor(null);
        syndFeed.setAuthors(null);
        syndFeed.setAuthor(" Clover ");

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getAuthor()).hasValue("Clover"));
    }

    @Test
    void returnsEmptyAuthorsIfBothItemAndFeedDoNotContainAuthors() throws Exception {
        syndEntry.setAuthors(null);
        syndEntry.setAuthor(null);
        syndFeed.setAuthors(null);
        syndFeed.setAuthor(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getAuthor()).isEmpty());
    }

    @Test
    void usesUpdatedDateAsDateIfItIsAvailable() throws Exception {
        syndEntry.setUpdatedDate(Date.from(Instant.parse("2000-01-01T10:00:01Z")));
        syndEntry.setPublishedDate(Date.from(Instant.parse("2010-11-11T20:00:02Z")));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getDate()).hasValue(Instant.parse("2000-01-01T10:00:01Z")));
    }

    @Test
    void usesPublishedDateAsDateIfUpdatedDateIsNotPresent() throws Exception {
        //updated date is null by default
        syndEntry.setPublishedDate(Date.from(Instant.parse("2010-11-11T20:00:02Z")));

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getDate()).hasValue(Instant.parse("2010-11-11T20:00:02Z")));
    }

    @Test
    void returnsEmptyDateIfDatesAreNotAvailable() throws Exception {
        //updated date is null by default
        syndEntry.setPublishedDate(null);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.emptyList());

        assertThat(items).hasOnlyOneElementSatisfying(item -> assertThat(item.getDate()).isEmpty());
    }

    @Test
    void doesNotReturnItemsIfTheyHaveBeenSent() throws Exception {
        Item item = mock(Item.class);
        when(item.toBase64()).thenReturn("offset1");
        when(spyItemBuilder.build()).thenReturn(item);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.singleton("offset1"));

        assertThat(items).isEmpty();
    }

    @Test
    void addsItemsWithOffsetsThatHaveNotBeenSent() throws Exception {
        Item item = mock(Item.class);
        when(item.toBase64()).thenReturn("offset2");
        ArgumentCaptor<String> offsetCaptor = ArgumentCaptor.forClass(String.class);
        when(spyItemBuilder.build()).thenReturn(item);

        List<Item> items = feedProvider(syndFeed).getNewEvents(Collections.singleton("offset1"));

        assertThat(items).hasSize(1);
        verify(spyItemBuilder).withOffset(offsetCaptor.capture());
        assertThat(offsetCaptor.getAllValues()).containsOnly("offset2");
    }

    @Test
    void returnsMultipleItemsWithCombinedOffsets() throws Exception {
        Item item = mock(Item.class);
        when(item.toBase64()).thenReturn("offset1");
        Item item2 = mock(Item.class);
        when(item2.toBase64()).thenReturn("offset2");
        Item item3 = mock(Item.class);
        when(item3.toBase64()).thenReturn("offset3");

        ArgumentCaptor<String> offsetCaptor = ArgumentCaptor.forClass(String.class);
        when(spyItemBuilder.build()).thenReturn(item).thenReturn(item2).thenReturn(item3);

        List<Item> items = feedProvider(syndFeed(Arrays.asList(syndEntry, syndEntry, syndEntry))).getNewEvents(Collections.emptySet());

        assertThat(items).hasSize(3);
        verify(spyItemBuilder, times(3)).withOffset(offsetCaptor.capture());
        assertThat(offsetCaptor.getAllValues()).containsExactly("offset1", "offset1|offset2", "offset1|offset2|offset3");
    }

    @Test
    void doesNotReturnItemsWithOffsetsThatAlreadyWereSentButKeepsOldOffsetsInNewItems() throws Exception {
        Item item = mock(Item.class);
        when(item.toBase64()).thenReturn("offset1");
        Item item2 = mock(Item.class);
        when(item2.toBase64()).thenReturn("offset2");

        ArgumentCaptor<String> offsetCaptor = ArgumentCaptor.forClass(String.class);
        when(spyItemBuilder.build()).thenReturn(item).thenReturn(item2);

        List<Item> items = feedProvider(syndFeed(Arrays.asList(syndEntry, syndEntry))).getNewEvents(Collections.singletonList("offset1"));

        assertThat(items).hasSize(1);
        verify(spyItemBuilder, times(1)).withOffset(offsetCaptor.capture());
        assertThat(offsetCaptor.getAllValues()).containsExactly("offset1|offset2");
    }

    @Test
    void ignoresOffsetsOfItemsThatWereNotPresentInNewFeedButLeavesOffsetsOfAlreadySentItemsThatArePresentInFeed() throws Exception {
        Item item = mock(Item.class);
        when(item.toBase64()).thenReturn("offset1");
        Item item2 = mock(Item.class);
        when(item2.toBase64()).thenReturn("offset2");
        Item item3 = mock(Item.class);
        when(item3.toBase64()).thenReturn("offset3");

        ArgumentCaptor<String> offsetCaptor = ArgumentCaptor.forClass(String.class);
        when(spyItemBuilder.build()).thenReturn(item).thenReturn(item2).thenReturn(item3);

        List<Item> items = feedProvider(syndFeed(Arrays.asList(syndEntry, syndEntry, syndEntry)))
                .getNewEvents(Arrays.asList("offset0", "offset2"));

        assertThat(items).hasSize(2);
        verify(spyItemBuilder, times(2)).withOffset(offsetCaptor.capture());
        assertThat(offsetCaptor.getAllValues()).containsExactly("offset2|offset1", "offset2|offset1|offset3");
    }

    private FeedProvider feedProvider(SyndFeed syndFeed) throws MalformedURLException {
        return new FeedProvider("http://random.host",
                (url) -> Optional.of(syndFeed),
                Feed.Builder::aFeed,
                () -> spyItemBuilder
        );
    }

    private SyndFeedImpl syndFeed(List<SyndEntry> entries) {
        SyndFeedImpl feed = new SyndFeedImpl();
        feed.setTitle("title");
        feed.setUri("url");
        feed.setEntries(entries);
        return feed;
    }

    private SyndEntryImpl syndEntry() {
        // we are not populating Author as it won't be overwritten anymore in SyndEntryImpl.setAuthor
        SyndEntryImpl syndEntry = new SyndEntryImpl();
        syndEntry.setTitle("title");
        syndEntry.setLink("link");
        syndEntry.setUri("uri");
        syndEntry.setDescription(syndContent("content"));
        return syndEntry;
    }

    private SyndContentImpl syndContent(String content) {
        SyndContentImpl syndContent = new SyndContentImpl();
        syndContent.setValue(content);
        return syndContent;
    }

    private SyndLinkImpl syndLink(String href) {
        SyndLinkImpl syndLink = new SyndLinkImpl();
        syndLink.setHref(href);
        return syndLink;
    }

    private SyndPersonImpl syndPerson(String name) {
        SyndPersonImpl syndPerson = new SyndPersonImpl();
        syndPerson.setName(name);
        return syndPerson;
    }

    private URL url(String file) {
        return FeedProviderTest.class.getClassLoader().getResource(file);
    }
}
