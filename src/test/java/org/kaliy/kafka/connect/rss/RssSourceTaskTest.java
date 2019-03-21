package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig;
import org.kaliy.kafka.connect.rss.model.Item;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class RssSourceTaskTest {
    private RssSourceTask rssSourceTask = new RssSourceTask();
    private FeedProvider mockFeedProvider = mock(FeedProvider.class);
    private RssSourceConnectorConfig mockConfig = mock(RssSourceConnectorConfig.class);
    private OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);

    @BeforeEach
    void setUp() {
        rssSourceTask.setFeedProviderFactory((url) -> mockFeedProvider);
        when(mockConfig.getUrls()).thenReturn(Collections.singletonList("url"));
        when(mockConfig.getSleepInSeconds()).thenReturn(0);
        when(mockConfig.getTopic()).thenReturn("test_topic");
        rssSourceTask.setConfigFactory((map) -> mockConfig);
        SourceTaskContext mockContext = mock(SourceTaskContext.class);
        when(mockOffsetStorageReader.offset(any())).thenReturn(null);
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        rssSourceTask.initialize(mockContext);
    }

    @Test
    void setsOffsetsFromItems() throws InterruptedException {
        Item item1 = mockItem(), item2 = mockItem();
        when(item1.getOffset()).thenReturn("offset1");
        when(item2.getOffset()).thenReturn("offset1|offset2");
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Arrays.asList(item1, item2));

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).hasSize(2);
        assertSoftly(softly -> {
            softly.assertThat(records.get(0).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "offset1")});
            softly.assertThat(records.get(1).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "offset1|offset2")});
        });
    }

    @Test
    void createsPartitionFromUrl() throws InterruptedException {
        when(mockConfig.getUrls()).thenReturn(Collections.singletonList("http://pikachu.com"));
        Item item = mockItem();
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Collections.singletonList(item));

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).sourcePartition()).containsOnly(new MapEntry[] {entry("url", "http://pikachu.com")});
    }

    @Test
    void ignoresItemsThatCanNotBeConvertedToStruct() throws InterruptedException {
        Item item1 = mockItem(), item2 = mockItem();
        when(item1.toStruct()).thenReturn(Optional.empty());
        when(item2.toStruct()).thenReturn(Optional.of(mock(Struct.class)));
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Arrays.asList(item1, item2));

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).hasSize(1);
    }

    @Test
    void usesStructFromItem() throws InterruptedException {
        Item item = mockItem();
        Struct struct = mock(Struct.class);
        when(item.toStruct()).thenReturn(Optional.of(struct));
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Collections.singletonList(item));

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).extracting(SourceRecord::value).containsOnly(struct);
    }

    @Test
    void usesTopicFromConfiguration() throws InterruptedException {
        Item item = mockItem();
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Collections.singletonList(item));
        when(mockConfig.getTopic()).thenReturn("pokemons_topic");

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).extracting(SourceRecord::topic).containsOnly("pokemons_topic");
    }

    @Test
    void usesOffsetsWhenQueryingForNewItems() throws InterruptedException {
        when(mockOffsetStorageReader.offset(any())).thenReturn(singletonMap("sent_items", "one|two|three"));
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass((Class) Set.class);

        rssSourceTask.start(null);
        rssSourceTask.poll();

        verify(mockFeedProvider).getNewEvents(captor.capture());
        assertThat(captor.getValue()).containsOnly("one", "two", "three");
    }

    @Test
    void usesEmptySetIfOffsetsMapDoesNotContainPreviouslySentItems() throws InterruptedException {
        when(mockOffsetStorageReader.offset(any())).thenReturn(singletonMap("some_other_offset", "one|two|three"));
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass((Class) Set.class);

        rssSourceTask.start(null);
        rssSourceTask.poll();

        verify(mockFeedProvider).getNewEvents(captor.capture());
        assertThat(captor.getValue()).isEmpty();
    }

    @Test
    void usesOffsetsFromTheLastItemInPollResultsOnNextPolling() throws InterruptedException {
        Item item1 = mockItem(), item2 = mockItem();
        when(item1.getOffset()).thenReturn("two|three|four");
        when(item2.getOffset()).thenReturn("two|three|four|five");
        when(mockOffsetStorageReader.offset(any())).thenReturn(singletonMap("sent_items", "one|two|three"));
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Arrays.asList(item1, item2));
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass((Class) Set.class);
        rssSourceTask.start(null);
        rssSourceTask.poll();
        reset(mockFeedProvider);

        rssSourceTask.poll();

        verify(mockFeedProvider).getNewEvents(captor.capture());
        assertThat(captor.getValue()).containsOnly("two", "three", "four", "five");
    }

    @Test
    void doesNotOverrideOffsetsIfFeedProvidersDoesNotReturnAnything() throws InterruptedException {
        when(mockOffsetStorageReader.offset(any())).thenReturn(singletonMap("sent_items", "one|two|three"));
        when(mockFeedProvider.getNewEvents(any())).thenReturn(Collections.emptyList());
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass((Class) Set.class);
        rssSourceTask.start(null);
        rssSourceTask.poll();
        reset(mockFeedProvider);

        rssSourceTask.poll();

        verify(mockFeedProvider).getNewEvents(captor.capture());
        assertThat(captor.getValue()).containsOnly("one", "two", "three");
    }

    @Test
    void usesEmptySetIfNoOffsetsArePresent() throws InterruptedException {
        when(mockOffsetStorageReader.offset(any())).thenReturn(null);
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass((Class) Set.class);

        rssSourceTask.start(null);
        rssSourceTask.poll();

        verify(mockFeedProvider).getNewEvents(captor.capture());
        assertThat(captor.getValue()).isEmpty();
    }

    @Test
    void usesOffsetsOnlyForGivenPartitionsInMultipleFeedProviders() throws InterruptedException {
        when(mockConfig.getUrls()).thenReturn(Arrays.asList("url1", "url2"));
        when(mockOffsetStorageReader.offset(singletonMap("url", "url1"))).thenReturn(singletonMap("sent_items", "one|two|three"));
        when(mockOffsetStorageReader.offset(singletonMap("url", "url2"))).thenReturn(singletonMap("sent_items", "four|five|six"));
        FeedProvider mockFeedProvider1 = mock(FeedProvider.class);
        FeedProvider mockFeedProvider2 = mock(FeedProvider.class);
        rssSourceTask.setFeedProviderFactory((url) -> {
            switch (url) {
                case "url1":
                    return mockFeedProvider1;
                case "url2":
                    return mockFeedProvider2;
                default:
                    throw new RuntimeException();
            }
        });

        ArgumentCaptor<Set<String>> captor1 = ArgumentCaptor.forClass((Class) Set.class);
        ArgumentCaptor<Set<String>> captor2 = ArgumentCaptor.forClass((Class) Set.class);

        rssSourceTask.start(null);
        rssSourceTask.poll();

        verify(mockFeedProvider1).getNewEvents(captor1.capture());
        verify(mockFeedProvider2).getNewEvents(captor2.capture());
        assertThat(captor1.getValue()).containsOnly("one", "two", "three");
        assertThat(captor2.getValue()).containsOnly("four", "five", "six");
    }

    @Test
    void combinesItemsFromMultipleFeedProviders() throws InterruptedException {
        when(mockConfig.getUrls()).thenReturn(Arrays.asList("url1", "url2"));
        FeedProvider mockFeedProvider1 = mock(FeedProvider.class);
        FeedProvider mockFeedProvider2 = mock(FeedProvider.class);
        rssSourceTask.setFeedProviderFactory((url) -> {
            switch (url) {
                case "url1":
                    return mockFeedProvider1;
                case "url2":
                    return mockFeedProvider2;
                default:
                    throw new RuntimeException();
            }
        });
        List<Item> items = IntStream.rangeClosed(0, 3).boxed().map((i) -> mockItem()).collect(Collectors.toList());
        List<Struct> structs = items.stream().map(item -> {
            Struct struct = mock(Struct.class);
            when(item.toStruct()).thenReturn(Optional.of(struct));
            return struct;
        }).collect(Collectors.toList());
        List<String> offsets = Arrays.asList("1", "1|2", "3", "3|4");
        IntStream.rangeClosed(0, 3).forEach(i -> when(items.get(i).getOffset()).thenReturn(offsets.get(i)));
        when(mockFeedProvider1.getNewEvents(any())).thenReturn(Arrays.asList(items.get(0), items.get(1)));
        when(mockFeedProvider2.getNewEvents(any())).thenReturn(Arrays.asList(items.get(2), items.get(3)));

        rssSourceTask.start(null);
        List<SourceRecord> records = rssSourceTask.poll();

        assertThat(records).hasSize(4);
        assertSoftly(softly -> {
            softly.assertThat(records.get(0).sourcePartition()).containsOnly(new MapEntry[] {entry("url", "url1")});
            softly.assertThat(records.get(0).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "1")});
            softly.assertThat(records.get(0).value()).isSameAs(structs.get(0));
            softly.assertThat(records.get(1).sourcePartition()).containsOnly(new MapEntry[] {entry("url", "url1")});
            softly.assertThat(records.get(1).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "1|2")});
            softly.assertThat(records.get(1).value()).isSameAs(structs.get(1));
            softly.assertThat(records.get(2).sourcePartition()).containsOnly(new MapEntry[] {entry("url", "url2")});
            softly.assertThat(records.get(2).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "3")});
            softly.assertThat(records.get(2).value()).isSameAs(structs.get(2));
            softly.assertThat(records.get(3).sourcePartition()).containsOnly(new MapEntry[] {entry("url", "url2")});
            softly.assertThat(records.get(3).sourceOffset()).containsOnly(new MapEntry[] {entry("sent_items", "3|4")});
            softly.assertThat(records.get(3).value()).isSameAs(structs.get(3));
        });
    }

    @Test
    void waitsForTheNextPoll() throws InterruptedException {
        when(mockConfig.getSleepInSeconds()).thenReturn(4);
        rssSourceTask.start(null);
        long current = System.currentTimeMillis();
        rssSourceTask.poll();
        long afterFirstPoll = System.currentTimeMillis();
        assertThat(afterFirstPoll - current).isLessThan(4000);
        rssSourceTask.poll();
        assertThat(System.currentTimeMillis() - afterFirstPoll).isGreaterThanOrEqualTo(4000);
    }

    @Test
    void hasVersion() {
        assertThat(rssSourceTask.version()).isEqualTo("0.1.0");
    }

    private Item mockItem() {
        Item item = mock(Item.class);
        when(item.getOffset()).thenReturn(UUID.randomUUID().toString());
        when(item.toStruct()).thenReturn(Optional.of(mock(Struct.class)));
        return item;
    }
}
