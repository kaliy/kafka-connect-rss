package org.kaliy.kafka.connect.rss.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ItemTest {

    private static final String TITLE = "title";
    private static final String LINK = "link";
    private static final String ID = "id";
    private static final String CONTENT = "content";
    private static final String AUTHOR = "author";
    private static final String OFFSET = "offset";

    @Test
    void convertsItemToBase64StringWithAllRequiredFields() {
        Item item = new Item(TITLE, LINK, ID, CONTENT, AUTHOR, Instant.EPOCH, OFFSET);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64));
        assertThat(decoded).isEqualTo("%s|%s|%s|%s|%s", TITLE, LINK, ID, CONTENT, AUTHOR);
    }

    @Test
    void addsNullFieldsOnConvertingToBase64() {
        Item item = new Item(null, null, null, null, null, null, null);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64));
        assertThat(decoded).isEqualTo("null|null|null|null|null");
    }

    @Test
    void supportsUnicodeOnConvertingItemToBase64String() {
        Item item = new Item("название", "ウェブリンク", "መለያ", "يحتوى", "ผู้เขียน", Instant.EPOCH, OFFSET);

        String base64 = item.toBase64();

        String decoded = new String(Base64.getDecoder().decode(base64.getBytes(StandardCharsets.UTF_8)));
        assertThat(decoded).isEqualTo("название|ウェブリンク|መለያ|يحتوى|ผู้เขียน");
    }

}
