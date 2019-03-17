package org.kaliy.kafka.connect.rss.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class UrlListValidatorTest {

    private UrlListValidator urlListValidator = new UrlListValidator();

    @Test
    void validatesSingleUrl() {
        assertThatExceptionOfType(ConfigException.class)
                .isThrownBy(() -> urlListValidator.ensureValid("rss.url", "http:/incorrect"))
                .withMessage("Invalid value http:/incorrect for configuration rss.url: Following URLs are invalid: [http:/incorrect]");
    }

    @Test
    void validatesMultipleUrls() {
        assertThatExceptionOfType(ConfigException.class)
                .isThrownBy(() -> urlListValidator.ensureValid("rss.url", "http:/invalid kurochka:///invalid"))
                .withMessage("Invalid value http:/invalid kurochka:///invalid for configuration rss.url: Following URLs are invalid: [http:/invalid, kurochka:///invalid]");
    }

    @Test
    void allowsUrlsWithUnderscoreInPath() {
        assertThatCode(() -> urlListValidator.ensureValid("rss.url", "http://topkek.com/pepe_the_great"))
                .doesNotThrowAnyException();
    }

    @Test
    void throwsExceptionIfUrlWithSpaceIsPassed() {
        assertThatExceptionOfType(ConfigException.class)
                .isThrownBy(() -> urlListValidator.ensureValid("rss.url", "http://rss.com/location with-space"))
                .withMessage("Invalid value http://rss.com/location with-space for configuration rss.url: Following URLs are invalid: [with-space]");
    }

    @Test
    void hasToStringMethodForDocumentation() {
        assertThat(urlListValidator).hasToString("Percent-encoded URLs separated by spaces");
    }

    @Test
    void allowsLocalhostUrls() {
        assertThatCode(() -> urlListValidator.ensureValid("rss.url", "http://localhost:8888/feed.atom"))
                .doesNotThrowAnyException();
    }
}
