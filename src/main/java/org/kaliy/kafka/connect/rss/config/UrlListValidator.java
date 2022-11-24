package org.kaliy.kafka.connect.rss.config;

import com.rometools.utils.Strings;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.RSS_URL_SEPARATOR;

public class UrlListValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        UrlValidator validator = new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS);
        List<String> urls = getUrlsWithFallback(value);
        if (urls.isEmpty()) {
            throw new ConfigException(name, value, "No URLs found");
        }
        List<String> invalidUrls = urls.stream().filter(url -> !validator.isValid(url))
                .collect(Collectors.toList());
        if (!invalidUrls.isEmpty()) {
            throw new ConfigException(name, value, String.format("Following URLs are invalid: %s", invalidUrls));
        }
    }

    private static List<String> getUrlsWithFallback(Object value) {
        String urls = (String) value;
        return Strings.isBlank(urls) ? Collections.emptyList() : Arrays.asList(urls.split(RSS_URL_SEPARATOR));
    }

    @Override
    public String toString() {
        return "Percent-encoded URLs separated by spaces";
    }
}
