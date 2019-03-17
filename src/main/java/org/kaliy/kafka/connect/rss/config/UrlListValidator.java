package org.kaliy.kafka.connect.rss.config;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.kaliy.kafka.connect.rss.config.RssSourceConnectorConfig.RSS_URL_SEPARATOR;

public class UrlListValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        UrlValidator validator = new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS);
        String urls = (String) value;
        List<String> urlsAsList = Arrays.asList(urls.split(RSS_URL_SEPARATOR));
        List<String> invalidUrls = urlsAsList.stream().filter(url -> !validator.isValid(url))
                .collect(Collectors.toList());
        if (!invalidUrls.isEmpty()) {
            throw new ConfigException(name, value, String.format("Following URLs are invalid: %s", invalidUrls));
        }
    }

    @Override
    public String toString() {
        return "Percent-encoded URLs separated by spaces";
    }
}
