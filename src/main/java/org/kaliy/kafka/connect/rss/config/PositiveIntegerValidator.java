package org.kaliy.kafka.connect.rss.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PositiveIntegerValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        int sleepInSecods = (int) value;
        if (sleepInSecods < 0) {
            throw new ConfigException(name, value, "sleep interval should be 0 or higher");
        }
    }

    @Override
    public String toString() {
        return "0 or higher";
    }
}
