package org.kaliy.kafka.connect.rss.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PositiveIntegerValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        int number = (int) value;
        if (number < 0) {
            throw new ConfigException(name, value, "should be 0 or higher");
        }
    }

    @Override
    public String toString() {
        return "0 or higher";
    }
}
