package org.kaliy.kafka.connect.rss;

import org.apache.kafka.connect.data.Struct;

public class RssData {
    private final Struct data;
    private final String offset;

    public RssData(Struct data, String offset) {
        this.data = data;
        this.offset = offset;
    }

    public Struct getData() {
        return data;
    }

    public String getOffset() {
        return offset;
    }
}
