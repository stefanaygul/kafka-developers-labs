package com.example.orders.retry;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Serializer;

/** DLT key'i normalde String, bozuk kayıtta ham bayt olabilir. */
class StringOrBytesKeySerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        return data instanceof byte[] bytes ? bytes : data.toString().getBytes(StandardCharsets.UTF_8);
    }
}
