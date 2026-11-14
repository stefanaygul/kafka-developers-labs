package com.example.orders;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonSerde;

/** Açık SerDe'ler. Producer'ın tip header'ina bakmaz: hedef tip burada bellidir. */
public final class JsonSerdes {

    private JsonSerdes() {
    }

    public static <T> Serde<T> of(Class<T> type) {
        JsonSerde<T> serde = new JsonSerde<>(type, JacksonUtils.enhancedObjectMapper());
        serde.noTypeInfo();
        serde.ignoreTypeHeaders();
        return serde;
    }
}
