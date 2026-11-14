package com.example.orders.retry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/** DLT'ye düşen kaydı insan okuyabilsin diye header'ları çözüp loglar. */
@Component
@Profile("retry")
public class DltInspector {

    private static final Logger log = LoggerFactory.getLogger(DltInspector.class);

    @KafkaListener(topics = "${app.topic}-dlt", groupId = "${app.retry-group}-dlt-inspector",
            properties = {"value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"})
    public void onDlt(ConsumerRecord<String, byte[]> record) {
        log.warn("DLT topic={} originalTopic={} originalPartition={} originalOffset={} exception={} message={} payload={}",
                record.topic(),
                text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                number(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                number(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                text(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8));
    }

    private static String text(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Partition 4 bayt, offset 8 bayt ikili yazılır; console consumer'da okunmaz görünür. */
    private static Long number(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(header.value());
        return header.value().length == Integer.BYTES ? (long) buffer.getInt() : buffer.getLong();
    }
}
