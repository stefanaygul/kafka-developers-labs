package com.example.orders;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Window'lar kaydın Kafka'ya yazıldığı ana göre değil, siparişin gerçekten oluştuğu ana göre
 * hesaplansın diye zamanı payload'daki eventTime alanından okur (slayt 69).
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof OrderEvent order && order.eventTime() != null) {
            try {
                return Instant.parse(order.eventTime()).toEpochMilli();
            } catch (DateTimeParseException e) {
                return record.timestamp();
            }
        }
        return record.timestamp();
    }
}
