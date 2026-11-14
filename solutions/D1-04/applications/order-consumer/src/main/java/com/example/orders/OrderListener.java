package com.example.orders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.stereotype.Component;

/**
 * Labs D1-04 ve D2-05: Bu dosyada çalışıyorsun.
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final String instance;
    private final long slowMs;

    public OrderListener(@Value("${app.instance}") String instance, @Value("${app.slow-ms}") long slowMs) {
        this.instance = instance;
        this.slowMs = slowMs;
    }

    @KafkaListener(topics = "${app.topic}")
    public void onOrder(ConsumerRecord<String, OrderEvent> record) {
        // TODO D2-05-1: slowMs sıfırdan büyükse o kadar milisaniye bekle. Dış servisi yavaşlayan
        //   bir listener'ı taklit ediyoruz; lag'in nasıl büyüdüğünü izleyeceksin.
        logRecord(record);
    }

    /** Beklenen log: groupId, partition, offset, eventId. */
    private void logRecord(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        log.info("CONSUMED instance={} groupId={} partition={} offset={} key={} eventId={}",
                instance, KafkaUtils.getConsumerGroupId(), record.partition(), record.offset(), record.key(),
                event == null ? null : event.eventId());
    }
}
