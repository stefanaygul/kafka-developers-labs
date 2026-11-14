package com.example.orders.retry;

import com.example.orders.OrderEvent;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Lab D2-04, Senaryo C: Bu dosyada çalışıyorsun.
 * Bildirimler birbirinden bağımsızdır, bu yüzden sıra bozulması burada kabul edilebilir.
 * Sipariş durum event'lerinde bu deseni kullanma (slayt 52).
 */
@Component
@Profile("retry")
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    // TODO D2-04-4: Bu listener'a @RetryableTopic ekle. attempts 4, backoff 1 sn ile başlayıp iki katına
    //   çıksın. numPartitions 6, replicationFactor 3 olsun. Slayttaki -retry-0/1/2 adları için
    //   topicSuffixingStrategy'yi index değerine ayarla; varsayılan gecikme değerini ekler.
    @KafkaListener(topics = "${app.notification-topic}", groupId = "${app.notification-group}")
    public void onNotification(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        log.info("NOTIFY_ATTEMPT topic={} partition={} offset={} orderId={} at={}", record.topic(),
                record.partition(), record.offset(), event.orderId(), Instant.now());
        if ("fail-notify".equals(event.customerId())) {
            throw new PaymentGatewayException("Bildirim servisi yanıt vermiyor");
        }
        log.info("NOTIFIED topic={} orderId={} at={}", record.topic(), event.orderId(), Instant.now());
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, OrderEvent> record) {
        log.warn("NOTIFY_DLT topic={} orderId={} at={}", record.topic(), record.value().orderId(), Instant.now());
    }
}
