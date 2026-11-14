package com.example.orders;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Labs D1-04 ve D2-02: Bu dosyada çalışıyorsun.
 * Tüm Kafka gönderimi buradan geçer.
 */
@Component
public class OrderPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);
    private static final long WAIT_SECONDS = 40;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final FailedEventStore failedEvents;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public OrderPublisher(KafkaTemplate<String, OrderEvent> kafkaTemplate, FailedEventStore failedEvents) {
        this.kafkaTemplate = kafkaTemplate;
        this.failedEvents = failedEvents;
    }

    /** Normal yol: key olarak orderId kullanılır. */
    public CompletableFuture<SendResult<String, OrderEvent>> publish(OrderEvent event, String topic) {
        return publish(event, topic, event.orderId());
    }

    public CompletableFuture<SendResult<String, OrderEvent>> publish(OrderEvent event, String topic, String key) {
        long start = System.nanoTime();

        CompletableFuture<SendResult<String, OrderEvent>> future = kafkaTemplate.send(topic, key, event);

        // TODO D2-02-1: send() bazı hataları callback'e bırakmadan hemen fırlatır (ör. olmayan topic).
        //   Gönderimi try-catch içine al; org.springframework.kafka.KafkaException yakalarsan failedFuture'a çevir. Böylece
        //   iki hata yolu da aşağıdaki tek noktadan geçer.

        sent.incrementAndGet();
        log.info("SEND_RETURNED eventId={} topic={} sendReturnedMs={}", event.eventId(), topic, elapsedMs(start));

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                onAck(event, result.getRecordMetadata(), start);
            } else {
                onFailure(event, topic, ex, start);
            }
        });

        return future;
    }

    /** Broker onayı gelene kadar bekler. Onay yoksa çağırana hata iletir. */
    public RecordMetadata publishAndWait(OrderEvent event, String topic) {
        // TODO D2-02-4: publish'ten dönen future'ı en fazla WAIT_SECONDS saniye bekle ve
        //   RecordMetadata'yi döndür. Başarısızlıkta DeliveryFailedException fırlat; böylece
        //   controller 201 yerine 503 döner ve çağıran gerçek sonucu öğrenir.
        publish(event, topic);
        throw new DeliveryFailedException("TODO D2-02-4 tamamlanmadı", null);
    }

    public Map<String, Long> stats() {
        return Map.of("sent", sent.get(), "acked", acked.get(), "failed", failed.get());
    }

    private void onAck(OrderEvent event, RecordMetadata metadata, long start) {
        acked.incrementAndGet();
        // TODO D2-02-2: Bu satıra metadata.timestamp() ve elapsedMs(start) değerlerini
        //   "timestamp=" ve "ackMs=" alanları olarak ekle. send() ile açk arasındaki farkı gör.
        log.info("ACK eventId={} orderId={} partition={} offset={} result=SUCCESS",
                event.eventId(), event.orderId(), metadata.partition(), metadata.offset());
    }

    private void onFailure(OrderEvent event, String topic, Throwable ex, long start) {
        Throwable cause = rootCause(ex);
        // TODO D2-02-3: Hatayı yalnızca loglamak yetmez. failed sayacını artır ve event'i
        //   failedEvents.record ile kalıcı olarak kaydet; yoksa uygulama kapanınca iz kalmaz.
        log.error("NACK eventId={} orderId={} topic={} exception={} message={} result=FAILED",
                event.eventId(), event.orderId(), topic, cause.getClass().getName(), cause.getMessage());
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static Throwable rootCause(Throwable ex) {
        Throwable current = ex;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

}
