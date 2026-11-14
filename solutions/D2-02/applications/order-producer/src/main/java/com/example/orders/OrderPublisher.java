package com.example.orders;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
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

        CompletableFuture<SendResult<String, OrderEvent>> future;
        try {
            future = kafkaTemplate.send(topic, key, event);
        } catch (KafkaException e) {
            future = CompletableFuture.failedFuture(e);
        }

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
        try {
            return publish(event, topic).get(WAIT_SECONDS, TimeUnit.SECONDS).getRecordMetadata();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeliveryFailedException("Bekleme kesildi: " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = rootCause(e);
            throw new DeliveryFailedException(
                    "Broker onayı alınamadı: " + cause.getClass().getSimpleName() + ": " + cause.getMessage(), cause);
        }
    }

    public Map<String, Long> stats() {
        return Map.of("sent", sent.get(), "acked", acked.get(), "failed", failed.get());
    }

    private void onAck(OrderEvent event, RecordMetadata metadata, long start) {
        acked.incrementAndGet();
        log.info("ACK eventId={} orderId={} partition={} offset={} timestamp={} ackMs={} result=SUCCESS",
                event.eventId(), event.orderId(), metadata.partition(), metadata.offset(),
                metadata.timestamp(), elapsedMs(start));
    }

    private void onFailure(OrderEvent event, String topic, Throwable ex, long start) {
        Throwable cause = rootCause(ex);
        failed.incrementAndGet();
        failedEvents.record(event, topic, cause);
        log.error("NACK eventId={} orderId={} topic={} exception={} message={} ackMs={} result=FAILED",
                event.eventId(), event.orderId(), topic, cause.getClass().getName(), cause.getMessage(),
                elapsedMs(start));
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
