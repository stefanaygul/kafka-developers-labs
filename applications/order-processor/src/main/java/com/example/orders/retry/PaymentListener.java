package com.example.orders.retry;

import com.example.orders.OrderEvent;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kontrollü hata üreten listener. Davranışı customerId belirler:
 * fail-transient iki kez hata verir, fail-permanent hep hata verir,
 * fail-invalid retry edilmemesi gereken bir hata verir. Bu dosyayı değiştirmen gerekmiyor.
 */
@Component
@Profile("retry")
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

    @KafkaListener(topics = "${app.topic}", groupId = "${app.retry-group}")
    public void onOrder(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        String position = record.partition() + "-" + record.offset();
        int attempt = attempts.merge(position, 1, Integer::sum);
        String customer = event == null ? "" : String.valueOf(event.customerId());
        log.info("ATTEMPT attempt={} partition={} offset={} customerId={} at={}", attempt, record.partition(),
                record.offset(), customer, Instant.now());
        switch (customer) {
            case "fail-transient" -> {
                if (attempt <= 2) {
                    throw new PaymentGatewayException("Ödeme servisi geçici olarak yanıt vermiyor");
                }
            }
            case "fail-permanent" -> throw new PaymentGatewayException("Ödeme servisi kalıcı olarak reddetti");
            case "fail-invalid" -> throw new PaymentValidationException("Tutar geçersiz");
            default -> {
                // Normal kayıt: hata yok.
            }
        }
        log.info("PAID attempt={} partition={} offset={} at={}", attempt, record.partition(), record.offset(),
                Instant.now());
    }
}
