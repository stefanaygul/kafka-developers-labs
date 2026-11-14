package com.example.orders;

import com.shop.Order;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Lab D3-02: Bu dosyada çalışıyorsun.
 */
@Component
public class AvroOrderPublisher {

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String topic;

    public AvroOrderPublisher(KafkaTemplate<String, Order> kafkaTemplate, @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, Order>> publish(String orderId, String customerId, double amount) {
        // TODO D3-02-2: Order.newBuilder ile üretilmiş Avro sınıfından bir nesne kur (orderId, customerId,
        //   amount). kafkaTemplate ile topic'e, key olarak orderId vererek gönder ve future'ı döndür.
        return CompletableFuture.failedFuture(new UnsupportedOperationException("TODO D3-02-2 tamamlanmadı"));
    }
}
