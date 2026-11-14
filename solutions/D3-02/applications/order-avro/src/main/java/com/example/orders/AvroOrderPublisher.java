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
        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setAmount(amount)
                .build();
        return kafkaTemplate.send(topic, orderId, order);
    }
}
