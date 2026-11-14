package com.example.orders;

import java.time.Instant;
import java.util.UUID;

/** Üç gün boyunca kullanılan OrderCreated event'i. eventId event oluşurken bir kez üretilir. */
public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        double amount,
        String currency,
        String status,
        String eventTime) {

    public static OrderEvent created(String orderId, String customerId, double amount) {
        return new OrderEvent(UUID.randomUUID().toString(), orderId, customerId, amount, "TRY", "CREATED",
                Instant.now().toString());
    }
}
