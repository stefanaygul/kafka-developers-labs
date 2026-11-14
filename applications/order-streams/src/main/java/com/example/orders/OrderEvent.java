package com.example.orders;

/** Producer'ın yazdığı event ile aynı JSON. */
public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        double amount,
        String currency,
        String status,
        String eventTime) {
}
