package com.example.orders;

/** orders-high topic'ine yazılan sade görünüm. */
public record OrderSummary(String orderId, String customerId, double amount, String label) {

    public static OrderSummary from(OrderEvent order) {
        return new OrderSummary(order.orderId(), order.customerId(), order.amount(), "HIGH_VALUE");
    }
}
