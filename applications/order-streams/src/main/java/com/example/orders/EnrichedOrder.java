package com.example.orders;

/** Müşteri bilgisiyle zenginleştirilmiş sipariş. */
public record EnrichedOrder(String orderId, String customerId, double amount, String customerName, String city) {

    public static EnrichedOrder of(OrderEvent order, Customer customer) {
        return new EnrichedOrder(order.orderId(), order.customerId(), order.amount(), customer.name(), customer.city());
    }
}
