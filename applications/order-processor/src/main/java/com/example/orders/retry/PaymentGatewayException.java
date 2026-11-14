package com.example.orders.retry;

/** Geçici hata: tekrar denemek sonucu değiştirebilir. */
public class PaymentGatewayException extends RuntimeException {

    public PaymentGatewayException(String message) {
        super(message);
    }
}
