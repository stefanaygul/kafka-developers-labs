package com.example.orders.retry;

/** Kalıcı hata: tekrar denemek sonucu değiştirmez. */
public class PaymentValidationException extends RuntimeException {

    public PaymentValidationException(String message) {
        super(message);
    }
}
