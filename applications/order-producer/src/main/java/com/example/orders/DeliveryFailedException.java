package com.example.orders;

/** Broker onayı alınamadı: çağırana iletilecek hata. */
public class DeliveryFailedException extends RuntimeException {

    public DeliveryFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
