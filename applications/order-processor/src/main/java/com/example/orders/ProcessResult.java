package com.example.orders;

public enum ProcessResult {
    /** İlk kez görüldü, iş sonucu yazıldı. */
    PROCESSED,
    /** Bu eventId daha önce işlendi, iş sonucu değişmedi. */
    DUPLICATE,
    /** eventId yok: bu kayıt idempotent işlenemez (ör. elle yazılmış CLI kaydı). */
    INVALID
}
