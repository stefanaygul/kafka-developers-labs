package com.example.orders;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Lab D2-03: Bu dosyada çalışıyorsun.
 * Tekrar işareti ile iş sonucu aynı DB transaction'inda yazılır: ya ikisi de olur ya hiçbiri.
 */
@Service
@Profile("idempotency")
public class OrderProcessingService {

    private final JdbcTemplate jdbc;

    public OrderProcessingService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Bir event'i iş sonucuna en fazla bir kez yansıtır.
     * Kafka aynı kaydı tekrar teslim edebilir (at-least-once); bu metot ikinci gelişi zararsız yapar.
     * Transaction kritik: işaret ve iş sonucu birlikte yazılır ya da birlikte geri alınır.
     */
    @Transactional
    public ProcessResult process(OrderEvent event) {
        // eventId yoksa tekrar tanınamaz. Elle yazılmış CLI kayıtları buraya düşer.
        if (event == null || event.eventId() == null || event.customerId() == null) {
            return ProcessResult.INVALID;
        }

        // 1. Önce işareti yazmayı dene. Kararı uygulama değil, event_id primary key'i verir.
        //    Önce SELECT sonra INSERT yapılsaydı iki instance aynı anda "yok" görüp ikisi de işlerdi.
        int inserted = jdbc.update("""
                INSERT INTO processed_events (event_id, order_id)
                VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId(), event.orderId());

        // 2. 0 satır: bu event_id zaten var, yani daha önce işlendi. İşi atla, yalnızca tekrarı say.
        if (inserted == 0) {
            markRedelivered(event);
            return ProcessResult.DUPLICATE;
        }

        // 3. 1 satır: ilk geliş. İş sonucu, işaretle aynı transaction'da commit olur.
        applyBusinessEffect(event);
        return ProcessResult.PROCESSED;
    }

    /** Bilerek artırma yapar: korunmayan bir tekrar müşterinin toplamını bozar. */
    private void applyBusinessEffect(OrderEvent event) {
        jdbc.update("""
                INSERT INTO customer_stats (customer_id, order_count, total_amount)
                VALUES (?, 1, ?)
                ON CONFLICT (customer_id) DO UPDATE
                SET order_count = customer_stats.order_count + 1,
                    total_amount = customer_stats.total_amount + EXCLUDED.total_amount
                """, event.customerId(), event.amount());
    }

    /** Tekrar teslimatı ölçebilmek için sayar. İş sonucuna dokunmaz. */
    private void markRedelivered(OrderEvent event) {
        jdbc.update("UPDATE processed_events SET redelivery_count = redelivery_count + 1 WHERE event_id = ?",
                event.eventId());
    }
}
