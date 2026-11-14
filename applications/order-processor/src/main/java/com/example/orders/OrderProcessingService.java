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

    @Transactional
    public ProcessResult process(OrderEvent event) {
        if (event == null || event.eventId() == null || event.customerId() == null) {
            return ProcessResult.INVALID;
        }

        // TODO D2-03-3: jdbc.update ile processed_events tablosuna event_id ve order_id yaz.
        //   ON CONFLICT (event_id) DO NOTHING kullan. update'in döndürdüğü satır sayısını sakla.

        // TODO D2-03-4: Etkilenen satır 0 ise bu event daha önce işlenmiştir. markRedelivered'ı
        //   çağır ve ProcessResult.DUPLICATE döndür; iş sonucu ikinci kez yazılmasın.

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
