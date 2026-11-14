-- D2-03: dayanıklı idempotency için tablolar.
-- processed_events: tekrar gelen event'i tanıyan işaret.
-- customer_stats: iş etkisi. Bilerek artırma (count + 1) yapar; tekrar gelen
-- mesaj korunmazsa sonuç gözle görülür biçimde bozulur.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id         VARCHAR(64) PRIMARY KEY,
    order_id         VARCHAR(64) NOT NULL,
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    redelivery_count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS customer_stats (
    customer_id  VARCHAR(64) PRIMARY KEY,
    order_count  BIGINT NOT NULL DEFAULT 0,
    total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0
);
