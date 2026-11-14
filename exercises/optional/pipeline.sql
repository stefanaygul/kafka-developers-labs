-- Opsiyonel ksqlDB demosu. Slayt 93, 94, 95.
-- Kafka Streams ana hattın yerini almaz; bu dosya yalnızca karşılaştırma içindir.

SET 'auto.offset.reset' = 'earliest';

-- Mevcut topic'e schema giydirir. Veri kopyalanmaz.
CREATE STREAM IF NOT EXISTS orders_stream (
  order_id VARCHAR KEY,
  customerId VARCHAR,
  amount DOUBLE,
  status VARCHAR
) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

-- Materialized view: sürekli çalışan query, state store ve ORDER_TOTALS topic'i.
CREATE TABLE IF NOT EXISTS order_totals AS
  SELECT customerId, COUNT(*) AS cnt, SUM(amount) AS total
  FROM orders_stream
  WHERE customerId IS NOT NULL
  GROUP BY customerId
  EMIT CHANGES;
