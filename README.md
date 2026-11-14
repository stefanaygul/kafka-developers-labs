# Kafka for Developers · Uygulamalı Lab Platformu

Üç günlük Apache Kafka eğitiminin lab'leri. Bir sipariş akışı üç gün boyunca büyür: event üretilir, güvenilir biçimde tüketilir, akan veri işlenir ve schema'lar evrilir.

[BASLANGIC.md](BASLANGIC.md) ile başla.

## Lab haritası

| Lab | Konu | Slayt | Süre |
|---|---|---|---|
| [D1-01](exercises/day1/D1-01-ilk-mesaj/README.md) | İlk mesaj | 19 | 45 dk |
| [D1-02](exercises/day1/D1-02-partition-dagilimi/README.md) | Partition dağılımı | 20 | 20 dk |
| [D1-03](exercises/day1/D1-03-producer-olcum/README.md) | Producer ölçümü | 27 | 20 dk |
| [D1-04](exercises/day1/D1-04-ilk-spring-uygulamasi/README.md) | İlk Spring Boot uygulaması | 33 | 90 dk |
| [D2-01](exercises/day2/D2-01-kraft-cluster/README.md) | KRaft cluster | 41 | 25 dk |
| [D2-02](exercises/day2/D2-02-producer-teslim/README.md) | Producer teslim sonucu | 44 | 15 dk |
| [D2-03](exercises/day2/D2-03-manuel-commit-idempotency/README.md) | Manuel commit, idempotency | 49 | 20 dk |
| [D2-04](exercises/day2/D2-04-retry-dlt/README.md) | Retry ve DLT | 53 | 20 dk |
| [D2-05](exercises/day2/D2-05-lag-rebalance-failover/README.md) | Lag, rebalance, failover | 58 | 25 dk |
| [D2-06](exercises/day2/D2-06-production-provasi/README.md) | Production provası | 59 | 60 dk |
| [D3-01](exercises/day3/D3-01-kafka-streams/README.md) | Kafka Streams | 73 | 50 dk |
| [D3-02](exercises/day3/D3-02-avro-schema-registry/README.md) | Avro, Schema Registry | 80 | 55 dk |
| [D3-03](exercises/day3/D3-03-mimari-workshop/README.md) | Mimari workshop | 83 | 40 dk |
| [Opsiyonel](exercises/optional/README.md) | Connect, ksqlDB | 81, 82, 93–96 | – |

## Adresler

| Bileşen | Host'tan | Container içinden |
|---|---|---|
| Kafka broker 1, 2, 3 | `localhost:9092`, `9094`, `9096` | `kafka-1:19092`, `kafka-2:19092`, `kafka-3:19092` |
| Controller | – | `kafka-N:9093` |
| Schema Registry | `localhost:8081` | `schema-registry:8081` |
| PostgreSQL | `localhost:15432` | `postgres:5432` |
| order-producer | `localhost:8080` | – |
| order-avro | `localhost:8084` | – |
| ksqlDB (opsiyonel) | `localhost:8088` | `ksqldb-server:8088` |
| Kafka Connect (opsiyonel) | `localhost:8083` | `connect:8083` |

Kullanıcı adı ve parola PostgreSQL için `orders`. Yalnızca yerel lab içindir.

## Komutlar

`./lab.sh` bütün komutları listeler. Windows'ta `.\lab.ps1` aynı komutları alır.

## Depo yapısı

| Yol | İçerik |
|---|---|
| `applications/` | Öğrenenin üzerinde çalıştığı kod, TODO'larla |
| `exercises/` | Lab rehberleri ve şablonlar |
| `solutions/<LAB>/` | Lab başına çözülmüş dosyalar |
| `infrastructure/` | Compose dosyaları |
| `tests/lab-check/` | `lab.sh` ve `lab.ps1` arkasındaki doğrulama aracı |
| `docs/` | Sorun giderme |

## Sürümler

Java 17, Spring Boot 3.5.16, Spring Kafka 3.3.16, kafka-clients 3.9.2, Kafka broker 4.3.2 (KRaft), Schema Registry 8.3.2.
