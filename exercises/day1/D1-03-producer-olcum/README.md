# D1-03 · Producer ayarlarını ölçün

## Nerede çalışıyorsun

```text
exercises/day1/D1-03-producer-olcum/OLCUM.md  → ölçümlerini buraya yaz
```

Kod yok.

| | |
|---|---|
| Slayt | 27 (kavram: 23, 24, 25, 26) |
| Süre | 20 dakika |
| Ön koşul | D1-01 |

## Hedef

`acks` ve `linger.ms` etkisini slayttan değil, kendi sayılarından öğrenmek.

## Problem

`linger.ms` artınca throughput artar deniyor. Sende kaç kat artıyor? `acks=all` gerçekte ne kadar pahalı?

## Tahmin

`OLCUM.md` dosyasına bakmadan tahmin et: `linger.ms=50` throughput'u kaç katına çıkarır? `acks=all`, `acks=1`'e göre yüzde kaç yavaştır?

## Önemli uyarılar

- Sonuçlar yerel Docker cluster'ı içindir. Production benchmark'ı değildir ve bir ayarın her zaman hızlı olacağını kanıtlamaz.
- Ölçüm için ayrı `orders-perf` topic'i kullanılır. Perf test rastgele veri yazar; `orders` topic'ine yazılırsa sonraki JSON consumer'ları bozar.
- `--command-property` **bir kez** yazılır, değerler arka arkaya gelir. Tekrar yazarsan önceki değer **sessizce düşer** ve yanlış ayarı ölçersin.

## Adımlar

**Adım 1 · Topic aç.** `docker exec -it kafka-1 bash` ve `cd /opt/kafka/bin`, sonra:

```bash
./kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic orders-perf \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2
```

**Adım 2 · Koşu A: `acks=1`.** Kayıt sayısı, boyut ve sıkıştırma her koşuda aynı kalır.

```bash
./kafka-producer-perf-test.sh --bootstrap-server kafka-1:19092 --topic orders-perf \
  --num-records 50000 --record-size 200 --throughput -1 \
  --command-property acks=1 linger.ms=0 enable.idempotence=false compression.type=none
```

**Adım 3 · Koşu B: `acks=all`.** Aynı komutta yalnızca `acks=all` yap.

**Adım 4 · Koşu C: `linger.ms=50`.** Koşu B'de yalnızca `linger.ms=50` yap.

**Adım 5 · Koşu D: idempotence.** Koşu B'de yalnızca `enable.idempotence=true` yap.

**Adım 6 · Uyumluluk kuralı.** `acks=1` ile `enable.idempotence=true` birlikte çalıştır. Bu bir ölçüm değil, yapılandırma hatasıdır:

```text
ConfigException: Must set acks to all in order to use the idempotent producer.
```

**Adım 7 · Key'siz üretim.** Perf test key'siz yazar. Dağılıma bak; sticky partitioner batch'leri bir partition'a toplar:

```bash
./kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic orders-perf
```

Her koşunun son satırını `OLCUM.md` tablosuna yaz.

## Beklenen çıktı

```text
50000 records sent, 60060.06 records/sec (11.46 MB/sec), 85.28 ms avg latency, ... 138 ms 99th ...
```

Sayılar makineden makineye değişir. `acks` farkının küçük çıkması normaldir: tek makinede ağ gecikmesi yoktur.

## Doğrula

```bash
./lab.sh check D1-03
```

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Yalnızca producer ayarlarını.
- **Kafka'nın davranışı nasıl değişti?** Batch boyutu, istek sayısı ve gecikme değişti.
- **Production dersi:** Varsayılana güvenme, açıkça yaz. `linger.ms` varsayılanı client sürümüne göre değişir: Kafka 4.0+ CLI'da 5, Spring Boot 3.5'in kafka-clients 3.9'unda 0.

## Kavram soruları

1. `linger.ms=0` iken hiç batch oluşmaz mı?
2. Ölçtüğün `acks=all` bedeli, bir broker kaybında veri kaybetme riskinden daha mı pahalı?
3. Açıkça `enable.idempotence=true` yazmak neden `acks=1` ile çakışınca hata verir, yazmamak neden vermez?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `usage: kafka-producer-perf-test` | `--bootstrap-server` eksik | Slayttaki komuta bu seçeneği ekle |
| Beklenen hata gelmiyor | `--command-property` iki kez yazıldı | Tek bayrak, çoklu değer yaz |
| Sayılar çok oynuyor | Başka iş yükü var | Her koşuyu iki kez çalıştır |

## Temizlik

Gerek yok.

## Takıldıysan

`./lab.sh solution D1-03`
