# D3-01 · Kafka Streams ile filtre, join ve window

## Nerede çalışıyorsun

```text
applications/order-streams/src/main/java/com/example/orders/OrderTopology.java
  → TODO D3-01-1, 2, 3, 4
```

Başka hiçbir dosyaya dokunmana gerek yok. SerDe, event time ve Spring bağlantıları hazır.

| | |
|---|---|
| Slayt | 73 (kavram: 62–72, opsiyonel: 74) |
| Süre | 50 dakika |
| Ön koşul | D1-04 (producer ile sipariş göndereceksin) |

## Hedef

Çalışan bir Java Kafka Streams topology'si kurmak ve geç gelen event'e ne olduğunu kendi gözünle görmek.

## Problem

Sipariş event'leri akıyor. Yüksek tutarlıları ayırmak, müşteri bilgisiyle birleştirmek ve beş dakikalık sayım yapmak istiyorsun. Peki geç gelen event nereye sayılır?

## Tahmin

Slayt 69'daki tabloya bakmadan: window 10:00–10:05, grace 2 dakika. Stream time 10:08 iken 10:04'lü event gelirse ne olur? Peki stream time 10:06 iken 10:03'lü gelirse?

## Başlat

```bash
./lab.sh up D3-01
```

`customers` topic'i compacted olarak açılır ve 50 müşteri yazılır. Uygulama `orders` topic'ini yalnızca açıldıktan sonra gelen kayıtlar için okur; eski lab kayıtları stream time'ı bozmaz.

## Bölüm A · Stateless işlem

1. **TODO D3-01-1:** `filter`, `mapValues`, `to`. Çalıştır ve dene:

```bash
./lab.sh run order-streams
./lab.sh run order-producer
curl -X POST "localhost:8080/orders?orderId=order-big&amount=15000"
```

`orders-high` topic'inde yalnızca 10.000 ve üzeri siparişler görünmeli.

## Bölüm B · KStream ve KTable

`orders` key'i `orderId`, `customers` key'i `customerId`. Join için iki taraf aynı key ve aynı partition sayısına sahip olmalı: buna co-partitioning denir.

2. **TODO D3-01-2:** Key'i değiştir ve repartition et. Sıralama bedelini düşün: siparişler artık müşteri bazında gruplanır.
3. **TODO D3-01-3:** Join'i kur. Uygulamayı yeniden başlat ve açılış logundaki `TOPOLOGY` çıktısını oku. İki sub-topology görmelisin: araya repartition girdi.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list
```

`order-streams-` ile başlayan repartition ve changelog topic'lerini Streams kendisi oluşturdu.

## Bölüm C · Window'lu sayım ve geç event

4. **TODO D3-01-4:** Beş dakikalık window, iki dakika grace. Uygulamayı yeniden başlat.
5. Slayt 69'daki sırayla beş event gönder:

```bash
./lab.sh emit late-events
```

6. Sonucu oku. Sayaçlar `Long` olduğu için deserializer verilir:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 \
  --topic order-counts-5m --from-beginning --timeout-ms 6000 --formatter-property print.key=true \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Beş event **aynı key ile** yazılır. Stream time task başınadır: farklı partition'lara dağılsalar sonuç rastgele değişirdi.

## Opsiyonel · exactly_once_v2

Süre kalmazsa atla. Yeni bir `application-id` ile aç, çıktıyı `read_committed` oku:

```bash
./lab.sh run order-streams --spring.kafka.streams.application-id=order-streams-eos \
  --spring.kafka.streams.properties.processing.guarantee=exactly_once_v2
```

Console consumer'a `--isolation-level read_committed` ekle. Garanti Kafka'dan Kafka'yadır; veritabanını kapsamaz.

## Beklenen çıktı

```text
customer-1@2026-11-04T06:30:00Z   1
customer-1@2026-11-04T06:35:00Z   1
customer-1@2026-11-04T06:30:00Z   2
customer-1@2026-11-04T06:35:00Z   2
```

İlk window 2: A ve grace içindeki B. Üçüncü olamaz: E düşürüldü. Uygulama logunda:

```text
Skipping record for expired window. topic=[...] partition=[3] offset=[4] ...
```

## Doğrula

```bash
./lab.sh check D3-01
```

Önce `TopologyTestDriver` testleri, sonra gerçek cluster'da benzersiz topic'lerle aynı senaryo çalışır.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Topology'yi: filtre, repartition, join, window.
- **Kafka'nın davranışı nasıl değişti?** Yeni topic'ler ve iç topic'ler oluştu; grace dışındaki kayıt düşürüldü.
- **Production dersi:** Key değiştirmek fatura keser (fazladan yazma ve okuma). `describe()` çıktısı topology'nin en değerli dokümanıdır.

## Kavram soruları

1. Düşük trafikli bir topic'te window sonucu neden hiç gelmeyebilir?
2. Sipariş müşteri kaydından önce gelirse inner join'de ne olur?
3. `map` yerine `mapValues` kullanmak neden repartition'ı önler?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Uygulama `InvalidTimestampException` ile duruyor | Event time 1 saatten ileride; broker reddetti | Saati kontrol et. `log.message.timestamp.after.max.ms` varsayılanı 1 saat |
| `MissingSourceTopicException` | `orders` ya da `customers` yok | `./lab.sh up D3-01` |
| Join çıktısı boş | Müşteri yok | `customerId` `customer-1`…`customer-50` olmalı |
| E düşürülmüyor | Stream time ilerlemedi | Aynı key ile gönderildiğinden emin ol, `emit late-events` kullan |
| `emit` "çok ilerisine geçti" diyor | Kısa sürede çok tekrar edildi | Hatadaki yönergeyi izle |

## Temizlik

Uygulamaları `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D3-01`
