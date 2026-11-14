# D2-02 · Producer teslim sonucu

## Nerede çalışıyorsun

```text
applications/order-producer/src/main/java/com/example/orders/OrderPublisher.java
  → TODO D2-02-1, 2, 3, 4
```

Başka hiçbir dosyaya dokunmana gerek yok. D1-04'te yazdığın kod üzerinden devam ediyorsun. Bitirmediysen: `./lab.sh solution D1-04 --apply`

| | |
|---|---|
| Slayt | 44 (kavram: 42, 43) |
| Süre | 15 dakika |
| Ön koşul | D1-04 |

## Hedef

Teslim sonucunu okuyan, hatayı yutmayan ve çağırana ileten bir producer yazmak.

## Problem

REST endpoint'in `202` döndü. Müşteri başarılı cevap aldı. Event Kafka'ya yazıldı mı?

## Tahmin

İki hata türü var: broker kaydı reddediyor ve topic hiç yok. Hangisi `send()` çağrısından fırlar, hangisi callback'e gelir?

## Adımlar

```bash
./lab.sh up D2-02
```

1. **TODO D2-02-1:** `send()` bazı hataları hemen fırlatır. Gönderimi koru ki iki hata yolu tek noktadan geçsin.
2. **TODO D2-02-2:** ACK loguna `timestamp` ve `ackMs` ekle.
3. **TODO D2-02-3:** Hata dalını doldur. Yalnızca loglamak yetmez; event kalıcı olarak kaydedilmeli.
4. **TODO D2-02-4:** `publishAndWait` ile broker onayını bekle, onay yoksa çağırana hata ilet.

5. Uygulamayı başlat, sonra üç durumu dene:

```bash
./lab.sh run order-producer
curl -X POST "localhost:8080/orders/confirmed?orderId=order-ok"
curl -X POST "localhost:8080/orders?orderId=order-big&topic=orders-tiny"
curl -X POST "localhost:8080/orders?orderId=order-yok&topic=olmayan-topic"
```

`orders-tiny` topic'i 64 bayttan büyük kaydı reddeder. Otomatik topic oluşturma kapalı olduğu için `olmayan-topic` hiç oluşmaz.

6. Sonuçlara bak:

```bash
curl "localhost:8080/orders/stats"
curl "localhost:8080/orders/failed"
```

7. **`acks` karşılaştırması.** Aynı yükü iki ayarla gönder, `elapsedMs` değerlerini kıyasla:

```bash
curl -X POST "localhost:8080/orders/bulk?count=5000"
./lab.sh run order-producer --spring.kafka.producer.acks=1 --spring.kafka.producer.properties.enable.idempotence=false
```

`enable.idempotence: true` açıkça yazılı olduğu için yalnızca `acks=1` vermek yetmez. Uygulama yine açılır, çünkü Spring producer'ı ilk gönderimde oluşturur; ilk istek **500** döner ve logda `ConfigException` görürsün. Bu yüzden iki ayarı birlikte veriyoruz.

## Beklenen çıktı

Broker reddi: `send()` hızlı döner, hata sonradan gelir.

```text
SEND_RETURNED eventId=8ea4... topic=orders-tiny sendReturnedMs=4
NACK eventId=8ea4... exception=...RecordTooLargeException ... ackMs=23 result=FAILED
```

Olmayan topic: `send()` `max.block.ms` kadar bloklanır, hata senkron gelir.

```text
SEND_RETURNED eventId=9058... topic=olmayan-topic sendReturnedMs=5014
NACK eventId=9058... exception=...UnknownTopicOrPartitionException ... result=FAILED
```

İkisinde de `POST /orders` **202** döner. `POST /orders/confirmed` ise onay yoksa **503** döner.

## Doğrula

```bash
./lab.sh check D2-02
```

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Hata yollarını ve bekleme davranışını.
- **Kafka'nın davranışı nasıl değişti?** Kafka'nın değil, uygulamanın davranışı değişti: artık broker'ın "hayır" cevabını duyuyor.
- **Production dersi:** `send()` onay değildir. Hot path'te `.get()` kullanma, ama iş gerektiriyorsa sonucu bekle ve çağırana doğruyu söyle.

## Kavram soruları

1. Callback içinde hata alınca aynı mesajı tekrar `send()` etmek iyi fikir mi?
2. `delivery.timeout.ms` ile `max.block.ms` neyi ayrı ayrı sınırlar?
3. Ölçtüğün `acks` farkı, broker kaybında veri kaybetme riskinden daha mı pahalı?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `POST /orders` 500 dönüyor | `send()` senkron fırlattı | TODO D2-02-1 |
| İlk istek 500, logda `Must set acks to all` | `acks=1` ve açık idempotence | `enable.idempotence=false` de ver |
| Topic hatası gelmiyor | Topic gerçekten var | Başka bir ad dene |
| `try-catch` var ama hâlâ 500 | Yanlış sınıf: Apache'nin `KafkaException`'ı | `org.springframework.kafka.KafkaException` yakala |

## Temizlik

Uygulamayı `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D2-02`
