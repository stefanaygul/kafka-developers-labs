# D2-03 · Manuel commit ve dayanıklı idempotency

## Nerede çalışıyorsun

```text
applications/order-processor/src/main/resources/application-idempotency.yml     → TODO D2-03-1
applications/order-processor/src/main/java/com/example/orders/OrderListener.java → TODO D2-03-2
applications/order-processor/src/main/java/com/example/orders/OrderProcessingService.java → TODO D2-03-3, 4
```

Başka hiçbir dosyaya dokunmana gerek yok.

| | |
|---|---|
| Slayt | 49 (kavram: 45–48) |
| Süre | 20 dakika |
| Ön koşul | D1-04 (producer ile sipariş göndereceksin) |

## Hedef

Crash olsa da işi iki kez yapmayan bir consumer kurmak.

## Problem

Dün tahtada konuştuk: consumer veritabanına yazdı, tam commit edecekken pod yeniden başladı. Bugün aynı senaryoyu kendi kodunda yaşayacaksın.

## Tahmin

Crash'ten sonra mesaj kaybolur mu, ikinci kez mi işlenir? `customer_stats` tablosunda `order_count` kaç olur?

## Adımlar

```bash
./lab.sh up D2-03
./lab.sh run order-producer
```

**Önce problemi gör, TODO'lara dokunmadan.**

1. Başka bir terminalde `./lab.sh run order-processor` başlat.
2. `orderId`'si `crash-` ile başlayan bir sipariş gönder. Kontrollü arıza noktası DB commit'inden sonra, offset onayından önce süreci öldürür:

```bash
curl -X POST "localhost:8080/orders?orderId=crash-1&customerId=musteri-1"
```

3. Uygulama `CRASH` yazıp kapanır. Yeniden başlat ve tabloya bak:

```bash
./lab.sh run order-processor
docker exec orders-postgres psql -U orders -d orders -c "select * from customer_stats where customer_id='musteri-1'"
```

`order_count` **2**. Müşteri iki kez sayıldı. Crash olan consumer bir süre daha grupta kayıtlı kaldığı için tekrar teslimat yaklaşık 10 saniye sürebilir (`session.timeout.ms`).

**Şimdi düzelt.**

4. **TODO D2-03-1:** Commit kararını listener'a ver.
5. **TODO D2-03-2:** Kaydı iş tamamen bittikten sonra onayla. Sıra önemli: önce işle, sonra commit.
6. **TODO D2-03-3:** Tekrar işaretini aynı transaction'da yaz. Bellekte bir `HashSet` restart'ta kaybolur; bu yüzden PostgreSQL.
7. **TODO D2-03-4:** Etkilenen satır 0 ise işi atla.

8. Yeni bir `orderId` ve yeni bir müşteriyle aynı crash'i tekrarla (`crash-2`, `musteri-2`).

## Beklenen çıktı

Crash:

```text
PROCESSED instance=A partition=1 offset=7 orderId=crash-2 eventId=3f1c...
CRASH orderId=crash-2 eventId=3f1c...: DB commit oldu, offset onaylanmadı. Süreç şimdi ölüyor.
```

Yeniden başlatınca aynı kayıt tekrar gelir, ama iş sonucu değişmez:

```text
DUPLICATE instance=A partition=1 offset=7 orderId=crash-2 eventId=3f1c...
```

```bash
docker exec orders-postgres psql -U orders -d orders -c "select * from processed_events where order_id='crash-2'"
```

`redelivery_count=1`, müşterinin `order_count=1`. Tekrar teslimat ortadan kalkmadı, zararsız hale geldi.

## Doğrula

```bash
./lab.sh check D2-03
```

Check gerçekten crash ettirir, yeniden başlatır ve tabloyu okur. Yaklaşık 30 saniye sürer.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Commit sırasını ve tekrar kontrolünü.
- **Kafka'nın davranışı nasıl değişti?** Hiç değişmedi: aynı mesaj yine geldi. Değişen, ikinci gelişin zarar vermemesi.
- **Production dersi:** Commit sırası kaybı önler, idempotency tekrarı zararsızlaştırır. İkisi ayrı problemdir ve exactly-once veritabanını kapsamaz.

## Kavram soruları

1. `processed_events` sonsuza kadar büyür mü? Ne zaman temizleyebilirsin?
2. `eventId` neden consumer'da değil, producer'da üretilmeli?
3. `acknowledge` çağırmayıp sonraki kaydı onaylarsan ne olur?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `No Acknowledgment available` | `ack-mode` manuel değil | TODO D2-03-1 |
| Crash olmuyor | `orderId` daha önce kullanıldı | Yeni `crash-N` kullan; işaret her id için bir kez çalışır |
| `Connection to localhost:15432 refused` | PostgreSQL kapalı | `./lab.sh up D2-03` |
| Eski kayıtlar işleniyor | `earliest` ile yeni group | Beklenen; `eventId=null` kayıtlar `INVALID` sayılır |

## Temizlik

Uygulamaları `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D2-03`
