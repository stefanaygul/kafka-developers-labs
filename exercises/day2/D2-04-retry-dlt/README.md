# D2-04 · Blocking retry, non-blocking retry ve DLT

## Nerede çalışıyorsun

```text
applications/order-processor/src/main/java/com/example/orders/retry/ErrorHandlingConfig.java  → TODO D2-04-1, 2
applications/order-processor/src/main/resources/application-retry.yml                         → TODO D2-04-3
applications/order-processor/src/main/java/com/example/orders/retry/NotificationListener.java → TODO D2-04-4
```

Başka hiçbir dosyaya dokunmana gerek yok. `PaymentListener` kontrollü hata üretir; hazırdır.

| | |
|---|---|
| Slayt | 53 (kavram: 50, 51, 52) |
| Süre | 20 dakika |
| Ön koşul | D1-04 (producer ile sipariş göndereceksin) |

## Hedef

Üç farklı hata türünü ayırt etmek ve her biri için doğru yolu kurmak.

## Problem

Tek bir bozuk mesaj bütün partition'ı kilitleyebilir. Ama "bozuk mesaj" tek bir şey değil: geçici iş hatası, kalıcı iş hatası ve okunamayan bayt farklı davranır.

## Tahmin

Bozuk JSON gönderirsen listener'ın `catch` bloğuna düşer mi? `FixedBackOff(1000L, 3)` toplam kaç deneme yapar?

## Başlat

```bash
./lab.sh up D2-04
./lab.sh run order-producer
./lab.sh run order-processor --spring.profiles.active=retry
```

Bu profilde PostgreSQL gerekmez. `order-processor`'ı her TODO'dan sonra yeniden başlat.

## Senaryo A · Geçici iş hatası, blocking retry

`customerId` hatayı belirler: `fail-transient` iki kez, `fail-permanent` her zaman, `fail-invalid` retry edilmemesi gereken bir hata verir.

1. **TODO D2-04-1:** Recoverer'ı kur. DLT'ye yazma başarısızsa kayıt kurtarılmış sayılmamalı.
2. **TODO D2-04-2:** `DefaultErrorHandler`'ı kur.
3. Geçici hatayı gönder. Üçüncü denemede başarılı olur:

```bash
curl -X POST "localhost:8080/orders?orderId=order-70&customerId=fail-transient"
```

4. Partition'ın beklediğini gör. **Aynı key** ile iki kayıt gönder; ikincisi birincinin retry'larını bekler:

```bash
curl -X POST "localhost:8080/orders?orderId=order-77&customerId=fail-permanent"
curl -X POST "localhost:8080/orders?orderId=order-77&customerId=customer-1"
```

## Senaryo B · Deserialization hatası

Bu bir iş hatası değildir: kayıt listener'a hiç ulaşmaz.

5. Düzeltmeden önce bozuk bayt yaz ve logu izle:

```bash
./lab.sh emit poison
```

Log saniyede on binlerce satır akar: consumer aynı offset'te dönüp durur, arkadaki kayıtlar işlenmez. **Birkaç saniye izle ve `Ctrl+C` ile uygulamayı kapat.** Açık bırakırsan terminal ve CPU kilitlenir. Production'da bu log diski doldurur.

6. **TODO D2-04-3:** Deserializer'ı sarmala ve uygulamayı yeniden başlat. Bozuk kayıt DLT'ye gider, partition akmaya devam eder. Orijinal bayt `dltTemplate` sayesinde korunur.

## Senaryo C · Non-blocking retry

7. **TODO D2-04-4:** Bildirim listener'ına `@RetryableTopic` ekle. Bildirimler bağımsızdır; sıra bozulması burada kabul edilebilir.
8. Aynı key ile hatalı ve sağlam iki bildirim gönder:

```bash
curl -X POST "localhost:8080/orders?orderId=n-1&customerId=fail-notify&topic=order-notifications"
curl -X POST "localhost:8080/orders?orderId=n-1&customerId=customer-1&topic=order-notifications"
```

9. Oluşan topic'leri listele:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list
```

Sağlam kayıt, hatalı kaydın retry'ları bitmeden işlenir. **Orijinal sıra kaybolur.** Sipariş durum event'lerinde bu deseni kullanma: "ödendi" retry'dayken "iptal edildi" işlenirse iptal edilmiş sipariş ödenmiş görünür.

## Beklenen çıktı

```text
ATTEMPT attempt=1 partition=3 offset=9 customerId=fail-permanent at=...
ATTEMPT attempt=4 partition=3 offset=9 customerId=fail-permanent at=...
PAID attempt=1 partition=3 offset=10 at=...
DLT topic=orders-dlt originalTopic=orders originalPartition=3 originalOffset=9 exception=... message=...
```

`FixedBackOff(1000L, 3)`: ilk deneme artı 3 retry, toplam 4 deneme. Arkadaki kayıt yaklaşık 3 saniye bekler.

Bildirim denemeleri arasında yaklaşık 1, 2, 4 saniye olur. Topic'ler: `order-notifications-retry-0`, `-retry-1`, `-retry-2`, `-dlt`.

Spring Kafka 3.3'te DLT adı `orders-dlt`'dir. Slayt 50 ve 51'deki `orders.DLT` eski sürümlere aittir.

Header'daki partition ve offset ikili yazılır; console consumer'da okunmaz görünür. Bu yüzden `DltInspector` onları çözüp loglar. Düzeltip yeniden basmak istersen kaynağı bu satırdan okursun.

## Doğrula

```bash
./lab.sh check D2-04
```

Check üç senaryoyu çalıştırır, ayrıca DLT topic'ini silip kaydın kaybolmadığını, offset'in ilerlemediğini doğrular.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Hata hattını: backoff, recoverer, deserializer sarmalayıcı, retry topic'leri.
- **Kafka'nın davranışı nasıl değişti?** Hatalı kayıt partition'ı sonsuza dek kilitlemiyor, izlenebilir bir yere gidiyor.
- **Production dersi:** Geçici hata retry'a, kalıcı hata doğrudan DLT'ye. Sıra mı, akış mı: bunu event türü belirler.

## Kavram soruları

1. Ödeme servisi beş dakika kapalı kalırsa bu ayarlarla hangi mesajlar DLT'ye düşer?
2. DLT yazılamazsa offset neden ilerlememeli?
3. Hangi event tiplerinde sıra bozulması sorun olmaz?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Log saniyede on binlerce satır akıyor | Deserializer sarmalanmamış | Hemen `Ctrl+C`, sonra TODO D2-04-3 |
| Topic adları `-retry-1000` | Varsayılan gecikme son eki | `topicSuffixingStrategy` index olmalı |
| DLT boş | Recoverer bağlı değil | TODO D2-04-1 |
| Eski bozuk kayıt tekrar geliyor | Group başta kalmış | Beklenen; TODO D2-04-3'ten sonra DLT'ye gider |

## Temizlik

Uygulamaları `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D2-04`
