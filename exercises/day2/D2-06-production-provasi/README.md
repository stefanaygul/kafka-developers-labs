# D2-06 · Production provası

## Nerede çalışıyorsun

```text
exercises/day2/D2-06-production-provasi/RAPOR.md  → bulgularını buraya yaz
```

Kod yok. Önceki lab'lerin kodunu kullanırsın. Eksik kaldıysa: `./lab.sh solution D2-02 --apply` ve `./lab.sh solution D2-03 --apply`

| | |
|---|---|
| Slayt | 59 |
| Süre | 60 dakika |
| Ön koşul | D2-02, D2-03 |

## Hedef

İki günde öğrenilen her ayarı tek akışta sınamak ve ölçtüğünü yorumlamak.

## Problem

Canlı trafik altında bir broker ve bir consumer düşüyor. Kayıp var mı, tekrar var mı, kaç saniye durdu? Bunu ayarlarla nasıl açıklarsın?

## Tahmin

Raporu doldurmadan önce yaz: tekrar teslimat olur mu? Üretim durur mu? Lag kaç saniyede sıfırlanır?

## Adımlar

**Kurulum**

```bash
./lab.sh up D2-06
```

Üç ayrı terminalde üç instance:

```bash
./lab.sh run order-processor --app.instance=A
./lab.sh run order-processor --app.instance=B
./lab.sh run order-processor --app.instance=C
```

Önceki lab'lerden kalan kayıtlar da işlenir. Lag sıfırlanana kadar bekle, sonra sayımı başlat; yoksa "işlenen" sayısı ack sayısından büyük çıkar:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 \
  --describe --group order-processor-db
./lab.sh incident start
```

Dördüncü terminalde üretici:

```bash
./lab.sh run order-producer
curl -X POST "localhost:8080/orders/load?perSecond=50&seconds=300"
```

Beşinci terminalde yukarıdaki `--describe` komutunu tekrar tekrar çalıştır.

**Arıza 1 · Broker.** Zamanı not et ve durdur:

```bash
docker stop kafka-2
```

Üretim devam etti mi? `curl "localhost:8080/orders/stats"` ile `failed` değerine bak. Hangi ayar sayesinde?

**Arıza 2 · Consumer commit edemeden ölüyor.**

```bash
curl -X POST "localhost:8080/orders?orderId=crash-olay-1&customerId=olay-musteri"
```

Instance'lardan biri ölür. Rebalance'ı `REBALANCE` değil, grubun durumuyla izle. Kaç saniye sürdü?

**Toparlanma.** `docker start kafka-2`. Yük bitince lag'in sıfırlanmasını bekle, sonra:

```bash
./lab.sh incident verify
curl "localhost:8080/orders/stats"
```

**Raporla.** `RAPOR.md` dosyasını doldur. Her bulguyu bir ayara bağla: `acks`, `min.insync.replicas`, commit sırası, idempotency.

## Beklenen çıktı

```text
İşlenen benzersiz event : 1201
İş sonucu (order_count) : 1201
Tekrar teslimat         : 1
Her event iş sonucuna tam bir kez yansıdı.
```

Sayıların tutması tek başına "sıfır kayıp" kanıtı **değildir**. Bu yalnızca ack alan kayıtların işlendiğini gösterir; tek bir koşudur ve her arıza türünü kapsamaz. Raporun son bölümü bunun için var.

## Doğrula

```bash
./lab.sh check D2-06
```

Check önce raporu kontrol eder, sonra aynı olayı otomatik tatbik eder. Yaklaşık 1 dakika sürer ve `kafka-2`'yi durdurup geri açar.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Hiçbir kod. Sistemi bozdun ve ölçtün.
- **Kafka'nın davranışı nasıl değişti?** Leader, ISR ve group üyeliği değişti; üretim ve tüketim sürdü.
- **Production dersi:** Mühendislik ölçmekle değil, ölçtüğünü yorumlamakla olur.

## Kavram soruları

1. Tekrar sayısı sıfır çıkarsa bu hata mı, şans mı?
2. Üretim durduysa neden durdu?
3. Aynı senaryoda farklı gruplar farklı sayılar bulduysa sebebi ne olabilir?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `failed` artıyor | İki broker birden kapalı | `docker start kafka-2 kafka-3` |
| Crash olmuyor | Aynı `orderId` daha önce kullanıldı | Yeni `crash-` değeri kullan |
| `incident verify` uyumsuz | Dedup tamamlanmamış | D2-03'e dön |

## Temizlik

```bash
docker start kafka-2
```

Uygulamaları `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D2-06` örnek raporu gösterir.
