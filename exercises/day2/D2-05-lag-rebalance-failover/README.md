# D2-05 · Lag, rebalance ve failover

## Nerede çalışıyorsun

```text
applications/order-consumer/src/main/java/com/example/orders/OrderListener.java  → TODO D2-05-1
```

Gerisi komut. Başka hiçbir dosyaya dokunmana gerek yok.

| | |
|---|---|
| Slayt | 58 (kavram: 54–57) |
| Süre | 25 dakika |
| Ön koşul | D1-04 |

## Hedef

Lag'i partition bazında okumak, rebalance'ı ölçmek ve broker düşünce ne olduğunu kendi gözünle görmek.

## Problem

Toplam lag küçük görünüyor ama müşteri şikâyet ediyor. Broker düştü, üretim devam mı ediyor? Rebalance ne kadar sürüyor?

## Tahmin

Yeni protokol (KIP-848) eskisinden kaç kat hızlı? Bir broker düşerse `acks=all` yazma durur mu?

## Adımlar

```bash
./lab.sh up D2-05
```

1. **TODO D2-05-1:** Listener'ı `slowMs` kadar yavaşlat.
2. Yükü başlat ve yavaş consumer'ı aç:

```bash
./lab.sh run order-producer
curl -X POST "localhost:8080/orders/load?perSecond=30&seconds=120"
./lab.sh run order-consumer --app.slow-ms=200 --spring.kafka.consumer.max-poll-records=10
```

`max-poll-records=10` önemli. Varsayılan 500 ile commit ancak poll bitince yapılır: 500 kayıt × 200 ms, ilk commit'i 100 saniye geciktirir ve o süre boyunca `LAG` sütunu `-` görünür.

3. **Lag'i oku.** Komutu birkaç kez çalıştır. Tek sayıya değil dağılıma ve eğime bak:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 \
  --describe --group order-processor
```

`LAG = LOG-END-OFFSET - CURRENT-OFFSET`. `CURRENT-OFFSET` işlenen değil, commit edilen konumdur.

4. **Rebalance.** İkinci instance'ı aç ve `REBALANCE` satırlarındaki `pause=` değerini dünkü notunla karşılaştır:

```bash
./lab.sh run order-consumer --app.slow-ms=200 --spring.kafka.consumer.max-poll-records=10 --app.instance=B
```

5. **Protokolleri ölç.** Spring Boot 3.5'in kafka-clients 3.9'u KIP-848'i desteklemez. Bu yüzden ölçüm Kafka 4.x client ile yapılır:

```bash
./lab.sh measure rebalance
```

6. **CLI ile yeni protokol.** Container içinde iki terminalde aynı komutu çalıştır, sonra grubu incele. `ASSIGNMENT-STRATEGY` değeri `uniform` görünür; atamayı broker yapar:

```bash
./kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic orders --group d2-05-kip848 \
  --command-property group.protocol=consumer
./kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group d2-05-kip848 --state
```

7. **Failover.** Yük sürerken bir broker'ı durdur ve aynı anda izle:

```bash
docker stop kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic orders
curl "localhost:8080/orders/stats"
```

`Replicas` değişmez, `Isr` kısalır, leader `2` olan partition'lar yeni leader alır. `failed` sayacı artmamalı.

8. Vaktin varsa ikinci broker'ı da durdur (`docker stop kafka-3`). ISR 1'e düşer, `min.insync.replicas=2` yazmayı reddeder, `NotEnoughReplicasException` görürsün. Bu bir arıza değil, sözleşmenin çalışmasıdır. Consumer'lar okumaya devam eder. Lab'de üç node aynı zamanda controller olduğu için quorum da gider: mevcut topic'ler okunur, ama topic oluşturmak gibi metadata değişiklikleri ve `kafka-metadata-quorum.sh` yanıt vermez. Ctrl+C ile çık.

9. Geri getir ve ISR'ın dolmasını izle: `docker start kafka-2 kafka-3`

## Beklenen çıktı

```text
Protokol                     Atama bitti (ms)      Duran partition     En uzun kesinti (ms)
eager (classic, Range)                   2594                    6                       10
cooperative (classic)                    3464                    2                     3018
KIP-848 (consumer)                       5042                    2                     2351
```

Süreler her koşuda değişir, bu yüzden bir rakam ezberleme. Bu küçük senaryoda süreyi heartbeat aralığı belirler: classic 3 sn, KIP-848 5 sn. **Protokolün asıl farkı süre değil, kaç partition'ın durduğudur:** eager'da grubun tamamı durur, diğerlerinde yalnızca taşınanlar.

Broker geri geldikten sonra leader'lar hemen eski yerine dönmez; bir süre yük tek broker'da toplanabilir.

## Doğrula

```bash
./lab.sh check D2-05
```

Check `kafka-2`'yi durdurur ve geri açar. Yaklaşık 1 dakika sürer.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Listener'a gecikme ekledin, broker durdurdun.
- **Kafka'nın davranışı nasıl değişti?** Lag büyüdü, partition'lar el değiştirdi, leader ve ISR değişti.
- **Production dersi:** Toplamı değil dağılımı izle. Süreyi tahmin etme, ölç.

## Kavram soruları

1. `CONSUMER-ID` boşken LAG artıyorsa bu ne anlama gelir?
2. İkinci broker da durduğunda consumer'lar okuyabilir mi?
3. `session.timeout.ms` ile `max.poll.interval.ms` farkı nedir? Yavaş listener hangisini tetikler?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Lag oluşmuyor | Yavaşlatma çalışmıyor | TODO D2-05-1 |
| `LAG` sütunu `-` | Henüz commit yok; BATCH modda poll bitmedi | `max-poll-records=10` ile başlat |
| `--describe` boş | Group henüz yok | Consumer açıkken çalıştır |
| Broker geri gelmiyor | Sağlık kontrolü bekleniyor | `./lab.sh status`, gerekirse `./lab.sh up D2-05` |

## Temizlik

```bash
docker start kafka-2 kafka-3
```

Uygulamaları `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D2-05`
