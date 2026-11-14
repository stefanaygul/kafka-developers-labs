# D1-04 · İlk Spring Boot uygulamanız ve rebalance

## Nerede çalışıyorsun

```text
applications/order-producer/src/main/java/com/example/orders/OrderPublisher.java  → TODO D1-04-1, 2
applications/order-consumer/src/main/resources/application.yml                    → TODO D1-04-3, 4
applications/order-consumer/src/main/java/com/example/orders/OrderListener.java   → TODO D1-04-5
```

Başka hiçbir dosyaya dokunmana gerek yok. Bu dosyalardaki `D2-` ile başlayan TODO'lar yarın içindir.

| | |
|---|---|
| Slayt | 33 (kavram: 28–32) |
| Süre | 90 dakika |
| Ön koşul | D1-01 |

## Hedef

Kendi kodunla Kafka'ya yazmak, okumak ve ilk rebalance'ı gözlemek.

## Problem

`send()` çağırdın ve hata almadın. Mesaj Kafka'ya yazıldı mı? Consumer eklediğinde çalışan consumer'a ne olur?

## Tahmin

İkinci instance açıldığında birinci instance'ın logunda ne görürsün? Kaç partition elinde kalır?

## Adımlar

```bash
./lab.sh up D1-04
```

1. **TODO D1-04-1:** `kafkaTemplate` ile kaydı gönder.
2. **TODO D1-04-2:** `whenComplete` ile sonucu işle. Bu adımı atlama: "send() çağırdım, gitti" sahadaki en pahalı varsayımlardan biridir.
3. **TODO D1-04-3 ve 4:** Consumer'ın `application.yml` dosyasını doldur.
4. **TODO D1-04-5:** `onOrder` metodunu `@KafkaListener` ile bağla. Topic adını sabit yazma, `app.topic` property'sini placeholder olarak kullan.

5. İki terminalde uygulamaları başlat:

```bash
./lab.sh run order-consumer
./lab.sh run order-producer
```

6. Üçüncü terminalden bir sipariş oluştur:

```bash
curl -X POST "localhost:8080/orders?orderId=order-42"
```

7. **Key dağılımı.** Üç key setiyle üret ve yanıttaki `partitions` alanını karşılaştır. Dengeli mi?

```bash
curl -X POST "localhost:8080/orders/bulk?count=300&keySet=order"
curl -X POST "localhost:8080/orders/bulk?count=300&keySet=country"
curl -X POST "localhost:8080/orders/bulk?count=300&keySet=customer"
```

8. **Rebalance.** Dördüncü terminalde ikinci instance'ı aç, loglardaki `REBALANCE` satırlarını izle, sonra kapat:

```bash
./lab.sh run order-consumer --app.instance=B
```

`pause=` değerini not et. Yarın protokolleri karşılaştırırken bu sayıya döneceğiz.

## Beklenen çıktı

Producer:

```text
SEND_RETURNED eventId=beb2... topic=orders sendReturnedMs=325
ACK eventId=beb2... orderId=order-42 partition=0 offset=1 result=SUCCESS
```

Consumer:

```text
REBALANCE instance=A event=ASSIGNED ... partitions=[0, 1, 2, 3, 4, 5] pause=ilk-atama
CONSUMED instance=A groupId=order-processor partition=0 offset=1 key=order-42 eventId=beb2...
```

`country` setinde partition'lar dengesizdir: örneğin 300 kaydın 180'i tek partition'a düşer.

İlk `sendReturnedMs` yüksek çıkar: producer önce metadata alır. Sonrakiler milisaniyedir.

D1-01'de yazdığın kayıtlarda `eventId=null` görünür; onlar elle yazıldı. JSON olmayan bir satır yazdıysan logda `DeserializationException` görürsün, consumer onu atlayıp devam eder. Neden atlayabildiğini D2-04'te göreceğiz.

## Doğrula

Uygulamaların açık kalması sorun değil. Check kendi kopyalarını benzersiz topic ve group ile başlatır:

```bash
./lab.sh check D1-04
```

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Producer'ın gönderimini, sonucu okumasını ve consumer bağlantısını.
- **Kafka'nın davranışı nasıl değişti?** İkinci instance gelince partition'lar el değiştirdi, gidince geri döndü.
- **Production dersi:** `send()` yalnızca kuyruğa koyar; kanıt callback'tir. Deploy sırasında rebalance olur ve bu normaldir.

## Kavram soruları

1. `auto-offset-reset: earliest` her açılışta çalışır mı?
2. 6 partition için 2 instance ve `concurrency=4` verirsen kaç thread boşta kalır?
3. Bootstrap listesine neden üç adresi de yazdık?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Consumer hiçbir şey basmıyor | `@KafkaListener` yok | TODO D1-04-5 |
| `Could not resolve placeholder` | Yanlış property adı | `${app.topic}` yaz |
| Port 8080 dolu | Başka uygulama | `ORDER_PRODUCER_PORT=8085 ./lab.sh run order-producer` |
| `Connection refused` | Cluster kapalı | `./lab.sh status` |

Host'tan `localhost:9092`, container içinden `kafka-1:19092`.

## Temizlik

Uygulamaları `Ctrl+C` ile kapat. `./lab.sh down` veriyi silmez.

## Takıldıysan

`./lab.sh solution D1-04` farkı gösterir. `--apply` önce dosyalarını yedekler, `./lab.sh restore D1-04` geri getirir.
