# D1-01 · İlk mesajınız

## Nerede çalışıyorsun

Kod yok. Bu lab'de yalnızca terminal kullanırsın. Komutları kopyalamak yerine elle yaz; parametre adlarını bir kez yazan kişi production'da da hatırlar.

| | |
|---|---|
| Slayt | 19 (hazırlık: 17, 18) |
| Süre | 45 dakika |
| Ön koşul | [BASLANGIC.md](../../../BASLANGIC.md) tamamlandı |

## Hedef

Bu lab'in sonunda broker, topic, partition, offset ve consumer group kavramlarını gerçek CLI çıktısından açıklayabileceksin.

## Problem

Kafka'ya bir mesaj yazdın. Nereye gitti, kim okuyabilir, ikinci bir consumer açarsan ne olur?

## Tahmin

Başlamadan bir kâğıda yaz:

1. Aynı key ile beş mesaj gönderirsen kaç farklı partition'a düşer?
2. Aynı group ile ikinci consumer açarsan iki consumer da aynı mesajı görür mü?

## Adımlar

**Adım 1 · Ortamı başlat.** Repo kökünde çalıştır, sonra container'a gir:

```bash
./lab.sh up D1-01
docker exec -it kafka-1 bash
cd /opt/kafka/bin
```

Bundan sonraki komutlar container içinde çalışır. Bu yüzden adres `kafka-1:19092`. Host'tan bağlanan uygulamalar `localhost:9092` kullanır.

**Adım 2 · Cluster'ı doğrula.** `docker ps` yalnızca sürecin yaşadığını söyler. Quorum'a ve broker'lara sor:

```bash
./kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status
./kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 | grep "id:"
```

`LeaderId` bir sayı olmalı, ikinci komut üç satır basmalı.

**Adım 3 · Topic oluştur.** `orders`: 6 partition, replication factor 3, `min.insync.replicas=2`.

```bash
./kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic orders \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2
./kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic orders
```

Çıktıda `Leader`, `Replicas` ve `Isr` sütunlarını oku. `Elr` ve `LastKnownElr` sütunları yeni Kafka sürümlerinde gelir, şimdilik boş kalır.

**Adım 4 · Key'li mesaj üret.** Value olarak kısa bir JSON yaz; D1-04'te Spring uygulaması bu kayıtları okuyacak.

```bash
./kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic orders \
  --reader-property parse.key=true --reader-property key.separator=:
```

Satır satır yaz, sonra `Ctrl+C` ile çık. `order-1` ile en az üç satır gönder:

```text
order-1:{"orderId":"order-1","status":"CREATED"}
order-1:{"orderId":"order-1","status":"PAID"}
order-2:{"orderId":"order-2","status":"CREATED"}
```

**Adım 5 · Key'siz mesaj üret.** Aynı komutu `--reader-property` seçenekleri olmadan çalıştır ve bir satır JSON yaz.

**Adım 6 · Tüket.** Key, partition, offset ve timestamp'i birlikte gör:

```bash
./kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic orders --from-beginning \
  --group d1-01-grup --formatter-property print.key=true --formatter-property print.partition=true \
  --formatter-property print.offset=true --formatter-property print.timestamp=true
```

**Adım 7 · İkinci consumer.** Tüketici açıkken ikinci bir terminalde `docker exec -it kafka-1 bash` ile girip **aynı** komutu çalıştır. Üçüncü terminalden grubu izle:

```bash
./kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group d1-01-grup --members
```

Terminallerden birini kapat ve komutu tekrar çalıştır. Partition'ların el değiştirdiğini gör.

## Beklenen çıktı

```text
CreateTime:1793770553687  Partition:4  Offset:0  order-1  {"orderId":"order-1","status":"CREATED"}
CreateTime:1793770553694  Partition:3  Offset:0  order-2  {"orderId":"order-2","status":"CREATED"}
```

Aynı key her zaman aynı partition numarasıyla gelir. Offset yalnızca partition içinde anlamlıdır.

Consumer açılırken KIP-848 hakkında bir bilgi satırı görürsün. Hata değildir, yarın konuşacağız.

## Doğrula

Container'dan `exit` ile çık, repo kökünde çalıştır:

```bash
./lab.sh check D1-01
```

Son satır `SONUÇ: BAŞARILI` olmalı.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Kod değil; topic'i ve kayıtları sen oluşturdun.
- **Kafka'nın davranışı nasıl değişti?** Key partition'ı belirledi, ikinci consumer partition'ları paylaştı.
- **Production dersi:** Sıra yalnızca partition içinde garantidir ve grubun ölçeklenmesi partition sayısıyla sınırlıdır.

## Kavram soruları

1. Üç consumer'ı olan bir grupta 6 partition nasıl paylaşılır, yedinci consumer ne yapar?
2. Key'siz kayıtlarda sıra garantisi var mı? Neden?
3. `--from-beginning` ikinci çalıştırmada neden eski kayıtları getirmedi?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `Connection to node -1 could not be established` | Container içinde `localhost:9094` kullanıldı | Container içinde `kafka-1:19092` kullan |
| `No key separator found` | `parse.key` açıkken `:` içermeyen satır yazıldı | Satırı `key:value` biçiminde yaz |
| `UNKNOWN_TOPIC_OR_PARTITION` uyarısı | Topic yok; otomatik oluşturma kapalı | Önce Adım 3'ü yap |
| Container'lar açılmıyor | Docker'a yetersiz bellek | [TROUBLESHOOTING](../../../docs/TROUBLESHOOTING.md) |

## Temizlik

Gerek yok. `orders` topic'i sonraki lab'lerde kullanılır. Günü bitirirken: `./lab.sh down` (veri silinmez).

## Takıldıysan

`./lab.sh solution D1-01` komutları gösterir. `--apply` eklersen onları senin yerine çalıştırır.
