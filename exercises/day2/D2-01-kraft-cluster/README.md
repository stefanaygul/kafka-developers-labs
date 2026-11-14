# D2-01 · KRaft cluster ve topic yönetimi

## Nerede çalışıyorsun

```text
exercises/day2/D2-01-kraft-cluster/compose-broken.yml  → TODO D2-01-1
```

Başka hiçbir dosyaya dokunmana gerek yok. Üç broker'lı Compose dosyasını sıfırdan yazmıyorsun; hazır dosya [infrastructure/compose-cluster.yml](../../../infrastructure/compose-cluster.yml).

| | |
|---|---|
| Slayt | 41 (kavram: 36–40) |
| Süre | 25 dakika |
| Ön koşul | Docker açık |

## Hedef

KRaft rollerini, listener ayrımını ve ISR'ı işletmeci gözüyle okumak; yanlış `advertised.listeners` hatasını teşhis edip düzeltmek.

## Problem

Container içinden CLI çalışıyor, ama host'taki uygulama yazamıyor. Bootstrap bağlantısı da başarılı. Neden?

## Tahmin

`kafka-2` kendini `kafka-2:19092` olarak tanıtırsa 6 partition'ın kaçına host'tan yazabilirsin?

## Adımlar

**Adım 1 · Tek node ve storage format.** Broker ve controller aynı süreçte:

```bash
./lab.sh up D2-01 --single
docker run --rm -it --entrypoint bash apache/kafka:4.3.2
```

Geçici container içinde cluster id'yi kendin üret ve log dizinini formatla:

```bash
cd /opt/kafka
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format -t <id> -c config/server.properties --standalone
cat /tmp/kraft-combined-logs/meta.properties
```

`exit` ile çık. Imaj bu adımı açılışta kendisi yapar; `CLUSTER_ID` bu yüzden üç node'da aynı olmalı.

**Adım 2 · Üç broker.** Tek node'dan üç node'a geçiş yerinde bir yükseltme değildir; ayrı bir yapılandırma açılır. Önceki veri silinmez.

```bash
./lab.sh up D2-01
```

[compose-cluster.yml](../../../infrastructure/compose-cluster.yml) içinde `KAFKA_LISTENERS` ile `KAFKA_ADVERTISED_LISTENERS` satırlarını karşılaştır. Biri "nerede dinliyorum", diğeri "beni nasıl bulacaksınız" der.

**Adım 3 · Arızayı üret ve teşhis et.**

```bash
./lab.sh up D2-01 --broken
./lab.sh probe
```

Hangi broker hangi adresi tanıtıyor? Hangi partition'lar yazılamıyor? Aynı anda container içinden `kafka-topics.sh --bootstrap-server kafka-2:19092 --list` çalışır.

**Adım 4 · TODO D2-01-1.** `compose-broken.yml` dosyasındaki adresi düzelt, sonra uygula ve tekrar dene:

```bash
./lab.sh up D2-01 --broken
./lab.sh probe
```

**Adım 5 · Topic.** `orders` yoksa D1-01'deki komutla aç. `min.insync.replicas` ayarını topic düzeyinde ver: broker düzeyinde varsayılanı değiştirir, topic düzeyinde sözleşmeyi o topic'e yazar.

**Adım 6 · Dağılımı oku.**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic orders
```

Leader'lar broker'lara dengeli mi dağılmış?

## Beklenen çıktı

Arızalı durumda:

```text
Broker 2 kendini şöyle tanıtıyor: kafka-2:19092
  P1 leader=localhost:9092 -> YAZILDI
  P2 leader=kafka-2:19092 -> YAZILAMADI: TimeoutException
```

Düzeltince:

```text
Tüm partition'lara host'tan yazılabiliyor.
```

Host'taki client hatayı "connection refused" olarak değil, adı çözememe ve zaman aşımı olarak görür.

## Doğrula

```bash
./lab.sh check D2-01
```

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Tek bir `advertised.listeners` satırını.
- **Kafka'nın davranışı nasıl değişti?** Client'ın metadata'dan öğrendiği adres değişti ve o broker'ın partition'larına ulaşabildi.
- **Production dersi:** Bootstrap yalnızca ilk kapıdır. Sonrası broker'ın kendini nasıl tanıttığına bağlıdır.

## Kavram soruları

1. 3 controller'dan 2'si düşerse mevcut topic'lere yazmaya devam edebilir misin?
2. Neden 4 controller kurmak mantıksızdır?
3. `/etc/hosts` dosyasına `kafka-2` eklemek neden iyi bir çözüm değil?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Port 9092 dolu | Tek node hâlâ açık | `./lab.sh up D2-01` tek node'u kapatır |
| `probe` hâlâ yazamıyor | Dosya düzeltildi ama uygulanmadı | `./lab.sh up D2-01 --broken` tekrar çalıştır |
| Broker cluster'a katılmıyor | Farklı `CLUSTER_ID` | Üç node'da aynı değer olmalı |

Host'tan `localhost:9092`, container içinden `kafka-1:19092`.

## Temizlik

Normal cluster'a dön: `./lab.sh up D2-01`

## Takıldıysan

`./lab.sh solution D2-01`
