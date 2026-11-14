# Sorun Giderme

Önce `./lab.sh status`. Çoğu sorun burada görünür.

**Host'tan `localhost:9092`, container içinden `kafka-1:19092`.** En sık yaşanan hata budur.

## Ortam

| Belirti | Sebep | Çözüm |
|---|---|---|
| `Docker çalışmıyor` | Docker Desktop kapalı | Docker Desktop'ı başlat |
| `Port 9092 dolu` | Başka bir Kafka ya da tek node lab'i | Araç süreci adıyla söyler; durdur. `./lab.sh up D2-01` tek node'u kapatır |
| `Port 8081 dolu` | Başka bir uygulama | `SCHEMA_REGISTRY_PORT=18081 ./lab.sh up D3-02`. Aynı değişkeni uygulamayı çalıştırırken de ver |
| PostgreSQL bağlanmıyor | Lab portu 15432'dir, 5432 değil | `POSTGRES_PORT` ile değiştirilebilir |
| Container'lar sürekli yeniden başlıyor | Docker belleği yetersiz | Docker'a en az 6 GB ver |
| Java hatası | Java 17'den eski | `java -version`; `JAVA_HOME` doğru JDK'yı göstermeli |
| `mvnw: Permission denied` | Çalıştırma izni yok | `chmod +x mvnw lab.sh` |
| Windows'ta `^M` hatası | Satır sonu değişmiş | `git config core.autocrlf input` ve repoyu yeniden al |
| Windows'ta Türkçe karakter bozuk | Konsol kod sayfası | `lab.ps1` UTF-8 ayarlar; eski konsolda `chcp 65001` çalıştır |
| `curl` PowerShell'de hata veriyor | `curl` takma adı | `curl.exe` yaz |
| Kurumsal proxy arkasında indirme başarısız | Maven ve Docker proxy ayarı | `~/.m2/settings.xml` ve Docker Desktop proxy ayarlarını yap |

## Kafka

| Belirti | Sebep | Çözüm |
|---|---|---|
| `Connection to node ... could not be established` | Yanlış adres ya da broker kapalı | Adres tablosuna bak, `./lab.sh status` |
| İlk bağlantı başarılı, sonra `TimeoutException` | Yanlış `advertised.listeners` | `./lab.sh probe` hangi broker'ın hangi adresi tanıttığını gösterir |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic yok, otomatik oluşturma kapalı | Topic'i oluştur |
| `NotEnoughReplicasException` | ISR, `min.insync.replicas` altına düştü | Bu beklenen sözleşmedir: `docker start kafka-2 kafka-3` |
| `--command-property` ayarı etkisiz | Bayrak iki kez yazıldı, önceki değer düştü | Tek bayrak, ardışık değerler |
| `--property is deprecated` uyarısı | Kafka 4.3'te ad değişti | `--reader-property` ya da `--formatter-property` |
| Broker cluster'a katılmıyor | Farklı `CLUSTER_ID` | Üç node'da aynı olmalı |
| Broker geri geldi ama leader'lar dönmedi | Normal; preferred leader election arka planda çalışır | Bekle |
| `failed` arttı, logda `Expiring 1 record(s)` | Dizüstü uykuya geçti, kaydın `delivery.timeout.ms` süresi doldu | Uzun ölçümlerde uyku modunu kapat |

## Spring ve uygulama

| Belirti | Sebep | Çözüm |
|---|---|---|
| `DeserializationException` logda, consumer devam ediyor | `orders` içinde JSON olmayan kayıt var | D1-04'te beklenen; ayrıntı D2-04 |
| Consumer aynı offset'te dönüyor | `ErrorHandlingDeserializer` yok | D2-04 TODO 3 |
| `Must set acks to all` | `acks=1` ile açık `enable.idempotence=true` | `enable.idempotence=false` de ver |
| `No Acknowledgment available` | Listener `Acknowledgment` istiyor, `ack-mode` manuel değil | D2-03 TODO 1 |
| Crash sonrası kayıt hemen gelmiyor | Eski üye `session.timeout.ms` dolana kadar grupta | `order-processor`'da 10 sn; varsayılan 45 sn |
| DLT adı beklenenden farklı | Spring Kafka 3.3'te `orders-dlt` | Slayttaki `orders.DLT` eski addır |

## Kafka Streams

| Belirti | Sebep | Çözüm |
|---|---|---|
| `InvalidTimestampException`, uygulama duruyor | Event time 1 saatten ileride | Broker `log.message.timestamp.after.max.ms` varsayılanı 1 saat. Üreticinin saatini kontrol et |
| Window sonucu gelmiyor | Stream time yalnızca yeni kayıtla ilerler | Yeni kayıt gönder |
| Geç event beklenen gibi düşmüyor | Event'ler farklı partition'lara dağıldı; stream time task başınadır | Tek key kullan, `./lab.sh emit late-events` |
| Join çıktısı boş | Müşteri yok ya da sipariş tablodan önce işlendi | `customer-1`…`customer-50` kullan |
| `State store` kilit hatası | Aynı `application-id` ile iki süreç aynı makinede | Birini kapat ya da `application-id` değiştir |

## Schema Registry

| Belirti | Sebep | Çözüm |
|---|---|---|
| `Unknown magic byte!` | Topic'te Avro olmayan kayıt var | Ayrı `orders-avro` topic'i kullan |
| `ClassCastException: GenericData$Record` | `specific.avro.reader` eksik | D3-02 TODO 1 |
| HTTP 409 | Schema uyumsuz | Beklenen olabilir; gövdedeki `errorType` alanını oku |
| 200 ama JSON değil | Portta başka uygulama var | `SCHEMA_REGISTRY_PORT` ile değiştir |

## Sıfırdan başlamak

Veriyi silmeden durdur: `./lab.sh down`

**Tüm** topic'leri, PostgreSQL verisini ve schema'ları siler, geri alınamaz:

```bash
./lab.sh reset --destructive-reset
```
