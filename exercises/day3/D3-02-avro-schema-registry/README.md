# D3-02 · Avro ve Schema Registry

## Nerede çalışıyorsun

```text
applications/order-avro/src/main/resources/application.yml                      → TODO D3-02-1
applications/order-avro/src/main/java/com/example/orders/AvroOrderPublisher.java → TODO D3-02-2
applications/order-avro/src/main/avro/order.avsc                                 → TODO D3-02-3
```

Başka hiçbir dosyaya dokunmana gerek yok. Kırıcı schema hazır: [order-v3-breaking.json](order-v3-breaking.json)

| | |
|---|---|
| Slayt | 80 (kavram: 75–79) |
| Süre | 55 dakika |
| Ön koşul | Yok |

## Hedef

Producer ile consumer arasındaki sözleşmeyi Schema Registry'de yönetmek ve kırıcı değişikliği üretime çıkmadan yakalamak.

## Problem

Bir ekip alan ekledi, consumer'lar patladı. Bunu deploy'dan önce nasıl yakalarsın?

## Tahmin

Default'lu ve default'suz alan eklemenin farkı ne? Mesajın içinde schema'nın kaçı taşınır?

## Başlat

```bash
./lab.sh up D3-02
```

JSON ve Avro kayıtları karışmasın diye ayrı `orders-avro` topic'i kullanılır. Bu yüzden subject adı `orders-avro-value`'dur. Slayt 80'deki `orders-value` eski addır.

8081 portu doluysa: `SCHEMA_REGISTRY_PORT=18081 ./lab.sh up D3-02`. Aşağıdaki adreslerde de aynı portu kullan.

## Adımlar

**Adım 1 · Avro ile yaz.** **TODO D3-02-1** ve **TODO D3-02-2**'yi tamamla. `Order` sınıfı `order.avsc` dosyasından build sırasında üretilir.

```bash
./lab.sh run order-avro
curl -X POST "localhost:8084/avro/orders?orderId=order-avro-1"
```

Logda `AVRO_CONSUMED class=com.shop.Order` görmelisin. `specific.avro.reader` yoksa `GenericRecord` gelir.

**Adım 2 · REST ile incele.**

```bash
curl "localhost:8081/subjects"
curl "localhost:8081/subjects/orders-avro-value/versions"
curl "localhost:8081/subjects/orders-avro-value/versions/latest"
curl "localhost:8081/config/orders-avro-value?defaultToGlobal=true"
```

Yanıttaki `id` değerini not et, sonra mesajın ilk baytlarına bak:

```bash
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 \
  --topic orders-avro --from-beginning --max-messages 1 --timeout-ms 8000 2>/dev/null | od -A d -t x1 | head -2"
```

İlk bayt magic byte (`00`), sonraki dört bayt schema id. Schema'nın kendisi mesajda taşınmaz.

**Adım 3 · Uyumlu değişiklik.** **TODO D3-02-3:** Default'lu `currency` alanı ekle. Uygulamayı yeniden başlat ve bir sipariş daha gönder. Versiyon listesi `[1,2]` olur.

**Adım 4 · Kırıcı değişiklik.** Önce modun doğru subject'te olduğundan emin ol:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' "localhost:8081/config/orders-avro-value"
```

Kaydetmeden önce test et:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @exercises/day3/D3-02-avro-schema-registry/order-v3-breaking.json \
  "localhost:8081/compatibility/subjects/orders-avro-value/versions/latest?verbose=true"
```

Şimdi kaydetmeyi dene:

```bash
curl -i -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @exercises/day3/D3-02-avro-schema-registry/order-v3-breaking.json \
  "localhost:8081/subjects/orders-avro-value/versions"
```

409 gövdesini oku: hangi alan, hangi kural?

**Adım 5 · Uygulama seviyesinde.** Aynı reddi uygulamadan gör. `order.avsc` dosyasına geçici olarak default'suz `warehouseId` alanı ekle ve uygulamayı başlatıp sipariş gönder. Hata henüz Registry'den gelmez: `AvroMissingFieldException`, yani `build()` alanı doldurmanı ister. Şimdi `AvroOrderPublisher` içinde alanı `setWarehouseId` ile doldur ve yeniden dene. Bu kez hata `send()` anında, Registry'den gelir: yanıtta `RestClientException` ve `READER_FIELD_MISSING_DEFAULT_VALUE` görürsün. Bitince iki dosyayı da geri al.

**Adım 6 · Deploy sırası.** BACKWARD altında önce kim güncellenir? Neden?

## Beklenen çıktı

```text
0000000 00 00 00 00 04 18 6f 72 64 65 72 2d 61 76 72 6f
```

```text
HTTP/1.1 409 Conflict
{"error_code":409,"message":"Schema being registered is incompatible with an earlier schema
 for subject \"orders-avro-value\", details: [{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', ...
```

## Doğrula

```bash
./lab.sh check D3-02
```

Check benzersiz bir subject kullanır ve bitince siler.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Serializer'ı ve schema'yı.
- **Kafka'nın davranışı nasıl değişti?** Broker hiç değişmedi; broker baytın içine bakmaz. Denetim kayıt anında, Registry'de çalıştı.
- **Production dersi:** Production'da `auto.register.schemas=false`; schema CI'da kaydedilir ve uyumluluk orada test edilir.

## Kavram soruları

1. Schema Registry beş dakika kapanırsa çalışan producer ve consumer'lar durur mu?
2. BACKWARD modda bir alanı yeniden adlandırmak isterseniz hangi adımları izlersiniz?
3. Aynı topic'e iki farklı event tipi yazmak için hangi subject stratejisi gerekir?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `Unknown magic byte!` | Topic'e Avro olmayan kayıt yazıldı | Topic'i karıştırma; `orders-avro` ayrı |
| `ClassCastException: GenericData$Record` | `specific.avro.reader` eksik | TODO D3-02-1 |
| `Connection refused` 8081 | Schema Registry kapalı | `./lab.sh up D3-02` |
| 200 ama JSON değil | 8081'de başka uygulama var | `SCHEMA_REGISTRY_PORT` ile değiştir |
| `order.avsc` derlenmiyor | Virgül eksik | Önceki alanın sonuna virgül koy |

## Temizlik

Uygulamayı `Ctrl+C` ile kapat.

## Takıldıysan

`./lab.sh solution D3-02`
