# Schema evrimi örnekleri

Her dosya, `currency` alanı eklenmiş v2 schema'sında yapılan tek bir değişikliktir. Sonuçlar gerçek Schema Registry 8.3.2 üzerinde alındı.

Kural: Okuyucu aradığı alanı veride bulamazsa default'u kullanır, default yoksa hata verir. Veride fazladan alan varsa atlar.

| Dosya | Değişiklik | BACKWARD | FORWARD | FULL |
|---|---|---|---|---|
| `1-default-li-alan-ekle.json` | Default'lu alan ekle | Uyumlu | Uyumlu | Uyumlu |
| `2-zorunlu-alan-ekle.json` | Default'suz alan ekle | Red | Uyumlu | Red |
| `3-default-li-alan-sil.json` | Default'lu alanı sil | Uyumlu | Uyumlu | Uyumlu |
| `4-zorunlu-alan-sil.json` | Default'suz alanı sil | Uyumlu | Red | Red |
| `5-yeniden-adlandir.json` | Alan adını değiştir | Red | Red | Red |
| `6-alias-ile-adlandir.json` | Alias ile adını değiştir | Uyumlu | Red | Red |
| `7-tip-degistir.json` | `amount` tipini değiştir | Red | Red | Red |
| `8-nullable-yap.json` | Alanın null almasına izin ver | Uyumlu | Red | Red |

## Çalıştırma

Önce D3-02 Adım 3'ü bitir; `orders-avro-value` subject'inde `currency` alanı olmalı. Test endpoint'i versiyon oluşturmaz. Port farklıysa `8081` yerine onu yaz.

Modu seç:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"FORWARD"}' "localhost:8081/config/orders-avro-value"
```

Dosyayı test et:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @exercises/day3/D3-02-avro-schema-registry/ornekler/2-zorunlu-alan-ekle.json \
  "localhost:8081/compatibility/subjects/orders-avro-value/versions/latest?verbose=true"
```

Bitince modu `BACKWARD` yap.
