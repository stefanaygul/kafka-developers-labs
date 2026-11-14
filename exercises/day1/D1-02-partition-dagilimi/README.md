# D1-02 · Partition dağılımı ve hesap

## Nerede çalışıyorsun

```text
applications/partition-calc/src/main/java/com/example/orders/calc/PartitionMath.java
  → TODO D1-02-1, TODO D1-02-2, TODO D1-02-3
```

Başka hiçbir dosyaya dokunmana gerek yok.

| | |
|---|---|
| Slayt | 20 (kavram: 12, 13, 14) |
| Süre | 20 dakika |
| Ön koşul | Cluster açık (`./lab.sh up D1-02`) |

## Hedef

Key ve partition kararını tahminle değil, sayıyla vermek.

## Problem

Hash fonksiyonu key'leri eşit dağıtıyor. O hâlde neden tek bir partition diğerlerinin toplamından fazla yük taşıyabiliyor?

## Tahmin

`order-id`, `country`, `big-customer`, `keyless`. Hangisi eşit dağılır, hangisinde hot partition çıkar? Önce yaz.

## Adımlar

1. **TODO D1-02-1:** `partitionFor` metodunu tamamla. Kafka'nın varsayılan partitioner'ının formülü slayt 20'de.
2. **TODO D1-02-2:** `histogram` döngüsünde her kaydın partition'ını say.
3. **TODO D1-02-3:** `movedKeyPercent` ile partition sayısı değişince yer değiştiren key oranını hesapla.
4. Programı çalıştır ve dört setin 3, 6, 12 partition sonuçlarını oku:

```bash
./lab.sh run partition-calc
./lab.sh run partition-calc --set country --partitions 6
```

5. Histogramda `<-- HOT` işaretli partition'ı bul. Hangi key yüzünden?
6. `3 -> 6` satırına bak: bu sayı sıra garantisi için ne anlama gelir?
7. **Hesap:** Hedef 30.000 mesaj/sn, tek consumer 4.000 mesaj/sn işliyor. Kaç partition seçersin? Gerekçeni yaz. 7,5'i yukarı yuvarlamak neden yetmez?

## Beklenen çıktı

```text
=== Set: country | partition: 6 | kayıt: 30000 | farklı key: 7 ===
P0  ########################################  19500 (65.0%)  <-- HOT
P4                                                0 ( 0.0%)
```

`country` setinde iki partition hiç kayıt almaz. `order-id` seti neredeyse eşit dağılır. Büyük müşteri setinde tek partition kayıtların yarısından fazlasını taşır. `3 -> 6` geçişinde `order-id` key'lerinin yaklaşık yarısı yer değiştirir.

Key'siz sette dağılım gerçek producer'ın sadeleştirilmiş modelidir.

## Doğrula

```bash
./lab.sh check D1-02
```

Kontrol senin hesabını gerçek broker'ın seçtiği partition ile karşılaştırır.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Partitioner formülünü kendin yazdın.
- **Kafka'nın davranışı nasıl değişti?** Aynı key kümesi 3, 6, 12 partition'da farklı dağıldı; trafik eşit olmadığında hash eşitliği işe yaramadı.
- **Production dersi:** Partition eklemek key'lerin çoğunu taşır. Sıraya bağımlı topic'lerde bu masum bir işlem değildir.

## Kavram soruları

1. Sipariş durumunu (5 değer) key yapan bir ekip 12 partition'ın en fazla kaçını kullanabilir?
2. Büyük müşteri setinde sıra şartı müşteri değil sipariş bazındaysa key'i nasıl değiştirirsin?
3. Key serializer'ı `StringSerializer`'dan `JsonSerializer`'a çevirirsen aynı key aynı partition'a gider mi?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| `partition=0` her key için | TODO D1-02-1 boş | Formülü tamamla |
| Negatif partition ya da `ArrayIndexOutOfBounds` | `toPositive` kullanılmadı | Hash'i önce pozitife çevir |
| Check broker ile uyuşmuyor | Key bayta farklı çevrildi | `StringSerializer` kullan |

## Temizlik

Gerek yok. Check kendi test topic'ini siler.

## Takıldıysan

Önce 10 dakika dene. Sonra `./lab.sh solution D1-02` farkı gösterir, dosyalarına dokunmaz.
