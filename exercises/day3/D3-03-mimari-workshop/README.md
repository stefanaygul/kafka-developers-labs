# D3-03 · Mimari değerlendirme workshop'u

## Nerede çalışıyorsun

```text
exercises/day3/D3-03-mimari-workshop/SABLON.md  → grup kararlarınızı buraya yazın
```

Kod yok.

| | |
|---|---|
| Slayt | 83 |
| Süre | 40 dakika |
| Ön koşul | Üç günün tamamı |

## Hedef

Üç günün kararlarını taşıyan, savunulabilir bir event mimarisi taslağı çıkarmak.

## Problem

Sipariş servisinden ödeme, stok ve raporlamaya uzanan bir akış tasarlıyorsunuz. Ödeme servisi on dakika düşerse ne olur?

## Tahmin

Başlamadan her grup üyesi tek cümle yazsın: bu sistemde sıra garantisi en çok nerede gerekli?

## Adımlar

Grupları üç dört kişilik tutun.

1. **Akışı çizin (10 dk).** Event akışını tahtaya çizin. Diyagramın güzelliği değerlendirilmez.
2. **Kararları yazın (15 dk).** `SABLON.md` içinde topic sınırlarını, key'leri, partition sayısını, replication factor'ü, retention'ı, consumer group stratejisini, retry ve DLT davranışını, idempotency'yi ve schema evrimini doldurun.
3. **Arıza senaryosu (5 dk).** Ödeme servisi on dakika düşerse retry mı, DLT mi, replay mi? Sıra garantisi korunur mu? Bir broker düşerse ne olur?
4. **Savunun (10 dk).** Her kararı bir cümleyle gerekçelendirin, diğer grupların sorularını yanıtlayın.

## Beklenen çıktı

Doğru cevap yoktur, gerekçe vardır. Örnek bir karar satırı:

```text
sales.order.events | key: orderId | 12 partition | RF 3 | 7 gün | delete
Gerekçe: Aynı siparişin event'leri sıralı kalmalı; 12, 2-3-4-6 instance'a eşit bölünür.
```

## Doğrula

```bash
./lab.sh check D3-03
```

Bu kontrol yalnızca şablonun dolu olduğuna bakar. Asıl değerlendirme eğitmendedir.

## Bu lab'de ne oldu?

- **Ne değiştirdin?** Mimariyi, kâğıt üzerinde.
- **Kafka'nın davranışı nasıl değişti?** Kararlarınız hangi davranışı seçtiğinizi gösterdi.
- **Production dersi:** Sıra mı paralellik mi, tazelik mi maliyet mi, exactly-once mu basitlik mi? Cevap değil, karar verme biçimi önemlidir.

## Kavram soruları

1. Bu topic'in key'i ne ve o key ile sıra kime garantili?
2. Tekrar gelen bir event downstream'de ne yapar?
3. Hangi karar en kolay geri alınır, hangisi en zor?

## Sık karşılaşılan sorunlar

| Belirti | Sebep | Çözüm |
|---|---|---|
| Grup tek cevapta takıldı | Doğru cevabı arıyor | İki seçeneği yazın, bedellerini karşılaştırın |
| Her yere exactly-once yazıldı | Kapsam karışmış | Dış sisteme yazınca EOS yoktur |

## Temizlik

Eğitim bitti. Tüm ortamı kapatmak için `./lab.sh down`. Veriyi de silmek istersen `./lab.sh reset --destructive-reset`

## Takıldıysan

`./lab.sh solution D3-03` boş bir örnek yapı gösterir. Asıl içerik sizin kararlarınızdır.
