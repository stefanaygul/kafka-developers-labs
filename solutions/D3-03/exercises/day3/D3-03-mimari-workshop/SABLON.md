# Mimari Değerlendirme Şablonu

**Grup:** Örnek karar 1

Değerlendirme ölçütü diyagram kalitesi değil, gerekçedir. Her karar için bir cümle yeterlidir.

## 1. Akış

Sipariş servisinden ödeme, stok ve raporlamaya uzanan event akışı: Örnek karar 2

## 2. Topic kararları

| Topic | Key | Partition | RF | Retention | cleanup.policy | Gerekçe |
|---|---|---|---|---|---|---|
| Örnek karar 3 | Örnek karar 4 | Örnek karar 5 | Örnek karar 6 | Örnek karar 7 | Örnek karar 8 | Örnek karar 9 |
| Örnek karar 10 | Örnek karar 11 | Örnek karar 12 | Örnek karar 13 | Örnek karar 14 | Örnek karar 15 | Örnek karar 16 |
| Örnek karar 17 | Örnek karar 18 | Örnek karar 19 | Örnek karar 20 | Örnek karar 21 | Örnek karar 22 | Örnek karar 23 |

## 3. Consumer kararları

| Karar | Seçim | Gerekçe |
|---|---|---|
| Consumer group stratejisi | Örnek karar 24 | Örnek karar 25 |
| Commit yöntemi | Örnek karar 26 | Örnek karar 27 |
| Retry ve DLT | Örnek karar 28 | Örnek karar 29 |
| Idempotency | Örnek karar 30 | Örnek karar 31 |

## 4. Schema evrimi

Format, compatibility modu ve deploy sırası: Örnek karar 32

## 5. Arıza senaryosu

Ödeme servisi on dakika düşerse ne olur? Retry mı, DLT mi, replay mi? Sıra garantisi korunur mu? Örnek karar 33

Bir broker düşerse hangi bileşenler bunu fark eder? Örnek karar 34

## 6. Trade-off'lar

Sıra mı paralellik mi? Örnek karar 35

Tazelik mi maliyet mi? Örnek karar 36

Exactly-once mu basitlik mi? Örnek karar 37
