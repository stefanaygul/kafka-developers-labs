# Mimari Değerlendirme Şablonu

**Grup:** ___

Değerlendirme ölçütü diyagram kalitesi değil, gerekçedir. Her karar için bir cümle yeterlidir.

## 1. Akış

Sipariş servisinden ödeme, stok ve raporlamaya uzanan event akışı: ___

## 2. Topic kararları

| Topic | Key | Partition | RF | Retention | cleanup.policy | Gerekçe |
|---|---|---|---|---|---|---|
| ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| ___ | ___ | ___ | ___ | ___ | ___ | ___ |
| ___ | ___ | ___ | ___ | ___ | ___ | ___ |

## 3. Consumer kararları

| Karar | Seçim | Gerekçe |
|---|---|---|
| Consumer group stratejisi | ___ | ___ |
| Commit yöntemi | ___ | ___ |
| Retry ve DLT | ___ | ___ |
| Idempotency | ___ | ___ |

## 4. Schema evrimi

Format, compatibility modu ve deploy sırası: ___

## 5. Arıza senaryosu

Ödeme servisi on dakika düşerse ne olur? Retry mı, DLT mi, replay mi? Sıra garantisi korunur mu? ___

Bir broker düşerse hangi bileşenler bunu fark eder? ___

## 6. Trade-off'lar

Sıra mı paralellik mi? ___

Tazelik mi maliyet mi? ___

Exactly-once mu basitlik mi? ___
