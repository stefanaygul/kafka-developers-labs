# D1-03 Ölçüm Tablosu

Her koşuda aynı koşulları koru: 50.000 kayıt, 200 bayt, sıkıştırma yok.
Sonuçlar yerel Docker cluster'ı içindir; production benchmark'ı değildir.

| Koşu | acks | linger.ms | enable.idempotence | Throughput (kayıt/sn) | Ortalama gecikme (ms) | p99 (ms) |
|---|---|---|---|---|---|---|
| A | 1 | 0 | false | (örnek) 91000 | 120 | 210 |
| B | all | 0 | false | 60000 | 190 | 320 |
| C | all | 50 | false | 150000 | 95 | 180 |
| D | all | 0 | true | 58000 | 195 | 330 |

**A ile B arasındaki fark neyi gösteriyor?** acks=all, ISR onayını beklediği için gecikmeyi artırır.

**B ile C arasındaki fark neyi gösteriyor?** linger.ms batch'leri büyütür, aynı veri daha az istekle gider.

**Tahminin tuttu mu?** Örnek çözüm: sayılar her makinede farklıdır, kendi ölçümünü yaz.
