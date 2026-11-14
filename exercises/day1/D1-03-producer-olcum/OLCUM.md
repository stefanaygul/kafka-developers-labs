# D1-03 Ölçüm Tablosu

Her koşuda aynı koşulları koru: 50.000 kayıt, 200 bayt, sıkıştırma yok.
Sonuçlar yerel Docker cluster'ı içindir; production benchmark'ı değildir.

| Koşu | acks | linger.ms | enable.idempotence | Throughput (kayıt/sn) | Ortalama gecikme (ms) | p99 (ms) |
|---|---|---|---|---|---|---|
| A | 1 | 0 | false | ___ | ___ | ___ |
| B | all | 0 | false | ___ | ___ | ___ |
| C | all | 50 | false | ___ | ___ | ___ |
| D | all | 0 | true | ___ | ___ | ___ |

**A ile B arasındaki fark neyi gösteriyor?** ___

**B ile C arasındaki fark neyi gösteriyor?** ___

**Tahminin tuttu mu?** ___
