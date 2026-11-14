# Olay Raporu · Production Provası

**Ekip:** Örnek
**Tarih:** 2026-11-04

## Ayarlar

| Ayar | Değer | Nerede tanımlı? |
|---|---|---|
| acks | all | producer |
| min.insync.replicas | 2 | topic |
| replication.factor | 3 | topic |
| ack-mode | manual_immediate | listener |
| Idempotency yöntemi | processed_events | aynı DB transaction |

## Zaman çizelgesi

| Zaman | Olay | Gözlem | Kanıt (komut ya da log) |
|---|---|---|---|
| 10:00:06 | Broker durduruldu | ISR {1,3}, yazma sürdü | kafka-topics --describe |
| 10:00:12 | Consumer öldürüldü | Instance CRASH yazıp kapandı | order-processor logu |
| 10:00:20 | Rebalance bitti | 2 üye kaldı | kafka-consumer-groups --describe |
| 10:00:45 | Lag sıfırlandı | LAG=0 | kafka-consumer-groups --describe |

## Sayılar

| Ölçüm | Değer |
|---|---|
| Producer ack sayısı (`/orders/stats`) | 1201 |
| Producer hata sayısı | 0 |
| İşlenen benzersiz event | 1201 |
| Tekrar teslimat | 1 |
| Tüketimin durduğu en uzun süre | 8 sn |

## Sonuçlar

**Broker düşünce üretim neden durmadı (ya da durdu)? Hangi ayar sayesinde?** min.insync.replicas=2 ve iki ISR üyesi kaldığı için yazma sürdü.

**Tekrar teslimat oldu mu? Bu hata mı, beklenen davranış mı?** Oldu. At-least-once için beklenen davranış: commit edilmemiş offset yeniden gelir.

**İş sonucu neden değişmedi (ya da değişti)?** processed_events ve iş sonucu aynı transaction'da yazıldı.

## Doğrulamanın sınırları

Sayıların tutması tek başına "sıfır kayıp" kanıtı değildir. Bu tatbikatın **kanıtlamadığı** şey nedir? Kayıtların sayıca tutması, ack almayan ya da hiç üretilmemiş kayıtları kanıtlamaz; tek bir koşudur ve her arıza türünü kapsamaz.
