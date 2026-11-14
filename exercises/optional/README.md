# Opsiyonel · Kafka Connect ve ksqlDB

Slayt 81, 82 ve 93–96. Bu demolar zorunlu değildir, Kafka Streams lab'lerinin yerini almaz. Yalnızca zaman kalırsa yapılır. Yaklaşık 1 GB ek bellek ister.

```bash
./lab.sh up optional
```

## ksqlDB

**Stream ve materialized view.** ksqlDB CLI'a dosyayı içeri aktar:

```bash
docker exec -i ksqldb-server ksql http://localhost:8088 < exercises/optional/pipeline.sql
```

Birkaç sipariş üret (`./lab.sh run order-producer`, sonra `curl -X POST "localhost:8080/orders?customerId=customer-7&amount=120"`).

**Pull query:** tek cevap döner, query biter.

```bash
docker exec -it ksqldb-server ksql http://localhost:8088
SELECT * FROM order_totals WHERE customerId='customer-7';
```

**Push query:** query açık kalır, sonuç akar. Çıkmak için `Ctrl+C`.

```sql
SELECT order_id, amount FROM orders_stream EMIT CHANGES;
```

Ekran boş kalırsa `SET 'auto.offset.reset'='earliest';` çalıştır: push query varsayılan olarak topic'in sonundan başlar.

Doğrulanmış çıktı örneği: `["ksql-musteri",2,200.0]`

## Kafka Connect

Worker bir JVM sürecidir, connector bir plandır, veriyi task taşır. Burada Apache Kafka ile gelen dosya connector'ı kullanılır.

```bash
docker exec connect sh -c 'printf "satir-1\nsatir-2\n" > /tmp/lab-input.txt'
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --if-not-exists \
  --topic connect-demo --partitions 1 --replication-factor 3
curl -X PUT -H "Content-Type: application/json" localhost:8083/connectors/lab-file-source/config \
  -d '{"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","tasks.max":"1","file":"/tmp/lab-input.txt","topic":"connect-demo"}'
```

Connector `RUNNING` görünüp task `FAILED` olabilir; her zaman status'ü oku:

```bash
curl localhost:8083/connectors/lab-file-source/status
```

Kayıtları console consumer ile oku. Dosyaya yeni satır ekle ve kaydın geldiğini gör. Kaynak pozisyonunu Connect saklar.

## Kapatmak

`./lab.sh down` ana cluster ile birlikte bu servisleri de durdurur. Veri silinmez.
