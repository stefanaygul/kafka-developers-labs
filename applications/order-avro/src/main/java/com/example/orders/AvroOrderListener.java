package com.example.orders;

import com.shop.Order;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Uygulama seviyesinde doğrulama: kayıt gerçekten Order nesnesine çözüldü mu? */
@Component
public class AvroOrderListener {

    private static final Logger log = LoggerFactory.getLogger(AvroOrderListener.class);

    @KafkaListener(topics = "${app.topic}", groupId = "${app.group}")
    public void onOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();
        log.info("AVRO_CONSUMED class={} partition={} offset={} orderId={} fields={}",
                order.getClass().getName(), record.partition(), record.offset(), order.getOrderId(),
                order.getSchema().getFields().stream().map(Schema.Field::name).toList());
    }
}
