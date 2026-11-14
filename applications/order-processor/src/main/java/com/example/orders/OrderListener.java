package com.example.orders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Lab D2-03: Bu dosyada çalışıyorsun.
 */
@Component
@Profile("idempotency")
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final OrderProcessingService service;
    private final CrashInjector crashInjector;
    private final String instance;

    public OrderListener(OrderProcessingService service, CrashInjector crashInjector,
            @Value("${app.instance}") String instance) {
        this.service = service;
        this.crashInjector = crashInjector;
        this.instance = instance;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "${app.group}")
    public void onOrder(ConsumerRecord<String, OrderEvent> record) {
        ProcessResult result = service.process(record.value());
        logResult(record, result);
        crashInjector.maybeCrash(record.value());
        // TODO D2-03-2: Metoda Acknowledgment parametresi ekle ve kaydı burada, iş tamamen bittikten
        //   sonra onayla. Sıra önemli: önce işle, sonra commit (slayt 30).
    }

    private void logResult(ConsumerRecord<String, OrderEvent> record, ProcessResult result) {
        OrderEvent event = record.value();
        log.info("{} instance={} partition={} offset={} orderId={} eventId={}", result, instance,
                record.partition(), record.offset(),
                event == null ? null : event.orderId(), event == null ? null : event.eventId());
    }
}
