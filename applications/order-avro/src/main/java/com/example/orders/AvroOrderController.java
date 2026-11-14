package com.example.orders;

import com.shop.Order;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Bu dosyayı değiştirmen gerekmiyor. */
@RestController
public class AvroOrderController {

    private final AvroOrderPublisher publisher;

    public AvroOrderController(AvroOrderPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/avro/orders")
    public ResponseEntity<Map<String, Object>> create(
            @RequestParam(required = false) String orderId,
            @RequestParam(defaultValue = "customer-1") String customerId,
            @RequestParam(defaultValue = "349.90") double amount) throws Exception {
        String id = orderId == null ? "order-" + UUID.randomUUID() : orderId;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", id);
        try {
            SendResult<String, Order> result = publisher.publish(id, customerId, amount).get(20, TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
            body.put("partition", metadata.partition());
            body.put("offset", metadata.offset());
            body.put("result", "ACKED");
            return ResponseEntity.status(201).body(body);
        } catch (ExecutionException | RuntimeException e) {
            // Schema reddi send() sırasında, eksik alan build() sırasında fırlar: nedeni çağırana göster.
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            body.put("result", "FAILED");
            body.put("error", root.getClass().getSimpleName() + ": " + root.getMessage());
            return ResponseEntity.status(500).body(body);
        }
    }
}
