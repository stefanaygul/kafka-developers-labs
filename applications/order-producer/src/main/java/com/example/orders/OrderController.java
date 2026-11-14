package com.example.orders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Lab'ler için küçük REST yüzeyi. Bu dosyayı değiştirmen gerekmiyor. */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private static final List<String> COUNTRIES = List.of(
            "TR", "TR", "TR", "TR", "TR", "TR", "DE", "US", "GB", "NL");

    private final OrderPublisher publisher;
    private final FailedEventStore failedEvents;
    private final String defaultTopic;
    private final AtomicBoolean loadRunning = new AtomicBoolean();

    public OrderController(OrderPublisher publisher, FailedEventStore failedEvents,
            @Value("${app.topic}") String defaultTopic) {
        this.publisher = publisher;
        this.failedEvents = failedEvents;
        this.defaultTopic = defaultTopic;
    }

    /** Hemen döner: yanıt broker onayı anlamına gelmez. */
    @PostMapping
    public ResponseEntity<Map<String, Object>> create(
            @RequestParam(required = false) String orderId,
            @RequestParam(defaultValue = "customer-1") String customerId,
            @RequestParam(defaultValue = "349.90") double amount,
            @RequestParam(required = false) String topic) {
        OrderEvent event = OrderEvent.created(orderIdOrRandom(orderId), customerId, amount);
        publisher.publish(event, topicOrDefault(topic));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventId", event.eventId());
        body.put("orderId", event.orderId());
        body.put("result", "QUEUED");
        return ResponseEntity.accepted().body(body);
    }

    /** Broker onayını bekler: 201 yalnızca ack geldiyse döner. */
    @PostMapping("/confirmed")
    public ResponseEntity<Map<String, Object>> createConfirmed(
            @RequestParam(required = false) String orderId,
            @RequestParam(defaultValue = "customer-1") String customerId,
            @RequestParam(defaultValue = "349.90") double amount,
            @RequestParam(required = false) String topic) {
        OrderEvent event = OrderEvent.created(orderIdOrRandom(orderId), customerId, amount);
        RecordMetadata metadata = publisher.publishAndWait(event, topicOrDefault(topic));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventId", event.eventId());
        body.put("orderId", event.orderId());
        body.put("partition", metadata.partition());
        body.put("offset", metadata.offset());
        body.put("result", "ACKED");
        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }

    /** Key setine göre toplu üretir ve partition dağılımını döndürür. */
    @PostMapping("/bulk")
    public Map<String, Object> bulk(
            @RequestParam(defaultValue = "100") int count,
            @RequestParam(defaultValue = "order") String keySet,
            @RequestParam(required = false) String topic) {
        long start = System.nanoTime();
        List<CompletableFuture<SendResult<String, OrderEvent>>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            OrderEvent event = OrderEvent.created("order-" + UUID.randomUUID(), customerFor(keySet, i), 100 + i);
            futures.add(publisher.publish(event, topicOrDefault(topic), keyFor(keySet, event, i)));
        }
        Map<Integer, Integer> partitions = new TreeMap<>();
        int failed = 0;
        for (CompletableFuture<SendResult<String, OrderEvent>> future : futures) {
            try {
                partitions.merge(future.join().getRecordMetadata().partition(), 1, Integer::sum);
            } catch (RuntimeException e) {
                failed++;
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("keySet", keySet);
        body.put("count", count);
        body.put("failed", failed);
        body.put("elapsedMs", Duration.ofNanos(System.nanoTime() - start).toMillis());
        body.put("partitions", partitions);
        return body;
    }

    /** Arka planda sabit hızda üretir. D2-05 ve D2-06 senaryolari için. */
    @PostMapping("/load")
    public ResponseEntity<Map<String, Object>> load(
            @RequestParam(defaultValue = "20") int perSecond,
            @RequestParam(defaultValue = "120") int seconds,
            @RequestParam(required = false) String topic) {
        if (!loadRunning.compareAndSet(false, true)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("result", "ALREADY_RUNNING"));
        }
        String target = topicOrDefault(topic);
        Thread worker = new Thread(() -> runLoad(perSecond, seconds, target), "order-load");
        worker.setDaemon(true);
        worker.start();
        return ResponseEntity.accepted().body(Map.of("perSecond", perSecond, "seconds", seconds, "topic", target));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> body = new LinkedHashMap<>(publisher.stats());
        body.put("loadRunning", loadRunning.get());
        return body;
    }

    @GetMapping("/failed")
    public List<Map<String, Object>> failed() {
        return failedEvents.all();
    }

    @ExceptionHandler(DeliveryFailedException.class)
    public ResponseEntity<Map<String, Object>> onDeliveryFailed(DeliveryFailedException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("result", "NOT_ACKED");
        body.put("error", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    private void runLoad(int perSecond, int seconds, String topic) {
        long pauseNanos = 1_000_000_000L / Math.max(1, perSecond);
        long end = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        long next = System.nanoTime();
        int sequence = 0;
        try {
            while (System.nanoTime() < end) {
                sequence++;
                publisher.publish(OrderEvent.created("load-" + UUID.randomUUID(), "customer-" + (sequence % 50), 100),
                        topic);
                next += pauseNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            loadRunning.set(false);
            log.info("LOAD bitti: {} kayıt gönderildi", sequence);
        }
    }

    private String topicOrDefault(String topic) {
        return topic == null || topic.isBlank() ? defaultTopic : topic;
    }

    private static String orderIdOrRandom(String orderId) {
        return orderId == null || orderId.isBlank() ? "order-" + UUID.randomUUID() : orderId;
    }

    private static String customerFor(String keySet, int index) {
        if ("customer".equals(keySet)) {
            return index % 10 < 6 ? "customer-0001" : "customer-" + (index % 40 + 2);
        }
        return "customer-" + (index % 40 + 1);
    }

    private static String keyFor(String keySet, OrderEvent event, int index) {
        return switch (keySet) {
            case "country" -> COUNTRIES.get(index % COUNTRIES.size());
            case "customer" -> event.customerId();
            default -> event.orderId();
        };
    }
}
