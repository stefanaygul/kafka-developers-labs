package com.example.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Gönderilemeyen event'leri uygulama kapansa da kaybolmayacak bir dosyaya yazar.
 * Production'da burası bir outbox tablosu olur.
 */
@Component
public class FailedEventStore {

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Map<String, Object>> failures = new CopyOnWriteArrayList<>();
    private final Path file;

    public FailedEventStore(@Value("${app.failed-events-file}") String file) {
        this.file = Path.of(file);
    }

    public synchronized void record(OrderEvent event, String topic, Throwable cause) {
        Map<String, Object> entry = Map.of(
                "eventId", String.valueOf(event.eventId()),
                "orderId", String.valueOf(event.orderId()),
                "topic", topic,
                "exception", cause.getClass().getName(),
                "message", String.valueOf(cause.getMessage()),
                "failedAt", Instant.now().toString());
        failures.add(entry);
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            Files.writeString(file, mapper.writeValueAsString(entry) + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Başarısız event kaydedilemedi: " + file, e);
        }
    }

    public List<Map<String, Object>> all() {
        return List.copyOf(failures);
    }
}
