package com.example.orders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Kontrollü arıza noktasi: DB commit olduktan sonra, Kafka offset onayından önce süreci öldürür.
 * orderId "crash-" ile başlıyorsa çalışır. Her orderId için yalnızca bir kez tetiklenir;
 * yoksa aynı kayıt tekrar geldiğinde uygulama sonsuza kadar çöker.
 */
@Component
@Profile("idempotency")
public class CrashInjector {

    private static final Logger log = LoggerFactory.getLogger(CrashInjector.class);

    private final Path markerDir;

    public CrashInjector(@Value("${app.crash-marker-dir}") String markerDir) {
        this.markerDir = Path.of(markerDir);
    }

    public void maybeCrash(OrderEvent event) {
        if (event == null || event.orderId() == null || !event.orderId().startsWith("crash-")) {
            return;
        }
        Path marker = markerDir.resolve(event.orderId().replaceAll("[^A-Za-z0-9_-]", "_"));
        try {
            Files.createDirectories(markerDir);
            if (Files.exists(marker)) {
                return;
            }
            Files.createFile(marker);
        } catch (IOException e) {
            throw new IllegalStateException("Crash işareti yazılamadı: " + marker, e);
        }
        log.error("CRASH orderId={} eventId={}: DB commit oldu, offset onaylanmadı. Süreç şimdi ölüyor.",
                event.orderId(), event.eventId());
        Runtime.getRuntime().halt(137);
    }
}
