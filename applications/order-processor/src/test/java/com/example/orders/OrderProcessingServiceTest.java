package com.example.orders;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/** Tekrar gelen event'in iş sonucunu değiştirmediğini gerçek PostgreSQL ile doğrular. */
@Tag("solution")
@Testcontainers
@ActiveProfiles("idempotency")
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class OrderProcessingServiceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../../infrastructure/init/postgres-schema.sql")),
                    "/docker-entrypoint-initdb.d/01-schema.sql");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private OrderProcessingService service;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void redeliveryDoesNotChangeBusinessResult() {
        OrderEvent event = OrderEvent.created("order-1", "customer-test", 100);

        assertEquals(ProcessResult.PROCESSED, service.process(event));
        assertEquals(ProcessResult.DUPLICATE, service.process(event));

        assertEquals(1L, jdbc.queryForObject(
                "SELECT order_count FROM customer_stats WHERE customer_id = 'customer-test'", Long.class));
        assertEquals(1, jdbc.queryForObject(
                "SELECT redelivery_count FROM processed_events WHERE event_id = ?", Integer.class, event.eventId()));
    }
}
