package com.example.orders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Slayt 69'daki tablo burada birebir test edilir. */
@Tag("solution")
class OrderTopologyTest {

    private static final TopologySettings SETTINGS = new TopologySettings(
            "orders", "customers", "orders-high", "orders-enriched", "order-counts-5m", 10_000);
    private static final Serde<OrderEvent> ORDER = JsonSerdes.of(OrderEvent.class);
    private static final Serde<Customer> CUSTOMER = JsonSerdes.of(Customer.class);

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderEvent> orders;
    private TestInputTopic<String, Customer> customers;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        OrderTopology.build(builder, SETTINGS);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(builder.build(), config);
        orders = driver.createInputTopic("orders", new StringSerializer(), ORDER.serializer());
        customers = driver.createInputTopic("customers", new StringSerializer(), CUSTOMER.serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void onlyHighValueOrdersReachOutputTopic() {
        orders.pipeInput("o-1", order("o-1", "c-1", 500, "2026-01-01T10:00:00Z"));
        orders.pipeInput("o-2", order("o-2", "c-1", 15_000, "2026-01-01T10:00:10Z"));

        TestOutputTopic<String, OrderSummary> output = driver.createOutputTopic("orders-high",
                new StringDeserializer(), JsonSerdes.of(OrderSummary.class).deserializer());
        List<KeyValue<String, OrderSummary>> records = output.readKeyValuesToList();

        assertEquals(1, records.size());
        assertEquals("o-2", records.get(0).key);
        assertEquals("HIGH_VALUE", records.get(0).value.label());
    }

    @Test
    void ordersAreEnrichedWithCustomerAfterRepartition() {
        customers.pipeInput("c-1", new Customer("c-1", "Ayşe", "Ankara"), Instant.parse("2026-01-01T00:00:00Z"));
        orders.pipeInput("o-1", order("o-1", "c-1", 100, "2026-01-01T10:00:00Z"));
        orders.pipeInput("o-2", order("o-2", "c-unknown", 100, "2026-01-01T10:00:01Z"));

        TestOutputTopic<String, EnrichedOrder> output = driver.createOutputTopic("orders-enriched",
                new StringDeserializer(), JsonSerdes.of(EnrichedOrder.class).deserializer());
        List<KeyValue<String, EnrichedOrder>> records = output.readKeyValuesToList();

        assertEquals(1, records.size(), "inner join: müşterisi olmayan sipariş düşmeli");
        assertEquals("c-1", records.get(0).key, "key customerId olmalı");
        assertEquals("Ankara", records.get(0).value.city());
    }

    @Test
    void graceAcceptsLateEventAndDropsTooLateEvent() {
        orders.pipeInput("A", order("A", "c-1", 1, "2026-01-01T10:01:00Z"));
        orders.pipeInput("C", order("C", "c-1", 1, "2026-01-01T10:06:00Z"));
        orders.pipeInput("B", order("B", "c-1", 1, "2026-01-01T10:03:00Z"));
        orders.pipeInput("D", order("D", "c-1", 1, "2026-01-01T10:08:00Z"));
        orders.pipeInput("E", order("E", "c-1", 1, "2026-01-01T10:04:00Z"));

        TestOutputTopic<String, Long> output = driver.createOutputTopic("order-counts-5m",
                new StringDeserializer(), new LongDeserializer());
        Map<String, Long> latest = output.readKeyValuesToMap();

        assertEquals(2L, latest.get("c-1@2026-01-01T10:00:00Z"), "A ve B sayılmalı, E düşmeli");
        assertEquals(2L, latest.get("c-1@2026-01-01T10:05:00Z"), "C ve D sayılmalı");
        double dropped = driver.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals("dropped-records-total"))
                .mapToDouble(entry -> ((Number) entry.getValue().metricValue()).doubleValue())
                .sum();
        assertTrue(dropped >= 1.0, "E dropped-records metriğine yansımalı: " + dropped);
    }

    private static OrderEvent order(String orderId, String customerId, double amount, String eventTime) {
        return new OrderEvent("event-" + orderId, orderId, customerId, amount, "TRY", "CREATED", eventTime);
    }
}
