package lab;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Kafka yardımcıları. Tüm kontroller gerçek cluster'a gider. */
final class Kafka implements AutoCloseable {

    private final Admin admin;
    private final KafkaProducer<byte[], byte[]> producer;

    Kafka() {
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Env.BOOTSTRAP);
        adminConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);
        adminConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15000);
        admin = Admin.create(adminConfig);
        producer = new KafkaProducer<>(producerConfig(), new ByteArraySerializer(), new ByteArraySerializer());
    }

    static Properties producerConfig() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.BOOTSTRAP);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        return config;
    }

    Admin admin() {
        return admin;
    }

    Collection<Node> brokers() {
        return get(admin.describeCluster().nodes());
    }

    void createTopic(String name, int partitions, short replicas, Map<String, String> configs) {
        try {
            admin.createTopics(List.of(new NewTopic(name, partitions, replicas).configs(configs))).all()
                    .get(20, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new CheckFailed("Topic oluşturulamadı: " + name + ": " + e.getCause().getMessage());
            }
        } catch (Exception e) {
            throw new CheckFailed("Topic oluşturulamadı: " + name + ": " + e.getMessage());
        }
    }

    void createOrdersLikeTopic(String name) {
        createTopic(name, 6, (short) 3, Map.of("min.insync.replicas", "2"));
    }

    /** Check sonunda kendi topic'lerini temizler. Hata ana sonucu etkilemez. */
    void deleteTopics(Collection<String> names) {
        try {
            Set<String> existing = get(admin.listTopics().names());
            List<String> toDelete = names.stream().filter(existing::contains).toList();
            if (!toDelete.isEmpty()) {
                admin.deleteTopics(toDelete).all().get(20, TimeUnit.SECONDS);
            }
        } catch (Exception ignored) {
            // Temizlik başarısız olsa da check sonucu değişmez.
        }
    }

    void deleteTopicsWithPrefix(String prefix) {
        deleteTopics(topicNames().stream().filter(name -> name.startsWith(prefix)).toList());
    }

    Set<String> topicNames() {
        return get(admin.listTopics().names());
    }

    boolean topicExists(String name) {
        return topicNames().contains(name);
    }

    TopicDescription describeTopic(String name) {
        try {
            return admin.describeTopics(List.of(name)).allTopicNames().get(10, TimeUnit.SECONDS).get(name);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return null;
            }
            throw new CheckFailed("Topic okunamadı: " + name + ": " + e.getCause().getMessage());
        } catch (Exception e) {
            throw new CheckFailed("Topic okunamadı: " + name + ": " + e.getMessage());
        }
    }

    String topicConfig(String topic, String key) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = get(admin.describeConfigs(List.of(resource)).all()).get(resource);
        return config.get(key) == null ? null : config.get(key).value();
    }

    RecordMetadata send(String topic, String key, byte[] value) {
        return send(new ProducerRecord<>(topic, key == null ? null : bytes(key), value));
    }

    RecordMetadata sendJson(String topic, String key, String json) {
        return send(topic, key, bytes(json));
    }

    RecordMetadata send(ProducerRecord<byte[], byte[]> record) {
        try {
            return producer.send(record).get(35, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new CheckFailed("Kayıt yazılamadı (" + record.topic() + "): " + e.getMessage());
        }
    }

    /** Topic'i baştan sona okur. Consumer group kullanmaz, kimsenin offset'ine dokunmaz. */
    List<ConsumerRecord<byte[], byte[]>> readAll(String topic, int limit, Duration timeout) {
        TopicDescription description = describeTopic(topic);
        if (description == null) {
            return List.of();
        }
        List<TopicPartition> partitions = description.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition())).toList();
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.BOOTSTRAP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline && records.size() < limit) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(300));
                batch.forEach(records::add);
                boolean finished = partitions.stream().allMatch(tp -> consumer.position(tp) >= end.get(tp));
                if (finished) {
                    break;
                }
            }
        }
        return records;
    }

    /** Partition başına log-end offset. */
    Map<Integer, Long> endOffsets(String topic) {
        TopicDescription description = describeTopic(topic);
        Map<TopicPartition, OffsetSpec> request = description.partitions().stream()
                .collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result = get(admin.listOffsets(request).all());
        Map<Integer, Long> offsets = new HashMap<>();
        result.forEach((tp, info) -> offsets.put(tp.partition(), info.offset()));
        return offsets;
    }

    /** Partition başına commit edilmiş offset. Commit yoksa partition listede yoktur. */
    Map<Integer, Long> committed(String group, String topic) {
        Map<TopicPartition, OffsetAndMetadata> offsets = get(admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata());
        Map<Integer, Long> result = new HashMap<>();
        offsets.forEach((tp, metadata) -> {
            if (metadata != null && tp.topic().equals(topic)) {
                result.put(tp.partition(), metadata.offset());
            }
        });
        return result;
    }

    long totalLag(String group, String topic) {
        Map<Integer, Long> end = endOffsets(topic);
        Map<Integer, Long> committed = committed(group, topic);
        return end.entrySet().stream()
                .mapToLong(entry -> entry.getValue() - committed.getOrDefault(entry.getKey(), 0L))
                .sum();
    }

    void deleteGroups(Collection<String> groups) {
        try {
            admin.deleteConsumerGroups(groups).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Temizlik.
        }
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static String text(byte[] data) {
        return data == null ? null : new String(data, StandardCharsets.UTF_8);
    }

    /** OrderEvent ile aynı JSON. */
    static String orderJson(String eventId, String orderId, String customerId, double amount, String eventTime) {
        return String.format(java.util.Locale.ROOT,
                "{\"eventId\":\"%s\",\"orderId\":\"%s\",\"customerId\":\"%s\",\"amount\":%.2f,"
                        + "\"currency\":\"TRY\",\"status\":\"CREATED\",\"eventTime\":\"%s\"}",
                eventId, orderId, customerId, amount, eventTime);
    }

    static String orderJson(String orderId, String customerId, double amount) {
        return orderJson(UUID.randomUUID().toString(), orderId, customerId, amount, java.time.Instant.now().toString());
    }

    private static <T> T get(org.apache.kafka.common.KafkaFuture<T> future) {
        try {
            return future.get(15, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new CheckFailed("Kafka isteği başarısız: " + e.getCause().getMessage(),
                    "Cluster ayakta mi? ./lab.sh status");
        } catch (Exception e) {
            throw new CheckFailed("Kafka'ya ulaşılamadı: " + e.getMessage(), "Cluster ayakta mi? ./lab.sh status");
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(3));
        admin.close(Duration.ofSeconds(3));
    }

    static Map<String, String> compact() {
        return Collections.singletonMap("cleanup.policy", "compact");
    }
}
