package com.example.orders;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

/**
 * Rebalance'i görünür yapar: partition'lar ne zaman bırakıldı, ne zaman alındı, kaç ms durdu.
 * Spring Boot bu bean'i varsayılan container factory'ye kendisi bağlar.
 */
@Component
public class RebalanceLogger implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLogger.class);

    private final String instance;
    private final AtomicLong revokedAt = new AtomicLong();

    public RebalanceLogger(@Value("${app.instance}") String instance) {
        this.instance = instance;
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        revokedAt.set(System.currentTimeMillis());
        log.info("REBALANCE instance={} event=REVOKED at={} partitions={}",
                instance, Instant.now(), numbers(partitions));
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        long started = revokedAt.getAndSet(0);
        String pause = started == 0 ? "ilk-atama" : (System.currentTimeMillis() - started) + "ms";
        log.info("REBALANCE instance={} event=ASSIGNED at={} partitions={} pause={}",
                instance, Instant.now(), numbers(partitions), pause);
    }

    private static List<Integer> numbers(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).sorted().toList();
    }
}
