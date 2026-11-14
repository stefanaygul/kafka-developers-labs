package com.example.orders.retry;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Broker'da otomatik topic oluşturma kapalı. DLT önceden yoksa recoverer yazamaz (slayt 50).
 * DLT, ana topic ile aynı partition sayısına sahip olmalı: kayıt aynı numaralı partition'a gider.
 */
@Configuration
@Profile("retry")
public class RetryTopicsConfig {

    @Bean
    public NewTopic ordersDlt(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic + "-dlt").partitions(6).replicas(3)
                .config("min.insync.replicas", "2").build();
    }

    @Bean
    public NewTopic notifications(@Value("${app.notification-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(3)
                .config("min.insync.replicas", "2").build();
    }
}
