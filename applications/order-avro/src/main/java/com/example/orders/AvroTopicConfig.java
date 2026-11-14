package com.example.orders;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AvroTopicConfig {

    @Bean
    public NewTopic ordersAvro(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(3).config("min.insync.replicas", "2").build();
    }
}
