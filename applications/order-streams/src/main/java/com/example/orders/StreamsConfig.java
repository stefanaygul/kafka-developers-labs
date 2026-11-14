package com.example.orders;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Spring bağlantıları. Bu dosyayı değiştirmen gerekmiyor. */
@Configuration
@EnableConfigurationProperties(TopologySettings.class)
public class StreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    @Bean
    public Object orderTopology(StreamsBuilder builder, TopologySettings settings) {
        OrderTopology.build(builder, settings);
        return "order-topology";
    }

    @Bean
    public KafkaAdmin.NewTopics outputTopics(TopologySettings settings) {
        return new KafkaAdmin.NewTopics(output(settings.highValueTopic()), output(settings.enrichedTopic()),
                output(settings.countsTopic()));
    }

    /** Topology'nin en değerli dokümanı: slayt 64. */
    @EventListener(ApplicationReadyEvent.class)
    public void printTopology(ApplicationReadyEvent event) {
        StreamsBuilderFactoryBean factory = event.getApplicationContext().getBean(StreamsBuilderFactoryBean.class);
        log.info("TOPOLOGY{}{}", System.lineSeparator(), factory.getTopology().describe());
    }

    private static NewTopic output(String name) {
        return TopicBuilder.name(name).partitions(6).replicas(3).config("min.insync.replicas", "2").build();
    }
}
