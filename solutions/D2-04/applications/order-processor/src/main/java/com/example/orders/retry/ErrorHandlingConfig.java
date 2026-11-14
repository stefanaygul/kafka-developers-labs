package com.example.orders.retry;

import com.example.orders.OrderEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Lab D2-04, Senaryo A ve B: Bu dosyada çalışıyorsun.
 */
@Configuration
@Profile("retry")
public class ErrorHandlingConfig {

    /**
     * DLT'ye iki tur kayıt gider: nesneye çevrilmiş OrderEvent ve hiç çevrilememiş ham bayt.
     * JsonSerializer ham baytı yazamaz, bu yüzden tipe göre serializer seçilir (slayt 51).
     */
    @Bean
    public KafkaTemplate<Object, Object> dltTemplate(KafkaProperties properties) {
        Map<String, Object> config = new HashMap<>(properties.buildProducerProperties(null));
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 6000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(OrderEvent.class, new JsonSerializer<OrderEvent>());
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config, new StringOrBytesKeySerializer(),
                new DelegatingByTypeSerializer(delegates)));
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> dltTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate);
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(8));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        handler.addNotRetryableExceptions(PaymentValidationException.class);
        return handler;
    }
}
