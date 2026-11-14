package com.example.orders;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Topic adları koda gömülü değil: check sırasında benzersiz adlarla çalışabilsin. */
@ConfigurationProperties(prefix = "app")
public record TopologySettings(
        String ordersTopic,
        String customersTopic,
        String highValueTopic,
        String enrichedTopic,
        String countsTopic,
        double highValueThreshold) {
}
