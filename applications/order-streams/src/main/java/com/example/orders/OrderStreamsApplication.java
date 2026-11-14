package com.example.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrderStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderStreamsApplication.class, args);
    }
}
