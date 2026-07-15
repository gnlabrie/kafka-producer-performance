package com.kafkaproducertest.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Registers application configuration and serialization infrastructure. */
@Configuration
@EnableConfigurationProperties(KafkaProducerTestProperties.class)
public class ApplicationConfig {
  /** Creates an ObjectMapper that supports Java time values. */
  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }
}
