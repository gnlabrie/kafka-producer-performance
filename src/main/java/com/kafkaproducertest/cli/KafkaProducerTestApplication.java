package com.kafkaproducertest.cli;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Spring Boot entry point for the non-web Kafka producer test tool. */
@SpringBootApplication(scanBasePackages = "com.kafkaproducertest")
public class KafkaProducerTestApplication {
  /** Starts the command-line application. */
  public static void main(final String[] arguments) {
    System.exit(
        SpringApplication.exit(
            SpringApplication.run(KafkaProducerTestApplication.class, arguments)));
  }
}
