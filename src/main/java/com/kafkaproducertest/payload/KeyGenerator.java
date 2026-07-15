package com.kafkaproducertest.payload;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.KeyMode;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.springframework.stereotype.Component;

/** Generates optional Kafka record keys. */
@Component
public final class KeyGenerator {
  private final KafkaProducerTestProperties properties;
  private final Random random = new Random();

  /** Creates a key generator. */
  public KeyGenerator(final KafkaProducerTestProperties properties) {
    this.properties = properties;
  }

  /** Generates a key for the supplied sequence number, or null for keyless records. */
  public byte[] generate(final long sequence) {
    KeyMode mode = properties.getKey().getMode();
    if (mode == KeyMode.NONE) {
      return null;
    }
    if (mode == KeyMode.SEQUENCE) {
      return Long.toString(sequence).getBytes(StandardCharsets.UTF_8);
    }
    if (mode == KeyMode.FIXED) {
      return properties.getKey().getFixedValue().getBytes(StandardCharsets.UTF_8);
    }
    byte[] result = new byte[properties.getKey().getLength()];
    random.nextBytes(result);
    return result;
  }
}
