package com.kafkaproducertest.payload;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.KeyMode;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KeyGeneratorTest {
  @Test
  void generatesKeysForEachSupportedMode() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    KeyGenerator generator = new KeyGenerator(properties);

    assertThat(generator.generate(1)).isNull();

    properties.getKey().setMode(KeyMode.SEQUENCE);
    assertThat(new String(generator.generate(42), StandardCharsets.UTF_8)).isEqualTo("42");

    properties.getKey().setMode(KeyMode.FIXED);
    properties.getKey().setFixedValue("fixed");
    assertThat(new String(generator.generate(0), StandardCharsets.UTF_8)).isEqualTo("fixed");

    properties.getKey().setMode(KeyMode.RANDOM);
    properties.getKey().setLength(9);
    assertThat(generator.generate(0)).hasSize(9);
  }
}
