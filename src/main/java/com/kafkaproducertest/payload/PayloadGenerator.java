package com.kafkaproducertest.payload;

import com.kafkaproducertest.config.ContentMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.PayloadMode;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.springframework.stereotype.Component;

/** Generates payloads with exact configured byte lengths. */
@Component
public final class PayloadGenerator {
  private final KafkaProducerTestProperties properties;
  private final Random random;

  /** Creates a payload generator using the optional deterministic seed. */
  public PayloadGenerator(final KafkaProducerTestProperties properties) {
    this.properties = properties;
    Long seed = properties.getPayload().getSeed();
    random = seed == null ? new Random() : new Random(seed);
  }

  /** Generates the next serialized Kafka value. */
  public byte[] generate() {
    int length = length();
    byte[] result = new byte[length];
    ContentMode mode = properties.getPayload().getContentMode();
    if (mode == ContentMode.RANDOM) {
      random.nextBytes(result);
    } else {
      byte[] source =
          (mode == ContentMode.TEXT ? properties.getPayload().getText() : "kpt")
              .getBytes(StandardCharsets.UTF_8);
      for (int index = 0; index < length; index++) {
        result[index] = source[index % source.length];
      }
    }
    return result;
  }

  private int length() {
    KafkaProducerTestProperties.Payload payload = properties.getPayload();
    if (payload.getMode() == PayloadMode.FIXED) {
      return Math.toIntExact(payload.getLength().toBytes());
    }
    long minimum = payload.getMinimumLength().toBytes();
    long maximum = payload.getMaximumLength().toBytes();
    return Math.toIntExact(minimum + random.nextLong(maximum - minimum + 1));
  }
}
