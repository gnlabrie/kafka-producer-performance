package com.kafkaproducertest.payload;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.ContentMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.PayloadMode;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class PayloadGeneratorTest {
  @Test
  void generatesFixedPayloadWithExactLength() {
    KafkaProducerTestProperties properties = properties();
    properties.getPayload().setLength(DataSize.ofBytes(13));

    assertThat(new PayloadGenerator(properties).generate()).hasSize(13);
  }

  @Test
  void generatesVariablePayloadWithinInclusiveBounds() {
    KafkaProducerTestProperties properties = properties();
    properties.getPayload().setMode(PayloadMode.VARIABLE);
    properties.getPayload().setMinimumLength(DataSize.ofBytes(2));
    properties.getPayload().setMaximumLength(DataSize.ofBytes(6));
    properties.getPayload().setSeed(42L);
    PayloadGenerator generator = new PayloadGenerator(properties);

    for (int count = 0; count < 20; count++) {
      assertThat(generator.generate().length).isBetween(2, 6);
    }
  }

  @Test
  void usesSeedToProduceIdenticalSequences() {
    KafkaProducerTestProperties first = properties();
    KafkaProducerTestProperties second = properties();
    first.getPayload().setSeed(123L);
    second.getPayload().setSeed(123L);
    PayloadGenerator firstGenerator = new PayloadGenerator(first);
    PayloadGenerator secondGenerator = new PayloadGenerator(second);

    assertThat(firstGenerator.generate()).isEqualTo(secondGenerator.generate());
    assertThat(firstGenerator.generate()).isEqualTo(secondGenerator.generate());
  }

  @Test
  void repeatsTextAndBuiltInRepeatingContent() {
    KafkaProducerTestProperties text = properties();
    text.getPayload().setContentMode(ContentMode.TEXT);
    text.getPayload().setText("ab");
    text.getPayload().setLength(DataSize.ofBytes(5));
    assertThat(new String(new PayloadGenerator(text).generate(), StandardCharsets.UTF_8))
        .isEqualTo("ababa");

    KafkaProducerTestProperties repeating = properties();
    repeating.getPayload().setContentMode(ContentMode.REPEATING);
    repeating.getPayload().setLength(DataSize.ofBytes(5));
    assertThat(new String(new PayloadGenerator(repeating).generate(), StandardCharsets.UTF_8))
        .isEqualTo("kptkp");
  }

  private KafkaProducerTestProperties properties() {
    return new KafkaProducerTestProperties();
  }
}
