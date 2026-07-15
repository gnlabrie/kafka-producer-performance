package com.kafkaproducertest.payload;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class HeaderFactoryTest {
  @Test
  void includesDefaultAndCustomHeaders() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getHeaders().setCustom(Map.of("source", "test"));
    UUID runId = UUID.randomUUID();
    Instant createdAt = Instant.parse("2026-01-02T03:04:05Z");

    RecordHeaders headers = new HeaderFactory(properties).create(createdAt, 7, runId, 12);

    assertThat(value(headers, "kpt-created-at")).isEqualTo("2026-01-02T03:04:05Z");
    assertThat(value(headers, "kpt-created-at-epoch-ms")).isEqualTo("1767323045000");
    assertThat(value(headers, "kpt-sequence-number")).isEqualTo("7");
    assertThat(value(headers, "kpt-run-id")).isEqualTo(runId.toString());
    assertThat(value(headers, "kpt-payload-length")).isEqualTo("12");
    assertThat(value(headers, "source")).isEqualTo("test");
  }

  @Test
  void omitsDefaultsWhenDisabled() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getHeaders().setIncludeDefaultHeaders(false);
    properties.getHeaders().setCustom(Map.of("source", "test"));

    RecordHeaders headers =
        new HeaderFactory(properties).create(Instant.now(), 1, UUID.randomUUID(), 1);

    assertThat(headers).hasSize(1);
    assertThat(value(headers, "source")).isEqualTo("test");
  }

  private String value(final RecordHeaders headers, final String key) {
    return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
  }
}
