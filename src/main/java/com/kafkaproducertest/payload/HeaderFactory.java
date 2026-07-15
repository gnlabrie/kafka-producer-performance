package com.kafkaproducertest.payload;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Component;

/** Creates standard and configured static Kafka headers. */
@Component
public final class HeaderFactory {
  private final KafkaProducerTestProperties properties;

  /** Creates a header factory. */
  public HeaderFactory(final KafkaProducerTestProperties properties) {
    this.properties = properties;
  }

  /** Creates headers for a single record. */
  public RecordHeaders create(
      final Instant createdAt, final long sequence, final UUID runId, final int payloadLength) {
    RecordHeaders headers = new RecordHeaders();
    if (properties.getHeaders().isIncludeDefaultHeaders()) {
      add(headers, "kpt-created-at", createdAt.toString());
      add(headers, "kpt-created-at-epoch-ms", Long.toString(createdAt.toEpochMilli()));
      add(headers, "kpt-sequence-number", Long.toString(sequence));
      add(headers, "kpt-run-id", runId.toString());
      add(headers, "kpt-payload-length", Integer.toString(payloadLength));
    }
    properties.getHeaders().getCustom().forEach((key, value) -> add(headers, key, value));
    return headers;
  }

  private void add(final RecordHeaders headers, final String key, final String value) {
    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
  }
}
