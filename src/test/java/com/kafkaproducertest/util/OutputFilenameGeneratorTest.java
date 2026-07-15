package com.kafkaproducertest.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OutputFilenameGeneratorTest {
  @Test
  void includesPrefixUtcTimestampRunIdAndExtension() {
    UUID runId = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    String name =
        OutputFilenameGenerator.create(
            "statistics", Instant.parse("2026-01-02T03:04:05Z"), runId, "csv");

    assertThat(name)
        .isEqualTo("statistics-20260102T030405Z-123e4567-e89b-12d3-a456-426614174000.csv");
  }
}
