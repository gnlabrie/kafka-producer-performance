package com.kafkaproducertest.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvStatisticsWriterTest {
  @TempDir Path temporaryDirectory;

  @Test
  void writesSingleHeaderAndEscapesCsvValues() throws Exception {
    Path file = temporaryDirectory.resolve("nested/statistics.csv");
    try (CsvStatisticsWriter writer = new CsvStatisticsWriter(file)) {
      writer.write(statistic("a,b \"quoted\""));
      writer.write(statistic(null));
    }

    String output = Files.readString(file);
    assertThat(output).containsOnlyOnce("run_id,sequence_number");
    assertThat(output).contains("\"a,b \"\"quoted\"\"\"");
    assertThat(Files.readAllLines(file)).hasSize(3);
  }

  static MessageStatistic statistic(final String message) {
    Instant instant = Instant.parse("2026-01-01T00:00:00Z");
    return new MessageStatistic(
        UUID.randomUUID().toString(),
        1,
        instant,
        instant,
        instant,
        instant,
        1,
        2,
        3,
        4,
        "topic",
        0,
        1L,
        2,
        3,
        "none",
        true,
        null,
        message);
  }
}
