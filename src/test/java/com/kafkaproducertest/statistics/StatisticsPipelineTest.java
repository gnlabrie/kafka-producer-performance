package com.kafkaproducertest.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StatisticsPipelineTest {
  @TempDir Path temporaryDirectory;

  @Test
  void submitsRowsAndDrainsThemOnClose() throws Exception {
    Path csv = temporaryDirectory.resolve("statistics.csv");
    LatencyHistogram histogram = new LatencyHistogram();
    try (StatisticsPipeline pipeline = pipeline(csv, 4, histogram)) {
      assertThat(pipeline.submit(CsvStatisticsWriterTest.statistic(null))).isTrue();
      assertThat(pipeline.submit(CsvStatisticsWriterTest.statistic(null))).isTrue();
    }

    assertThat(Files.readAllLines(csv)).hasSize(3);
    assertThat(histogram.minimum()).isEqualTo(3);
  }

  @Test
  void recordsOverflowAsDroppedAndIncomplete() throws Exception {
    Path csv = temporaryDirectory.resolve("statistics.csv");
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getStatistics().setQueueCapacity(1);
    try (StatisticsPipeline pipeline =
        new StatisticsPipeline(properties, csv, new LatencyHistogram())) {
      while (pipeline.submit(CsvStatisticsWriterTest.statistic(null))) {
        // Keep submitting until the bounded queue rejects a row.
      }
      assertThat(pipeline.getDropped()).isPositive();
      assertThat(pipeline.isIncomplete()).isTrue();
    }
  }

  private StatisticsPipeline pipeline(
      final Path csv, final int capacity, final LatencyHistogram histogram) throws Exception {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getStatistics().setQueueCapacity(capacity);
    return new StatisticsPipeline(properties, csv, histogram);
  }
}
