package com.kafkaproducertest.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunSummaryBuilderTest {
  @TempDir Path temporaryDirectory;

  @Test
  void buildsReconciledCountsAndLatencyFields() throws Exception {
    Path csv = temporaryDirectory.resolve("statistics.csv");
    LatencyHistogram histogram = new LatencyHistogram();
    histogram.record(10);
    histogram.record(20);
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getRun().setName("sample");
    StatisticsPipeline pipeline = pipeline(csv, histogram);
    try (pipeline) {
      pipeline.submit(CsvStatisticsWriterTest.statistic(null));
    }

    RunSummary summary =
        new RunSummaryBuilder()
            .build(
                properties,
                "run-id",
                RunStatus.COMPLETED,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:02Z"),
                5,
                5,
                4,
                0,
                100,
                Map.of(),
                Map.of(0, 4L),
                pipeline,
                histogram,
                Map.of("acks", "all"),
                "test",
                csv,
                null,
                null);

    assertThat(summary.outstanding()).isEqualTo(1);
    assertThat(summary.latencyMinimumMicros()).isEqualTo(3);
    assertThat(summary.latencyP50Micros()).isGreaterThanOrEqualTo(10);
    assertThat(summary.messagesPerSecond()).isEqualTo(2.0);
  }

  private StatisticsPipeline pipeline(final Path csv, final LatencyHistogram histogram)
      throws Exception {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getStatistics().setQueueCapacity(2);
    return new StatisticsPipeline(properties, csv, histogram);
  }
}
