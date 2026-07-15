package com.kafkaproducertest.statistics;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/** Builds reconciled immutable run summaries from execution counters. */
public final class RunSummaryBuilder {
  /** Builds a run summary from the supplied values. */
  public RunSummary build(
      final KafkaProducerTestProperties properties,
      final String runId,
      final RunStatus status,
      final Instant started,
      final Instant ended,
      final long attempted,
      final long accepted,
      final long acknowledged,
      final long failed,
      final long payloadBytes,
      final Map<String, Long> errors,
      final Map<Integer, Long> partitions,
      final StatisticsPipeline pipeline,
      final LatencyHistogram histogram,
      final Map<String, Object> effectiveConfig,
      final String activeProfiles,
      final Path statisticsFile,
      final Path jsonFile,
      final Path yamlFile) {
    Duration elapsed = Duration.between(started, ended);
    double seconds = Math.max(elapsed.toNanos() / 1_000_000_000D, 0.000001D);
    return new RunSummary(
        runId,
        properties.getRun().getName(),
        properties.getRun().getDescription(),
        status,
        started,
        ended,
        elapsed,
        attempted,
        accepted,
        acknowledged,
        failed,
        accepted - acknowledged - failed,
        payloadBytes,
        acknowledged / seconds,
        payloadBytes / 1048576D / seconds,
        histogram.minimum(),
        histogram.mean(),
        histogram.maximum(),
        histogram.percentile(50),
        histogram.percentile(75),
        histogram.percentile(90),
        histogram.percentile(95),
        histogram.percentile(99),
        histogram.percentile(99.9),
        Map.copyOf(errors),
        Map.copyOf(partitions),
        pipeline.getWritten(),
        pipeline.getDropped(),
        Map.copyOf(effectiveConfig),
        activeProfiles,
        statisticsFile.toString(),
        path(jsonFile),
        path(yamlFile),
        System.getProperty("logging.file.name"));
  }

  private String path(final Path value) {
    return value == null ? null : value.toString();
  }
}
