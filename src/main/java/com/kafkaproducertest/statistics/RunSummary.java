package com.kafkaproducertest.statistics;

import com.kafkaproducertest.config.RunStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/** Immutable final workload summary suitable for JSON or YAML output. */
public record RunSummary(
    String runId,
    String runName,
    String description,
    RunStatus status,
    Instant startedAt,
    Instant endedAt,
    Duration elapsed,
    long attempted,
    long accepted,
    long acknowledged,
    long failed,
    long outstanding,
    long payloadBytes,
    double messagesPerSecond,
    double payloadMibPerSecond,
    long latencyMinimumMicros,
    double latencyAverageMicros,
    long latencyMaximumMicros,
    long latencyP50Micros,
    long latencyP75Micros,
    long latencyP90Micros,
    long latencyP95Micros,
    long latencyP99Micros,
    long latencyP999Micros,
    Map<String, Long> errorsByClass,
    Map<Integer, Long> recordsByPartition,
    long statisticsRowsWritten,
    long statisticsRowsDropped,
    Map<String, Object> effectiveProducerConfiguration,
    String activeProfiles,
    String csvFile,
    String jsonSummaryFile,
    String yamlSummaryFile,
    String applicationLogFile) { }
