package com.kafkaproducertest.execution;

import com.kafkaproducertest.config.RunStatus;
import com.kafkaproducertest.statistics.RunSummary;
import java.nio.file.Path;

/** Immutable result returned after a producer workload completes. */
public record RunResult(
    RunStatus status, RunSummary summary, Path jsonSummaryFile, Path yamlSummaryFile) { }
