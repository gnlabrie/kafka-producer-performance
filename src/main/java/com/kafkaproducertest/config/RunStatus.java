package com.kafkaproducertest.config;

/** Final workload run state. */
public enum RunStatus {
  COMPLETED,
  INCOMPLETE,
  FAILED,
  VALIDATED,
  CONFIG_SHOWN,
  INTERRUPTED
}
