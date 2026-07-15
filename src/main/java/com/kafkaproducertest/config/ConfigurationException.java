package com.kafkaproducertest.config;

import java.util.List;

/** Signals one or more invalid application configuration values. */
public final class ConfigurationException extends RuntimeException {
  private final List<String> violations;

  /** Creates an exception containing complete property-path violations. */
  public ConfigurationException(final List<String> value) {
    super(String.join(System.lineSeparator(), value));
    violations = List.copyOf(value);
  }

  /** Returns all validation violations. */
  public List<String> getViolations() {
    return violations;
  }
}
