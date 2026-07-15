package com.kafkaproducertest.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class ConfigurationExceptionTest {
  @Test
  void exposesAnImmutableListOfJoinedViolations() {
    List<String> violations = List.of("run.topic: required", "payload.length: positive");

    ConfigurationException exception = new ConfigurationException(violations);

    assertThat(exception)
        .hasMessageContaining("run.topic: required")
        .hasMessageContaining("payload.length: positive");
    assertThat(exception.getViolations()).containsExactlyElementsOf(violations);
    assertThatThrownBy(() -> exception.getViolations().add("another violation"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
