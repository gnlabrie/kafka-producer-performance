package com.kafkaproducertest.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Renders sanitized effective configuration for the CLI and summaries. */
@Component
public final class EffectiveConfigurationPrinter {
  private final ObjectMapper objectMapper;
  private final SensitiveValueMasker masker;

  /** Creates the configuration printer. */
  public EffectiveConfigurationPrinter(
      final ObjectMapper objectMapper, final SensitiveValueMasker masker) {
    this.objectMapper = objectMapper;
    this.masker = masker;
  }

  /** Returns a pretty JSON representation with secrets masked. */
  public String print(final Map<String, ?> configuration) {
    try {
      return objectMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(masker.mask(configuration));
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to render effective configuration", exception);
    }
  }
}
