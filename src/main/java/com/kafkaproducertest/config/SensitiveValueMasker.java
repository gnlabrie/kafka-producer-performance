package com.kafkaproducertest.config;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Masks sensitive configuration values before presentation or persistence. */
@Component
public final class SensitiveValueMasker {
  /** Returns a copy with sensitive values replaced by asterisks. */
  public Map<String, Object> mask(final Map<String, ?> values) {
    Map<String, Object> masked = new LinkedHashMap<>();
    values.forEach((key, value) -> masked.put(key, isSensitive(key) ? "***" : value));
    return masked;
  }

  /** Identifies property keys whose values must not be presented. */
  public boolean isSensitive(final String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("password")
        || lower.contains("jaas")
        || lower.contains("keytab")
        || lower.contains("private.key")
        || lower.contains("secret");
  }
}
