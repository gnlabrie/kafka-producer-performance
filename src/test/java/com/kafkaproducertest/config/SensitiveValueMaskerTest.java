package com.kafkaproducertest.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SensitiveValueMaskerTest {
  private final SensitiveValueMasker masker = new SensitiveValueMasker();

  @Test
  void masksPasswordsJaasKeytabsAndSecretKeys() {
    Map<String, Object> result =
        masker.mask(
            Map.of(
                "ssl.truststore.password", "password",
                "sasl.jaas.config", "jaas",
                "gssapi.keytab", "keytab",
                "api.secret", "secret"));

    assertThat(result.values()).containsOnly("***");
  }

  @Test
  void leavesNonSensitivePropertiesUnchanged() {
    assertThat(masker.mask(Map.of("bootstrap.servers", "broker:9092")))
        .containsEntry("bootstrap.servers", "broker:9092");
    assertThat(masker.isSensitive("bootstrap.servers")).isFalse();
  }
}
