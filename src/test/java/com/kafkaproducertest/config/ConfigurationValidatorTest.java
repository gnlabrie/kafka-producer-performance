package com.kafkaproducertest.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.unit.DataSize;

class ConfigurationValidatorTest {
  @TempDir Path temporaryDirectory;

  private final ConfigurationValidator validator = new ConfigurationValidator();

  @Test
  void acceptsMinimalRunConfiguration() {
    assertThatCode(() -> validator.validate(validProperties())).doesNotThrowAnyException();
  }

  @Test
  void rejectsMissingTopicAndBootstrapServers() {
    KafkaProducerTestProperties properties = validProperties();
    properties.getRun().setTopic(null);
    properties.getConnection().setBootstrapServers(List.of());

    assertInvalid(properties, "run.topic", "bootstrap-servers");
  }

  @Test
  void rejectsBothOrNeitherRunLimitsForRunMode() {
    KafkaProducerTestProperties both = validProperties();
    both.getRun().setDuration(Duration.ofSeconds(1));
    assertInvalid(both, "exactly one");

    KafkaProducerTestProperties neither = validProperties();
    neither.getRun().setMessageCount(null);
    assertInvalid(neither, "exactly one");
  }

  @Test
  void validatesFixedAndVariablePayloadRules() {
    KafkaProducerTestProperties fixed = validProperties();
    fixed.getPayload().setMinimumLength(DataSize.ofBytes(1));
    assertInvalid(fixed, "fixed mode");

    KafkaProducerTestProperties variable = validProperties();
    variable.getPayload().setMode(PayloadMode.VARIABLE);
    variable.getPayload().setMinimumLength(DataSize.ofBytes(10));
    variable.getPayload().setMaximumLength(DataSize.ofBytes(5));
    assertInvalid(variable, "variable mode");
  }

  @Test
  void rejectsReservedHeadersAndMissingFixedKeyValue() {
    KafkaProducerTestProperties properties = validProperties();
    properties.getHeaders().setCustom(Map.of("kpt-user", "value"));
    properties.getKey().setMode(KeyMode.FIXED);

    assertInvalid(properties, "reserved", "fixed-value");
  }

  @Test
  void requiresTruststoreForSslAndKeystoreForMtls() {
    KafkaProducerTestProperties ssl = validProperties();
    ssl.getSecurity().setMode(AuthenticationMode.SSL);
    assertInvalid(ssl, "truststore-path", "truststore-password");

    KafkaProducerTestProperties mtls = validProperties();
    mtls.getSecurity().setMode(AuthenticationMode.MTLS);
    mtls.getSecurity().setTruststorePath(readableFile("truststore"));
    mtls.getSecurity().setTruststorePassword("secret");
    assertInvalid(mtls, "keystore-path", "keystore-password");
  }

  @Test
  void rejectsKeytabWithTicketCacheAndIncompatibleIdempotence() {
    KafkaProducerTestProperties properties = validProperties();
    properties.getSecurity().setMode(AuthenticationMode.SASL_PLAINTEXT_GSSAPI);
    properties.getSecurity().setGssapiLoginMode(GssapiLoginMode.TICKET_CACHE);
    properties.getSecurity().setGssapiKeytab(readableFile("client.keytab"));
    properties.getProducer().setAcks("1");

    assertInvalid(properties, "cannot be configured with ticket-cache", "idempotence requires");
  }

  @Test
  void validateModeSkipsOutputDirectoryChecks() {
    KafkaProducerTestProperties properties = validProperties();
    properties.setMode(ExecutionMode.VALIDATE);
    properties.getStatistics().setOutputDirectory(Path.of("Z:/missing-parent/output"));
    properties.getSummary().setOutputDirectory(Path.of("Z:/missing-parent/output"));

    assertThatCode(() -> validator.validate(properties)).doesNotThrowAnyException();
  }

  @Test
  void rejectsNonPositiveTopicPartitionsAndReplicationFactor() {
    KafkaProducerTestProperties partitions = validProperties();
    partitions.getTopic().setPartitions(0);
    assertInvalid(partitions, "topic.partitions");

    KafkaProducerTestProperties replication = validProperties();
    replication.getTopic().setReplicationFactor((short) 0);
    assertInvalid(replication, "topic.replication-factor");
  }

  @Test
  void rejectsNullTopicConfigValues() {
    KafkaProducerTestProperties properties = validProperties();
    java.util.Map<String, String> configs = new java.util.LinkedHashMap<>();
    configs.put("retention.ms", null);
    properties.getTopic().setConfigs(configs);

    assertInvalid(properties, "topic.configs.retention.ms");
  }

  private KafkaProducerTestProperties validProperties() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getRun().setTopic("test-topic");
    properties.getRun().setMessageCount(1L);
    properties.getConnection().setBootstrapServers(List.of("localhost:9092"));
    properties.getStatistics().setOutputDirectory(temporaryDirectory);
    properties.getSummary().setOutputDirectory(temporaryDirectory);
    return properties;
  }

  private Path readableFile(final String name) {
    try {
      return Files.writeString(temporaryDirectory.resolve(name), "fixture");
    } catch (java.io.IOException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private void assertInvalid(
      final KafkaProducerTestProperties properties, final String... fragments) {
    assertThatThrownBy(() -> validator.validate(properties))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(fragments[0]);
    for (String fragment : fragments) {
      assertThatThrownBy(() -> validator.validate(properties)).hasMessageContaining(fragment);
    }
  }
}
