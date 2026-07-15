package com.kafkaproducertest.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/** Validates configuration without creating network or output resources. */
@Component
public final class ConfigurationValidator {
  /** Validates the supplied configuration or throws a configuration exception. */
  public void validate(final KafkaProducerTestProperties properties) {
    List<String> errors = new ArrayList<>();
    validateConnection(properties, errors);
    validateRun(properties, errors);
    validateTopic(properties, errors);
    validatePayload(properties, errors);
    validateSecurity(properties, errors);
    validateOutput(properties, errors);
    validateProducer(properties, errors);
    validateHeaders(properties, errors);
    if (!errors.isEmpty()) {
      throw new ConfigurationException(errors);
    }
  }

  private void validateConnection(final KafkaProducerTestProperties p, final List<String> e) {
    if (p.getConnection().getBootstrapServers() == null
        || p.getConnection().getBootstrapServers().isEmpty()) {
      e.add("kafka-producer-test.connection.bootstrap-servers: at least one server is required");
    }
  }

  private void validateRun(final KafkaProducerTestProperties p, final List<String> e) {
    KafkaProducerTestProperties.Run run = p.getRun();
    if (blank(run.getTopic())) {
      e.add("kafka-producer-test.run.topic: a topic is required");
    }
    if (p.getMode() == ExecutionMode.RUN
        && (run.getMessageCount() == null) == (run.getDuration() == null)) {
      e.add("kafka-producer-test.run: configure exactly one of message-count or duration");
    }
    if (run.getMessageCount() != null && run.getMessageCount() <= 0) {
      e.add("kafka-producer-test.run.message-count: must be positive");
    }
    if (run.getDuration() != null
        && (run.getDuration().isZero() || run.getDuration().isNegative())) {
      e.add("kafka-producer-test.run.duration: must be positive");
    }
    if (run.getMessagesPerSecond() != null && run.getMessagesPerSecond() <= 0) {
      e.add("kafka-producer-test.run.messages-per-second: must be positive");
    }
    if (run.getShutdownTimeout() == null
        || run.getShutdownTimeout().isNegative()
        || run.getShutdownTimeout().isZero()) {
      e.add("kafka-producer-test.run.shutdown-timeout: must be positive");
    }
    if (run.getMaximumErrors() < 0) {
      e.add("kafka-producer-test.run.maximum-errors: cannot be negative");
    }
  }

  private void validateTopic(final KafkaProducerTestProperties p, final List<String> e) {
    KafkaProducerTestProperties.Topic topic = p.getTopic();
    if (topic.getPartitions() < 1) {
      e.add("kafka-producer-test.topic.partitions: must be at least 1");
    }
    if (topic.getReplicationFactor() < 1) {
      e.add("kafka-producer-test.topic.replication-factor: must be at least 1");
    }
    if (topic.getConfigs() != null) {
      topic
          .getConfigs()
          .forEach(
              (key, value) -> {
                if (blank(key)) {
                  e.add("kafka-producer-test.topic.configs: keys must be non-blank");
                }
                if (value == null) {
                  e.add("kafka-producer-test.topic.configs." + key + ": value must be a string");
                }
              });
    }
  }

  private void validatePayload(final KafkaProducerTestProperties p, final List<String> e) {
    KafkaProducerTestProperties.Payload payload = p.getPayload();
    if (payload.getMode() == PayloadMode.FIXED
        && (payload.getLength() == null
            || payload.getLength().toBytes() <= 0
            || payload.getMinimumLength() != null
            || payload.getMaximumLength() != null)) {
      e.add("kafka-producer-test.payload: fixed mode requires only a positive length");
    }
    if (payload.getMode() == PayloadMode.VARIABLE
        && (payload.getMinimumLength() == null
            || payload.getMaximumLength() == null
            || payload.getMinimumLength().toBytes() <= 0
            || payload.getMinimumLength().toBytes() > payload.getMaximumLength().toBytes())) {
      e.add(
          "kafka-producer-test.payload: variable mode requires positive minimum-length not greater"
              + " than maximum-length");
    }
    if (payload.getContentMode() == ContentMode.TEXT && blank(payload.getText())) {
      e.add("kafka-producer-test.payload.text: required for TEXT content mode");
    }
    if (p.getKey().getMode() == KeyMode.FIXED && blank(p.getKey().getFixedValue())) {
      e.add("kafka-producer-test.key.fixed-value: required for FIXED key mode");
    }
  }

  private void validateSecurity(final KafkaProducerTestProperties p, final List<String> e) {
    KafkaProducerTestProperties.Security s = p.getSecurity();
    if (s.getMode() == AuthenticationMode.SSL
        || s.getMode() == AuthenticationMode.MTLS
        || s.getMode() == AuthenticationMode.SASL_SSL_GSSAPI) {
      requiredFile(s.getTruststorePath(), "kafka-producer-test.security.truststore-path", e);
      requiredValue(
          s.getTruststorePassword(), "kafka-producer-test.security.truststore-password", e);
    }
    if (s.getMode() == AuthenticationMode.MTLS) {
      requiredFile(s.getKeystorePath(), "kafka-producer-test.security.keystore-path", e);
      requiredValue(s.getKeystorePassword(), "kafka-producer-test.security.keystore-password", e);
    }
    if (s.getMode() == AuthenticationMode.SASL_PLAINTEXT_GSSAPI
        || s.getMode() == AuthenticationMode.SASL_SSL_GSSAPI) {
      if (blank(s.getGssapiServiceName())) {
        e.add("kafka-producer-test.security.gssapi-service-name: required for GSSAPI");
      }
      if (s.getGssapiLoginMode() == GssapiLoginMode.KEYTAB) {
        if (blank(s.getGssapiPrincipal())) {
          e.add("kafka-producer-test.security.gssapi-principal: required for keytab login");
        }
        requiredFile(s.getGssapiKeytab(), "kafka-producer-test.security.gssapi-keytab", e);
      }
      if (s.getGssapiLoginMode() == GssapiLoginMode.TICKET_CACHE && s.getGssapiKeytab() != null) {
        e.add(
            "kafka-producer-test.security.gssapi-keytab: cannot be configured with ticket-cache"
                + " login");
      }
    }
  }

  private void validateOutput(final KafkaProducerTestProperties p, final List<String> e) {
    if (p.getStatistics().getQueueCapacity() <= 0) {
      e.add("kafka-producer-test.statistics.queue-capacity: must be positive");
    }
    if (p.getStatistics().getFlushInterval() == null
        || p.getStatistics().getFlushInterval().isNegative()
        || p.getStatistics().getFlushInterval().isZero()) {
      e.add("kafka-producer-test.statistics.flush-interval: must be positive");
    }
    if (p.getMode() == ExecutionMode.RUN) {
      writable(
          p.getStatistics().getOutputDirectory(),
          "kafka-producer-test.statistics.output-directory",
          e);
      writable(
          p.getSummary().getOutputDirectory(), "kafka-producer-test.summary.output-directory", e);
    }
  }

  private void validateProducer(final KafkaProducerTestProperties p, final List<String> e) {
    KafkaProducerTestProperties.Producer producer = p.getProducer();
    if (producer.getEnableIdempotence()
        && (!"all".equalsIgnoreCase(producer.getAcks())
            || producer.getRetries() == null
            || producer.getRetries() <= 0
            || producer.getMaxInFlightRequests() == null
            || producer.getMaxInFlightRequests() > 5)) {
      e.add(
          "kafka-producer-test.producer: idempotence requires acks=all, retries>0, and"
              + " max-in-flight-requests<=5");
    }
  }

  private void validateHeaders(final KafkaProducerTestProperties p, final List<String> e) {
    p.getHeaders().getCustom().keySet().stream()
        .filter(key -> key.toLowerCase().startsWith("kpt-"))
        .forEach(
            key ->
                e.add(
                    "kafka-producer-test.headers.custom." + key + ": kpt-* headers are reserved"));
  }

  private void requiredFile(final Path file, final String path, final List<String> errors) {
    if (file == null || !Files.isReadable(file)) {
      errors.add(path + ": readable file is required");
    }
  }

  private void requiredValue(final String value, final String path, final List<String> errors) {
    if (blank(value)) {
      errors.add(path + ": a value is required");
    }
  }

  private void writable(final Path directory, final String path, final List<String> errors) {
    if (directory == null) {
      errors.add(path + ": directory is required");
    } else if (Files.exists(directory) && !Files.isWritable(directory)) {
      errors.add(path + ": directory is not writable");
    } else if (!Files.exists(directory) && firstExistingParent(directory) == null) {
      errors.add(path + ": directory cannot be created because no parent directory exists");
    }
  }

  private Path firstExistingParent(final Path directory) {
    Path candidate = directory.toAbsolutePath();
    while (candidate != null && !Files.exists(candidate)) {
      candidate = candidate.getParent();
    }
    return candidate;
  }

  private boolean blank(final String value) {
    return value == null || value.isBlank();
  }
}
