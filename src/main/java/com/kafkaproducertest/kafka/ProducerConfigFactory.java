package com.kafkaproducertest.kafka;

import com.kafkaproducertest.config.AuthenticationMode;
import com.kafkaproducertest.config.GssapiLoginMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Builds native Kafka producer properties from typed application configuration. */
@Component
public final class ProducerConfigFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConfigFactory.class);

  /** Merges typed producer settings over raw advanced properties. */
  public Map<String, Object> create(final KafkaProducerTestProperties properties) {
    Map<String, Object> values = new LinkedHashMap<>(properties.getProducer().getProperties());
    put(
        values,
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        String.join(",", properties.getConnection().getBootstrapServers()));
    put(values, ProducerConfig.CLIENT_ID_CONFIG, properties.getConnection().getClientId());
    KafkaProducerTestProperties.Producer p = properties.getProducer();
    put(values, ProducerConfig.ACKS_CONFIG, p.getAcks());
    put(values, ProducerConfig.COMPRESSION_TYPE_CONFIG, p.getCompressionType().kafkaName());
    put(values, ProducerConfig.LINGER_MS_CONFIG, toIntMillis(p.getLinger()));
    put(values, ProducerConfig.BATCH_SIZE_CONFIG, toIntBytes(p.getBatchSize()));
    put(values, ProducerConfig.BUFFER_MEMORY_CONFIG, p.getBufferMemory().toBytes());
    put(values, ProducerConfig.MAX_REQUEST_SIZE_CONFIG, toIntBytes(p.getMaxRequestSize()));
    put(values, ProducerConfig.RETRIES_CONFIG, p.getRetries());
    put(values, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, toIntMillis(p.getRequestTimeout()));
    put(values, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, toIntMillis(p.getDeliveryTimeout()));
    put(values, ProducerConfig.MAX_BLOCK_MS_CONFIG, p.getMaxBlock().toMillis());
    put(values, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, p.getMaxInFlightRequests());
    put(values, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, p.getEnableIdempotence());
    put(values, ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, p.getClientDnsLookup());
    put(values, ProducerConfig.METADATA_MAX_AGE_CONFIG, duration(p.getMetadataMaxAge()));
    put(values, ProducerConfig.SEND_BUFFER_CONFIG, toIntBytes(p.getSendBuffer()));
    put(values, ProducerConfig.RECEIVE_BUFFER_CONFIG, toIntBytes(p.getReceiveBuffer()));
    security(values, properties.getSecurity());
    return values;
  }

  /**
   * Builds AdminClient connection settings from bootstrap servers and security configuration.
   *
   * @param properties application configuration
   * @return Kafka client properties suitable for AdminClient
   */
  public Map<String, Object> createAdmin(final KafkaProducerTestProperties properties) {
    Map<String, Object> values = new LinkedHashMap<>();
    put(
        values,
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        String.join(",", properties.getConnection().getBootstrapServers()));
    String clientId = properties.getConnection().getClientId();
    if (clientId != null && !clientId.isBlank()) {
      put(values, CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-admin");
    }
    security(values, properties.getSecurity());
    return values;
  }

  private void security(
      final Map<String, Object> values, final KafkaProducerTestProperties.Security security) {
    AuthenticationMode mode = security.getMode();
    if (mode == AuthenticationMode.SSL
        || mode == AuthenticationMode.MTLS
        || mode == AuthenticationMode.SASL_SSL_GSSAPI) {
      put(
          values,
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          mode == AuthenticationMode.SASL_SSL_GSSAPI ? "SASL_SSL" : "SSL");
      put(values, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, path(security.getTruststorePath()));
      put(values, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, security.getTruststorePassword());
      put(values, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, security.getTruststoreType());
      put(
          values,
          SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          security.getEndpointIdentificationAlgorithm());
    }
    if (mode == AuthenticationMode.MTLS) {
      put(values, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path(security.getKeystorePath()));
      put(values, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, security.getKeystorePassword());
      put(values, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, security.getKeystoreType());
      put(values, SslConfigs.SSL_KEY_PASSWORD_CONFIG, security.getKeyPassword());
    }
    if (mode == AuthenticationMode.SASL_PLAINTEXT_GSSAPI
        || mode == AuthenticationMode.SASL_SSL_GSSAPI) {
      if (mode == AuthenticationMode.SASL_PLAINTEXT_GSSAPI) {
        put(values, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      }
      put(values, SaslConfigs.SASL_MECHANISM, "GSSAPI");
      put(values, SaslConfigs.SASL_KERBEROS_SERVICE_NAME, security.getGssapiServiceName());
      put(values, SaslConfigs.SASL_JAAS_CONFIG, jaasConfig(security));
    }
  }

  private void put(final Map<String, Object> values, final String key, final Object value) {
    if (value == null) {
      return;
    }
    if (values.containsKey(key)) {
      LOGGER.warn("Typed producer property '{}' overrides advanced property", key);
    }
    values.put(key, value);
  }

  private String jaasConfig(final KafkaProducerTestProperties.Security security) {
    if (security.getJaasConfig() != null && !security.getJaasConfig().isBlank()) {
      return security.getJaasConfig();
    }
    if (security.getGssapiLoginMode() == GssapiLoginMode.KEYTAB) {
      return "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true"
          + " keyTab=\""
          + security.getGssapiKeytab()
          + "\" principal=\""
          + security.getGssapiPrincipal()
          + "\";";
    }
    return "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true"
        + " renewTGT=true;";
  }

  private String path(final java.nio.file.Path value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }

  private Long duration(final java.time.Duration value) {
    return value == null ? null : value.toMillis();
  }

  private Integer toIntMillis(final java.time.Duration value) {
    return value == null ? null : Math.toIntExact(value.toMillis());
  }

  private Integer toIntBytes(final org.springframework.util.unit.DataSize value) {
    return value == null ? null : Math.toIntExact(value.toBytes());
  }
}
