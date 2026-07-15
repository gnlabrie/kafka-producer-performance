package com.kafkaproducertest.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkaproducertest.config.AuthenticationMode;
import com.kafkaproducertest.config.GssapiLoginMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

class ProducerConfigFactoryTest {
  private final ProducerConfigFactory factory = new ProducerConfigFactory();

  @Test
  void typedPropertiesOverrideAdvancedValuesAndPlaintextAddsNoSecurityProperties() {
    KafkaProducerTestProperties properties = base();
    properties.getProducer().setProperties(Map.of(ProducerConfig.ACKS_CONFIG, "0"));

    Map<String, Object> values = factory.create(properties);

    assertThat(values).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
    assertThat(values).doesNotContainKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    assertThat(values)
        .doesNotContainKeys(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
  }

  @Test
  void mapsSslAndMtlsPathsAsStrings() {
    KafkaProducerTestProperties properties = base();
    properties.getSecurity().setMode(AuthenticationMode.MTLS);
    properties.getSecurity().setTruststorePath(Path.of("certs/truststore.p12"));
    properties.getSecurity().setTruststorePassword("trust");
    properties.getSecurity().setKeystorePath(Path.of("certs/keystore.p12"));
    properties.getSecurity().setKeystorePassword("key");

    Map<String, Object> values = factory.create(properties);

    assertThat(values)
        .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
        .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "certs\\truststore.p12")
        .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "certs\\keystore.p12");
  }

  @Test
  void buildsGssapiJaasForKeytabAndTicketCache() {
    KafkaProducerTestProperties keytab = base();
    keytab.getSecurity().setMode(AuthenticationMode.SASL_PLAINTEXT_GSSAPI);
    keytab.getSecurity().setGssapiLoginMode(GssapiLoginMode.KEYTAB);
    keytab.getSecurity().setGssapiKeytab(Path.of("client.keytab"));
    keytab.getSecurity().setGssapiPrincipal("user@EXAMPLE.TEST");
    assertThat(factory.create(keytab).get(SaslConfigs.SASL_JAAS_CONFIG))
        .asString()
        .contains("useKeyTab=true", "client.keytab", "user@EXAMPLE.TEST");

    KafkaProducerTestProperties ticketCache = base();
    ticketCache.getSecurity().setMode(AuthenticationMode.SASL_PLAINTEXT_GSSAPI);
    assertThat(factory.create(ticketCache).get(SaslConfigs.SASL_JAAS_CONFIG))
        .asString()
        .contains("useTicketCache=true");
  }

  @Test
  void mapsSaslSslAndMtlsSecuritySettings() throws Exception {
    KafkaProducerTestProperties saslSsl = base();
    saslSsl.getSecurity().setMode(AuthenticationMode.SASL_SSL_GSSAPI);
    saslSsl.getSecurity().setTruststorePath(java.nio.file.Files.createTempFile("trust", ".p12"));
    saslSsl.getSecurity().setTruststorePassword("trust");

    Map<String, Object> saslSslValues = factory.create(saslSsl);

    assertThat(saslSslValues)
        .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
        .containsEntry(SaslConfigs.SASL_MECHANISM, "GSSAPI")
        .containsEntry(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");

    KafkaProducerTestProperties mtls = base();
    mtls.getSecurity().setMode(AuthenticationMode.MTLS);
    mtls.getSecurity().setTruststorePath(java.nio.file.Files.createTempFile("trust", ".p12"));
    mtls.getSecurity().setTruststorePassword("trust");
    mtls.getSecurity().setKeystorePath(java.nio.file.Files.createTempFile("key", ".p12"));
    mtls.getSecurity().setKeystorePassword("keystore");
    mtls.getSecurity().setKeyPassword("key");

    assertThat(factory.create(mtls))
        .containsEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "keystore")
        .containsEntry(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "key");
  }

  @Test
  void createAdminIncludesBootstrapAndSecurityOnly() {
    KafkaProducerTestProperties properties = base();
    properties.getConnection().setClientId("workload");
    properties.getSecurity().setMode(AuthenticationMode.SSL);
    properties.getSecurity().setTruststorePath(Path.of("certs/truststore.p12"));
    properties.getSecurity().setTruststorePassword("trust");

    Map<String, Object> values = factory.createAdmin(properties);

    assertThat(values)
        .containsEntry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker:9092")
        .containsEntry(CommonClientConfigs.CLIENT_ID_CONFIG, "workload-admin")
        .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
        .containsEntry(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Path.of("certs/truststore.p12").toString())
        .doesNotContainKey(ProducerConfig.ACKS_CONFIG);
  }

  private KafkaProducerTestProperties base() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getConnection().setBootstrapServers(List.of("broker:9092"));
    return properties;
  }
}
