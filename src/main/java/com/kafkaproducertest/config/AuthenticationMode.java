package com.kafkaproducertest.config;

/** Supported Kafka authentication transports. */
public enum AuthenticationMode {
  PLAINTEXT,
  SSL,
  MTLS,
  SASL_PLAINTEXT_GSSAPI,
  SASL_SSL_GSSAPI
}
