package com.kafkaproducertest.integration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class SecurityInfrastructureIT {
  @Test
  @Tag("ssl")
  @Disabled("Requires dedicated SSL broker and truststore infrastructure")
  void sslInfrastructurePlaceholder() {}

  @Test
  @Tag("mtls")
  @Disabled("Requires dedicated mTLS broker, truststore, and keystore infrastructure")
  void mtlsInfrastructurePlaceholder() {}

  @Test
  @Tag("gssapi")
  @Disabled("Requires dedicated Kerberos KDC and GSSAPI broker infrastructure")
  void gssapiInfrastructurePlaceholder() {}
}
