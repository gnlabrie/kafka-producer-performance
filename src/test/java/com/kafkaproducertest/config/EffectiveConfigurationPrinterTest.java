package com.kafkaproducertest.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EffectiveConfigurationPrinterTest {
  @Test
  void rendersPrettyJsonWithSensitiveValuesMasked() {
    EffectiveConfigurationPrinter printer =
        new EffectiveConfigurationPrinter(new ObjectMapper(), new SensitiveValueMasker());

    String rendered = printer.print(Map.of("acks", "all", "ssl.keystore.password", "secret"));

    assertThat(rendered).contains("\"acks\" : \"all\"", "\"ssl.keystore.password\" : \"***\"");
  }
}
