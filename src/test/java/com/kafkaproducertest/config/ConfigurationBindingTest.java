package com.kafkaproducertest.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.unit.DataSize;

@SpringBootTest(classes = ApplicationConfig.class)
@ActiveProfiles("test")
class ConfigurationBindingTest {
  @Autowired private KafkaProducerTestProperties properties;

  @Test
  void bindsDurationAndDataSizeFromTestYaml() {
    assertThat(properties.getRun().getTopic()).isEqualTo("binding-topic");
    assertThat(properties.getRun().getDuration()).isEqualTo(Duration.ofSeconds(5));
    assertThat(properties.getPayload().getLength()).isEqualTo(DataSize.ofKilobytes(2));
    assertThat(properties.getTopic().isCreateIfAbsent()).isTrue();
    assertThat(properties.getTopic().getPartitions()).isEqualTo(3);
    assertThat(properties.getTopic().getReplicationFactor()).isEqualTo((short) 1);
    assertThat(properties.getTopic().getConfigs()).containsEntry("retention.ms", "86400000");
  }
}
