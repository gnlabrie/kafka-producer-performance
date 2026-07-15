package com.kafkaproducertest.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import com.kafkaproducertest.config.SensitiveValueMasker;
import com.kafkaproducertest.execution.ProducerRunExecutor;
import com.kafkaproducertest.execution.RunResult;
import com.kafkaproducertest.kafka.NativeKafkaProducerFactory;
import com.kafkaproducertest.kafka.ProducerConfigFactory;
import com.kafkaproducertest.kafka.TopicProvisioner;
import com.kafkaproducertest.payload.HeaderFactory;
import com.kafkaproducertest.payload.KeyGenerator;
import com.kafkaproducertest.payload.PayloadGenerator;
import com.kafkaproducertest.statistics.SummaryWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.mock.env.MockEnvironment;

class TopicProvisioningIT {
  private static EmbeddedKafkaBroker embeddedKafka;

  @TempDir Path temporaryDirectory;

  @BeforeAll
  static void startBroker() {
    embeddedKafka = new EmbeddedKafkaZKBroker(1, true, 1);
    embeddedKafka.afterPropertiesSet();
  }

  @AfterAll
  static void stopBroker() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  @Test
  void createsMissingTopicWithConfiguredPartitionsThenProduces() throws Exception {
    String topicName = "provisioned-it-" + System.nanoTime();
    KafkaProducerTestProperties properties = baseProperties(topicName);
    properties.getTopic().setCreateIfAbsent(true);
    properties.getTopic().setPartitions(2);
    properties.getTopic().setReplicationFactor((short) 1);

    ProducerConfigFactory configFactory = new ProducerConfigFactory();
    RunResult result =
        new ProducerRunExecutor(
                properties,
                configFactory,
                new NativeKafkaProducerFactory(),
                new TopicProvisioner(configFactory),
                new PayloadGenerator(properties),
                new KeyGenerator(properties),
                new HeaderFactory(properties),
                new SummaryWriter(new ObjectMapper().registerModule(new JavaTimeModule())),
                new SensitiveValueMasker(),
                new MockEnvironment())
            .execute();

    assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);
    assertThat(describe(topicName).partitions()).hasSize(2);
  }

  @Test
  void leavesExistingTopicPartitionsUnchanged() throws Exception {
    String topicName = "existing-it-" + System.nanoTime();
    embeddedKafka.addTopics(new org.apache.kafka.clients.admin.NewTopic(topicName, 1, (short) 1));

    KafkaProducerTestProperties properties = baseProperties(topicName);
    properties.getTopic().setCreateIfAbsent(true);
    properties.getTopic().setPartitions(3);

    ProducerConfigFactory configFactory = new ProducerConfigFactory();
    new TopicProvisioner(configFactory).ensureTopic(properties);

    assertThat(describe(topicName).partitions()).hasSize(1);
  }

  private KafkaProducerTestProperties baseProperties(final String topicName) {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getRun().setTopic(topicName);
    properties.getRun().setMessageCount(2L);
    properties.getRun().setShutdownTimeout(Duration.ofSeconds(15));
    properties.getConnection().setBootstrapServers(List.of(embeddedKafka.getBrokersAsString()));
    properties.getProducer().setAcks("1");
    properties.getProducer().setEnableIdempotence(false);
    properties.getProducer().setRetries(0);
    properties.getStatistics().setOutputDirectory(temporaryDirectory);
    properties.getSummary().setOutputDirectory(temporaryDirectory);
    properties.getSummary().setPrintConsole(false);
    properties.getSummary().setWriteJson(false);
    return properties;
  }

  private TopicDescription describe(final String topicName) throws Exception {
    Map<String, Object> adminProps =
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    try (AdminClient admin = AdminClient.create(adminProps)) {
      return admin
          .describeTopics(List.of(topicName))
          .allTopicNames()
          .get(30, TimeUnit.SECONDS)
          .get(topicName);
    }
  }
}
