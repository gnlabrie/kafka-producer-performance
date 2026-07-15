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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mock.env.MockEnvironment;

class PlaintextProducerIT {
  private static EmbeddedKafkaBroker embeddedKafka;

  @TempDir Path temporaryDirectory;

  @BeforeAll
  static void startBroker() {
    embeddedKafka = new EmbeddedKafkaZKBroker(1, true, 1, "plaintext-it");
    embeddedKafka.afterPropertiesSet();
  }

  @AfterAll
  static void stopBroker() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  @Test
  void completesCountLimitedRunAndWritesCsvRows() throws Exception {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getRun().setTopic("plaintext-it");
    properties.getRun().setMessageCount(3L);
    properties.getRun().setShutdownTimeout(Duration.ofSeconds(15));
    properties.getConnection().setBootstrapServers(List.of(embeddedKafka.getBrokersAsString()));
    properties.getProducer().setAcks("1");
    properties.getProducer().setEnableIdempotence(false);
    properties.getProducer().setRetries(0);
    properties.getStatistics().setOutputDirectory(temporaryDirectory);
    properties.getSummary().setOutputDirectory(temporaryDirectory);
    properties.getSummary().setPrintConsole(false);
    properties.getSummary().setWriteJson(true);

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
    assertThat(result.summary().acknowledged()).isEqualTo(3);
    assertThat(Files.readAllLines(Path.of(result.summary().csvFile()))).hasSize(4);
    assertThat(result.jsonSummaryFile()).exists();

    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps("plaintext-it-group", "true", embeddedKafka);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "plaintext-it");
      var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 3);
      assertThat(records.count()).isEqualTo(3);
      ConsumerRecord<byte[], byte[]> first = records.iterator().next();
      assertThat(first.value()).hasSize(1024);
      assertThat(first.headers().lastHeader("kpt-sequence-number")).isNotNull();
    }
  }
}
