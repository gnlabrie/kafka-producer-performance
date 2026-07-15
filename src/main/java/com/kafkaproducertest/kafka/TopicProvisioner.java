package com.kafkaproducertest.kafka;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Creates the configured topic with partitions, replication factor, and configs when missing and
 * create-if-absent is enabled. Existing topics are left unchanged.
 */
@Component
public final class TopicProvisioner {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicProvisioner.class);
  private static final long ADMIN_TIMEOUT_SECONDS = 60L;

  private final ProducerConfigFactory configFactory;

  /**
   * Creates a topic provisioner.
   *
   * @param configFactory factory that supplies AdminClient connection settings
   */
  public TopicProvisioner(final ProducerConfigFactory configFactory) {
    this.configFactory = configFactory;
  }

  /**
   * Ensures the run topic exists when create-if-absent is enabled.
   *
   * @param properties application configuration
   */
  public void ensureTopic(final KafkaProducerTestProperties properties) {
    if (!properties.getTopic().isCreateIfAbsent()) {
      return;
    }
    Map<String, Object> adminConfig = configFactory.createAdmin(properties);
    try (AdminClient admin = AdminClient.create(adminConfig)) {
      ensureTopic(admin, properties.getRun().getTopic(), properties.getTopic());
    }
  }

  /**
   * Ensures a topic exists using the supplied AdminClient. Visible for unit tests.
   *
   * @param admin Kafka admin client
   * @param topicName topic name from run.topic
   * @param settings topic provisioning settings
   */
  void ensureTopic(
      final AdminClient admin,
      final String topicName,
      final KafkaProducerTestProperties.Topic settings) {
    try {
      Set<String> names = admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (names.contains(topicName)) {
        LOGGER.info(
            "Topic '{}' already exists; leaving partitions and configs unchanged", topicName);
        return;
      }
      NewTopic topic =
          new NewTopic(topicName, settings.getPartitions(), settings.getReplicationFactor());
      if (settings.getConfigs() != null && !settings.getConfigs().isEmpty()) {
        topic.configs(settings.getConfigs());
      }
      CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));
      result.all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      LOGGER.info(
          "Created topic '{}' with partitions={} replication-factor={}",
          topicName,
          settings.getPartitions(),
          settings.getReplicationFactor());
    } catch (ExecutionException exception) {
      if (exception.getCause() instanceof TopicExistsException) {
        LOGGER.info(
            "Topic '{}' already exists; leaving partitions and configs unchanged", topicName);
        return;
      }
      throw new IllegalStateException(
          "Unable to ensure topic '" + topicName + "': " + exception.getCause().getMessage(),
          exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while ensuring topic '" + topicName + "'", exception);
    } catch (TimeoutException exception) {
      throw new IllegalStateException(
          "Timed out while ensuring topic '" + topicName + "'", exception);
    }
  }
}
