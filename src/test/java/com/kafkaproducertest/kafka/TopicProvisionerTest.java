package com.kafkaproducertest.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TopicProvisionerTest {
  private final ProducerConfigFactory configFactory = mock(ProducerConfigFactory.class);
  private final TopicProvisioner provisioner = new TopicProvisioner(configFactory);

  @Test
  void skipsAdminWhenCreateIfAbsentDisabled() {
    KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
    properties.getTopic().setCreateIfAbsent(false);

    provisioner.ensureTopic(properties);

    verify(configFactory, never()).createAdmin(any());
  }

  @Test
  void skipsCreateWhenTopicAlreadyExists() throws Exception {
    AdminClient admin = mock(AdminClient.class);
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    when(admin.listTopics()).thenReturn(listResult);
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("existing-topic")));

    KafkaProducerTestProperties.Topic settings = new KafkaProducerTestProperties.Topic();
    settings.setCreateIfAbsent(true);
    settings.setPartitions(3);

    assertThatCode(() -> provisioner.ensureTopic(admin, "existing-topic", settings))
        .doesNotThrowAnyException();
    verify(admin, never()).createTopics(any());
  }

  @Test
  void createsMissingTopicWithPartitionsReplicationAndConfigs() throws Exception {
    AdminClient admin = mock(AdminClient.class);
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    when(admin.listTopics()).thenReturn(listResult);
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
    when(admin.createTopics(any())).thenReturn(createResult);
    when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));

    KafkaProducerTestProperties.Topic settings = new KafkaProducerTestProperties.Topic();
    settings.setCreateIfAbsent(true);
    settings.setPartitions(3);
    settings.setReplicationFactor((short) 1);
    settings.setConfigs(Map.of("retention.ms", "86400000"));

    provisioner.ensureTopic(admin, "new-topic", settings);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<java.util.Collection<NewTopic>> captor =
        ArgumentCaptor.forClass(java.util.Collection.class);
    verify(admin).createTopics(captor.capture());
    NewTopic created = captor.getValue().iterator().next();
    assertThat(created.name()).isEqualTo("new-topic");
    assertThat(created.numPartitions()).isEqualTo(3);
    assertThat(created.replicationFactor()).isEqualTo((short) 1);
    assertThat(created.configs()).containsEntry("retention.ms", "86400000");
  }

  @Test
  void treatsTopicExistsRaceAsSuccess() throws Exception {
    AdminClient admin = mock(AdminClient.class);
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    @SuppressWarnings("unchecked")
    KafkaFuture<Void> topicExistsFuture = mock(KafkaFuture.class);
    when(admin.listTopics()).thenReturn(listResult);
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
    when(admin.createTopics(any())).thenReturn(createResult);
    when(createResult.all()).thenReturn(topicExistsFuture);
    when(topicExistsFuture.get(anyLong(), any(TimeUnit.class)))
        .thenThrow(new ExecutionException(new TopicExistsException("exists")));

    KafkaProducerTestProperties.Topic settings = new KafkaProducerTestProperties.Topic();
    settings.setPartitions(1);

    assertThatCode(() -> provisioner.ensureTopic(admin, "raced-topic", settings))
        .doesNotThrowAnyException();
  }
}
