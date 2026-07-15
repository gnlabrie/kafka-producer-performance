package com.kafkaproducertest.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Component;

/** Creates native byte-array Kafka producers for workload execution. */
@Component
public final class NativeKafkaProducerFactory {
  /** Creates a producer from fully merged native Kafka properties. */
  public KafkaProducer<byte[], byte[]> create(final Map<String, Object> properties) {
    return new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer());
  }
}
