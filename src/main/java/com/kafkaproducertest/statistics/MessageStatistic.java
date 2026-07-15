package com.kafkaproducertest.statistics;

import java.time.Instant;

/** Immutable timing and result measurement for one produced message. */
public record MessageStatistic(
    String runId,
    long sequenceNumber,
    Instant createdAt,
    Instant sendStartedAt,
    Instant sendReturnedAt,
    Instant callbackAt,
    long creationToSendMicros,
    long sendCallMicros,
    long acknowledgementMicros,
    long totalMicros,
    String topic,
    Integer partition,
    Long offset,
    int keySize,
    int payloadSize,
    String compressionType,
    boolean success,
    String errorClass,
    String sanitizedErrorMessage) { }
