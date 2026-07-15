package com.kafkaproducertest.config;

/** Kafka record key generation mode. */
public enum KeyMode {
  NONE,
  SEQUENCE,
  RANDOM,
  FIXED
}
