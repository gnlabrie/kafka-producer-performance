package com.kafkaproducertest.config;

/** Kafka compression codecs. */
public enum CompressionType {
  NONE("none"),
  GZIP("gzip"),
  SNAPPY("snappy"),
  LZ4("lz4"),
  ZSTD("zstd");
  private final String kafkaName;

  CompressionType(final String value) {
    kafkaName = value;
  }

  /** Returns the Kafka configuration value. */
  public String kafkaName() {
    return kafkaName;
  }
}
