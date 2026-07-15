package com.kafkaproducertest.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencyHistogramTest {
  @Test
  void calculatesKnownSamplesAndClampsNegativeLatency() {
    LatencyHistogram histogram = new LatencyHistogram();
    histogram.record(-1);
    histogram.record(10);
    histogram.record(20);
    histogram.record(30);
    histogram.record(40);

    assertThat(histogram.minimum()).isZero();
    assertThat(histogram.maximum()).isEqualTo(40);
    assertThat(histogram.mean()).isEqualTo(20.0);
    assertThat(histogram.percentile(50)).isEqualTo(20);
    assertThat(histogram.percentile(100)).isEqualTo(40);
  }
}
