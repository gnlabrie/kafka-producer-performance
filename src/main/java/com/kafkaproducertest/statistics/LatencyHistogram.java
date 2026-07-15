package com.kafkaproducertest.statistics;

import org.HdrHistogram.Histogram;

/** Thread-safe acknowledgement latency percentile accumulator in microseconds. */
public final class LatencyHistogram {
  private final Histogram histogram = new Histogram(3);

  /** Records a non-negative latency in microseconds. */
  public synchronized void record(final long micros) {
    histogram.recordValue(Math.max(0, micros));
  }

  /** Returns a percentile latency in microseconds. */
  public synchronized long percentile(final double percentile) {
    return histogram.getValueAtPercentile(percentile);
  }

  /** Returns the minimum latency. */
  public synchronized long minimum() {
    return histogram.getMinValue();
  }

  /** Returns the maximum latency. */
  public synchronized long maximum() {
    return histogram.getMaxValue();
  }

  /** Returns the mean latency. */
  public synchronized double mean() {
    return histogram.getMean();
  }
}
