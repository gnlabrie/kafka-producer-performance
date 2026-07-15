package com.kafkaproducertest.statistics;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Transfers callback measurements to a dedicated bounded CSV writer thread. */
public final class StatisticsPipeline implements AutoCloseable {
  private final BlockingQueue<MessageStatistic> queue;
  private final AtomicLong dropped = new AtomicLong();
  private final AtomicLong written = new AtomicLong();
  private final AtomicBoolean incomplete = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread writerThread;
  private final LatencyHistogram histogram;

  /** Starts a dedicated writer for the supplied CSV path. */
  public StatisticsPipeline(
      final KafkaProducerTestProperties properties,
      final Path csvFile,
      final LatencyHistogram histogram)
      throws IOException {
    queue = new ArrayBlockingQueue<>(properties.getStatistics().getQueueCapacity());
    this.histogram = histogram;
    CsvStatisticsWriter writer = new CsvStatisticsWriter(csvFile);
    writerThread = Thread.ofPlatform().name("kpt-statistics-writer").start(() -> writeLoop(writer));
  }

  /** Offers a callback result without blocking Kafka's callback thread. */
  public boolean submit(final MessageStatistic statistic) {
    if (queue.offer(statistic)) {
      return true;
    }
    dropped.incrementAndGet();
    incomplete.set(true);
    return false;
  }

  /** Returns whether data loss made the run incomplete. */
  public boolean isIncomplete() {
    return incomplete.get();
  }

  /** Returns dropped detailed statistics rows. */
  public long getDropped() {
    return dropped.get();
  }

  /** Returns successfully written detailed statistics rows. */
  public long getWritten() {
    return written.get();
  }

  private void writeLoop(final CsvStatisticsWriter writer) {
    try (writer) {
      while (running.get() || !queue.isEmpty()) {
        MessageStatistic statistic = queue.poll(100, TimeUnit.MILLISECONDS);
        if (statistic != null) {
          writer.write(statistic);
          if (statistic.success()) {
            histogram.record(statistic.acknowledgementMicros());
          }
          written.incrementAndGet();
        }
      }
    } catch (IOException exception) {
      incomplete.set(true);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      incomplete.set(true);
    }
  }

  /** Drains all queued data and terminates the writer. */
  @Override
  public void close() {
    running.set(false);
    try {
      writerThread.join();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      incomplete.set(true);
    }
  }
}
