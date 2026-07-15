package com.kafkaproducertest.execution;

import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import com.kafkaproducertest.config.SensitiveValueMasker;
import com.kafkaproducertest.kafka.NativeKafkaProducerFactory;
import com.kafkaproducertest.kafka.ProducerConfigFactory;
import com.kafkaproducertest.kafka.TopicProvisioner;
import com.kafkaproducertest.payload.HeaderFactory;
import com.kafkaproducertest.payload.KeyGenerator;
import com.kafkaproducertest.payload.PayloadGenerator;
import com.kafkaproducertest.statistics.LatencyHistogram;
import com.kafkaproducertest.statistics.MessageStatistic;
import com.kafkaproducertest.statistics.RunSummary;
import com.kafkaproducertest.statistics.RunSummaryBuilder;
import com.kafkaproducertest.statistics.StatisticsPipeline;
import com.kafkaproducertest.statistics.SummaryWriter;
import com.kafkaproducertest.util.OutputFilenameGenerator;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Executes asynchronous count- or duration-limited Kafka producer workloads. */
@Component
public final class ProducerRunExecutor {
  private final KafkaProducerTestProperties properties;
  private final ProducerConfigFactory configFactory;
  private final NativeKafkaProducerFactory producerFactory;
  private final TopicProvisioner topicProvisioner;
  private final PayloadGenerator payloadGenerator;
  private final KeyGenerator keyGenerator;
  private final HeaderFactory headerFactory;
  private final SummaryWriter summaryWriter;
  private final SensitiveValueMasker masker;
  private final Environment environment;

  /** Creates a workload executor. */
  public ProducerRunExecutor(
      final KafkaProducerTestProperties properties,
      final ProducerConfigFactory configFactory,
      final NativeKafkaProducerFactory producerFactory,
      final TopicProvisioner topicProvisioner,
      final PayloadGenerator payloadGenerator,
      final KeyGenerator keyGenerator,
      final HeaderFactory headerFactory,
      final SummaryWriter summaryWriter,
      final SensitiveValueMasker masker,
      final Environment environment) {
    this.properties = properties;
    this.configFactory = configFactory;
    this.producerFactory = producerFactory;
    this.topicProvisioner = topicProvisioner;
    this.payloadGenerator = payloadGenerator;
    this.keyGenerator = keyGenerator;
    this.headerFactory = headerFactory;
    this.summaryWriter = summaryWriter;
    this.masker = masker;
    this.environment = environment;
  }

  /** Runs the configured workload and returns its summary and output locations. */
  public RunResult execute() throws IOException {
    UUID runId =
        properties.getRun().getRunId() == null ? UUID.randomUUID() : properties.getRun().getRunId();
    Instant startedAt = Instant.now();
    long startedNanos = System.nanoTime();
    Path csvFile =
        outputFile(
            properties.getStatistics().getOutputDirectory(),
            properties.getStatistics().getFileNamePrefix(),
            startedAt,
            runId,
            "csv");
    Path jsonFile =
        properties.getSummary().isWriteJson()
            ? outputFile(
                properties.getSummary().getOutputDirectory(),
                properties.getSummary().getFileNamePrefix(),
                startedAt,
                runId,
                "json")
            : null;
    Path yamlFile =
        properties.getSummary().isWriteYaml()
            ? outputFile(
                properties.getSummary().getOutputDirectory(),
                properties.getSummary().getFileNamePrefix(),
                startedAt,
                runId,
                "yaml")
            : null;
    Counters counters = new Counters();
    AtomicBoolean stop = new AtomicBoolean();
    Thread hook = new Thread(() -> stop.set(true), "kpt-shutdown");
    Runtime.getRuntime().addShutdownHook(hook);
    LatencyHistogram histogram = new LatencyHistogram();
    Map<String, Object> producerConfig = configFactory.create(properties);
    topicProvisioner.ensureTopic(properties);
    RunStatus status;
    try (KafkaProducer<byte[], byte[]> producer = producerFactory.create(producerConfig);
        StatisticsPipeline pipeline = new StatisticsPipeline(properties, csvFile, histogram)) {
      sendLoop(producer, pipeline, runId, startedNanos, counters, stop);
      producer.flush();
      awaitAcknowledgements(counters, stop);
      status = status(counters, pipeline, stop);
      RunSummary summary =
          buildSummary(
              runId,
              status,
              startedAt,
              counters,
              pipeline,
              histogram,
              producerConfig,
              csvFile,
              jsonFile,
              yamlFile);
      writeSummary(summary, jsonFile, yamlFile);
      return new RunResult(status, summary, jsonFile, yamlFile);
    } finally {
      Runtime.getRuntime().removeShutdownHook(hook);
    }
  }

  private void sendLoop(
      final KafkaProducer<byte[], byte[]> producer,
      final StatisticsPipeline pipeline,
      final UUID runId,
      final long startedNanos,
      final Counters counters,
      final AtomicBoolean stop) {
    long sequence = 0;
    while (continueRun(sequence, startedNanos, counters, pipeline, stop)) {
      sequence++;
      send(producer, pipeline, runId, sequence, counters);
      rateLimit(sequence, startedNanos, stop);
    }
  }

  private boolean continueRun(
      final long count,
      final long startedNanos,
      final Counters counters,
      final StatisticsPipeline pipeline,
      final AtomicBoolean stop) {
    if (stop.get()
        || (pipeline.isIncomplete() && properties.getStatistics().isStopOnQueueOverflow())) {
      return false;
    }
    if (counters.failed.get() > properties.getRun().getMaximumErrors()) {
      return false;
    }
    if (properties.getRun().getMessageCount() != null) {
      return count < properties.getRun().getMessageCount();
    }
    return System.nanoTime() - startedNanos < properties.getRun().getDuration().toNanos();
  }

  private void send(
      final KafkaProducer<byte[], byte[]> producer,
      final StatisticsPipeline pipeline,
      final UUID runId,
      final long sequence,
      final Counters counters) {
    Instant createdAt = Instant.now();
    long creationNanos = System.nanoTime();
    byte[] key = keyGenerator.generate(sequence);
    byte[] payload = payloadGenerator.generate();
    Instant sendStartedAt = Instant.now();
    long sendStartedNanos = System.nanoTime();
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(
            properties.getRun().getTopic(),
            null,
            null,
            key,
            payload,
            headerFactory.create(createdAt, sequence, runId, payload.length));
    counters.attempted.incrementAndGet();
    counters.payloadBytes.addAndGet(payload.length);
    counters.outstanding.incrementAndGet();
    try {
      producer.send(
          record,
          (metadata, error) ->
              callback(
                  pipeline,
                  runId,
                  sequence,
                  createdAt,
                  sendStartedAt,
                  sendStartedNanos,
                  creationNanos,
                  key,
                  payload,
                  metadata,
                  error,
                  counters));
      Instant sendReturnedAt = Instant.now();
      long sendReturnedNanos = System.nanoTime();
      counters.accepted.incrementAndGet();
      counters.sendReturnedAt.put(sequence, new SendReturn(sendReturnedAt, sendReturnedNanos));
    } catch (RuntimeException exception) {
      counters.outstanding.decrementAndGet();
      counters.failed.incrementAndGet();
      counters.errorsByClass.merge(exception.getClass().getName(), 1L, ProducerRunExecutor::sumCounts);
    }
  }

  private void callback(
      final StatisticsPipeline pipeline,
      final UUID runId,
      final long sequence,
      final Instant createdAt,
      final Instant sendStartedAt,
      final long sendStartedNanos,
      final long creationNanos,
      final byte[] key,
      final byte[] payload,
      final RecordMetadata metadata,
      final Exception error,
      final Counters counters) {
    long callbackNanos = System.nanoTime();
    Instant callbackAt = Instant.now();
    SendReturn returned = counters.sendReturnedAt.remove(sequence);
    Instant sendReturnedAt = returned == null ? sendStartedAt : returned.at();
    long sendReturnedNanos = returned == null ? sendStartedNanos : returned.nanos();
    if (error == null) {
      counters.acknowledged.incrementAndGet();
      counters.recordsByPartition.merge(metadata.partition(), 1L, ProducerRunExecutor::sumCounts);
    } else {
      counters.failed.incrementAndGet();
      counters.errorsByClass.merge(error.getClass().getName(), 1L, ProducerRunExecutor::sumCounts);
    }
    counters.outstanding.decrementAndGet();
    pipeline.submit(
        new MessageStatistic(
            runId.toString(),
            sequence,
            createdAt,
            sendStartedAt,
            sendReturnedAt,
            callbackAt,
            micros(sendStartedNanos - creationNanos),
            micros(sendReturnedNanos - sendStartedNanos),
            micros(callbackNanos - sendReturnedNanos),
            micros(callbackNanos - creationNanos),
            properties.getRun().getTopic(),
            metadata == null ? null : metadata.partition(),
            metadata == null ? null : metadata.offset(),
            key == null ? 0 : key.length,
            payload.length,
            properties.getProducer().getCompressionType().kafkaName(),
            error == null,
            error == null ? null : error.getClass().getName(),
            error == null ? null : sanitize(error.getMessage())));
  }

  private void awaitAcknowledgements(final Counters counters, final AtomicBoolean stop) {
    long deadline = System.nanoTime() + properties.getRun().getShutdownTimeout().toNanos();
    while (counters.outstanding.get() > 0 && System.nanoTime() < deadline && !stop.get()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        stop.set(true);
      }
    }
  }

  private RunStatus status(
      final Counters counters, final StatisticsPipeline pipeline, final AtomicBoolean stop) {
    if (stop.get()) {
      return RunStatus.INTERRUPTED;
    }
    if (pipeline.isIncomplete() || counters.outstanding.get() > 0) {
      return RunStatus.INCOMPLETE;
    }
    return counters.failed.get() > 0 ? RunStatus.FAILED : RunStatus.COMPLETED;
  }

  private RunSummary buildSummary(
      final UUID runId,
      final RunStatus status,
      final Instant startedAt,
      final Counters counters,
      final StatisticsPipeline pipeline,
      final LatencyHistogram histogram,
      final Map<String, Object> producerConfig,
      final Path csvFile,
      final Path jsonFile,
      final Path yamlFile) {
    return new RunSummaryBuilder()
        .build(
            properties,
            runId.toString(),
            status,
            startedAt,
            Instant.now(),
            counters.attempted.get(),
            counters.accepted.get(),
            counters.acknowledged.get(),
            counters.failed.get(),
            counters.payloadBytes.get(),
            counters.errorsByClass,
            counters.recordsByPartition,
            pipeline,
            histogram,
            masker.mask(producerConfig),
            String.join(",", environment.getActiveProfiles()),
            csvFile,
            jsonFile,
            yamlFile);
  }

  private void writeSummary(final RunSummary summary, final Path jsonFile, final Path yamlFile)
      throws IOException {
    if (jsonFile != null) {
      summaryWriter.writeJson(summary, jsonFile);
    }
    if (yamlFile != null) {
      summaryWriter.writeYaml(summary, yamlFile);
    }
  }

  private Path outputFile(
      final Path directory,
      final String prefix,
      final Instant startedAt,
      final UUID runId,
      final String extension) {
    return directory.resolve(OutputFilenameGenerator.create(prefix, startedAt, runId, extension));
  }

  private void rateLimit(final long count, final long startedNanos, final AtomicBoolean stop) {
    Double rate = properties.getRun().getMessagesPerSecond();
    if (rate == null) {
      return;
    }
    long expectedNanos = (long) (count * 1_000_000_000D / rate);
    long remainingNanos = expectedNanos - (System.nanoTime() - startedNanos);
    if (remainingNanos <= 0) {
      return;
    }
    try {
      Thread.sleep(remainingNanos / 1_000_000L, (int) (remainingNanos % 1_000_000L));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      stop.set(true);
    }
  }

  private String sanitize(final String message) {
    if (message == null) {
      return null;
    }
    return message.replaceAll("(?i)(password|secret|jaas|keytab)\\s*=\\s*[^\\s,;]+", "$1=***");
  }

  private long micros(final long nanos) {
    return nanos / 1_000;
  }

  private static Long sumCounts(final Long left, final Long right) {
    long a = left == null ? 0L : left;
    long b = right == null ? 0L : right;
    return a + b;
  }

  private record SendReturn(Instant at, long nanos) { }

  private static final class Counters {
    private final AtomicLong attempted = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong outstanding = new AtomicLong();
    private final AtomicLong payloadBytes = new AtomicLong();
    private final Map<String, Long> errorsByClass = new ConcurrentHashMap<>();
    private final Map<Integer, Long> recordsByPartition = new ConcurrentHashMap<>();
    private final Map<Long, SendReturn> sendReturnedAt = new ConcurrentHashMap<>();
  }
}
