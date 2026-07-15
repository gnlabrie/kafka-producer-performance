package com.kafkaproducertest.statistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Writes message measurements to a CSV file on the statistics writer thread. */
public final class CsvStatisticsWriter implements AutoCloseable {
  private final CSVPrinter printer;

  /** Creates the detailed CSV output and writes its single header row. */
  public CsvStatisticsWriter(final Path file) throws IOException {
    Path parent = file.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    printer =
        new CSVPrinter(
            Files.newBufferedWriter(file),
            CSVFormat.DEFAULT
                .builder()
                .setHeader(
                    "run_id",
                    "sequence_number",
                    "created_at",
                    "send_started_at",
                    "send_returned_at",
                    "acknowledgement_at",
                    "creation_to_send_us",
                    "send_call_us",
                    "acknowledgement_us",
                    "total_us",
                    "topic",
                    "partition",
                    "offset",
                    "key_size",
                    "payload_size",
                    "compression_type",
                    "success",
                    "error_class",
                    "error_message")
                .get());
  }

  /** Appends one statistic row. */
  public void write(final MessageStatistic value) throws IOException {
    printer.printRecord(
        value.runId(),
        value.sequenceNumber(),
        value.createdAt(),
        value.sendStartedAt(),
        value.sendReturnedAt(),
        value.callbackAt(),
        value.creationToSendMicros(),
        value.sendCallMicros(),
        value.acknowledgementMicros(),
        value.totalMicros(),
        value.topic(),
        value.partition(),
        value.offset(),
        value.keySize(),
        value.payloadSize(),
        value.compressionType(),
        value.success(),
        value.errorClass(),
        value.sanitizedErrorMessage());
  }

  /** Flushes and closes the CSV stream. */
  @Override
  public void close() throws IOException {
    printer.close();
  }
}
