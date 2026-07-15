package com.kafkaproducertest.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/** Produces deterministic UTC output filenames without filesystem side effects. */
public final class OutputFilenameGenerator {
  private static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  private OutputFilenameGenerator() { }

  /** Returns a filename using prefix, UTC start time, run id, and extension. */
  public static String create(
      final String prefix, final Instant startedAt, final UUID runId, final String extension) {
    return prefix + "-" + TIMESTAMP.format(startedAt) + "-" + runId + "." + extension;
  }
}
