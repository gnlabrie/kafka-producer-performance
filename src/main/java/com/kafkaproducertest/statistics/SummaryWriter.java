package com.kafkaproducertest.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/** Serializes run summaries as JSON and optional YAML. */
@Component
public final class SummaryWriter {
  private final ObjectMapper json;

  /** Creates a summary writer. */
  public SummaryWriter(final ObjectMapper json) {
    this.json = json;
  }

  /** Writes a JSON summary file. */
  public void writeJson(final RunSummary summary, final Path file) throws IOException {
    write(json, summary, file);
  }

  /** Writes a YAML summary file. */
  public void writeYaml(final RunSummary summary, final Path file) throws IOException {
    write(new ObjectMapper(new YAMLFactory()).findAndRegisterModules(), summary, file);
  }

  private void write(final ObjectMapper mapper, final RunSummary summary, final Path file)
      throws IOException {
    Path parent = file.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    mapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), summary);
  }
}
