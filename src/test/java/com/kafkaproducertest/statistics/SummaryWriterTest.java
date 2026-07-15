package com.kafkaproducertest.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SummaryWriterTest {
  @TempDir Path temporaryDirectory;

  @Test
  void writesJsonAndYamlToMissingParentDirectories() throws Exception {
    SummaryWriter writer = new SummaryWriter(new ObjectMapper().findAndRegisterModules());
    Path json = temporaryDirectory.resolve("json/summary.json");
    Path yaml = temporaryDirectory.resolve("yaml/summary.yaml");

    writer.writeJson(null, json);
    writer.writeYaml(null, yaml);

    assertThat(Files.readString(json)).isEqualTo("null");
    assertThat(Files.readString(yaml)).contains("null");
  }
}
