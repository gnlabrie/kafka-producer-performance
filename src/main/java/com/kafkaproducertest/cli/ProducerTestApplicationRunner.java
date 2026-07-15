package com.kafkaproducertest.cli;

import com.kafkaproducertest.config.ConfigurationException;
import com.kafkaproducertest.config.ConfigurationValidator;
import com.kafkaproducertest.config.EffectiveConfigurationPrinter;
import com.kafkaproducertest.config.ExecutionMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import com.kafkaproducertest.execution.ProducerRunExecutor;
import com.kafkaproducertest.execution.RunResult;
import com.kafkaproducertest.kafka.ProducerConfigFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

/** Dispatches application modes and exposes deterministic process exit codes. */
@Component
public final class ProducerTestApplicationRunner implements ApplicationRunner, ExitCodeGenerator {
  private final KafkaProducerTestProperties properties;
  private final ConfigurationValidator validator;
  private final EffectiveConfigurationPrinter printer;
  private final ProducerConfigFactory producerConfigFactory;
  private final ProducerRunExecutor executor;
  private int exitCode;

  /** Creates the CLI application runner. */
  public ProducerTestApplicationRunner(
      final KafkaProducerTestProperties properties,
      final ConfigurationValidator validator,
      final EffectiveConfigurationPrinter printer,
      final ProducerConfigFactory producerConfigFactory,
      final ProducerRunExecutor executor) {
    this.properties = properties;
    this.validator = validator;
    this.printer = printer;
    this.producerConfigFactory = producerConfigFactory;
    this.executor = executor;
  }

  /** Validates then dispatches the requested execution mode. */
  @Override
  public void run(final ApplicationArguments arguments) {
    try {
      validator.validate(properties);
      if (properties.getMode() == ExecutionMode.VALIDATE) {
        System.out.println("Configuration validation succeeded.");
        return;
      }
      if (properties.getMode() == ExecutionMode.SHOW_EFFECTIVE_CONFIG) {
        System.out.println(printer.print(producerConfigFactory.create(properties)));
        return;
      }
      RunResult result = executor.execute();
      System.out.println("Run status: " + result.status());
      if (properties.getSummary().isPrintConsole()) {
        System.out.println(
            "Attempted: "
                + result.summary().attempted()
                + ", acknowledged: "
                + result.summary().acknowledged()
                + ", failed: "
                + result.summary().failed()
                + ", throughput: "
                + result.summary().messagesPerSecond()
                + " msg/s");
      }
      if (result.status() == RunStatus.COMPLETED) {
        exitCode = 0;
      } else if (result.status() == RunStatus.INCOMPLETE) {
        exitCode = 3;
      } else {
        exitCode = 2;
      }
    } catch (ConfigurationException exception) {
      System.err.println(exception.getMessage());
      exitCode = 1;
    } catch (RuntimeException | java.io.IOException exception) {
      System.err.println("Kafka producer test failed: " + exception.getMessage());
      exitCode = 2;
    }
  }

  /** Returns the process exit code for SpringApplication.exit. */
  @Override
  public int getExitCode() {
    return exitCode;
  }
}
