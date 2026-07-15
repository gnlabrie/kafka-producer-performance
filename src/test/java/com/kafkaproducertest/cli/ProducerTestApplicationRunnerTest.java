package com.kafkaproducertest.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kafkaproducertest.config.ConfigurationException;
import com.kafkaproducertest.config.ConfigurationValidator;
import com.kafkaproducertest.config.EffectiveConfigurationPrinter;
import com.kafkaproducertest.config.ExecutionMode;
import com.kafkaproducertest.config.KafkaProducerTestProperties;
import com.kafkaproducertest.config.RunStatus;
import com.kafkaproducertest.execution.ProducerRunExecutor;
import com.kafkaproducertest.execution.RunResult;
import com.kafkaproducertest.kafka.ProducerConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationArguments;

class ProducerTestApplicationRunnerTest {
  private final KafkaProducerTestProperties properties = new KafkaProducerTestProperties();
  private final ConfigurationValidator validator = mock(ConfigurationValidator.class);
  private final EffectiveConfigurationPrinter printer = mock(EffectiveConfigurationPrinter.class);
  private final ProducerConfigFactory configFactory = mock(ProducerConfigFactory.class);
  private final ProducerRunExecutor executor = mock(ProducerRunExecutor.class);
  private final ApplicationArguments arguments = mock(ApplicationArguments.class);

  @Test
  void validatesWithoutExecutingInValidateMode() throws Exception {
    properties.setMode(ExecutionMode.VALIDATE);

    ProducerTestApplicationRunner runner = runner();
    runner.run(arguments);

    verify(validator).validate(properties);
    verify(executor, never()).execute();
    assertThat(runner.getExitCode()).isZero();
  }

  @Test
  void printsEffectiveConfigurationWithoutExecuting() throws Exception {
    properties.setMode(ExecutionMode.SHOW_EFFECTIVE_CONFIG);
    Map<String, Object> configuration = Map.of("acks", "all");
    when(configFactory.create(properties)).thenReturn(configuration);
    when(printer.print(configuration)).thenReturn("{ }");

    runner().run(arguments);

    verify(printer).print(configuration);
    verify(executor, never()).execute();
  }

  @Test
  void mapsRunStatusesToProcessExitCodes() throws Exception {
    properties.setMode(ExecutionMode.RUN);
    properties.getSummary().setPrintConsole(false);
    when(executor.execute()).thenReturn(new RunResult(RunStatus.INCOMPLETE, null, null, null));

    ProducerTestApplicationRunner runner = runner();
    runner.run(arguments);

    assertThat(runner.getExitCode()).isEqualTo(3);
  }

  @Test
  void mapsFailedRunToErrorExitCode() throws Exception {
    properties.setMode(ExecutionMode.RUN);
    properties.getSummary().setPrintConsole(false);
    when(executor.execute()).thenReturn(new RunResult(RunStatus.FAILED, null, null, null));

    ProducerTestApplicationRunner runner = runner();
    runner.run(arguments);

    assertThat(runner.getExitCode()).isEqualTo(2);
  }

  @Test
  void mapsConfigurationFailuresToValidationExitCode() throws Exception {
    doThrow(new ConfigurationException(List.of("invalid configuration")))
        .when(validator)
        .validate(properties);

    ProducerTestApplicationRunner runner = runner();
    runner.run(arguments);

    assertThat(runner.getExitCode()).isEqualTo(1);
    verify(executor, never()).execute();
  }

  private ProducerTestApplicationRunner runner() {
    return new ProducerTestApplicationRunner(properties, validator, printer, configFactory, executor);
  }
}
