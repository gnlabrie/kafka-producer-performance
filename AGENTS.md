# Kafka Producer Test Tool — Agent Instructions

## Project purpose

This repository contains a Spring Boot command-line tool for generating controlled Kafka producer workloads.

The tool must:

- connect to Kafka using PLAINTEXT, SSL/mTLS, SASL_PLAINTEXT with GSSAPI, or SASL_SSL with GSSAPI;
- generate fixed-length or variable-length message payloads;
- support Kafka compression codecs;
- expose important Kafka producer properties through YAML configuration;
- run for a message count or a specified duration;
- collect per-message send and acknowledgement timing;
- write detailed statistics to CSV;
- produce a complete summary when the run ends.

Do not add consumer or Kafka Streams functionality unless the current issue explicitly requests it. Structure the implementation so consumer and Streams modules can be added later.

## Technology baseline

- Java 21
- Spring Boot 3.5.x
- Maven
- Spring for Apache Kafka
- Spring Boot Configuration Processor
- Jackson YAML
- Apache Commons CSV
- HdrHistogram
- JUnit 5
- AssertJ
- Mockito
- Testcontainers Kafka
- ArchUnit
- Maven Enforcer
- Checkstyle
- SpotBugs
- JaCoCo

Do not upgrade Java, Spring Boot, Spring Kafka, Kafka clients, Maven plugins, or test-container images without explaining compatibility implications.

## Configuration strategy

YAML configuration files are the primary user interface.

Use:

1. packaged `application.yaml` for safe defaults;
2. optional Spring profiles for environment-level defaults;
3. one external run configuration file;
4. environment variables for secrets and environment-specific values;
5. limited command-line overrides using standard Spring Boot properties.

Spring profiles are appropriate for:

- local;
- development;
- staging;
- production.

Spring profiles are not the preferred mechanism for frequently changing test scenarios such as:

- duration;
- message count;
- payload size;
- compression;
- acknowledgement mode;
- producer batching;
- rate limits.

Use external run YAML files for those scenarios.

The normal launch must be simple:

```bash
./bin/kafka-producer-test.sh config/runs/staging-mtls-variable-5m.yaml
```

## Architecture

Use package-by-feature organization:

- `cli`: administrative commands and launch modes
- `config`: typed configuration, merging, sanitization, and validation
- `kafka`: producer creation and Kafka interactions
- `payload`: message key, payload, and header generation
- `execution`: run lifecycle, rate control, termination, and shutdown
- `statistics`: per-message measurements, CSV output, histograms, and summaries
- `util`: small reusable utilities

Maintain the following dependency direction:

```text
cli -> config -> execution
execution -> kafka
execution -> payload
execution -> statistics
kafka -> config
payload -> config
statistics -> config
```

Do not allow statistics, payload, or configuration classes to depend on CLI classes.

## Kafka implementation rules

Use the native `KafkaProducer<byte[], byte[]>` API for the test execution path.

Do not hide Kafka producer properties behind unnecessary custom terminology. Internally use Kafka property names such as:

- `acks`
- `batch.size`
- `buffer.memory`
- `compression.type`
- `delivery.timeout.ms`
- `enable.idempotence`
- `linger.ms`
- `max.block.ms`
- `max.in.flight.requests.per.connection`
- `max.request.size`
- `request.timeout.ms`
- `retries`

Configuration precedence within producer configuration is:

1. explicitly typed producer properties;
2. entries in `producer.properties`;
3. Kafka client defaults.

When the same property is set in typed configuration and `producer.properties`, use the typed value and report a sanitized warning.

Capture and report effective producer values after all configuration sources have been merged.

Do not log passwords, key passwords, truststore passwords, keystore passwords, JAAS configuration, ticket-cache contents, private keys, keytab contents, or complete authentication exceptions containing secrets.

Validate incompatible producer properties before opening the producer.

Do not call `Future.get()` after every send. Sends must remain asynchronous.

The Kafka producer callback must perform minimal work. It may create and enqueue an immutable statistics record, but it must not write files, block on locks, perform DNS requests, or calculate the final summary.

## Timing rules

Use `Instant.now()` for human-readable UTC timestamps.

Use `System.nanoTime()` for elapsed time and latency calculations.

For every message capture:

- sequence number;
- creation timestamp;
- send-start timestamp;
- send-return timestamp;
- callback timestamp;
- creation-to-send duration;
- send-call duration;
- acknowledgement latency;
- total creation-to-acknowledgement latency;
- topic;
- partition;
- offset;
- key size;
- payload size;
- success or failure;
- sanitized failure information.

Never calculate latency by subtracting wall-clock timestamps.

## Message rules

Every Kafka record must include these UTF-8 headers when default headers are enabled:

- `kpt-created-at`
- `kpt-created-at-epoch-ms`
- `kpt-sequence-number`
- `kpt-run-id`
- `kpt-payload-length`

Message length means the serialized Kafka value length in bytes.

For fixed-length generation, the output must contain exactly the requested number of bytes.

For variable-length generation, the generated length must be within the inclusive minimum and maximum.

Random generation must support an optional deterministic seed.

Payload modes, content modes, key modes, compression types, authentication modes, execution modes, and run statuses must be enums.

Do not select behavior through repeated raw string comparisons.

Reserved headers beginning with `kpt-` cannot be overridden by custom configuration.

## Statistics rules

CSV writing must occur on a dedicated writer thread.

Use a bounded queue between Kafka callbacks and the CSV writer.

Never silently discard statistics. If the queue is full:

1. increment the dropped-statistics counter;
2. mark the run incomplete;
3. stop the run by default.

Write CSV using Apache Commons CSV.

The CSV header must be written exactly once.

CSV timestamps must be UTC ISO-8601.

The run summary must include:

- sanitized effective configuration;
- start and end times;
- elapsed duration;
- attempted, accepted, acknowledged, failed, and outstanding counts;
- generated payload bytes;
- messages per second;
- payload throughput;
- minimum, average, maximum, and percentile latency;
- errors grouped by exception class;
- records grouped by partition;
- statistics rows written and dropped;
- detailed statistics filename;
- summary filename;
- application log filename;
- run status.

## Java coding style

Use constructor injection.

Prefer immutable records or final classes for values and measurements.

Use `Duration`, `DataSize`, `Path`, and enums rather than unqualified primitive values where practical.

Do not use field injection.

Do not use `System.out.println` outside the CLI presentation layer.

Do not catch `Exception` unless performing top-level shutdown or converting it into a defined application failure.

Do not suppress exceptions without recording their effect on run status.

Do not introduce Lombok.

Do not create generic `Helper`, `Manager`, or `Utils` classes. Use names that describe the responsibility.

Keep methods focused. Extract a component when a method mixes configuration, Kafka operations, statistics, and presentation logic.

Public APIs and configuration properties require Javadoc.

## Configuration style

Use typed `@ConfigurationProperties`.

Prefer immutable Java records where Spring Boot binding remains clear and testable.

Validate configuration before creating network clients or output files.

Configuration errors must:

- state the invalid property path;
- state why it is invalid;
- show the expected form when practical;
- avoid displaying secrets;
- return a distinct non-zero process exit code.

Exactly one run limit must normally be configured:

- message count; or
- duration.

Fixed message length cannot be combined with minimum or maximum message length.

Variable message length requires both minimum and maximum values, and minimum must not exceed maximum.

Default compression is `none`.

## Security rules

Support:

- PLAINTEXT;
- SSL server authentication;
- mTLS;
- SASL_PLAINTEXT with GSSAPI;
- SASL_SSL with GSSAPI.

Prefer typed security fields over raw JAAS text.

Allow externally supplied JAAS configuration only as an advanced escape hatch.

Secrets must be supplied through environment variables or protected external configuration.

Never include real credentials, internal certificates, keytabs, principals, or corporate hostnames in committed examples.

Centralize sensitive-key detection and value masking.

Security-related tests must verify that logs, summaries, validation output, and effective-configuration output do not expose secrets.

## Testing requirements

Every new production class must have tests unless it is a trivial Spring Boot entry point.

Use:

- JUnit 5;
- AssertJ;
- Mockito only at external boundaries;
- Testcontainers for Kafka integration;
- parameterized tests for configuration combinations;
- temporary directories for file output tests.

### Unit tests

Test:

- Spring Boot configuration binding;
- duration and data-size binding;
- external file loading;
- profile override behavior;
- configuration import behavior;
- environment-variable substitution;
- missing required environment variables;
- configuration precedence;
- cross-field validation;
- secret masking;
- fixed payload length;
- variable payload boundaries;
- deterministic seeded payloads;
- header creation;
- callback timing conversion;
- percentile summary generation;
- CSV escaping;
- filename generation;
- run termination by count;
- run termination by duration.

### Integration tests

Test:

- complete external YAML loading;
- profile activation;
- successful PLAINTEXT production;
- exact payload lengths;
- supported compression settings;
- default and custom headers;
- acknowledgement metadata;
- count-limited runs;
- duration-limited runs;
- graceful shutdown;
- unavailable broker handling;
- CSV row count;
- summary count consistency.

### Security integration tests

Maintain separate Maven profiles for:

- SSL server authentication;
- mTLS client authentication;
- SASL/GSSAPI.

Do not make GSSAPI tests mandatory in the normal unit-test phase unless the test environment provides a KDC.

### Architecture tests

Use ArchUnit to enforce package dependency direction and prevent CLI classes from leaking into domain components.

## Quality gates

Before declaring a task complete, run:

```bash
./mvnw clean verify
```

The build must include:

- compilation;
- unit tests;
- integration tests enabled by the standard verification profile;
- Checkstyle;
- SpotBugs;
- JaCoCo coverage checks;
- Maven Enforcer.

Do not reduce test coverage thresholds, disable static-analysis rules, or skip tests to make a build pass.

## Documentation rules

Update documentation in the same change as behavior.

Update `README.md` when changing:

- startup commands;
- required configuration;
- output format;
- authentication;
- supported producer properties.

Update `docs/configuration-reference.md` when adding or changing configuration.

Update `docs/configuration-composition.md` when changing profile, import, or precedence behavior.

Update `docs/output-formats.md` when changing CSV or summary fields.

Add an ADR for significant architectural changes.

All command examples must use placeholder hostnames and secret environment variables.

## Change workflow

Before modifying code:

1. inspect the relevant code and tests;
2. identify the smallest coherent change;
3. state assumptions in implementation notes;
4. add or update tests first when practical;
5. implement the change;
6. run targeted tests;
7. run the full verification build;
8. update documentation;
9. summarize changed files and remaining limitations.

Do not rewrite unrelated code.

Do not change configuration property names without preserving backward compatibility or documenting a migration.

Do not run destructive Kafka administration commands.

Do not create, alter, or delete Kafka topics unless the current task explicitly requires it.

## Definition of done

A change is complete only when:

- requirements are implemented;
- invalid configurations fail clearly;
- secrets are sanitized;
- unit and relevant integration tests pass;
- CSV and summary output remain consistent;
- graceful shutdown is tested;
- documentation is updated;
- `./mvnw clean verify` passes.
