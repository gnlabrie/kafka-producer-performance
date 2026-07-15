# Cursor implementation prompt — Kafka Producer Test Tool

Implement the initial version of the Kafka Producer Test Tool described by `AGENTS.md` and `.cursor/rules`.

The application must be driven primarily by Spring Boot YAML configuration files. Do not require individual Kafka producer or workload parameters to be passed as command-line options.

Do not implement Kafka consumer or Kafka Streams functionality in this task.

## Configuration strategy

Use Spring Boot externalized configuration and typed `@ConfigurationProperties`.

Support these configuration layers:

1. packaged `application.yaml`;
2. optional Spring profile configuration such as `application-local.yaml`, `application-staging.yaml`, and `application-production.yaml`;
3. one externally supplied test-run YAML configuration file;
4. environment-variable substitution for secrets and deployment-specific values;
5. limited command-line overrides using standard Spring Boot property syntax.

The normal launch must be:

```bash
./bin/kafka-producer-test.sh config/runs/staging-mtls-variable-5m.yaml
```

Also support direct launch:

```bash
java -jar kafka-producer-test-tool.jar \
  --spring.config.additional-location=file:./config/runs/staging-mtls-variable-5m.yaml
```

Spring profiles must be used for environment-level defaults, not individual test scenarios.

## Phase 1 — Bootstrap

Create a Java 21 Maven Spring Boot 3.5.x non-web command-line project.

Add:

- Spring Boot;
- Spring Kafka;
- Spring Boot Configuration Processor;
- Jackson YAML;
- Apache Commons CSV;
- HdrHistogram;
- JUnit 5;
- AssertJ;
- Mockito;
- Testcontainers Kafka;
- ArchUnit;
- Maven Enforcer;
- Checkstyle;
- SpotBugs;
- JaCoCo;
- Maven Wrapper.

Configure:

```yaml
spring:
  main:
    web-application-type: none
```

Create the package-by-feature structure defined in `AGENTS.md`.

## Phase 2 — Typed configuration

Use the top-level prefix:

```yaml
kafka-producer-test:
```

Create typed configuration sections for:

- execution mode;
- run;
- connection;
- security;
- message key;
- payload;
- headers;
- producer;
- statistics;
- summary.

Use `Duration`, `DataSize`, `Path`, enums, lists, and maps.

Prefer immutable Java records where binding remains clear and testable.

Support values such as:

```yaml
duration: 5m
linger: 100ms
batch-size: 480KB
buffer-memory: 64MB
maximum-length: 1MB
```

Generate Spring Boot configuration metadata.

## Phase 3 — Configuration loading and validation

Load external YAML using standard Spring Boot configuration mechanisms.

Do not manually parse the primary configuration.

Validate before:

- opening Kafka connections;
- creating the producer;
- creating output files;
- generating payloads.

Support execution modes:

- `RUN`;
- `VALIDATE`;
- `SHOW_EFFECTIVE_CONFIG`.

Validation errors must include the complete property path.

Validate:

- bootstrap servers;
- topic;
- security mode;
- exactly one of message count or duration;
- positive count, duration, and rate;
- fixed versus variable payload requirements;
- minimum not greater than maximum;
- positive producer sizes and timeouts;
- SSL truststore requirements;
- mTLS truststore and keystore requirements;
- GSSAPI service-name requirements;
- keytab principal and path requirements;
- ticket-cache and keytab conflicts;
- writable or creatable output directory;
- positive queue and flush settings;
- idempotence, acknowledgements, retry, and maximum-in-flight compatibility.

Validation mode must avoid Kafka connections and output-file creation.

Show-effective-config mode must print only sanitized values.

## Phase 4 — Configuration composition

Support Spring profiles and `spring.config.import`.

Provide examples that compose:

- environment defaults;
- security configuration;
- scenario configuration;
- final run configuration.

Document precedence and override behavior.

Use environment variables for secrets.

Do not provide password defaults.

## Phase 5 — Security modes

Implement:

```java
PLAINTEXT
SSL
MTLS
SASL_PLAINTEXT_GSSAPI
SASL_SSL_GSSAPI
```

Support:

- truststore settings;
- keystore settings;
- endpoint identification;
- GSSAPI service name;
- keytab authentication;
- ticket-cache authentication;
- optional raw JAAS configuration as an advanced escape hatch.

Prefer constructing JAAS from typed fields.

Never expose secrets in logs, summaries, exceptions, validation output, or effective configuration.

## Phase 6 — Producer settings

Expose typed YAML properties for:

- acknowledgements;
- compression type;
- linger;
- batch size;
- buffer memory;
- maximum request size;
- retries;
- request timeout;
- delivery timeout;
- maximum block time;
- maximum in-flight requests;
- idempotence;
- client DNS lookup;
- metadata age;
- socket send buffer;
- socket receive buffer.

Also support:

```yaml
producer:
  properties:
    metadata.max.age.ms: 300000
    connections.max.idle.ms: 540000
```

Typed properties override duplicate entries in `producer.properties`.

Log a sanitized warning for collisions.

Include sanitized effective producer configuration in validation and summary output.

## Phase 7 — Message generation

Support payload modes:

- `FIXED`;
- `VARIABLE`.

Support content modes:

- `RANDOM`;
- `REPEATING`;
- `TEXT`.

Support key modes:

- `NONE`;
- `SEQUENCE`;
- `RANDOM`;
- `FIXED`.

Support deterministic random seed.

Ensure serialized payload length is exact.

Add default UTF-8 headers:

- `kpt-created-at`;
- `kpt-created-at-epoch-ms`;
- `kpt-sequence-number`;
- `kpt-run-id`;
- `kpt-payload-length`.

Support static custom headers.

Reject custom `kpt-*` headers.

## Phase 8 — Execution

Support count-limited runs:

```yaml
run:
  message-count: 100000
```

Support duration-limited runs:

```yaml
run:
  duration: 5m
```

Support optional rate limiting:

```yaml
run:
  messages-per-second: 1000
```

Support shutdown timeout, fail-fast, and maximum-error settings.

Use `KafkaProducer<byte[], byte[]>`.

Do not wait synchronously after each send.

Use `Instant.now()` for traceable timestamps and `System.nanoTime()` for elapsed measurements.

Support graceful termination for:

- count completion;
- duration completion;
- maximum errors;
- statistics queue overflow;
- JVM shutdown;
- fatal Kafka exception;
- unrecoverable output error.

## Phase 9 — Statistics

Create immutable per-message statistics.

Use a bounded queue and dedicated CSV writer thread.

Callbacks must never write directly to disk.

CSV columns:

- run ID;
- sequence number;
- created UTC timestamp;
- send-start UTC timestamp;
- send-return UTC timestamp;
- acknowledgement UTC timestamp;
- creation-to-send microseconds;
- send-call duration microseconds;
- acknowledgement latency microseconds;
- total latency microseconds;
- topic;
- partition;
- offset;
- key byte length;
- value byte length;
- compression type;
- success;
- error class;
- sanitized error message.

Use Apache Commons CSV.

Generate filenames containing prefix, UTC start time, and run ID.

On queue overflow, count the drop, mark the run incomplete, and stop by default.

## Phase 10 — Summary

Print a console summary and write JSON.

Optionally support YAML summary output.

Include:

- run name and description;
- run ID and status;
- start, end, and elapsed time;
- active Spring profiles;
- source configuration filename when available;
- sanitized effective application configuration;
- sanitized effective Kafka producer configuration;
- attempted sends;
- accepted sends;
- acknowledged messages;
- failed messages;
- outstanding messages;
- generated payload bytes;
- messages per second;
- payload MiB per second;
- minimum, average, maximum, p50, p75, p90, p95, p99, and p99.9 acknowledgement latency;
- errors grouped by exception class;
- records grouped by partition;
- statistics rows written and dropped;
- CSV filename;
- JSON summary filename;
- application log filename.

Use HdrHistogram.

Ensure all counts reconcile.

## Phase 11 — Wrapper scripts

Create:

```text
bin/
├── kafka-producer-test.sh
├── validate-config.sh
└── show-effective-config.sh
```

The scripts must:

- verify Java availability;
- verify the configuration file exists and is readable;
- invoke the executable JAR;
- preserve the application exit code;
- avoid printing secret environment variables.

## Phase 12 — Tests

Add unit tests for:

- configuration binding;
- duration and data-size binding;
- external file loading;
- profile overrides;
- configuration imports;
- environment-variable substitution;
- missing required environment variables;
- cross-field validation;
- producer-property precedence;
- duplicate-property resolution;
- effective-configuration sanitization;
- fixed and variable payload generation;
- deterministic seed behavior;
- header creation;
- custom-header validation;
- sensitive-value masking;
- CSV escaping;
- percentile calculations;
- filename generation;
- count termination;
- duration termination.

Add Testcontainers integration tests for:

- complete external YAML loading;
- profile activation;
- successful PLAINTEXT production;
- exact payload length;
- headers;
- acknowledgement callbacks;
- count-limited runs;
- duration-limited runs;
- CSV output;
- summary consistency;
- graceful shutdown;
- unavailable broker handling.

Maintain separate Maven profiles for SSL, mTLS, and GSSAPI integration tests.

Add ArchUnit rules.

## Phase 13 — Example configuration tree

Create:

```text
config/
├── application.yaml
├── profiles/
│   ├── application-local.yaml
│   ├── application-staging.yaml
│   └── application-production.yaml
├── security/
│   ├── plaintext.yaml
│   ├── ssl.yaml
│   ├── mtls.yaml
│   ├── gssapi-keytab.yaml
│   └── gssapi-ticket-cache.yaml
├── scenarios/
│   ├── fixed-1k-10000-messages.yaml
│   ├── variable-1k-1m-5minutes.yaml
│   ├── throughput-lz4.yaml
│   └── latency-acks-all.yaml
└── runs/
    ├── local-plaintext.yaml
    ├── staging-mtls-variable-5m.yaml
    └── production-gssapi-controlled-rate.yaml
```

Use fictional hostnames and environment-variable placeholders.

## Phase 14 — Documentation

Create:

- `README.md`;
- `docs/architecture.md`;
- `docs/configuration-reference.md`;
- `docs/configuration-composition.md`;
- `docs/spring-profiles.md`;
- `docs/authentication.md`;
- `docs/output-formats.md`;
- `docs/examples.md`;
- `docs/testing.md`.

Document why YAML is primary, when profiles are appropriate, composition, precedence, environment variables, secret handling, validation, scripts, statistics, and summaries.

## Completion requirements

Before completing:

1. run targeted tests;
2. run `./mvnw clean verify`;
3. correct compilation, test, Checkstyle, SpotBugs, and coverage failures;
4. show the repository tree;
5. summarize the main configuration and execution classes;
6. provide complete PLAINTEXT, mTLS, and GSSAPI keytab examples;
7. provide wrapper-script examples;
8. provide the CSV header;
9. provide a sample summary;
10. list deferred functionality.

Do not disable tests or quality gates to obtain a successful build.
