# Kafka Producer Test Tool

A Java 21, Spring Boot command-line tool for controlled Kafka producer workloads. It sends records asynchronously, captures per-message timing, writes detailed CSV statistics, and produces a run summary.

## Quick start

Build the executable JAR:

```bash
./mvnw package
```

Run an example from Git Bash or another Unix-like shell:

```bash
./bin/kafka-producer-test.sh config/runs/local-plaintext.yaml
```

On Windows Command Prompt:

```cmd
bin\kafka-producer-test.cmd config\runs\local-plaintext.yaml
```

The wrappers check for Java, require a readable configuration file, locate the built JAR, and pass through the application's exit code. They never print environment variables.

## Configuration

YAML files are the primary interface. A run file imports an environment profile, security fragment, and scenario. Use Spring profiles for stable environment defaults; use run files for workloads that change frequently.

```text
config/
├── profiles/     environment defaults
├── security/     transport and authentication fragments
├── scenarios/    payload, rate, and producer tuning
└── runs/         composable runnable configurations
```

Provide credentials and environment-specific endpoints through environment variables, never committed YAML. For example:

```bash
export TRUSTSTORE_PASSWORD='...'
export KEYSTORE_PASSWORD='...'
export KEY_PASSWORD='...'
export KAFKA_BOOTSTRAP_SERVERS='kafka.staging.example.com:9093'
./bin/kafka-producer-test.sh config/runs/staging-mtls-variable-5m.yaml
```

Validate a composed configuration without producing records:

```bash
./bin/validate-config.sh config/runs/staging-mtls-variable-5m.yaml
./bin/show-effective-config.sh config/runs/staging-mtls-variable-5m.yaml
```

See [configuration composition](docs/configuration-composition.md), the [configuration reference](docs/configuration-reference.md), and [authentication](docs/authentication.md).

Topic names stay in `run.topic`. Optional `topic.create-if-absent`, `topic.partitions`, `topic.replication-factor`, and `topic.configs` control AdminClient creation when the topic is missing.

## Output

By default, detailed per-message CSV and JSON summary files are written under `output/`. Configuration and errors are sanitized so secrets are not emitted. See [output formats](docs/output-formats.md).

## Testing

Run all standard quality gates:

```bash
./mvnw clean verify
```

Optional TLS and GSSAPI integration profiles are documented in [testing](docs/testing.md).
