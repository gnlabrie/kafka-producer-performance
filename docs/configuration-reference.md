# Configuration reference

All application settings use the `kafka-producer-test` prefix. Spring Boot accepts kebab-case YAML keys, durations such as `5m`, and data sizes such as `16KB`.

## Run and connection

- `mode`: `RUN`, `VALIDATE`, or `SHOW_EFFECTIVE_CONFIG`.
- `run.name`, `run.description`, `run.topic`, `run.message-count`, `run.duration`.
- `run.messages-per-second`, `run.shutdown-timeout`, `run.fail-fast`, `run.maximum-errors`.
- `connection.bootstrap-servers`: list of `host:port` values.
- `connection.client-id`.

Set exactly one of `run.message-count` or `run.duration`.

## Topic provisioning

The topic **name** remains `run.topic`. Optional create settings live under `topic`:

- `topic.create-if-absent` (default `false`): when `true`, the tool uses AdminClient to create the topic if it is missing.
- `topic.partitions` (default `1`)
- `topic.replication-factor` (default `1`)
- `topic.configs`: map of Kafka topic configuration entries applied only at create time (for example `retention.ms`)

Existing topics are never altered. VALIDATE and SHOW_EFFECTIVE_CONFIG never open AdminClient or create topics. Prefer enabling `create-if-absent` instead of relying on broker auto-create when you need a specific partition count or replication factor.

## Payloads and headers

- `key.mode`: `NONE`, `SEQUENCE`, `RANDOM`, or `FIXED`; `key.fixed-value`, `key.length`.
- `payload.mode`: `FIXED` or `VARIABLE`; `payload.content-mode`, `payload.length`, `payload.minimum-length`, `payload.maximum-length`, `payload.text`, `payload.seed`.
- `headers.include-default-headers` and `headers.custom`.

Fixed payloads use only `length`. Variable payloads require both inclusive bounds. Reserved `kpt-` headers cannot be overridden.

## Producer

Typed properties include `acks`, `compression-type`, `linger`, `batch-size`, `buffer-memory`, `max-request-size`, `retries`, `request-timeout`, `delivery-timeout`, `max-block`, `max-in-flight-requests`, and `enable-idempotence`. Advanced native Kafka settings belong under `producer.properties`.

When both forms specify the same Kafka property, the typed property wins and the effective configuration reports a sanitized warning.

## Output

`statistics.enabled`, `statistics.output-directory`, `statistics.file-name-prefix`, `statistics.queue-capacity`, `statistics.flush-interval`, and `statistics.stop-on-queue-overflow` control detailed measurements. `summary.write-json`, `summary.write-yaml`, `summary.output-directory`, `summary.file-name-prefix`, and `summary.print-console` control summaries.

Security is documented separately in [authentication](authentication.md).
