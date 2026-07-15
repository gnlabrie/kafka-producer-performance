# Architecture

The tool is a non-web Spring Boot command-line application. The execution path uses the native `KafkaProducer<byte[], byte[]>` API so sends remain asynchronous.

Package responsibilities:

- `cli`: command modes and console presentation.
- `config`: typed binding, merging, validation, effective configuration, and masking.
- `execution`: run lifecycle, limits, rate control, and shutdown.
- `kafka`: producer creation and Kafka interaction.
- `payload`: keys, values, and headers.
- `statistics`: callback measurements, bounded queue, CSV writer, histograms, and summaries.
- `util`: small reusable primitives.

Dependencies flow from `cli` to `config` and `execution`; execution depends on Kafka, payload, and statistics. The callback only creates and enqueues an immutable measurement. A dedicated statistics thread writes CSV, preventing file I/O from distorting producer callbacks.

See [ADR 0001](adr/0001-yaml-primary-configuration.md) for the configuration decision.
