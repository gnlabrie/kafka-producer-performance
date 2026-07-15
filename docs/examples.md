# Examples

Build first with `mvn package`, then run from the repository root.

## Local PLAINTEXT smoke test

```bash
./bin/kafka-producer-test.sh config/runs/local-plaintext.yaml
```

Set `KAFKA_BOOTSTRAP_SERVERS` to override the local profile endpoint.

Scenario files may set `topic.create-if-absent` with `partitions` and `replication-factor` so the tool creates a missing topic with those settings before producing. Existing topics are left unchanged.

## Staging mTLS variable payloads

```bash
export KAFKA_BOOTSTRAP_SERVERS='kafka.staging.example.com:9093'
export TRUSTSTORE_PATH='/secure/path/truststore.p12'
export TRUSTSTORE_PASSWORD='...'
export KEYSTORE_PATH='/secure/path/keystore.p12'
export KEYSTORE_PASSWORD='...'
export KEY_PASSWORD='...'
./bin/validate-config.sh config/runs/staging-mtls-variable-5m.yaml
./bin/kafka-producer-test.sh config/runs/staging-mtls-variable-5m.yaml
```

## Production GSSAPI controlled rate

```bash
export KAFKA_BOOTSTRAP_SERVERS='kafka.production.example.com:9093'
export TRUSTSTORE_PATH='/secure/path/truststore.p12'
export TRUSTSTORE_PASSWORD='...'
export GSSAPI_PRINCIPAL='producer-test@EXAMPLE.COM'
export GSSAPI_KEYTAB='/secure/path/producer-test.keytab'
./bin/show-effective-config.sh config/runs/production-gssapi-controlled-rate.yaml
```

Review the masked effective configuration, then run the workload only with approved credentials and topic access.
