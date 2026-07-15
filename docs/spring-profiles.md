# Spring profiles

Profiles represent environment-level defaults, not test cases. This repository supplies `local`, `staging`, and `production` fragments under `config/profiles/`.

A run file activates its matching profile and imports its fragment. The profile controls stable details such as bootstrap endpoints and output directories. Put payload length, duration, message count, compression, acknowledgements, batching, and rate limits in scenario or run files instead.

You can override a profile with standard Spring Boot configuration:

```bash
./bin/kafka-producer-test.sh config/runs/local-plaintext.yaml \
  --spring.profiles.active=staging
```

The supplied wrappers forward the configuration path only; add extra Spring options when invoking `java -jar` directly. Validate the final composition before a production workload.
