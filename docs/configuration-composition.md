# Configuration composition

Use a run file as the launch argument. It imports reusable profile, security, and scenario fragments:

```yaml
spring:
  profiles:
    active: staging
  config:
    import:
      - optional:file:./config/profiles/application-staging.yaml
      - optional:file:./config/security/mtls.yaml
      - optional:file:./config/scenarios/variable-1k-1m-5minutes.yaml
kafka-producer-test:
  run:
    name: staging-mtls-variable-5m
```

Imports resolve relative to the process working directory; invoke wrappers from the repository root. The `optional:` prefix permits an intentionally absent local fragment, but a complete run must still pass validation.

Precedence, from low to high, is packaged `src/main/resources/application.yaml`, external additional configuration, imported fragments in their declared order, the run file's own values, environment variables, and command-line properties. Typed producer settings override duplicate `producer.properties` entries.

Keep configuration fragments declarative: profiles define stable environment defaults, security files select a transport, and scenarios define a workload. Put the composed run name and only necessary overrides in `config/runs/`.
