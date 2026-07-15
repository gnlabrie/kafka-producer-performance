# ADR 0001: YAML is the primary configuration interface

## Status

Accepted.

## Context

Producer performance workloads need repeatable, reviewable scenarios while security and environment defaults vary independently. Spring profiles alone make scenario changes hard to audit and encourage using environment labels for workload tuning.

## Decision

Use external YAML run files as the normal launch interface. A run file composes profile, security, and scenario fragments through `spring.config.import`. Spring profiles remain limited to stable environment defaults. Secrets and environment-specific values come from environment variables or protected external sources.

## Consequences

Workloads are portable and can be reviewed as files. Common TLS and scenario definitions are reusable. Operators must launch from a predictable working directory so relative imports resolve, and must validate compositions before a run. Standard Spring Boot environment and command-line overrides remain available, with documented precedence.
