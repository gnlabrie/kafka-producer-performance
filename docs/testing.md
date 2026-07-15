# Testing

Run the standard build and quality gates:

```bash
mvn clean verify
```

The verification build compiles the Java 21 application, runs unit and standard integration tests, Checkstyle, SpotBugs, JaCoCo, and Maven Enforcer. It excludes infrastructure-dependent TLS and GSSAPI groups.

Run optional security integration suites only in a provisioned test environment:

```bash
mvn verify -Pssl-integration
mvn verify -Pmtls-integration
mvn verify -Pgssapi-integration
```

Tests must use disposable infrastructure and fictional configuration. Never place real brokers, certificates, keytabs, principals, or credentials in test resources or command output.
