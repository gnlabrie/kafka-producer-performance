# Authentication

Supported modes are `PLAINTEXT`, `SSL`, `MTLS`, `SASL_PLAINTEXT_GSSAPI`, and `SASL_SSL_GSSAPI`. The example fragments in `config/security/` use only fictional endpoints and environment-variable placeholders.

- `plaintext.yaml`: no TLS or credentials.
- `ssl.yaml`: server authentication with `TRUSTSTORE_PATH` and `TRUSTSTORE_PASSWORD`.
- `mtls.yaml`: SSL plus `KEYSTORE_PATH`, `KEYSTORE_PASSWORD`, and `KEY_PASSWORD`.
- `gssapi-keytab.yaml`: Kerberos keytab login using `GSSAPI_PRINCIPAL` and `GSSAPI_KEYTAB`.
- `gssapi-ticket-cache.yaml`: Kerberos ticket-cache login.

Set secrets in the execution environment or a protected configuration source. Do not commit passwords, JAAS strings, principals, keytabs, private keys, or ticket-cache data. The effective-configuration output masks sensitive values; it is still intended for controlled operational use.

Use typed security fields first. `security.jaas-config` is an advanced escape hatch and must be supplied securely. TLS paths and Kerberos values are validated before the tool opens a Kafka connection.
