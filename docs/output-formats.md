# Output formats

Detailed statistics are UTF-8 CSV written by a dedicated thread. The header appears once:

```text
run_id,sequence_number,created_at,send_started_at,send_returned_at,acknowledgement_at,creation_to_send_us,send_call_us,acknowledgement_us,total_us,topic,partition,offset,key_size,payload_size,compression_type,success,error_class,error_message
```

Timestamps are UTC ISO-8601 values. Durations are microseconds derived from `System.nanoTime()`, not wall-clock differences. Failed sends retain sanitized error class and message; Kafka metadata fields may be empty.

The summary records the run identity and status; start, end, and elapsed times; attempted, acknowledged, failed, and outstanding counts; payload bytes and throughput; errors by exception class; records by partition; statistics rows written and dropped; and the statistics filename. JSON output is enabled by default and YAML output is optional.

If the bounded statistics queue overflows, the dropped counter is incremented, the run is incomplete, and the default behavior stops the run. Summary and effective configuration output mask sensitive values.
