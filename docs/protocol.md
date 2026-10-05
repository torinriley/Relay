<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Wire protocol

Relay uses UTF-8 newline-delimited JSON over TCP on port 7400. Each line is one complete request or response. `LinesCodec` handles partial reads, combined reads, and framing; lines over 1 MiB are rejected. One connection may carry sequential requests. Idle reads time out after 30 seconds.

Requests are tagged by `type`:

```json
{"type":"submit","job":{"queue":"email","payload":{"user_id":42},"priority":"high","max_attempts":5,"delay_ms":0,"retry_policy":{"kind":"exponential","base_ms":1000,"max_delay_ms":60000,"jitter":true}}}
{"type":"lease","worker_id":"worker_7","queues":["email"],"lease_ms":30000}
{"type":"renew","job_id":"job_...","worker_id":"worker_7","lease_token":"...","lease_ms":30000}
{"type":"ack","job_id":"job_...","worker_id":"worker_7","lease_token":"..."}
{"type":"fail","job_id":"job_...","worker_id":"worker_7","lease_token":"...","error":"timeout"}
```

A lease returns `{"type":"job","job":{...},"lease_token":"..."}` or `{"type":"no_job"}`. Mutations return `{"type":"ok"}`. Errors are structured as `{"type":"error","code":"stale_lease","message":"..."}`. Stable error codes are `malformed_request`, `invalid`, `not_found`, `stale_lease`, and `internal`.

Malformed data affects only its connection. Queue names, subscription counts, payloads, frames, errors, lease durations, and connections are bounded. The protocol is currently unauthenticated and intended for a trusted private network.
