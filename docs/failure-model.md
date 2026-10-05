<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Failure model

Relay assumes crash-stop processes, lost connections, delayed messages, duplicate client attempts, and abrupt server termination. It assumes one coordinator owns the SQLite database, a reliable local filesystem, and no Byzantine actors.

| Failure | Behavior |
|---|---|
| Worker crashes before side effect | Lease expires; another worker runs the job |
| Worker crashes after side effect, before ACK | Job can run twice; consumer idempotency is required |
| ACK is delayed past expiry | Rejected by expiry and token checks |
| Old worker returns after reassignment | Its fencing token is rejected |
| Server crashes during transaction | SQLite commits all or none of the transition |
| Server restarts with active leases | All old leases become ready; old tokens are invalid |
| Job repeatedly fails | Backoff is persisted; exhausted jobs become dead |
| Arrival exceeds processing | Durable depth rises; memory and worker leases remain bounded |

At-least-once is a delivery property, not an execution uniqueness property. Relay cannot atomically commit a user's external side effect with its SQLite ACK. Idempotency keys address duplicate submissions only.
