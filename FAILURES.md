# FAILURES.md

Honest list of ways this system can still lose a DM, double-send, or misreport a number.

### 1. SQLite is single-instance. Scaling to two web processes reintroduces a real race.
Dedup correctness (both `events.event_id` PK and `dm_attempts.UNIQUE(rule_id, user_id)`) comes
from SQLite's own constraint enforcement, which *is* safe across processes sharing one file — but
only because this deploys as a single instance against a single local DB file. If this were
horizontally scaled behind a load balancer with each instance writing to its own disk (as opposed
to one shared Postgres), the UNIQUE constraints would no longer be a single source of truth and
duplicate sends would become possible again. This app was built and tested as a single instance
only; the asyncio.Lock in `db.write()` prevents "database is locked" errors within one process but
does nothing across processes.

### 2. A crash between "mock API returned 202 queued" and the DB commit loses track of the dm_id.
`_attempt_send()` calls `client.send_dm()` and then writes the returned `dm_id` to `dm_attempts`.
If the process is killed in that exact window, the mock API has accepted and will eventually
deliver (or fail) a DM whose `dm_id` we never recorded. On restart, that row is still
`status='pending_send'`, so our retry loop will call `/v1/dm/send` again — using the *same*
idempotency key (`rule_id:user_id`) — hoping the mock API's `Idempotency-Key` handling returns
the original `dm_id` rather than sending a second message. I have not been able to verify how long
the mock API retains idempotency keys, so if that window has expired by the time we retry, this
becomes a genuine duplicate DM. This is a narrow window (one HTTP round trip) but it's real.

### 3. Reconciliation retries on "failed after accept" can duplicate-send if the idempotency window has closed.
Part C reconciliation (`reconcile_tick`) treats a `queued → failed` transition as retryable: it
resets the row to `pending_send` and lets the send loop try again with the same idempotency key.
The intent is "the API accepted it, then failed it downstream, so let's ask again." But if the
mock API doesn't perpetually honor the same idempotency key for a `failed` DM (i.e. it accepts a
fresh send under the same key instead of deduping), this path can produce a real second delivery
for the same logical DM. I tested the redelivery/duplicate-event_id path directly (see below) but
did not have a way to reliably force a post-accept failure in a short test window to verify this
specific retry path against the real mock API before submitting.

### 4. `duplicates_blocked` only counts DM-level duplicates, not every redelivered event.
An `event_id` that's redelivered but matched zero rules (e.g. a comment with no keyword, or a
`comment.deleted` for an already-deleted comment) is silently ignored via `INSERT OR IGNORE` on
`events` — correctly not double-processed, but also not incremented anywhere, because no DM was
ever at stake. If the grading script's "truth" counts *every* redelivered event as a
"duplicate the system should recognize," our `duplicates_blocked` number will read lower than
theirs even though no DM was ever lost or double-sent. I built the stat this way because the spec
frames it as "DMs you correctly chose not to send," but I'm flagging the ambiguity rather than
guessing silently.

### 5. Reconciliation and retry loops run on fixed intervals, not push notification.
`reconcile_tick` polls every 5 seconds and the send/retry loop ticks every 1 second. Under a genuine
500-events-in-10-seconds burst with many rate-limit backoffs stacking up, a DM that's ready to
retry could sit for up to ~1s longer than strictly necessary, and a `queued` DM's true terminal
status could be discovered up to ~5s late. This doesn't lose or duplicate anything, but it does mean
`/stats` briefly under-reports `sent` and over-reports `queued` relative to the mock API's own
internal state during a burst — a timing artifact, not a correctness bug, but worth naming since the
spec explicitly asks about `/stats` accuracy under load.

### 6. No persistent audit trail for signature-rejected requests.
Requests that fail HMAC verification return `401` and are never written to the `events` table at
all (rejected before the DB insert). This is correct behavior for security, but it means there is
currently no record to inspect afterward if someone wants to know how many forged requests hit the
endpoint — that number exists only in server logs (`uvicorn` stdout), which don't persist across a
restart on most free-tier hosts.
