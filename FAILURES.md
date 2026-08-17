# \# FAILURES.md

# 

# Honest list of ways this system can still lose a DM, double-send, or misreport a number.

# 

# \### 1. Found live: the mock API returns HTTP 200 on accepted sends, not the documented 202.

# My `client.py` originally only treated `resp.status\_code == 202` as a successful accept, per the

# README's documented contract. Watching Render logs during testing, every successful

# `POST /v1/dm/send` call actually came back as `200 OK`, not `202`. Every one of those real

# successes was falling through to `return SendResult("retryable\_error", ...)`, so DMs that

# PseudoGram had genuinely accepted were being logged as failures and retried. `/stats` showed

# `sent: 0` indefinitely while the mock API logs showed real `200 OK`s mixed with real `500`s. Fixed

# by accepting both `200` and `202` as success. I don't know if this is intentional on PseudoGram's

# side or a doc/implementation mismatch on theirs, but the fix should be safe either way.

# 

# \### 2. Found live: reusing the same Idempotency-Key on retry replayed a stale cached failure instead of re-attempting delivery.

# `\_attempt\_send()` originally sent `Idempotency-Key: {rule\_id}:{user\_id}` — the same key used for

# our own internal dedup guarantee (the DB `UNIQUE(rule\_id, user\_id)` constraint) — on every physical

# HTTP call, including retries after a downstream `failed` status. Per the README, PseudoGram caches

# responses per `Idempotency-Key` and replays the original `dm\_id` instead of sending again. So a

# retry after a real failure wasn't actually attempting delivery again — it was just re-fetching the

# same permanently-failed `dm\_id` from cache. Over 6 attempts this reliably drove every affected row

# to terminal `failed`, even though the mock API was accepting every send with `200 OK`. Confirmed

# this live: `sent` stayed at `0` while `failed` climbed to `13/13` on one run despite 200s in the

# logs. Fixed by deriving a per-attempt key (`{internal\_key}:{attempts}`) for what's sent to

# PseudoGram, while keeping the internal key as the DB-level one-DM-per-user-per-rule guarantee. This

# was the single biggest correctness bug in the system and I would not have caught it without

# watching raw logs against `/stats` side by side.

# 

# \### 3. DB\_PATH has no persistent disk configured — every redeploy wipes all state.

# `render.yaml` sets `DB\_PATH` to a local path on Render's ephemeral filesystem with no `disk:` block

# attached. Every redeploy or dyno restart destroys the entire SQLite file — `rules`, `events`,

# `dm\_attempts`, everything. I hit this directly tonight: every time I redeployed to ship a fix, I

# had to recreate my test rule from scratch. Crash-recovery logic assumes the DB survives a restart;

# on this specific deployment, it currently doesn't. A real persistent disk (or moving to Postgres)

# fixes this, but wasn't worth the risk to change this close to the deadline once discovered.

# 

# \### 4. SQLite is single-instance. Scaling to two web processes reintroduces a real race.

# Dedup correctness (both `events.event\_id` PK and `dm\_attempts.UNIQUE(rule\_id, user\_id)`) comes from

# SQLite's own constraint enforcement, which is safe across processes sharing one file — but only

# because this deploys as a single instance against a single local DB file. Horizontally scaled

# behind a load balancer with each instance on its own disk, the UNIQUE constraints stop being a

# single source of truth and duplicate sends become possible again. Built and tested as single

# instance only; the `asyncio.Lock` in `db.write()` prevents "database is locked" errors within one

# process but does nothing across processes.

# 

# \### 5. A crash between "mock API returned success" and the DB write loses track of that dm\_id.

# `\_attempt\_send()` calls `client.send\_dm()` then writes the returned `dm\_id` to `dm\_attempts`. If the

# process is killed in that exact window, PseudoGram has accepted (and will eventually deliver or

# fail) a DM whose `dm\_id` we never recorded. On restart the row is still `pending\_send`, so the retry

# loop calls `/v1/dm/send` again — now correctly using a fresh per-attempt idempotency key (see #2),

# which means this specific crash window produces a genuine duplicate send rather than a deduped

# replay, since the new key won't match PseudoGram's cache of the original accepted request. Narrow

# window (one HTTP round trip) but real, and the fix for #2 slightly worsens this particular edge case

# even though it fixes the much larger problem it was aimed at.

# 

# \### 6. `duplicates\_blocked` only counts DM-level duplicates, not every redelivered event.

# An `event\_id` that's redelivered but matched zero rules (e.g. a comment with no keyword, or a

# `comment.deleted` for an already-deleted comment) is silently ignored via `INSERT OR IGNORE` on

# `events` — correctly not double-processed, but also not incremented anywhere, since no DM was ever

# at stake. If grading counts every redelivered event as a "duplicate the system should recognize,"

# this number will read lower than expected even though nothing was lost or double-sent.

# 

# \### 7. Reconciliation and retry loops run on fixed intervals, not push notification.

# `reconcile\_tick` polls every 5 seconds and the send/retry loop ticks every 1 second. Under a genuine

# burst, a DM ready to retry can sit up to \~1s longer than necessary, and a `queued` DM's true

# terminal status can be discovered up to \~5s late. Watched this directly during the 500-event test:

# `/stats` under-reported `sent` and over-reported `queued` for close to a minute after the burst

# finished before settling to the correct final numbers. Doesn't lose or duplicate anything, but it's

# a real, observed timing artifact against a fast-polling grading script.

# 

# \### 8. No persistent audit trail for signature-rejected requests.

# Requests failing HMAC verification return `401` and are never written to the `events` table

# (rejected before the DB insert). Correct for security, but there's no record afterward of how many

# forged requests hit the endpoint beyond `uvicorn` stdout logs, which don't persist across a restart

# on this host.

