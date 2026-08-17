# LinkPlease — mini Instagram DM automation

A small backend that watches Instagram comment webhooks from a (deliberately unreliable) mock API,
matches keywords against user-defined rules, and DMs the commenter — exactly once per rule per
user — even though the mock API redelivers events, delivers them out of order, and sometimes lies
about whether a DM actually went through.

Stack: **FastAPI + SQLite (aiosqlite) + httpx**, all async, no external services required beyond
the mock API itself.

## How it works

```
POST /webhook  ──▶  events table (INSERT OR IGNORE on event_id)  ──▶  200 OK (fast)
                            │
                            ▼ (background loop, woken immediately)
                   match comment against rules
                            │
                            ▼
              dm_attempts row claimed atomically
              via UNIQUE(rule_id, user_id)  ──▶ duplicate? logged, nothing sent
                            │
                            ▼
                   send loop (rate-limited, retries
                   429/500 with backoff, gives up on 400/401/403)
                            │
                            ▼
              reconcile loop polls GET /v1/dm/{id} every 5s
              to catch "accepted but later failed" and retry it
```

Full design rationale and known edge cases: see [`FAILURES.md`](./FAILURES.md).

## Run locally

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in PSEUDOGRAM_API_KEY
uvicorn app.main:app --reload --port 8000
```

## Get an API key

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"name":"...", "email":"...", "phone":"...", "linkedin_url":"..."}'

curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email":"..."}'
```

Put the returned `api_key` in `.env` as `PSEUDOGRAM_API_KEY`.

## Test against the real mock API

```bash
# create a rule
curl -X POST http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword":"PRICE","dm_message":"Here'\''s the price list: ..."}'

# fire a load test straight at your deployed /webhook
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"webhook_url":"https://your-deployed-url/webhook","count":500,"duration_seconds":10}'

# check your numbers against ground truth
curl https://pseudogram-api.onrender.com/v1/simulate/{run_id}/truth \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY"

curl http://localhost:8000/stats
```

`test_webhook.py` in this repo also exercises the dedup / out-of-order-delete / forged-signature
paths directly against a local server without touching the real mock API — useful for a fast
sanity check before spending simulate-run quota.

## Deploy

Any host that can run a long-lived Python process works (Render, Railway, Fly.io). A `Procfile`
and `render.yaml` are included for Render. Set `PSEUDOGRAM_API_KEY` as an environment variable in
the host's dashboard — don't commit it.

**Caveat:** this uses SQLite on local disk. On most free-tier hosts (including Render's free web
service) the filesystem is ephemeral across deploys/restarts — fine for a 7-day grading window on
a single running instance, but worth knowing if the dyno restarts and you want history to survive.

## What's implemented

- **Part A** — rules, keyword matching, exactly-once DM per (rule, user), retried sends on
  failure, nothing silently dropped.
- **Part B** — HMAC-SHA256 webhook signature verification (raw body, API key as secret,
  `hmac.compare_digest`); `/stats` reflects live DB state on every call, no caching.
- **Part C** — reconciliation loop polls `GET /v1/dm/{id}` for every `queued` DM and retries ones
  that fail post-acceptance; `comment.deleted` cancels a not-yet-sent DM and is safe whether it
  arrives before or after the corresponding `comment.created`; rate limiting is tracked locally
  (sliding window) plus honored from real `429`/`Retry-After` responses, so a 500-event burst
  backs off instead of hammering the API past its limit.
