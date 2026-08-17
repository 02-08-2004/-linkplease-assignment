import hashlib
import hmac
import logging
import time

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

from app import db, worker, client
from app.config import PSEUDOGRAM_API_KEY

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("linkplease")

app = FastAPI(title="LinkPlease")


@app.on_event("startup")
async def startup():
    await db.init_db()
    await worker.start_background_loops()
    log.info("started up, background loops running")


@app.on_event("shutdown")
async def shutdown():
    await worker.stop_background_loops()
    await client.aclose()
    await db.close_db()


# ---------------------------------------------------------------------------
# POST /webhook
# ---------------------------------------------------------------------------

@app.post("/webhook")
async def webhook(request: Request):
    raw_body = await request.body()

    # --- Part B: verify signature, reject forged requests ---
    sig_header = request.headers.get("X-PseudoGram-Signature", "")
    if PSEUDOGRAM_API_KEY:
        expected = hmac.new(
            PSEUDOGRAM_API_KEY.encode(), raw_body, hashlib.sha256
        ).hexdigest()
        expected_header = f"sha256={expected}"
        if not hmac.compare_digest(expected_header, sig_header):
            # Return 200 anyway? No — a forged request should be rejected.
            # We still respond fast; rejection doesn't require background work.
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {}) or {}

    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="missing event_id or event_type")

    from_ = data.get("from", {}) or {}

    # Insert-or-ignore on event_id is the dedup guarantee for the ~8%
    # redelivered events — this is a real UNIQUE constraint, safe even if
    # two copies of the same event_id land at nearly the same instant.
    async with db.write() as conn:
        await conn.execute(
            """INSERT OR IGNORE INTO events
               (event_id, event_type, comment_id, post_id, text, user_id, username,
                created_at, sent_at, received_at, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
            (
                event_id,
                event_type,
                data.get("comment_id"),
                data.get("post_id"),
                data.get("text"),
                from_.get("user_id"),
                from_.get("username"),
                data.get("created_at"),
                payload.get("sent_at"),
                time.time(),
            ),
        )

    # Wake the processing loop immediately instead of waiting for the next
    # poll tick, without making the webhook response wait on it.
    worker.notify_new_event()

    return {"ok": True}


# ---------------------------------------------------------------------------
# POST /rules
# ---------------------------------------------------------------------------

class RuleIn(BaseModel):
    keyword: str
    dm_message: str


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn):
    if not rule.keyword.strip() or not rule.dm_message.strip():
        raise HTTPException(status_code=400, detail="keyword and dm_message are required")

    rule_id = db.new_id("rule")
    async with db.write() as conn:
        await conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
            (rule_id, rule.keyword, rule.dm_message, time.time()),
        )
    return {"rule_id": rule_id, "keyword": rule.keyword, "dm_message": rule.dm_message}


# ---------------------------------------------------------------------------
# GET /stats
# ---------------------------------------------------------------------------

@app.get("/stats")
async def stats():
    conn = db.get_conn()

    cur = await conn.execute("SELECT COUNT(*) c FROM dm_attempts WHERE status='delivered'")
    sent = (await cur.fetchone())["c"]

    cur = await conn.execute("SELECT COUNT(*) c FROM dm_attempts WHERE status='failed'")
    failed = (await cur.fetchone())["c"]

    cur = await conn.execute(
        "SELECT COUNT(*) c FROM dm_attempts WHERE status IN ('pending_send','queued')"
    )
    queued = (await cur.fetchone())["c"]

    cur = await conn.execute("SELECT COUNT(*) c FROM duplicate_log")
    duplicates_blocked = (await cur.fetchone())["c"]

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }


@app.get("/health")
async def health():
    return {"ok": True}
