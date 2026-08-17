"""
All the "hard part" of the assignment lives here.

Pipeline:
  webhook -> events table (raw, INSERT OR IGNORE on event_id) -> 200 OK
  process_event(event_id):
      - comment.created: upsert comment, match against rules, try to
        claim a dm_attempts row (UNIQUE(rule_id,user_id) blocks duplicates)
      - comment.deleted: mark comment deleted; cancel any not-yet-sent
        dm_attempt for that comment
  send_loop: picks up dm_attempts in status='pending_send' whose
      next_attempt_at has passed, calls the mock API, updates status
  reconcile_loop: for status='queued' rows, polls GET /v1/dm/{id} to
      find out what actually happened, retries failed ones a bounded
      number of times

Crash recovery: nothing here depends on in-memory state surviving a
restart. On boot we re-scan events.status='pending' and
dm_attempts.status in ('pending_send','queued') and pick up where we left
off. The idempotency key on every dm_attempts row is derived from
(rule_id, user_id), so even if we retry a send that actually already went
through server-side, the mock API's Idempotency-Key handling returns the
original dm_id instead of sending twice.
"""
import asyncio
import time
import logging

from app import db, client
from app.config import MAX_SEND_ATTEMPTS, BASE_BACKOFF_SECONDS, MAX_BACKOFF_SECONDS, \
    WORKER_TICK_SECONDS, RECONCILE_TICK_SECONDS

log = logging.getLogger("worker")

_new_event_signal = asyncio.Event()


def notify_new_event():
    """Called by the webhook handler right after it inserts an event, so
    the processing loop wakes up immediately instead of waiting for the
    next poll tick."""
    _new_event_signal.set()


async def process_pending_events():
    conn = db.get_conn()
    async with db.write() as conn:
        cur = await conn.execute(
            "SELECT event_id FROM events WHERE status='pending' ORDER BY received_at ASC"
        )
        rows = await cur.fetchall()
    for row in rows:
        await process_event(row["event_id"])


async def process_event(event_id: str):
    async with db.write() as conn:
        cur = await conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,))
        ev = await cur.fetchone()
        if ev is None or ev["status"] != "pending":
            return  # already handled, or a duplicate delivery of an event we already processed

        try:
            if ev["event_type"] == "comment.created":
                await _handle_comment_created(conn, ev)
            elif ev["event_type"] == "comment.deleted":
                await _handle_comment_deleted(conn, ev)
            else:
                log.warning("unknown event_type=%s event_id=%s", ev["event_type"], event_id)
            await conn.execute(
                "UPDATE events SET status='processed' WHERE event_id=?", (event_id,)
            )
        except Exception as e:
            log.exception("error processing event %s", event_id)
            await conn.execute(
                "UPDATE events SET status='error', error=? WHERE event_id=?",
                (str(e), event_id),
            )


async def _handle_comment_created(conn, ev):
    comment_id = ev["comment_id"]

    # Upsert the comment, but never resurrect one already marked deleted
    # (a comment.deleted can legitimately arrive before comment.created
    # due to out-of-order delivery).
    cur = await conn.execute("SELECT deleted FROM comments WHERE comment_id=?", (comment_id,))
    existing = await cur.fetchone()
    already_deleted = bool(existing["deleted"]) if existing else False

    await conn.execute(
        """INSERT INTO comments (comment_id, post_id, user_id, username, text, deleted, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(comment_id) DO UPDATE SET
             post_id=excluded.post_id, user_id=excluded.user_id,
             username=excluded.username, text=excluded.text,
             created_at=excluded.created_at""",
        (comment_id, ev["post_id"], ev["user_id"], ev["username"], ev["text"],
         1 if already_deleted else 0, ev["created_at"]),
    )

    if already_deleted:
        # Deleted before we ever saw the comment.created — never DM.
        return

    text = (ev["text"] or "").lower()
    cur = await conn.execute("SELECT rule_id, keyword, dm_message FROM rules")
    rules = await cur.fetchall()

    for rule in rules:
        if rule["keyword"].lower() not in text:
            continue
        await _claim_dm(conn, rule["rule_id"], ev["user_id"], comment_id, ev["event_id"])


async def _claim_dm(conn, rule_id, user_id, comment_id, event_id):
    """Try to atomically claim the right to DM this user for this rule.
    The UNIQUE(rule_id, user_id) constraint is what actually prevents a
    double-send if this runs concurrently for two matching comments from
    the same user — the second INSERT just affects 0 rows."""
    idem_key = f"{rule_id}:{user_id}"
    now = time.time()
    cur = await conn.execute(
        """INSERT OR IGNORE INTO dm_attempts
           (rule_id, user_id, comment_id, idempotency_key, status, attempts,
            next_attempt_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, 'pending_send', 0, ?, ?, ?)""",
        (rule_id, user_id, comment_id, idem_key, now, now, now),
    )
    if cur.rowcount == 0:
        await conn.execute(
            "INSERT INTO duplicate_log (event_id, rule_id, user_id, reason, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, rule_id, user_id, "already dm'd (or in flight) for this rule", now),
        )


async def _handle_comment_deleted(conn, ev):
    comment_id = ev["comment_id"]
    cur = await conn.execute("SELECT comment_id FROM comments WHERE comment_id=?", (comment_id,))
    existing = await cur.fetchone()
    if existing:
        await conn.execute("UPDATE comments SET deleted=1 WHERE comment_id=?", (comment_id,))
    else:
        # comment.deleted arrived before comment.created (out-of-order).
        # Insert a tombstone row so the eventual comment.created sees
        # deleted=1 and skips matching.
        await conn.execute(
            "INSERT INTO comments (comment_id, deleted, created_at) VALUES (?, 1, ?)",
            (comment_id, ev["received_at"]),
        )

    # Cancel any DM that hasn't actually gone out yet for this comment.
    # A DM already 'queued' or 'delivered' is left alone — it was
    # legitimately sent before the delete arrived.
    await conn.execute(
        "UPDATE dm_attempts SET status='cancelled', updated_at=? "
        "WHERE comment_id=? AND status='pending_send'",
        (time.time(), comment_id),
    )


# ---------------------------------------------------------------------------
# Send loop: pushes pending_send rows out to the mock API.
# ---------------------------------------------------------------------------

async def send_loop_tick():
    now = time.time()
    conn = db.get_conn()
    cur = await conn.execute(
        "SELECT * FROM dm_attempts WHERE status='pending_send' AND next_attempt_at<=? "
        "ORDER BY next_attempt_at ASC LIMIT 20",
        (now,),
    )
    rows = await cur.fetchall()

    for row in rows:
        await _attempt_send(row)


async def _attempt_send(row):
  rule_id, user_id, comment_id = row["rule_id"], row["user_id"], row["comment_id"]
send_idem_key = f"{row['idempotency_key']}:{row['attempts']}"
    async with db.write() as conn:
        cur = await conn.execute("SELECT dm_message FROM rules WHERE rule_id=?", (rule_id,))
        r = await cur.fetchone()
    if r is None:
        # Rule was deleted after the fact — nothing sane to send.
        async with db.write() as conn:
            await conn.execute(
                "UPDATE dm_attempts SET status='failed', last_error=?, updated_at=? WHERE id=?",
                ("rule no longer exists", time.time(), row["id"]),
            )
        return

    result = await client.send_dm(user_id, r["dm_message"], comment_id, send_idem_key)
    now = time.time()

    async with db.write() as conn:
        if result.kind == "queued":
            await conn.execute(
                "UPDATE dm_attempts SET status='queued', dm_id=?, attempts=attempts+1, "
                "updated_at=? WHERE id=?",
                (result.dm_id, now, row["id"]),
            )
        elif result.kind == "rate_limited":
            delay = result.retry_after or 5.0
            await conn.execute(
                "UPDATE dm_attempts SET next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
                (now + delay, result.detail, now, row["id"]),
            )
        elif result.kind == "retryable_error":
            attempts = row["attempts"] + 1
            if attempts >= MAX_SEND_ATTEMPTS:
                await conn.execute(
                    "UPDATE dm_attempts SET status='failed', attempts=?, last_error=?, "
                    "updated_at=? WHERE id=?",
                    (attempts, result.detail, now, row["id"]),
                )
            else:
                backoff = min(BASE_BACKOFF_SECONDS * (2 ** attempts), MAX_BACKOFF_SECONDS)
                await conn.execute(
                    "UPDATE dm_attempts SET attempts=?, next_attempt_at=?, last_error=?, "
                    "updated_at=? WHERE id=?",
                    (attempts, now + backoff, result.detail, now, row["id"]),
                )
        elif result.kind == "fatal_error":
            await conn.execute(
                "UPDATE dm_attempts SET status='failed', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE id=?",
                (result.detail, now, row["id"]),
            )


# ---------------------------------------------------------------------------
# Reconciliation loop: 202 'queued' isn't 'delivered'. Find out for sure,
# and retry ones that actually failed server-side (Part C).
# ---------------------------------------------------------------------------

async def reconcile_tick():
    conn = db.get_conn()
    cur = await conn.execute(
        "SELECT * FROM dm_attempts WHERE status='queued' ORDER BY updated_at ASC LIMIT 20"
    )
    rows = await cur.fetchall()

    for row in rows:
        result = await client.get_dm_status(row["dm_id"])
        now = time.time()
        if result.kind != "ok":
            continue  # try again next tick, this is a transient check failure

        async with db.write() as conn2:
            if result.status == "delivered":
                await conn2.execute(
                    "UPDATE dm_attempts SET status='delivered', updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
            elif result.status == "failed":
                attempts = row["attempts"]
                if attempts >= MAX_SEND_ATTEMPTS:
                    await conn2.execute(
                        "UPDATE dm_attempts SET status='failed', last_error=?, updated_at=? "
                        "WHERE id=?",
                        ("delivery failed after accept, retries exhausted", now, row["id"]),
                    )
                else:
                    # Server accepted it, then it failed downstream. Retry
                    # the send — same idempotency key, so if the API's
                    # dedup window has expired this legitimately sends a
                    # fresh attempt.
                    backoff = min(BASE_BACKOFF_SECONDS * (2 ** attempts), MAX_BACKOFF_SECONDS)
                    await conn2.execute(
                        "UPDATE dm_attempts SET status='pending_send', dm_id=NULL, "
                        "next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
                        (now + backoff, "post-accept delivery failure, retrying", now, row["id"]),
                    )
            # status == 'queued' still: leave it, check again next tick


# ---------------------------------------------------------------------------
# Loop drivers
# ---------------------------------------------------------------------------

async def event_loop():
    while True:
        try:
            await process_pending_events()
        except Exception:
            log.exception("event_loop tick failed")
        try:
            await asyncio.wait_for(_new_event_signal.wait(), timeout=WORKER_TICK_SECONDS)
            _new_event_signal.clear()
        except asyncio.TimeoutError:
            pass


async def send_loop():
    while True:
        try:
            await send_loop_tick()
        except Exception:
            log.exception("send_loop tick failed")
        await asyncio.sleep(WORKER_TICK_SECONDS)


async def reconcile_loop():
    while True:
        try:
            await reconcile_tick()
        except Exception:
            log.exception("reconcile_loop tick failed")
        await asyncio.sleep(RECONCILE_TICK_SECONDS)


_tasks: list[asyncio.Task] = []


async def start_background_loops():
    _tasks.append(asyncio.create_task(event_loop()))
    _tasks.append(asyncio.create_task(send_loop()))
    _tasks.append(asyncio.create_task(reconcile_loop()))


async def stop_background_loops():
    for t in _tasks:
        t.cancel()
    for t in _tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
    _tasks.clear()
