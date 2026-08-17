"""
All persistence lives here. SQLite, WAL mode, one shared connection guarded
by an asyncio.Lock so we never hit "database is locked" under concurrent
webhook traffic. Every write that must be atomic (dedup by event_id, dedup
by rule+user) goes through a real UNIQUE constraint, not an
"check-then-insert" race in application code — that's what actually makes
the dedup safe under concurrency, the lock is just a convenience.
"""
import aiosqlite
import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from app.config import DB_PATH

_conn: aiosqlite.Connection | None = None
_lock = asyncio.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id     TEXT PRIMARY KEY,
    keyword     TEXT NOT NULL,
    dm_message  TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,   -- dedup happens here, via PK + INSERT OR IGNORE
    event_type   TEXT NOT NULL,
    comment_id   TEXT,
    post_id      TEXT,
    text         TEXT,
    user_id      TEXT,
    username     TEXT,
    created_at   TEXT,
    sent_at      TEXT,
    received_at  REAL NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | processed | error
    error        TEXT
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id  TEXT PRIMARY KEY,
    post_id     TEXT,
    user_id     TEXT,
    username    TEXT,
    text        TEXT,
    deleted     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT
);

-- One row per (rule_id, user_id) — the UNIQUE constraint IS the "never DM
-- the same user twice for the same rule" guarantee. INSERT OR IGNORE +
-- checking rowcount is how we detect "someone already got this DM" even
-- if two matching comments are being processed at the same instant.
CREATE TABLE IF NOT EXISTS dm_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id          TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    comment_id       TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    dm_id            TEXT,
    status           TEXT NOT NULL DEFAULT 'pending_send',
    -- pending_send -> queued -> delivered
    --                        -> failed (terminal, or retried -> pending_send again)
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    next_attempt_at  REAL NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    UNIQUE (rule_id, user_id)
);

CREATE TABLE IF NOT EXISTS duplicate_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT,
    rule_id     TEXT,
    user_id     TEXT,
    reason      TEXT,
    created_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_dm_status ON dm_attempts(status);
CREATE INDEX IF NOT EXISTS idx_dm_next_attempt ON dm_attempts(next_attempt_at);
"""


async def init_db():
    global _conn
    _conn = await aiosqlite.connect(DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL;")
    await _conn.execute("PRAGMA synchronous=NORMAL;")
    await _conn.execute("PRAGMA foreign_keys=ON;")
    await _conn.executescript(SCHEMA)
    await _conn.commit()
    return _conn


async def close_db():
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None


def get_conn() -> aiosqlite.Connection:
    if _conn is None:
        raise RuntimeError("DB not initialized — call init_db() first")
    return _conn


@asynccontextmanager
async def write():
    """Serialize writes through one lock. See module docstring: this is a
    convenience against SQLite's single-writer behaviour, not the source of
    correctness — the UNIQUE constraints are."""
    async with _lock:
        conn = get_conn()
        yield conn
        await conn.commit()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def now() -> float:
    return time.time()
