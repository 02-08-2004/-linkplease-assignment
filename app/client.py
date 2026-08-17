"""
Thin wrapper around the PseudoGram mock API.

Rate limiting: we track our own outbound call timestamps in a sliding
window and refuse to call send() ourselves once we're at the limit,
returning a synthetic "local_rate_limited" result so the worker can
reschedule instead of wasting a real request. We also honor a real 429 +
Retry-After from the server as the source of truth if our local tracking
ever drifts (e.g. after a restart, when in-memory history is empty).
"""
import asyncio
import time
import httpx

from app.config import (
    PSEUDOGRAM_BASE_URL,
    PSEUDOGRAM_API_KEY,
    DM_SEND_RATE_LIMIT,
    DM_SEND_RATE_WINDOW_SECONDS,
)

_call_times: list[float] = []
_rate_lock = asyncio.Lock()

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=PSEUDOGRAM_BASE_URL,
            headers={"X-API-Key": PSEUDOGRAM_API_KEY},
            timeout=10.0,
        )
    return _client


async def aclose():
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def _local_rate_ok() -> float | None:
    """Returns None if we're clear to send, else seconds to wait."""
    async with _rate_lock:
        now = time.time()
        cutoff = now - DM_SEND_RATE_WINDOW_SECONDS
        global _call_times
        _call_times = [t for t in _call_times if t > cutoff]
        if len(_call_times) < DM_SEND_RATE_LIMIT:
            _call_times.append(now)
            return None
        wait = _call_times[0] + DM_SEND_RATE_WINDOW_SECONDS - now
        return max(wait, 0.1)


class SendResult:
    def __init__(self, kind: str, dm_id: str | None = None,
                 retry_after: float | None = None, detail: str | None = None):
        self.kind = kind  # "queued" | "rate_limited" | "retryable_error" | "fatal_error"
        self.dm_id = dm_id
        self.retry_after = retry_after
        self.detail = detail


async def send_dm(recipient_user_id: str, message: str, comment_id: str,
                   idempotency_key: str) -> SendResult:
    wait = await _local_rate_ok()
    if wait is not None:
        return SendResult("rate_limited", retry_after=wait, detail="local rate cap")

    client = get_client()
    try:
        resp = await client.post(
            "/v1/dm/send",
            json={
                "recipient_user_id": recipient_user_id,
                "message": message,
                "comment_id": comment_id,
            },
            headers={"Idempotency-Key": idempotency_key},
        )
    except httpx.RequestError as e:
        return SendResult("retryable_error", detail=f"network error: {e}")

    if resp.status_code == 202:
        data = resp.json()
        return SendResult("queued", dm_id=data.get("dm_id"))
    if resp.status_code == 429:
        retry_after = float(resp.headers.get("Retry-After", "5"))
        return SendResult("rate_limited", retry_after=retry_after, detail="server 429")
    if resp.status_code == 500:
        return SendResult("retryable_error", detail="server 500")
    if resp.status_code == 400:
        try:
            detail = resp.json().get("detail", "bad request")
        except Exception:
            detail = "bad request"
        return SendResult("fatal_error", detail=detail)
    if resp.status_code in (401, 403):
        # Auth problem with our own API key — retrying will not help.
        return SendResult("fatal_error", detail=f"auth error {resp.status_code}")

    return SendResult("retryable_error", detail=f"unexpected status {resp.status_code}")


class StatusResult:
    def __init__(self, kind: str, status: str | None = None, detail: str | None = None):
        self.kind = kind  # "ok" | "error"
        self.status = status  # queued | delivered | failed
        self.detail = detail


async def get_dm_status(dm_id: str) -> StatusResult:
    client = get_client()
    try:
        resp = await client.get(f"/v1/dm/{dm_id}")
    except httpx.RequestError as e:
        return StatusResult("error", detail=f"network error: {e}")

    if resp.status_code == 200:
        data = resp.json()
        return StatusResult("ok", status=data.get("status"))
    return StatusResult("error", detail=f"status check failed: {resp.status_code}")
