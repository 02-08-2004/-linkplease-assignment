import hashlib
import hmac
import json
import time
import urllib.request

BASE = "http://127.0.0.1:8000"
API_KEY = "testkey123"


def post_webhook(payload: dict, bad_sig: bool = False):
    body = json.dumps(payload).encode()
    sig = hmac.new(API_KEY.encode(), body, hashlib.sha256).hexdigest()
    if bad_sig:
        sig = "0" * 64
    req = urllib.request.Request(
        f"{BASE}/webhook",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": f"sha256={sig}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def event(event_id, event_type, comment_id, text, user_id, username):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "sent_at": "2026-08-17T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_1",
            "text": text,
            "created_at": "2026-08-17T09:14:21.900Z",
            "from": {"user_id": user_id, "username": username},
        },
    }


print("1) normal comment matching PRICE:")
print(post_webhook(event("evt_1", "comment.created", "cmt_1", "PRICE please", "usr_1", "arjun")))

print("2) exact duplicate event_id redelivered:")
print(post_webhook(event("evt_1", "comment.created", "cmt_1", "PRICE please", "usr_1", "arjun")))

print("3) same user comments PRICE again from a different comment (should also be blocked, rule+user already dm'd):")
print(post_webhook(event("evt_2", "comment.created", "cmt_2", "wait PRICE again", "usr_1", "arjun")))

print("4) different user, matches keyword case-insensitively:")
print(post_webhook(event("evt_3", "comment.created", "cmt_3", "price?", "usr_2", "meera")))

print("5) comment.deleted arrives BEFORE its comment.created (out of order):")
print(post_webhook(event("evt_5_delete", "comment.deleted", "cmt_9", "", "usr_9", "z")))
print(post_webhook(event("evt_5_create", "comment.created", "cmt_9", "PRICE", "usr_9", "z")))

print("6) forged signature should be rejected:")
print(post_webhook(event("evt_forged", "comment.created", "cmt_forged", "PRICE", "usr_x", "x"), bad_sig=True))

print("7) no keyword match, should not trigger a DM:")
print(post_webhook(event("evt_6", "comment.created", "cmt_6", "just saying hi", "usr_6", "sam")))

time.sleep(3)

req = urllib.request.Request(f"{BASE}/stats")
with urllib.request.urlopen(req) as resp:
    print("\nSTATS:", resp.read().decode())
