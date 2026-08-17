import os
from dotenv import load_dotenv

load_dotenv()

PSEUDOGRAM_BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
PSEUDOGRAM_API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")
PSEUDOGRAM_WEBHOOK_SECRET = os.getenv("PSEUDOGRAM_WEBHOOK_SECRET", "") 

# The README says webhook signatures are HMAC'd with "your API key as the
# secret", but that's not what actually happens: verified empirically by
# capturing a real webhook body + its X-PseudoGram-Signature and brute-forcing
# candidate secrets against it. The mock API signs with the account EMAIL
# string, not the api_key token used for X-API-Key auth. Set this explicitly
# rather than trying to derive it from the api_key.
PSEUDOGRAM_WEBHOOK_SECRET = os.getenv("PSEUDOGRAM_WEBHOOK_SECRET", "")

DB_PATH = os.getenv("DB_PATH", "linkplease.db")

# Mock API rate limit: 10 requests / rolling 60s
DM_SEND_RATE_LIMIT = 10
DM_SEND_RATE_WINDOW_SECONDS = 60

# Retry policy for transient failures (429 / 500) on POST /v1/dm/send
MAX_SEND_ATTEMPTS = 6
BASE_BACKOFF_SECONDS = 2  # exponential backoff base
MAX_BACKOFF_SECONDS = 60

# How often the background loops tick
WORKER_TICK_SECONDS = 1.0
RECONCILE_TICK_SECONDS = 5.0

if not PSEUDOGRAM_API_KEY:
    # We don't hard-fail here so the app can still boot (e.g. for local dev
    # before a key exists), but every outbound call will fail loudly.
    print("[config] WARNING: PSEUDOGRAM_API_KEY is not set.")
