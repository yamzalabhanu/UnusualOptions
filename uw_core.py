import os
import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from collections import OrderedDict

import httpx
from zoneinfo import ZoneInfo

# ----------------------------
# LOG
# ----------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("uw_app.core")

# ----------------------------
# ENV / CONFIG
# ----------------------------
UW_BASE_URL = os.getenv("UW_BASE_URL", "https://api.unusualwhales.com").rstrip("/")
UW_TOKEN = (os.getenv("UW_TOKEN", "") or "").replace("\n", "").replace("\r", "").strip()

TG_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()

WATCHLIST = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]

FLOW_POLL_SECONDS = int(os.getenv("FLOW_POLL_SECONDS", "10"))
CHAIN_POLL_SECONDS = int(os.getenv("CHAIN_POLL_SECONDS", "60"))
ALERTS_POLL_SECONDS = int(os.getenv("ALERTS_POLL_SECONDS", "30"))

MARKET_TIDE_CACHE_SECONDS = int(os.getenv("MARKET_TIDE_CACHE_SECONDS", "45"))
DARKPOOL_CACHE_SECONDS = int(os.getenv("DARKPOOL_CACHE_SECONDS", "300"))

MIN_FLOW_PREMIUM = float(os.getenv("MIN_FLOW_PREMIUM", "200000"))
MIN_FLOW_VOL_OI_RATIO = float(os.getenv("MIN_FLOW_VOL_OI_RATIO", "1.2"))
FLOW_ASK_ONLY = os.getenv("FLOW_ASK_ONLY", "1").strip() == "1"
FLOW_LIMIT = int(os.getenv("FLOW_LIMIT", "200"))

CHAIN_VOL_OI_RATIO = float(os.getenv("CHAIN_VOL_OI_RATIO", "2.0"))
CHAIN_MIN_VOLUME = int(os.getenv("CHAIN_MIN_VOLUME", "2500"))
CHAIN_MIN_OI = int(os.getenv("CHAIN_MIN_OI", "5000"))
CHAIN_MIN_PREMIUM = float(os.getenv("CHAIN_MIN_PREMIUM", "200000"))
CHAIN_MIN_OI_CHANGE = int(os.getenv("CHAIN_MIN_OI_CHANGE", "1500"))
CHAIN_VOL_GREATER_OI_ONLY = os.getenv("CHAIN_VOL_GREATER_OI_ONLY", "0") == "1"

# HARD GATES
MIN_HARD_VOLUME = int(os.getenv("MIN_HARD_VOLUME", "10000"))
MIN_HARD_OI = int(os.getenv("MIN_HARD_OI", "10000"))

# Flow score gate
MIN_SCORE_TO_ALERT = int(os.getenv("MIN_SCORE_TO_ALERT", "70"))

# Cooldown / dedupe
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "1800"))  # 30 min
DEDUP_MAX_KEYS = int(os.getenv("DEDUP_MAX_KEYS", "20000"))
CROSS_STREAM_SUPPRESS_SECONDS = int(os.getenv("CROSS_STREAM_SUPPRESS_SECONDS", "1800"))

# Per-cycle caps (Top-N sending)
FLOW_MAX_SEND_PER_CYCLE = int(os.getenv("FLOW_MAX_SEND_PER_CYCLE", "5"))
CHAINS_MAX_SEND_PER_CYCLE = int(os.getenv("CHAINS_MAX_SEND_PER_CYCLE", "8"))

# Redis (optional but recommended)
REDIS_URL = (os.getenv("REDIS_URL", "") or "").strip()
REDIS_PREFIX = (os.getenv("REDIS_PREFIX", "uw") or "uw").strip()
REDIS_ENABLED = bool(REDIS_URL)

ENABLE_CUSTOM_ALERTS_FEED = os.getenv("ENABLE_CUSTOM_ALERTS_FEED", "0") == "1"

DEFAULT_WATCHLIST = [
    "NVDA", "AMD", "MSFT", "META", "AAPL", "TSLA", "AMZN", "GOOGL",
    "PLTR", "CRWD", "SMCI", "MU", "ARM", "NFLX", "AVGO", "COIN", "MSTR"
]

# Market hours window (Eastern)
ET = ZoneInfo("America/New_York")
MARKET_OPEN_HHMM = (9, 30)
MARKET_CLOSE_HHMM = (16, 0)

# ----------------------------
# State (MUST BE DEFINED EARLY)
# ----------------------------
@dataclass
class CacheItem:
    value: Any
    expires_at: datetime

@dataclass
class State:
    running: bool = False
    flow_newer_than: Optional[str] = None
    alerts_newer_than: Optional[str] = None

    cooldown: Dict[str, datetime] = field(default_factory=dict)

    market_tide_cache: Optional[CacheItem] = None
    darkpool_cache: Dict[str, CacheItem] = field(default_factory=dict)

    chain_seen: Dict[str, datetime] = field(default_factory=dict)

    # Cross-module in-memory dedupe caches
    sent_cache: "OrderedDict[str, float]" = field(default_factory=OrderedDict)
    sent_contract_cache: "OrderedDict[str, float]" = field(default_factory=OrderedDict)

state = State()

# ----------------------------
# Helpers
# ----------------------------
def require_env() -> None:
    missing = []
    if not UW_TOKEN:
        missing.append("UW_TOKEN")
    if not TG_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TG_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    log.info("UW auth configured: token_present=%s token_len=%d", bool(UW_TOKEN), len(UW_TOKEN or ""))

def bearerize(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return ""
    if t.lower().startswith("bearer "):
        return t
    return f"Bearer {t}"

def uw_headers() -> Dict[str, str]:
    return {"Accept": "application/json", "Authorization": bearerize(UW_TOKEN)}

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_et() -> datetime:
    return datetime.now(tz=ET)

def market_hours_ok_now() -> bool:
    """
    True only during 9:30am–4:00pm ET, Mon–Fri.
    NOTE: does NOT account for US market holidays/half-days.
    """
    t = now_et()
    if t.weekday() >= 5:
        return False
    open_h, open_m = MARKET_OPEN_HHMM
    close_h, close_m = MARKET_CLOSE_HHMM
    start = t.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    end = t.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return start <= t <= end

def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    s = str(dt_str).strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)

def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"

def sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]

def normalize_contract(contract: str) -> str:
    return (contract or "").strip().upper()

def stable_fingerprint(parts: List[str]) -> str:
    norm = []
    for p in parts:
        s = (p or "").strip().upper()
        s = " ".join(s.split())
        norm.append(s)
    return sha16("|".join(norm))

def _prune_ordered_cache(cache: "OrderedDict[str, float]", ttl: int, max_keys: int) -> None:
    now = time.time()
    while cache:
        _, ts = next(iter(cache.items()))
        if (now - ts) <= ttl:
            break
        cache.popitem(last=False)
    while len(cache) > max_keys:
        cache.popitem(last=False)

def _mark_seen(cache: "OrderedDict[str, float]", key: str) -> None:
    now = time.time()
    cache[key] = now
    cache.move_to_end(key, last=True)

def _seen_recently(cache: "OrderedDict[str, float]", key: str, ttl: int) -> bool:
    now = time.time()
    ts = cache.get(key)
    if ts is None:
        return False
    return (now - ts) <= ttl

# ----------------------------
# Redis TTL Dedupe (optional)
# ----------------------------
_redis = None

async def redis_client():
    global _redis
    if not REDIS_ENABLED:
        return None
    if _redis is None:
        try:
            import redis.asyncio as redis  # pip install "redis>=4.2"
            _redis = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        except Exception as e:
            log.error("Redis init failed: %s", e)
            _redis = None
    return _redis

def _rk(kind: str, key: str) -> str:
    return f"{REDIS_PREFIX}:{kind}:{key}"

async def redis_setnx_ttl(kind: str, key: str, ttl_seconds: int) -> bool:
    """
    Atomic: set key if not exists, with TTL.
    Return True if newly set (allowed), False if already existed (duplicate).
    """
    r = await redis_client()
    if not r:
        return True
    try:
        redis_key = _rk(kind, key)
        ok = await r.set(redis_key, "1", nx=True, ex=int(ttl_seconds))
        return bool(ok)
    except Exception as e:
        log.error("Redis setnx failed kind=%s: %s", kind, e)
        return True

async def redis_touch_ttl(kind: str, key: str, ttl_seconds: int) -> None:
    r = await redis_client()
    if not r:
        return
    try:
        redis_key = _rk(kind, key)
        await r.set(redis_key, "1", ex=int(ttl_seconds))
    except Exception as e:
        log.error("Redis touch failed kind=%s: %s", kind, e)

# ----------------------------
# Async gates (Redis-first + in-mem)
# ----------------------------
async def dedupe_event_async(key: str) -> bool:
    """
    Return True if we should SEND; False if duplicate.
    Redis-first for persistence, in-memory also prevents very fast repeats.
    """
    ok = await redis_setnx_ttl("dedupe", key, DEDUP_TTL_SECONDS)
    if not ok:
        return False

    _prune_ordered_cache(state.sent_cache, DEDUP_TTL_SECONDS, DEDUP_MAX_KEYS)
    if _seen_recently(state.sent_cache, key, DEDUP_TTL_SECONDS):
        return False
    _mark_seen(state.sent_cache, key)
    return True

async def suppress_contract_async(contract: str) -> bool:
    """
    Cross-stream suppression by contract (Flow vs Chain), persistent with Redis.
    Return True if allowed, False if suppressed.
    """
    c = normalize_contract(contract)
    if not c:
        return True

    ok = await redis_setnx_ttl("contract", c, CROSS_STREAM_SUPPRESS_SECONDS)
    if not ok:
        return False

    _prune_ordered_cache(state.sent_contract_cache, CROSS_STREAM_SUPPRESS_SECONDS, DEDUP_MAX_KEYS)
    if _seen_recently(state.sent_contract_cache, c, CROSS_STREAM_SUPPRESS_SECONDS):
        return False
    _mark_seen(state.sent_contract_cache, c)
    return True

async def cooldown_ok_async(key: str, seconds: int) -> bool:
    """
    Persistent cooldown with Redis + in-memory.
    IMPORTANT: check in-memory first so we don't "burn" Redis keys unnecessarily.
    """
    now = now_utc()
    last = state.cooldown.get(key)
    if last and (now - last).total_seconds() < seconds:
        return False

    ok = await redis_setnx_ttl("cooldown", key, seconds)
    if not ok:
        return False

    state.cooldown[key] = now
    return True

async def cooldown_ok_ticker_dir_async(ticker: str, opt_type: str, seconds: int = 900) -> bool:
    return await cooldown_ok_async(f"tdir:{ticker}:{(opt_type or 'UNK').upper()}", seconds)

# ----------------------------
# Legacy sync cooldown (kept)
# ----------------------------
def cooldown_ok(key: str) -> bool:
    now = now_utc()
    last = state.cooldown.get(key)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return False
    state.cooldown[key] = now

    if len(state.cooldown) > 5000:
        cutoff = now - timedelta(seconds=COOLDOWN_SECONDS * 3)
        state.cooldown = {k: v for k, v in state.cooldown.items() if v >= cutoff}
    return True

# ----------------------------
# UW HTTP
# ----------------------------
async def uw_get(client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{UW_BASE_URL}{path}"
    r = await client.get(url, headers=uw_headers(), params=params or {}, timeout=30)

    if r.status_code in (401, 403):
        log.error("UW auth failed %s %s token_present=%s token_len=%d",
                  r.status_code, path, bool(UW_TOKEN), len(UW_TOKEN or ""))
        try:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.json()}")
        except Exception:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.text}")

    r.raise_for_status()
    return r.json()

# ----------------------------
# Market Tide (cached)
# ----------------------------
async def get_market_tide(client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    now = now_utc()
    if state.market_tide_cache and state.market_tide_cache.expires_at > now:
        return state.market_tide_cache.value
    try:
        data = await uw_get(client, "/api/market/market-tide", params={"interval_5m": "true"})
        state.market_tide_cache = CacheItem(value=data, expires_at=now + timedelta(seconds=MARKET_TIDE_CACHE_SECONDS))
        return data
    except Exception:
        return None

def summarize_market_tide(tide_json: Any) -> str:
    try:
        if isinstance(tide_json, dict) and isinstance(tide_json.get("data"), list) and tide_json["data"]:
            last = tide_json["data"][-1]
            call = safe_float(last.get("net_call_premium"))
            put = safe_float(last.get("net_put_premium"))
            net = (call - put) if (call is not None and put is not None) else None
            ts = last.get("timestamp") or last.get("time") or last.get("tape_time") or ""
            parts = []
            if net is not None:
                parts.append(f"net `{money(net)}`")
            if call is not None:
                parts.append(f"calls `{money(call)}`")
            if put is not None:
                parts.append(f"puts `{money(put)}`")
            if ts:
                parts.append(f"t `{ts}`")
            return " | ".join(parts) if parts else "n/a"
    except Exception:
        pass
    return "n/a"

# ----------------------------
# Darkpool (cached per ticker)
# ----------------------------
async def get_darkpool_for_ticker(client: httpx.AsyncClient, ticker: str) -> Optional[Dict[str, Any]]:
    now = now_utc()
    ci = state.darkpool_cache.get(ticker)
    if ci and ci.expires_at > now:
        return ci.value
    try:
        data = await uw_get(client, f"/api/darkpool/{ticker}", params={"limit": 25})
        state.darkpool_cache[ticker] = CacheItem(value=data, expires_at=now + timedelta(seconds=DARKPOOL_CACHE_SECONDS))
        return data
    except Exception:
        return None

def summarize_darkpool(dp_json: Any) -> str:
    try:
        rows = dp_json.get("data") if isinstance(dp_json, dict) else dp_json
        if not isinstance(rows, list) or not rows:
            return "n/a"
        top = rows[0]
        prem = safe_float(top.get("premium") or top.get("total_premium") or top.get("notional"))
        price = safe_float(top.get("price"))
        size = safe_int(top.get("size"))
        side = str(top.get("side") or "").lower()
        parts = []
        if prem is not None:
            parts.append(f"top `{money(prem)}`")
        if size is not None:
            parts.append(f"sz `{size}`")
        if price is not None:
            parts.append(f"px `{price}`")
        if side:
            parts.append(f"{side}")
        return " | ".join(parts) if parts else "n/a"
    except Exception:
        return "n/a"


__all__ = [
    # config
    "WATCHLIST", "DEFAULT_WATCHLIST",
    "FLOW_POLL_SECONDS", "CHAIN_POLL_SECONDS", "ALERTS_POLL_SECONDS",
    "MIN_FLOW_PREMIUM", "MIN_FLOW_VOL_OI_RATIO", "FLOW_ASK_ONLY", "FLOW_LIMIT",
    "CHAIN_VOL_OI_RATIO", "CHAIN_MIN_VOLUME", "CHAIN_MIN_OI",
    "CHAIN_MIN_PREMIUM", "CHAIN_MIN_OI_CHANGE", "CHAIN_VOL_GREATER_OI_ONLY",
    "MIN_HARD_VOLUME", "MIN_HARD_OI",
    "MIN_SCORE_TO_ALERT", "COOLDOWN_SECONDS",
    "FLOW_MAX_SEND_PER_CYCLE", "CHAINS_MAX_SEND_PER_CYCLE",

    # state + helpers
    "state", "now_utc", "parse_iso", "safe_float", "safe_int", "money", "sha16",
    "stable_fingerprint", "market_hours_ok_now",

    # uw api
    "uw_get", "get_market_tide", "get_darkpool_for_ticker", "summarize_darkpool",

    # gates
    "cooldown_ok",                # legacy sync
    "cooldown_ok_async",
    "cooldown_ok_ticker_dir_async",
    "dedupe_event_async",
    "suppress_contract_async",
]
