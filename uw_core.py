# uw_core.py
import os
import time
import json
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List
from collections import OrderedDict

import httpx

log = logging.getLogger("uw_app")

# ----------------------------
# ENV / CONFIG
# ----------------------------
UW_BASE_URL = os.getenv("UW_BASE_URL", "https://api.unusualwhales.com").rstrip("/")
UW_TOKEN = os.getenv("UW_TOKEN", "").strip()

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WATCHLIST = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]

FLOW_POLL_SECONDS = int(os.getenv("FLOW_POLL_SECONDS", "10"))
CHAIN_POLL_SECONDS = int(os.getenv("CHAIN_POLL_SECONDS", "60"))

MARKET_TIDE_CACHE_SECONDS = int(os.getenv("MARKET_TIDE_CACHE_SECONDS", "45"))
DARKPOOL_CACHE_SECONDS = int(os.getenv("DARKPOOL_CACHE_SECONDS", "300"))

MIN_FLOW_PREMIUM = float(os.getenv("MIN_FLOW_PREMIUM", "200000"))
MIN_FLOW_VOL_OI_RATIO = float(os.getenv("MIN_FLOW_VOL_OI_RATIO", "1.2"))
FLOW_ASK_ONLY = os.getenv("FLOW_ASK_ONLY", "1").strip() == "1"
FLOW_LIMIT = int(os.getenv("FLOW_LIMIT", "200"))

CHAIN_VOL_OI_RATIO = float(os.getenv("CHAIN_VOL_OI_RATIO", "2.0"))
CHAIN_MIN_VOLUME = int(os.getenv("CHAIN_MIN_VOLUME", "2500"))  # info-only
CHAIN_MIN_OI = int(os.getenv("CHAIN_MIN_OI", "5000"))          # info-only
CHAIN_MIN_PREMIUM = float(os.getenv("CHAIN_MIN_PREMIUM", "200000"))
CHAIN_MIN_OI_CHANGE = int(os.getenv("CHAIN_MIN_OI_CHANGE", "1500"))
CHAIN_VOL_GREATER_OI_ONLY = os.getenv("CHAIN_VOL_GREATER_OI_ONLY", "0") == "1"

# HARD GATES
MIN_HARD_VOLUME = int(os.getenv("MIN_HARD_VOLUME", "5000"))
MIN_HARD_OI = int(os.getenv("MIN_HARD_OI", "5000"))

# Flow score gate
MIN_SCORE_TO_ALERT = int(os.getenv("MIN_SCORE_TO_ALERT", "70"))

# Cooldown
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

# Dedupe tuning
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "1800"))
DEDUP_MAX_KEYS = int(os.getenv("DEDUP_MAX_KEYS", "20000"))
CROSS_STREAM_SUPPRESS_SECONDS = int(os.getenv("CROSS_STREAM_SUPPRESS_SECONDS", "1800"))

# Chain GPT gating
GPT_CHAIN_MIN_PREMIUM = float(os.getenv("GPT_CHAIN_MIN_PREMIUM", "1000000"))
GPT_CHAIN_MIN_VOLOI = float(os.getenv("GPT_CHAIN_MIN_VOLOI", "3.0"))
GPT_CHAIN_MIN_OI_CHANGE = int(os.getenv("GPT_CHAIN_MIN_OI_CHANGE", "5000"))

# Limit chain alerts per ticker
CHAIN_TOP_N = int(os.getenv("CHAIN_TOP_N", "1"))

DEFAULT_WATCHLIST = [
    "NVDA", "AMD", "MSFT", "META", "AAPL", "TSLA", "AMZN", "GOOGL",
    "PLTR", "CRWD", "SMCI", "MU", "ARM", "NFLX", "AVGO", "COIN", "MSTR"
]


# ----------------------------
# State
# ----------------------------
@dataclass
class CacheItem:
    value: Any
    expires_at: datetime


@dataclass
class State:
    running: bool = False

    # Flow cursor (server param: newer_than)
    flow_newer_than: Optional[str] = None

    # cooldown for keys (flow/chain)
    cooldown: Dict[str, datetime] = field(default_factory=dict)

    # caches
    market_tide_cache: Optional[CacheItem] = None
    darkpool_cache: Dict[str, CacheItem] = field(default_factory=dict)

    # chain contract cooldown
    chain_seen: Dict[str, datetime] = field(default_factory=dict)

    # dedupe
    sent_cache: "OrderedDict[str, float]" = field(default_factory=OrderedDict)
    sent_contract_cache: "OrderedDict[str, float]" = field(default_factory=OrderedDict)


state = State()


# ----------------------------
# Generic helpers
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


def bearerize(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return ""
    return t if t.lower().startswith("bearer ") else f"Bearer {t}"


def uw_headers() -> Dict[str, str]:
    return {"Accept": "application/json", "Authorization": bearerize(UW_TOKEN)}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    s = str(dt_str).strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


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


def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"


def sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]


# ----------------------------
# Dedupe / suppression
# ----------------------------
def _prune_ordered_cache(cache: "OrderedDict[str, float]", ttl: int, max_keys: int) -> None:
    now = time.time()
    while cache:
        _, ts = next(iter(cache.items()))
        if (now - ts) <= ttl:
            break
        cache.popitem(last=False)
    while len(cache) > max_keys:
        cache.popitem(last=False)


def _seen_recently(cache: "OrderedDict[str, float]", key: str, ttl: int) -> bool:
    ts = cache.get(key)
    if ts is None:
        return False
    return (time.time() - ts) <= ttl


def _mark_seen(cache: "OrderedDict[str, float]", key: str) -> None:
    cache[key] = time.time()
    cache.move_to_end(key, last=True)


def stable_fingerprint(parts: List[str]) -> str:
    norm: List[str] = []
    for p in parts:
        s = (p or "").strip().upper()
        s = " ".join(s.split())
        norm.append(s)
    return sha16("|".join(norm))


def dedupe_event_check(event_key: str) -> bool:
    _prune_ordered_cache(state.sent_cache, DEDUP_TTL_SECONDS, DEDUP_MAX_KEYS)
    return not _seen_recently(state.sent_cache, event_key, DEDUP_TTL_SECONDS)


def dedupe_event_mark(event_key: str) -> None:
    _mark_seen(state.sent_cache, event_key)


def contract_check(contract: str) -> bool:
    c = (contract or "").strip().upper()
    if not c:
        return True
    _prune_ordered_cache(state.sent_contract_cache, CROSS_STREAM_SUPPRESS_SECONDS, DEDUP_MAX_KEYS)
    return not _seen_recently(state.sent_contract_cache, c, CROSS_STREAM_SUPPRESS_SECONDS)


def contract_mark(contract: str) -> None:
    c = (contract or "").strip().upper()
    if not c:
        return
    _mark_seen(state.sent_contract_cache, c)


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
# UW HTTP client
# ----------------------------
async def uw_get(client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{UW_BASE_URL}{path}"
    r = await client.get(url, headers=uw_headers(), params=params or {}, timeout=30)
    if r.status_code in (401, 403):
        try:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.json()}")
        except Exception:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()


# ----------------------------
# Market Tide cache
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
            net = safe_float(last.get("net_premium"))
            ts = last.get("time") or last.get("timestamp") or last.get("tape_time") or ""
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
# Darkpool cache
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
            parts.append(side)
        return " | ".join(parts) if parts else "n/a"
    except Exception:
        return "n/a"
