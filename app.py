# main.py
import os
import re
import time
import json
import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import OrderedDict

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

from chatgpt import send_via_gpt_formatter, telegram_send, gpt_rewrite_alert

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
ALERTS_POLL_SECONDS = int(os.getenv("ALERTS_POLL_SECONDS", "30"))
MARKET_TIDE_CACHE_SECONDS = int(os.getenv("MARKET_TIDE_CACHE_SECONDS", "45"))
DARKPOOL_CACHE_SECONDS = int(os.getenv("DARKPOOL_CACHE_SECONDS", "300"))

MIN_FLOW_PREMIUM = float(os.getenv("MIN_FLOW_PREMIUM", "200000"))
MIN_FLOW_VOL_OI_RATIO = float(os.getenv("MIN_FLOW_VOL_OI_RATIO", "1.2"))
FLOW_ASK_ONLY = os.getenv("FLOW_ASK_ONLY", "1").strip() == "1"
FLOW_LIMIT = int(os.getenv("FLOW_LIMIT", "200"))

CHAIN_VOL_OI_RATIO = float(os.getenv("CHAIN_VOL_OI_RATIO", "2.0"))
CHAIN_MIN_VOLUME = int(os.getenv("CHAIN_MIN_VOLUME", "2500"))       # info-only
CHAIN_MIN_OI = int(os.getenv("CHAIN_MIN_OI", "5000"))               # info-only
CHAIN_MIN_PREMIUM = float(os.getenv("CHAIN_MIN_PREMIUM", "200000"))
CHAIN_MIN_OI_CHANGE = int(os.getenv("CHAIN_MIN_OI_CHANGE", "1500"))
CHAIN_VOL_GREATER_OI_ONLY = os.getenv("CHAIN_VOL_GREATER_OI_ONLY", "0") == "1"

# HARD GATES
MIN_HARD_VOLUME = int(os.getenv("MIN_HARD_VOLUME", "10000"))
MIN_HARD_OI = int(os.getenv("MIN_HARD_OI", "10000"))

# Flow score gate
MIN_SCORE_TO_ALERT = int(os.getenv("MIN_SCORE_TO_ALERT", "70"))

# Cooldown
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

ENABLE_CUSTOM_ALERTS_FEED = os.getenv("ENABLE_CUSTOM_ALERTS_FEED", "0") == "1"

# Dedupe tuning
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "1800"))  # 30 min
DEDUP_MAX_KEYS = int(os.getenv("DEDUP_MAX_KEYS", "20000"))
CROSS_STREAM_SUPPRESS_SECONDS = int(os.getenv("CROSS_STREAM_SUPPRESS_SECONDS", "1800"))

# Optional: only GPT-format chain alerts when "strong"
GPT_CHAIN_MIN_PREMIUM = float(os.getenv("GPT_CHAIN_MIN_PREMIUM", "1000000"))
GPT_CHAIN_MIN_VOLOI = float(os.getenv("GPT_CHAIN_MIN_VOLOI", "3.0"))
GPT_CHAIN_MIN_OI_CHANGE = int(os.getenv("GPT_CHAIN_MIN_OI_CHANGE", "5000"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("uw_app")

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

    flow_newer_than: Optional[str] = None
    alerts_newer_than: Optional[str] = None

    cooldown: Dict[str, datetime] = field(default_factory=dict)

    market_tide_cache: Optional[CacheItem] = None
    darkpool_cache: Dict[str, CacheItem] = field(default_factory=dict)

    chain_seen: Dict[str, datetime] = field(default_factory=dict)

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


def bearerize(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return ""
    return t if t.lower().startswith("bearer ") else f"Bearer {t}"


def uw_headers() -> Dict[str, str]:
    return {"Accept": "application/json", "Authorization": bearerize(UW_TOKEN)}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


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
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"


def sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]


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
# Robust Dedupe
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


# ----------------------------
# UW client
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
            parts.append(side)
        return " | ".join(parts) if parts else "n/a"
    except Exception:
        return "n/a"


# ----------------------------
# Flow
# ----------------------------
def get_flow_fields(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": str(a.get("ticker_symbol") or a.get("ticker") or a.get("symbol") or "UNK").upper(),
        "opt_type": "CALL" if bool(a.get("is_call")) else ("PUT" if bool(a.get("is_put")) else str(a.get("type") or "").upper()),
        "premium": safe_float(a.get("total_premium") or a.get("premium") or a.get("notional") or a.get("total_notional")),
        "vol_oi": safe_float(a.get("min_volume_oi_ratio") or a.get("volume_oi_ratio") or a.get("vol_oi_ratio") or a.get("volume_oi")),
        "side": "ask" if bool(a.get("is_ask_side")) else ("bid" if bool(a.get("is_bid_side")) else str(a.get("side") or a.get("trade_side") or "")),
        "strike": safe_float(a.get("strike")),
        "expiry": a.get("expiry") or a.get("expiration") or a.get("exp_date"),
        "delta": safe_float(a.get("delta") or a.get("option_delta")),
        "oi": safe_int(a.get("open_interest")),
        "volume": safe_int(a.get("volume")),
        "created_at": a.get("created_at") or a.get("tape_time"),
        "rule": a.get("alert_rule") or a.get("rule_name") or a.get("rule") or "FLOW",
        "contract": a.get("option_symbol") or a.get("contract") or a.get("option_chain") or "",
        "underlying": safe_float(a.get("underlying_price") or a.get("underlying") or a.get("stock_price") or a.get("spot_price")),
    }


def score_flow_alert(f: Dict[str, Any]) -> int:
    score = 50
    prem = f.get("premium") or 0.0
    if prem >= 1_000_000:
        score += 18
    elif prem >= 500_000:
        score += 12
    elif prem >= 200_000:
        score += 6

    voloi = f.get("vol_oi")
    if voloi is not None:
        if voloi >= 3.0:
            score += 14
        elif voloi >= 2.0:
            score += 10
        elif voloi >= 1.2:
            score += 6

    if str(f.get("side") or "").lower() == "ask":
        score += 8

    d = f.get("delta")
    if d is not None:
        ad = abs(float(d))
        if 0.40 <= ad <= 0.55:
            score += 8
        elif 0.30 <= ad <= 0.65:
            score += 3

    return max(0, min(100, score))


async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "min_premium": MIN_FLOW_PREMIUM,
        "min_volume_oi_ratio": MIN_FLOW_VOL_OI_RATIO,
        "limit": FLOW_LIMIT,
    }
    if FLOW_ASK_ONLY:
        params["is_ask_side"] = "true"
    if state.flow_newer_than:
        params["newer_than"] = state.flow_newer_than

    tickers = WATCHLIST or DEFAULT_WATCHLIST
    params["ticker_symbol"] = ",".join(tickers)

    data = await uw_get(client, "/api/option-trades/flow-alerts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


async def handle_flow_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> None:
    f = get_flow_fields(a)

    # HARD GATES
    vol = f.get("volume") or 0
    oi = f.get("oi") or 0
    if vol < MIN_HARD_VOLUME or oi < MIN_HARD_OI:
        return

    score = score_flow_alert(f)
    if score < MIN_SCORE_TO_ALERT:
        return

    contract = f.get("contract") or ""
    if not contract_check(contract):
        return

    fp = stable_fingerprint([
        "FLOW",
        f.get("ticker", ""),
        f.get("opt_type", ""),
        contract,
        str(int((f.get("premium") or 0) / 1000)),
        str(f.get("side") or ""),
        str(f.get("rule") or ""),
    ])
    event_key = "evt:" + fp
    if not dedupe_event_check(event_key):
        return

    if not cooldown_ok("flow:" + event_key):
        return

    ticker = f["ticker"]
    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}* | *Score:* `{score}/100`",
        f"• Type/Side: `{f.get('opt_type')}` / `{str(f.get('side') or 'n/a')}` | Rule: `{f.get('rule')}`",
        f"• Contract: `{contract or 'n/a'}`",
        f"• Premium: *{money(f.get('premium'))}* | Vol/OI: `{f.get('vol_oi')}` | Δ: `{f.get('delta')}`",
        f"• Vol: `{vol}` | OI: `{oi}` | Underlying: `{f.get('underlying')}`",
        f"• MarketTide: {summarize_market_tide(tide)}",
        f"• Darkpool: {summarize_darkpool(dp)}",
        f"• Time: `{f.get('created_at')}`",
    ]

    await send_via_gpt_formatter("\n".join(lines), client)
    contract_mark(contract)
    dedupe_event_mark(event_key)


# ----------------------------
# Chain
# ----------------------------
def chain_row_fields(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "option_symbol": r.get("option_symbol") or r.get("option_contract") or r.get("symbol") or "",
        "volume": safe_int(r.get("volume")),
        "open_interest": safe_int(r.get("open_interest")),
        "prev_oi": safe_int(r.get("prev_oi")),
        "total_premium": safe_float(r.get("total_premium") or r.get("premium") or r.get("notional")),
        "avg_price": safe_float(r.get("avg_price")),
        "iv": safe_float(r.get("implied_volatility")),
        "nbbo_ask": safe_float(r.get("nbbo_ask")),
        "nbbo_bid": safe_float(r.get("nbbo_bid")),
        "sweep_volume": safe_int(r.get("sweep_volume")),
        "multi_leg_volume": safe_int(r.get("multi_leg_volume")),
        "ask_volume": safe_int(r.get("ask_volume")),
        "bid_volume": safe_int(r.get("bid_volume")),
    }


def chain_is_unusual(f: Dict[str, Any]) -> Tuple[bool, List[str]]:
    vol = f.get("volume") or 0
    oi = f.get("open_interest") or 0
    prev_oi = f.get("prev_oi")
    prem = f.get("total_premium") or 0.0

    if vol < MIN_HARD_VOLUME:
        return False, [f"hard_vol<{MIN_HARD_VOLUME}"]
    if oi < MIN_HARD_OI:
        return False, [f"hard_oi<{MIN_HARD_OI}"]

    voloi = (vol / oi) if oi > 0 else None
    oi_chg = (oi - prev_oi) if (prev_oi is not None) else None

    if CHAIN_VOL_GREATER_OI_ONLY and not (oi > 0 and vol > oi):
        return False, ["need_vol>oi"]

    reasons: List[str] = []
    if prem >= CHAIN_MIN_PREMIUM:
        reasons.append(f"prem>={int(CHAIN_MIN_PREMIUM)}")
    if voloi is not None and voloi >= CHAIN_VOL_OI_RATIO:
        reasons.append(f"vol/oi>={CHAIN_VOL_OI_RATIO:g}")
    if oi_chg is not None and abs(oi_chg) >= CHAIN_MIN_OI_CHANGE:
        reasons.append(f"oi_chg>={CHAIN_MIN_OI_CHANGE}")

    if vol >= CHAIN_MIN_VOLUME:
        reasons.append(f"vol>={CHAIN_MIN_VOLUME}")
    if oi >= CHAIN_MIN_OI:
        reasons.append(f"oi>={CHAIN_MIN_OI}")

    real = [r for r in reasons if r.startswith(("prem>=", "vol/oi>=", "oi_chg>="))]
    return (len(real) > 0), reasons


async def fetch_option_contracts(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "limit": 500,
        "exclude_zero_vol_chains": "true",
        "exclude_zero_oi_chains": "true",
    }
    if CHAIN_VOL_GREATER_OI_ONLY:
        params["vol_greater_oi"] = "true"

    data = await uw_get(client, f"/api/stock/{ticker}/option-contracts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


async def scan_unusual_chains_for_ticker(client: httpx.AsyncClient, ticker: str) -> None:
    try:
        rows = await fetch_option_contracts(client, ticker)
    except Exception:
        return

    scored: List[Tuple[float, Dict[str, Any], List[str]]] = []
    for r in rows:
        f = chain_row_fields(r)
        ok, reasons = chain_is_unusual(f)
        if not ok:
            continue
        prem = f.get("total_premium") or 0.0
        scored.append((prem, f, reasons))

    if not scored:
        return

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:3]

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    for prem, f, reasons in top:
        contract = f.get("option_symbol") or ""
        unique = f"chain:{ticker}:{contract}"

        last = state.chain_seen.get(unique)
        if last and (now_utc() - last).total_seconds() < COOLDOWN_SECONDS:
            continue

        vol = f.get("volume") or 0
        oi = f.get("open_interest") or 0
        prev_oi = f.get("prev_oi")
        voloi = (vol / oi) if (oi > 0) else None
        oi_chg = (oi - prev_oi) if (prev_oi is not None) else None
        prem_val = f.get("total_premium") or 0.0

        strong = (
            (prem_val >= GPT_CHAIN_MIN_PREMIUM) or
            (voloi is not None and voloi >= GPT_CHAIN_MIN_VOLOI) or
            (oi_chg is not None and abs(oi_chg) >= GPT_CHAIN_MIN_OI_CHANGE)
        )
        if not strong:
            continue

        if not contract_check(contract):
            continue

        fp = stable_fingerprint([
            "CHAIN",
            ticker,
            contract,
            str(int((prem_val or 0) / 1000)),
            str(vol),
            str(oi),
        ])
        event_key = "evt:" + fp
        if not dedupe_event_check(event_key):
            continue

        if not cooldown_ok("chain:" + event_key):
            continue

        lines = [
            f"🔥 *UW Unusual Chain* — *{ticker}*",
            f"• Contract: `{contract or 'n/a'}`",
            f"• Premium: *{money(prem_val)}* | AvgPx: `{f.get('avg_price')}` | IV: `{f.get('iv')}`",
            f"• Vol: `{vol}` | OI: `{oi}` | PrevOI: `{prev_oi}` | OIΔ: `{oi_chg}` | Vol/OI: `{voloi}`",
            f"• AskVol: `{f.get('ask_volume')}` | BidVol: `{f.get('bid_volume')}` | Sweeps: `{f.get('sweep_volume')}` | MultiLeg: `{f.get('multi_leg_volume')}`",
            f"• Triggers: `{', '.join(reasons)}`",
            f"• MarketTide: {summarize_market_tide(tide)}",
            f"• Darkpool: {summarize_darkpool(dp)}",
            f"• Time: `{now_utc().isoformat()}`",
        ]

        await send_via_gpt_formatter("\n".join(lines), client)
        state.chain_seen[unique] = now_utc()
        contract_mark(contract)
        dedupe_event_mark(event_key)


# ----------------------------
# Background loops
# ----------------------------
async def flow_loop():
    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                rows = await fetch_flow_alerts(client)

                # cursor = max created_at in batch
                max_dt: Optional[datetime] = None
                max_raw: Optional[str] = None
                for x in rows:
                    raw = str(x.get("created_at") or x.get("tape_time") or "")
                    if not raw:
                        continue
                    dt = parse_iso(raw)
                    if max_dt is None or dt > max_dt:
                        max_dt = dt
                        max_raw = raw

                def _k(x: Dict[str, Any]) -> datetime:
                    raw = str(x.get("created_at") or x.get("tape_time") or "1970-01-01T00:00:00+00:00")
                    return parse_iso(raw)

                for a in sorted(rows, key=_k):
                    await handle_flow_alert(client, a)

                if max_raw:
                    state.flow_newer_than = max_raw

            except Exception:
                pass

            await asyncio.sleep(FLOW_POLL_SECONDS)


async def chains_loop():
    async with httpx.AsyncClient() as client:
        while state.running:
            tickers = WATCHLIST or DEFAULT_WATCHLIST
            try:
                for t in tickers:
                    await scan_unusual_chains_for_ticker(client, t)
            except Exception:
                pass
            await asyncio.sleep(CHAIN_POLL_SECONDS)


# ----------------------------
# FastAPI
# ----------------------------
app = FastAPI(title="Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide)")

@app.on_event("startup")
async def startup():
    require_env()
    state.running = True
    asyncio.create_task(flow_loop())
    asyncio.create_task(chains_loop())

@app.on_event("shutdown")
async def shutdown():
    state.running = False

@app.get("/health")
def health():
    return {
        "ok": True,
        "running": state.running,
        "base_url": UW_BASE_URL,
        "watchlist_count": len(WATCHLIST or DEFAULT_WATCHLIST),
        "hard_gates": {
            "min_hard_volume": MIN_HARD_VOLUME,
            "min_hard_oi": MIN_HARD_OI,
            "min_score_to_alert": MIN_SCORE_TO_ALERT,
        },
        "flow": {
            "poll_seconds": FLOW_POLL_SECONDS,
            "min_premium": MIN_FLOW_PREMIUM,
            "min_vol_oi_ratio": MIN_FLOW_VOL_OI_RATIO,
            "ask_only": FLOW_ASK_ONLY,
            "limit": FLOW_LIMIT,
            "cursor_newer_than": state.flow_newer_than,
        },
        "chains": {
            "poll_seconds": CHAIN_POLL_SECONDS,
            "trigger_premium": CHAIN_MIN_PREMIUM,
            "trigger_min_oi_change": CHAIN_MIN_OI_CHANGE,
            "trigger_vol_oi_ratio": CHAIN_VOL_OI_RATIO,
            "vol_greater_oi_only": CHAIN_VOL_GREATER_OI_ONLY,
        },
        "dedupe": {
            "dedup_ttl_seconds": DEDUP_TTL_SECONDS,
            "cross_stream_suppress_seconds": CROSS_STREAM_SUPPRESS_SECONDS,
        },
        "cooldown_seconds": COOLDOWN_SECONDS,
    }


class InboundAlert(BaseModel):
    raw_text: str
    send_to_telegram: bool = True


@app.post("/format")
async def format_only(body: InboundAlert):
    require_env()
    formatted = await gpt_rewrite_alert(body.raw_text)
    if body.send_to_telegram:
        async with httpx.AsyncClient() as client:
            await telegram_send(formatted, client)
    return {"formatted": formatted, "sent": body.send_to_telegram}


@app.post("/format-and-send")
async def format_and_send(body: InboundAlert):
    require_env()
    formatted = await gpt_rewrite_alert(body.raw_text)
    if body.send_to_telegram:
        async with httpx.AsyncClient() as client:
            await telegram_send(formatted, client)
    return {"ok": True, "sent": body.send_to_telegram}


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide + Darkpool)",
        "endpoints": [
            "/docs", "/health",
            "/format", "/format-and-send",
            "/control/start", "/control/stop", "/control/reset_cursors",
        ],
    }
