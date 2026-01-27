"""
app.py — Unusual Whales (Public API) → Telegram
Upgraded:
- Bearer auth (Authorization: Bearer <token>)
- Flow Alerts poller (efficient newer_than cursor)
- Unusual Chains scanner (volume/OI/vol>OI/prev_oi change)
- Market Tide context (cached)
- Darkpool context per ticker (cached)
- Optional: user custom alerts feed (/api/alerts)
"""

import os
import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
# ENV / CONFIG
# ----------------------------
UW_BASE_URL = os.getenv("UW_BASE_URL", "https://api.unusualwhales.com").rstrip("/")

# Put ONLY the token value here (script will add "Bearer " if needed)
UW_TOKEN = os.getenv("UW_TOKEN", "").strip()

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Watchlist: if empty -> uses DEFAULT_WATCHLIST below
WATCHLIST = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]

# Polling
FLOW_POLL_SECONDS = int(os.getenv("FLOW_POLL_SECONDS", "10"))
CHAIN_POLL_SECONDS = int(os.getenv("CHAIN_POLL_SECONDS", "60"))
ALERTS_POLL_SECONDS = int(os.getenv("ALERTS_POLL_SECONDS", "30"))  # /api/alerts (custom alerts feed)
MARKET_TIDE_CACHE_SECONDS = int(os.getenv("MARKET_TIDE_CACHE_SECONDS", "45"))
DARKPOOL_CACHE_SECONDS = int(os.getenv("DARKPOOL_CACHE_SECONDS", "300"))

# Flow alert filters (server-side light filters + local scoring)
MIN_FLOW_PREMIUM = float(os.getenv("MIN_FLOW_PREMIUM", "200000"))
MIN_FLOW_VOL_OI_RATIO = float(os.getenv("MIN_FLOW_VOL_OI_RATIO", "1.2"))
FLOW_ASK_ONLY = os.getenv("FLOW_ASK_ONLY", "1").strip() == "1"  # is_ask_side=true
FLOW_LIMIT = int(os.getenv("FLOW_LIMIT", "200"))

# Chain scanner thresholds (unusual volume/OI triggers)
CHAIN_VOL_OI_RATIO = float(os.getenv("CHAIN_VOL_OI_RATIO", "2.0"))         # volume/oi
CHAIN_MIN_VOLUME = int(os.getenv("CHAIN_MIN_VOLUME", "2500"))
CHAIN_MIN_OI = int(os.getenv("CHAIN_MIN_OI", "5000"))
CHAIN_MIN_PREMIUM = float(os.getenv("CHAIN_MIN_PREMIUM", "200000"))        # contract total_premium
CHAIN_MIN_OI_CHANGE = int(os.getenv("CHAIN_MIN_OI_CHANGE", "1500"))         # abs(oi - prev_oi)
CHAIN_VOL_GREATER_OI_ONLY = os.getenv("CHAIN_VOL_GREATER_OI_ONLY", "0") == "1"

# Telegram dedupe/cooldown
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

# Optional: enable forwarding user-created alerts (/api/alerts)
ENABLE_CUSTOM_ALERTS_FEED = os.getenv("ENABLE_CUSTOM_ALERTS_FEED", "0") == "1"


DEFAULT_WATCHLIST = [
    "NVDA","AMD","MSFT","META","AAPL","TSLA","AMZN","GOOGL",
    "PLTR","CRWD","SMCI","MU","ARM","NFLX","AVGO","COIN","MSTR"
]


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
    # If user already put "Bearer xxx", keep it
    if t.lower().startswith("bearer "):
        return t
    return f"Bearer {t}"


def uw_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": bearerize(UW_TOKEN),
    }


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


async def telegram_send(text: str, client: httpx.AsyncClient) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    r = await client.post(url, json=payload, timeout=20)
    r.raise_for_status()


# ----------------------------
# API clients
# ----------------------------
async def uw_get(client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{UW_BASE_URL}{path}"
    r = await client.get(url, headers=uw_headers(), params=params or {}, timeout=30)
    # If auth is wrong, raise with payload for quick debugging
    if r.status_code in (401, 403):
        try:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.json()}")
        except Exception:
            raise RuntimeError(f"UW auth error {r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()


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

    # Flow alerts cursor: uses `newer_than`
    flow_newer_than: Optional[str] = None

    # Custom alerts cursor
    alerts_newer_than: Optional[str] = None

    # Telegram dedupe
    cooldown: Dict[str, datetime] = field(default_factory=dict)

    # Caches
    market_tide_cache: Optional[CacheItem] = None
    darkpool_cache: Dict[str, CacheItem] = field(default_factory=dict)

    # Chain scanner dedupe: last seen per ticker+contract (so you don’t spam same chain every minute)
    chain_seen: Dict[str, datetime] = field(default_factory=dict)


state = State()


def cooldown_ok(key: str) -> bool:
    now = now_utc()
    last = state.cooldown.get(key)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return False
    state.cooldown[key] = now

    # prune
    if len(state.cooldown) > 5000:
        cutoff = now - timedelta(seconds=COOLDOWN_SECONDS * 3)
        state.cooldown = {k: v for k, v in state.cooldown.items() if v >= cutoff}
    return True


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
    """
    Tide payload is a time series; we’ll just pick last point if possible.
    We keep it defensive because UW may change fields.
    """
    try:
        if isinstance(tide_json, dict) and "data" in tide_json and isinstance(tide_json["data"], list) and tide_json["data"]:
            last = tide_json["data"][-1]
            # common-ish fields (defensive)
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
        # Recent darkpool trades for ticker; keep it light
        data = await uw_get(client, f"/api/darkpool/{ticker}", params={"limit": 25})
        state.darkpool_cache[ticker] = CacheItem(value=data, expires_at=now + timedelta(seconds=DARKPOOL_CACHE_SECONDS))
        return data
    except Exception:
        return None


def summarize_darkpool(dp_json: Any) -> str:
    try:
        # Many UW endpoints return dict with "data"
        rows = dp_json.get("data") if isinstance(dp_json, dict) else dp_json
        if not isinstance(rows, list) or not rows:
            return "n/a"
        # best guess fields
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


# ----------------------------
# Flow Alerts poller
# ----------------------------
def get_flow_fields(a: Dict[str, Any]) -> Dict[str, Any]:
    # Defensive mapping (UW can vary a bit)
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
    """
    Simple score: premium + vol/oi + ask-side + delta sweetspot.
    """
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
    # Use cursor for efficiency (API supports newer_than)
    if state.flow_newer_than:
        params["newer_than"] = state.flow_newer_than

    # If you want server-side ticker filter:
    tickers = WATCHLIST or DEFAULT_WATCHLIST
    # The OpenAPI shows `ticker_symbol` for flow-alerts filtering
    # (kept defensive: if UW accepts only one ticker at a time, it will just ignore)
    params["ticker_symbol"] = ",".join(tickers)

    data = await uw_get(client, "/api/option-trades/flow-alerts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


def flow_alert_key(f: Dict[str, Any]) -> str:
    # stable-ish dedupe key
    raw = "|".join([
        f.get("ticker",""),
        str(f.get("opt_type","")),
        str(f.get("strike","")),
        str(f.get("expiry","")),
        str(f.get("premium","")),
        str(f.get("created_at","")),
        str(f.get("contract","")),
    ])
    return sha16(raw)


async def handle_flow_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> None:
    f = get_flow_fields(a)
    ticker = f["ticker"]
    key = "flow:" + flow_alert_key(f)
    if not cooldown_ok(key):
        return

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    score = score_flow_alert(f)

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}* | *Score:* `{score}/100`",
        f"• Type/Side: `{f.get('opt_type')}` / `{str(f.get('side') or 'n/a')}` | Rule: `{f.get('rule')}`",
        f"• Contract: `{f.get('contract') or 'n/a'}`",
        f"• Premium: *{money(f.get('premium'))}* | Vol/OI: `{f.get('vol_oi')}` | Δ: `{f.get('delta')}`",
        f"• Vol: `{f.get('volume')}` | OI: `{f.get('oi')}` | Underlying: `{f.get('underlying')}`",
        f"• MarketTide: {summarize_market_tide(tide)}",
        f"• Darkpool: {summarize_darkpool(dp)}",
        f"• Time: `{f.get('created_at')}`",
    ]
    await telegram_send("\n".join(lines), client)

    # advance cursor
    if f.get("created_at"):
        state.flow_newer_than = f["created_at"]


# ----------------------------
# Unusual Chains scanner
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
    reasons = []
    vol = f.get("volume") or 0
    oi = f.get("open_interest") or 0
    prev_oi = f.get("prev_oi")
    prem = f.get("total_premium") or 0.0

    voloi = (vol / oi) if oi > 0 else None
    oi_chg = (oi - prev_oi) if (prev_oi is not None) else None

    if vol >= CHAIN_MIN_VOLUME:
        reasons.append(f"vol>={CHAIN_MIN_VOLUME}")
    if oi >= CHAIN_MIN_OI:
        reasons.append(f"oi>={CHAIN_MIN_OI}")
    if prem >= CHAIN_MIN_PREMIUM:
        reasons.append(f"prem>={int(CHAIN_MIN_PREMIUM)}")
    if voloi is not None and voloi >= CHAIN_VOL_OI_RATIO:
        reasons.append(f"vol/oi>={CHAIN_VOL_OI_RATIO:g}")
    if CHAIN_VOL_GREATER_OI_ONLY and not (oi > 0 and vol > oi):
        return False, ["need_vol>oi"]
    if oi_chg is not None and abs(oi_chg) >= CHAIN_MIN_OI_CHANGE:
        reasons.append(f"oi_chg>={CHAIN_MIN_OI_CHANGE}")

    return (len(reasons) > 0), reasons


async def fetch_option_contracts(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    # This endpoint returns up to 500 rows; use filters to reduce noise
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

    # rank by premium then volume (best effort)
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
    top = scored[:3]  # send top 3 per scan tick

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    for prem, f, reasons in top:
        # ticker+contract dedupe (separate from global cooldown)
        unique = f"chain:{ticker}:{f.get('option_symbol')}"
        last = state.chain_seen.get(unique)
        if last and (now_utc() - last).total_seconds() < COOLDOWN_SECONDS:
            continue
        state.chain_seen[unique] = now_utc()

        key = "chain:" + sha16(unique + ":" + json.dumps(reasons))
        if not cooldown_ok(key):
            continue

        vol = f.get("volume")
        oi = f.get("open_interest")
        prev_oi = f.get("prev_oi")
        voloi = (vol / oi) if (vol is not None and oi and oi > 0) else None
        oi_chg = (oi - prev_oi) if (oi is not None and prev_oi is not None) else None

        lines = [
            f"🔥 *UW Unusual Chain* — *{ticker}*",
            f"• Contract: `{f.get('option_symbol') or 'n/a'}`",
            f"• Premium: *{money(f.get('total_premium'))}* | AvgPx: `{f.get('avg_price')}` | IV: `{f.get('iv')}`",
            f"• Vol: `{vol}` | OI: `{oi}` | PrevOI: `{prev_oi}` | OIΔ: `{oi_chg}` | Vol/OI: `{voloi}`",
            f"• AskVol: `{f.get('ask_volume')}` | BidVol: `{f.get('bid_volume')}` | Sweeps: `{f.get('sweep_volume')}` | MultiLeg: `{f.get('multi_leg_volume')}`",
            f"• Triggers: `{', '.join(reasons)}`",
            f"• MarketTide: {summarize_market_tide(tide)}",
            f"• Darkpool: {summarize_darkpool(dp)}",
            f"• Time: `{now_utc().isoformat()}`",
        ]
        await telegram_send("\n".join(lines), client)


# ----------------------------
# Optional: Custom Alerts feed (/api/alerts)
# ----------------------------
async def fetch_custom_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": 100}
    if state.alerts_newer_than:
        params["newer_than"] = state.alerts_newer_than
    data = await uw_get(client, "/api/alerts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


def alert_key(a: Dict[str, Any]) -> str:
    raw = "|".join([
        str(a.get("id") or ""),
        str(a.get("name") or ""),
        str(a.get("noti_type") or ""),
        str(a.get("tape_time") or a.get("created_at") or ""),
        str(a.get("symbol") or a.get("ticker") or ""),
    ])
    return sha16(raw)


async def handle_custom_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> None:
    key = "alert:" + alert_key(a)
    if not cooldown_ok(key):
        return

    name = a.get("name") or "Custom Alert"
    noti_type = a.get("noti_type") or "unknown"
    sym = a.get("symbol") or a.get("ticker") or ""
    ts = a.get("tape_time") or a.get("created_at") or ""

    lines = [
        f"🔔 *UW Custom Alert* — `{noti_type}`",
        f"• Name: `{name}`",
        f"• Symbol: `{sym}`",
        f"• Payload: ```{json.dumps(a, indent=2)[:3000]}```",
        f"• Time: `{ts}`",
    ]
    await telegram_send("\n".join(lines), client)

    if ts:
        state.alerts_newer_than = ts


# ----------------------------
# Background loops
# ----------------------------
async def flow_loop():
    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                rows = await fetch_flow_alerts(client)
                # process oldest->newest so cursor advances correctly
                def _k(x: Dict[str, Any]) -> datetime:
                    return parse_iso(str(x.get("created_at") or "1970-01-01T00:00:00+00:00"))
                rows_sorted = sorted(rows, key=_k)
                for a in rows_sorted:
                    await handle_flow_alert(client, a)
            except Exception:
                pass
            await asyncio.sleep(FLOW_POLL_SECONDS)


async def chains_loop():
    async with httpx.AsyncClient() as client:
        while state.running:
            tickers = WATCHLIST or DEFAULT_WATCHLIST
            try:
                # scan sequentially (safe for rate limits). If you want, parallelize with a semaphore.
                for t in tickers:
                    await scan_unusual_chains_for_ticker(client, t)
            except Exception:
                pass
            await asyncio.sleep(CHAIN_POLL_SECONDS)


async def custom_alerts_loop():
    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                rows = await fetch_custom_alerts(client)
                def _k(x: Dict[str, Any]) -> datetime:
                    return parse_iso(str(x.get("tape_time") or x.get("created_at") or "1970-01-01T00:00:00+00:00"))
                for a in sorted(rows, key=_k):
                    await handle_custom_alert(client, a)
            except Exception:
                pass
            await asyncio.sleep(ALERTS_POLL_SECONDS)


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide)")

@app.on_event("startup")
async def startup():
    require_env()
    state.running = True
    asyncio.create_task(flow_loop())
    asyncio.create_task(chains_loop())
    if ENABLE_CUSTOM_ALERTS_FEED:
        asyncio.create_task(custom_alerts_loop())

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
        "flow": {
            "poll_seconds": FLOW_POLL_SECONDS,
            "min_premium": MIN_FLOW_PREMIUM,
            "min_vol_oi_ratio": MIN_FLOW_VOL_OI_RATIO,
            "ask_only": FLOW_ASK_ONLY,
            "cursor_newer_than": state.flow_newer_than,
        },
        "chains": {
            "poll_seconds": CHAIN_POLL_SECONDS,
            "min_volume": CHAIN_MIN_VOLUME,
            "min_oi": CHAIN_MIN_OI,
            "min_premium": CHAIN_MIN_PREMIUM,
            "min_oi_change": CHAIN_MIN_OI_CHANGE,
            "vol_oi_ratio": CHAIN_VOL_OI_RATIO,
            "vol_greater_oi_only": CHAIN_VOL_GREATER_OI_ONLY,
        },
        "custom_alerts_feed_enabled": ENABLE_CUSTOM_ALERTS_FEED,
        "cooldown_seconds": COOLDOWN_SECONDS,
    }


@app.get("/debug/market-tide")
async def debug_market_tide():
    require_env()
    async with httpx.AsyncClient() as client:
        tide = await get_market_tide(client)
    return {"tide": tide}


@app.get("/debug/darkpool/{ticker}")
async def debug_darkpool(ticker: str):
    require_env()
    t = ticker.upper()
    async with httpx.AsyncClient() as client:
        dp = await get_darkpool_for_ticker(client, t)
    return {"ticker": t, "darkpool": dp}


@app.get("/debug/flow-alerts")
async def debug_flow_alerts():
    require_env()
    async with httpx.AsyncClient() as client:
        rows = await fetch_flow_alerts(client)
    return {"count": len(rows), "sample": rows[:3]}


@app.get("/debug/option-contracts/{ticker}")
async def debug_option_contracts(ticker: str):
    require_env()
    t = ticker.upper()
    async with httpx.AsyncClient() as client:
        rows = await fetch_option_contracts(client, t)
    return {"ticker": t, "count": len(rows), "sample": rows[:3]}


class TestMessage(BaseModel):
    text: str


@app.post("/test/telegram")
async def test_telegram(body: TestMessage):
    require_env()
    async with httpx.AsyncClient() as client:
        await telegram_send(body.text, client)
    return {"sent": True}


@app.post("/control/stop")
def stop():
    state.running = False
    return {"running": state.running}


@app.post("/control/start")
def start():
    if state.running:
        return {"running": True}
    state.running = True
    asyncio.create_task(flow_loop())
    asyncio.create_task(chains_loop())
    if ENABLE_CUSTOM_ALERTS_FEED:
        asyncio.create_task(custom_alerts_loop())
    return {"running": True}


@app.post("/control/reset_cursors")
def reset_cursors():
    state.flow_newer_than = None
    state.alerts_newer_than = None
    return {"flow_newer_than": None, "alerts_newer_than": None}


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide + Darkpool)",
        "endpoints": [
            "/docs", "/health",
            "/debug/market-tide", "/debug/darkpool/{ticker}",
            "/debug/flow-alerts", "/debug/option-contracts/{ticker}",
            "/test/telegram",
            "/control/start", "/control/stop", "/control/reset_cursors",
        ],
    }
