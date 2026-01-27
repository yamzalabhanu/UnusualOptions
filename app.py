"""
app.py — Unusual Whales Options: Flow Alerts + Contract Scan → Telegram

Goal:
- Trigger alerts for CALL/PUT unusual VOLUME or unusual OPEN INTEREST (and optionally OI CHANGE)
- More accurate than flow-only: scans contracts across expiries
- Efficient: caches expiries, paginates, rate-limits, dedupes across pipelines

Requires env:
- UW_TOKEN
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

Optional env:
- TICKERS="NVDA,AMD,MSTR"
- POLL_FLOW_SECONDS=15
- SCAN_SECONDS=180
- UW_AUTH_BEARER=1

Trigger tuning:
- MIN_UNUSUAL_VOLUME=500
- MIN_UNUSUAL_OI=2000
- REQUIRE_VOL_GT_OI=0
- MIN_VOLUME_OI_RATIO=0        (0 disables)

Quality filters:
- ASK_ONLY=0/1
- REQUIRE_MIN_PREMIUM=0/1 ; MIN_PREMIUM=200000
- REQUIRE_DELTA_BAND=0/1 ; DELTA_MIN=0.30 ; DELTA_MAX=0.65
- REQUIRE_DTE_BAND=0/1 (tier-based)
- TIERS_JSON='{"NVDA":"T1","AMD":"T1","MSTR":"T3"}'

Optional OI-change scan:
- ENABLE_OI_CHANGE_SCAN=0/1
- MIN_ABS_OI_CHANGE=1500
"""

import os
import asyncio
import json
import hashlib
import html
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("uw-options")

# ----------------------------
# Config
# ----------------------------
UW_BASE_URL = "https://api.unusualwhales.com"

UW_FLOW_ALERTS_PATH = "/api/option-trades/flow-alerts"
UW_EXPIRY_BREAKDOWN_PATH = "/api/stock/{ticker}/expiry-breakdown"
UW_OPTION_CONTRACTS_PATH = "/api/stock/{ticker}/option-contracts"
UW_OI_CHANGE_TICKER_PATH = "/api/stock/{ticker}/oi-change"

UW_TOKEN = os.getenv("UW_TOKEN", "").strip()
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

UW_AUTH_BEARER = os.getenv("UW_AUTH_BEARER", "1").strip() == "1"

TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

POLL_FLOW_SECONDS = int(os.getenv("POLL_FLOW_SECONDS", "15"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "180"))

COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

# ----------------------------
# Unusual triggers
# ----------------------------
MIN_UNUSUAL_VOLUME = int(os.getenv("MIN_UNUSUAL_VOLUME", "500"))
MIN_UNUSUAL_OI = int(os.getenv("MIN_UNUSUAL_OI", "2000"))
REQUIRE_VOL_GT_OI = os.getenv("REQUIRE_VOL_GT_OI", "0").strip() == "1"
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "0"))  # 0 disables

# ----------------------------
# Quality filters (toggleable)
# ----------------------------
ASK_ONLY = os.getenv("ASK_ONLY", "0").strip() == "1"

REQUIRE_MIN_PREMIUM = os.getenv("REQUIRE_MIN_PREMIUM", "0").strip() == "1"
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))

REQUIRE_DELTA_BAND = os.getenv("REQUIRE_DELTA_BAND", "0").strip() == "1"
DELTA_MIN = float(os.getenv("DELTA_MIN", "0.30"))
DELTA_MAX = float(os.getenv("DELTA_MAX", "0.65"))

REQUIRE_DTE_BAND = os.getenv("REQUIRE_DTE_BAND", "0").strip() == "1"

# OI change scan
ENABLE_OI_CHANGE_SCAN = os.getenv("ENABLE_OI_CHANGE_SCAN", "0").strip() == "1"
MIN_ABS_OI_CHANGE = int(os.getenv("MIN_ABS_OI_CHANGE", "1500"))

# Concurrency / rate limit
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "8"))

# First-run behavior
FIRST_RUN_NO_HISTORY = os.getenv("FIRST_RUN_NO_HISTORY", "1").strip() == "1"

# Tier mapping (only used for DTE band if REQUIRE_DTE_BAND=1)
TIERS_JSON = os.getenv("TIERS_JSON", "").strip()
DEFAULT_TIERS = {t: "T2" for t in (TICKERS or [])}
if TIERS_JSON:
    try:
        DEFAULT_TIERS.update({k.upper(): str(v).upper() for k, v in json.loads(TIERS_JSON).items()})
    except Exception as e:
        log.warning("Bad TIERS_JSON; using defaults. err=%s", e)

# ----------------------------
# Models
# ----------------------------
class TestMessage(BaseModel):
    text: str

class SimulateContract(BaseModel):
    ticker: str
    option_type: str  # CALL/PUT
    expiry: str
    strike: float
    volume: int
    open_interest: int
    premium: Optional[float] = 0
    delta: Optional[float] = None
    underlying_price: Optional[float] = None

# ----------------------------
# Helpers
# ----------------------------
def require_env():
    missing = []
    if not UW_TOKEN:
        missing.append("UW_TOKEN")
    if not TG_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TG_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

def uw_headers() -> Dict[str, str]:
    if not UW_TOKEN:
        return {"Accept": "application/json, text/plain"}
    token = UW_TOKEN.strip()
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()
    return {
        "Accept": "application/json, text/plain",
        "Authorization": f"Bearer {token}",
    }


def h(x: Any) -> str:
    return html.escape("" if x is None else str(x))

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def safe_int(x, default=None):
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

def normalize_type(opt_type: Any) -> str:
    s = str(opt_type or "").lower()
    if "call" in s or s == "c":
        return "CALL"
    if "put" in s or s == "p":
        return "PUT"
    return s.upper() if s else "UNK"

def tier_dte_range(tier: str) -> Tuple[int, int]:
    t = (tier or "T2").upper()
    if t == "T1":
        return (14, 45)
    if t == "T2":
        return (7, 30)
    if t == "T3":
        return (7, 21)
    return (3, 14)  # T4

def days_to_expiry(expiry: Optional[str], now: datetime) -> Optional[int]:
    if not expiry:
        return None
    try:
        if "T" in expiry:
            exp_dt = parse_iso(expiry)
        else:
            exp_dt = datetime.fromisoformat(expiry).replace(tzinfo=timezone.utc)
        return max(0, (exp_dt.date() - now.date()).days)
    except Exception:
        return None

def alert_hash(parts: List[Any]) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]

# ----------------------------
# Telegram
# ----------------------------
async def telegram_send(text: str, client: httpx.AsyncClient) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = await client.post(url, json=payload, timeout=20)
    r.raise_for_status()

# ----------------------------
# State + caching
# ----------------------------
@dataclass
class CacheItem:
    value: Any
    expires_at: datetime

class State:
    running: bool = False

    # Flow cursor
    last_seen_created_at: Optional[str] = None

    # Dedupe/cooldown across ALL pipelines
    cooldown: Dict[str, datetime] = {}

    # Expiry cache per ticker
    expiries_cache: Dict[str, CacheItem] = {}

    # Metrics / debug
    last_error: Optional[str] = None
    last_poll_flow_at: Optional[str] = None
    last_scan_at: Optional[str] = None
    last_sent_count: int = 0
    last_fetch_flow_count: int = 0

state = State()

def cooldown_ok(key: str) -> bool:
    now = datetime.now(timezone.utc)
    last = state.cooldown.get(key)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return False
    state.cooldown[key] = now
    if len(state.cooldown) > 6000:
        cutoff = now - timedelta(seconds=COOLDOWN_SECONDS * 3)
        state.cooldown = {k: v for k, v in state.cooldown.items() if v >= cutoff}
    return True

def get_tier(ticker: str) -> str:
    return (DEFAULT_TIERS.get(ticker.upper()) or "T2").upper()

# ----------------------------
# UW API calls
# ----------------------------
async def uw_get(client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{UW_BASE_URL}{path}"
    r = await client.get(url, headers=uw_headers(), params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    if RULE_NAMES:
        params["rule_name[]"] = RULE_NAMES
    if TICKERS:
        params["tickers"] = ",".join(TICKERS)
    data = await uw_get(client, UW_FLOW_ALERTS_PATH, params=params)
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]
    if isinstance(data, list):
        return data
    return []

async def fetch_expiries(client: httpx.AsyncClient, ticker: str) -> List[str]:
    """
    Uses /api/stock/{ticker}/expiry-breakdown to discover expiries.
    Cached for ~6 hours.
    """
    now = datetime.now(timezone.utc)
    c = state.expiries_cache.get(ticker)
    if c and c.expires_at > now:
        return c.value

    path = UW_EXPIRY_BREAKDOWN_PATH.format(ticker=ticker)
    data = await uw_get(client, path, params={})
    expiries: List[str] = []

    # Try multiple common shapes safely:
    if isinstance(data, dict):
        # could be {"data":[...]} or {"expirations":[...]} etc
        if isinstance(data.get("expirations"), list):
            expiries = [str(x) for x in data["expirations"] if x]
        elif isinstance(data.get("data"), list):
            # sometimes includes dicts with "expiry" key
            for row in data["data"]:
                if isinstance(row, dict):
                    e = row.get("expiry") or row.get("expiration")
                    if e:
                        expiries.append(str(e))
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, str):
                expiries.append(row)
            elif isinstance(row, dict):
                e = row.get("expiry") or row.get("expiration")
                if e:
                    expiries.append(str(e))

    expiries = sorted(list({e for e in expiries if e}))
    state.expiries_cache[ticker] = CacheItem(value=expiries, expires_at=now + timedelta(hours=6))
    return expiries

async def scan_option_contracts(
    client: httpx.AsyncClient,
    ticker: str,
    expiries: List[str],
    sem: asyncio.Semaphore,
    limit_per_expiry: int = 200,
) -> List[Dict[str, Any]]:
    """
    Uses /api/stock/{ticker}/option-contracts with filters per-expiry.
    We page until we have enough or no more pages.
    """
    results: List[Dict[str, Any]] = []

    async def fetch_page(expiry: str, page: int) -> Any:
        async with sem:
            path = UW_OPTION_CONTRACTS_PATH.format(ticker=ticker)
            params = {
                "expiry": expiry,
                "limit": limit_per_expiry,
                "page": page,
                # Helpful server-side filtering knobs (if supported):
                # "exclude_zero_vol_chains": True,
                # "exclude_zero_oi_chains": True,
                # "exclude_zero_dte": True,
                # "vol_greater_oi": True,
            }
            return await uw_get(client, path, params=params)

    # Only scan a bounded number of expiries for efficiency (front expiries tend to matter most)
    expiries_to_scan = expiries[:6] if len(expiries) > 6 else expiries

    for expiry in expiries_to_scan:
        page = 0
        while page < 3:  # hard cap pages per expiry to avoid runaway
            data = await fetch_page(expiry, page)
            rows: List[Dict[str, Any]] = []
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                rows = [x for x in data["data"] if isinstance(x, dict)]
            elif isinstance(data, list):
                rows = [x for x in data if isinstance(x, dict)]

            if not rows:
                break

            results.extend(rows)

            # stop if less than limit => likely last page
            if len(rows) < limit_per_expiry:
                break
            page += 1

    return results

async def fetch_oi_change(client: httpx.AsyncClient, ticker: str, sem: asyncio.Semaphore) -> List[Dict[str, Any]]:
    """
    Optional: /api/stock/{ticker}/oi-change (daily OI changes).
    """
    async with sem:
        path = UW_OI_CHANGE_TICKER_PATH.format(ticker=ticker)
        data = await uw_get(client, path, params={"limit": 200, "order": "desc"})
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return [x for x in data["data"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []

# ----------------------------
# Common parsing (flow + contracts)
# ----------------------------
def contract_fields_from_any(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize contract-like record coming from:
    - flow alerts rows, or
    - option-contracts rows, or
    - oi-change rows
    """
    ticker = str(row.get("ticker") or row.get("symbol") or "UNK").upper()
    opt_type = normalize_type(row.get("type") or row.get("option_type") or row.get("right"))
    expiry = row.get("expiry") or row.get("expiration") or row.get("exp_date")
    strike = safe_float(row.get("strike") or row.get("strike_price"))
    volume = safe_int(row.get("volume") or row.get("total_volume") or row.get("contracts"))
    oi = safe_int(row.get("open_interest") or row.get("oi") or row.get("openInt"))
    premium = safe_float(row.get("total_premium") or row.get("premium") or row.get("notional") or row.get("total_notional"))
    delta = safe_float(row.get("delta") or row.get("option_delta"))
    voloi = safe_float(row.get("volume_oi_ratio") or row.get("vol_oi_ratio"))
    side = str(row.get("side") or row.get("trade_side") or "").lower()
    created_at = row.get("created_at")

    # Try to get a contract identifier/symbol
    contract = row.get("option_symbol") or row.get("contract") or row.get("option_chain") or row.get("symbol")
    underlying = safe_float(row.get("underlying_price") or row.get("underlying") or row.get("stock_price") or row.get("spot_price"))

    # OI change if available (ticker oi-change endpoint may provide an "oi_change" field)
    oi_change = safe_int(row.get("oi_change") or row.get("open_interest_change"))

    if voloi is None and volume is not None and oi and oi > 0:
        voloi = volume / oi

    return {
        "ticker": ticker,
        "type": opt_type,
        "expiry": expiry,
        "strike": strike,
        "volume": volume,
        "open_interest": oi,
        "premium": premium,
        "delta": delta,
        "voloi": voloi,
        "side": side,
        "created_at": created_at,
        "contract": contract,
        "underlying": underlying,
        "oi_change": oi_change,
        "raw": row,
    }

def trigger_unusual_volume_oi(x: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Main triggers:
    - vol >= MIN_UNUSUAL_VOLUME
    - oi  >= MIN_UNUSUAL_OI
    - optional vol > oi
    - optional voloi >= MIN_VOLUME_OI_RATIO
    """
    reasons: List[str] = []
    if x["type"] not in ("CALL", "PUT"):
        return False, ["not_call_put"]

    hit = False
    vol = x.get("volume")
    oi = x.get("open_interest")
    voloi = x.get("voloi")

    if vol is not None and vol >= MIN_UNUSUAL_VOLUME:
        hit = True; reasons.append(f"vol>={MIN_UNUSUAL_VOLUME}")
    if oi is not None and oi >= MIN_UNUSUAL_OI:
        hit = True; reasons.append(f"oi>={MIN_UNUSUAL_OI}")
    if REQUIRE_VOL_GT_OI and vol is not None and oi is not None and vol > oi:
        hit = True; reasons.append("vol_gt_oi")
    if MIN_VOLUME_OI_RATIO and MIN_VOLUME_OI_RATIO > 0 and voloi is not None and voloi >= MIN_VOLUME_OI_RATIO:
        hit = True; reasons.append(f"voloi>={MIN_VOLUME_OI_RATIO:g}")

    return (hit, reasons or ["no_trigger"])

def quality_ok(x: Dict[str, Any], tier: str) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if REQUIRE_MIN_PREMIUM:
        prem = x.get("premium")
        if prem is None or prem < MIN_PREMIUM:
            return False, ["premium_low"]

    if ASK_ONLY:
        side = x.get("side") or ""
        if side and side != "ask":
            return False, [f"side_{side}"]

    if REQUIRE_DELTA_BAND:
        d = x.get("delta")
        if d is None:
            return False, ["delta_missing"]
        if abs(d) < DELTA_MIN or abs(d) > DELTA_MAX:
            return False, ["delta_out_of_band"]

    if REQUIRE_DTE_BAND:
        now = datetime.now(timezone.utc)
        dte = days_to_expiry(x.get("expiry"), now)
        if dte is None:
            return False, ["expiry_missing"]
        dmin, dmax = tier_dte_range(tier)
        if dte < dmin or dte > dmax:
            return False, [f"dte_{dte}_out_{dmin}-{dmax}"]

    reasons.append("quality_ok")
    return True, reasons

def trigger_oi_change(x: Dict[str, Any]) -> Tuple[bool, List[str]]:
    if not ENABLE_OI_CHANGE_SCAN:
        return False, ["oi_change_scan_disabled"]
    c = x.get("oi_change")
    if c is None:
        return False, ["oi_change_missing"]
    if abs(int(c)) >= MIN_ABS_OI_CHANGE:
        return True, [f"abs_oi_change>={MIN_ABS_OI_CHANGE}"]
    return False, ["oi_change_small"]

def score(x: Dict[str, Any], tier: str, trig_tags: List[str], extra_tags: List[str]) -> Tuple[int, List[str]]:
    s = 45
    why: List[str] = []

    t = (tier or "T2").upper()
    if t == "T1":
        s += 10; why.append("tier_T1")
    elif t == "T2":
        s += 5; why.append("tier_T2")
    elif t == "T4":
        s -= 5; why.append("tier_T4")
    else:
        why.append("tier_T3")

    prem = x.get("premium") or 0
    if prem >= 1_000_000:
        s += 10; why.append("prem_1m+")
    elif prem >= 500_000:
        s += 6; why.append("prem_500k+")

    vol = x.get("volume") or 0
    if vol >= 5000:
        s += 10; why.append("vol_5k+")
    elif vol >= 2000:
        s += 6; why.append("vol_2k+")
    elif vol >= 500:
        s += 3; why.append("vol_500+")

    oi = x.get("open_interest") or 0
    if oi >= 20000:
        s += 8; why.append("oi_20k+")
    elif oi >= 5000:
        s += 5; why.append("oi_5k+")
    elif oi >= 2000:
        s += 3; why.append("oi_2k+")

    voloi = x.get("voloi")
    if voloi is not None:
        if voloi >= 3.0:
            s += 7; why.append("voloi_3+")
        elif voloi >= 2.0:
            s += 4; why.append("voloi_2+")

    if any("abs_oi_change" in t for t in extra_tags):
        s += 6; why.append("oi_change_spike")

    if len(trig_tags) + len(extra_tags) >= 2:
        s += 3; why.append("multi_signal")

    return max(0, min(100, s)), why

def fmt_msg(kind: str, x: Dict[str, Any], tier: str, tags: List[str], extra: List[str]) -> str:
    sc, why = score(x, tier, tags, extra)
    contract = x.get("contract")
    strike = x.get("strike")
    expiry = x.get("expiry")
    vol = x.get("volume")
    oi = x.get("open_interest")
    voloi = x.get("voloi")
    prem = x.get("premium")
    delta = x.get("delta")
    under = x.get("underlying")
    side = x.get("side") or "n/a"
    created = x.get("created_at") or "n/a"

    return "\n".join([
        f"🐳 <b>UW Options Alert</b> — <b>{h(x['ticker'])}</b> | <b>{h(x['type'])}</b> | <b>{h(kind)}</b> | <b>Score:</b> <code>{sc}/100</code>",
        f"• Tier: <code>{h(tier)}</code> | Side: <code>{h(side)}</code>",
        f"• Contract: <code>{h(contract)}</code>" if contract else f"• {h(x['type'])} {h(strike)} @ {h(expiry)}",
        f"• Underlying: <code>{h(under)}</code> | Δ: <code>{h(delta)}</code> | Vol/OI: <code>{h(voloi)}</code>",
        f"• Volume: <code>{h(vol)}</code> | OI: <code>{h(oi)}</code> | Premium: <b>{h(money(prem))}</b>",
        f"• Trigger: <code>{h(', '.join(tags))}</code>",
        f"• Extra: <code>{h(', '.join(extra))}</code>" if extra else "• Extra: <code>n/a</code>",
        f"• Why: <code>{h(', '.join(why[:10]))}</code>",
        f"• Time: <code>{h(created)}</code>",
    ])

# ----------------------------
# Loops
# ----------------------------
async def flow_loop(client: httpx.AsyncClient):
    """
    Fast real-time prints loop (flow alerts).
    """
    if not UW_TOKEN:
        return

    while state.running:
        state.last_poll_flow_at = datetime.now(timezone.utc).isoformat()
        try:
            alerts = await fetch_flow_alerts(client)
            state.last_fetch_flow_count = len(alerts)

            # sort by created_at
            alerts_sorted = sorted(alerts, key=lambda a: parse_iso(a.get("created_at") or ""))

            # init cursor to newest if first run
            if not state.last_seen_created_at and FIRST_RUN_NO_HISTORY and alerts_sorted:
                newest = alerts_sorted[-1].get("created_at")
                if newest:
                    state.last_seen_created_at = newest
                await asyncio.sleep(POLL_FLOW_SECONDS)
                continue

            # filter fresh
            last_dt = parse_iso(state.last_seen_created_at) if state.last_seen_created_at else datetime(1970, 1, 1, tzinfo=timezone.utc)
            fresh = []
            for a in alerts_sorted:
                ca = a.get("created_at")
                if not ca:
                    continue
                try:
                    if parse_iso(ca) > last_dt:
                        fresh.append(a)
                except Exception:
                    continue

            # advance cursor even if none pass
            if fresh:
                newest = fresh[-1].get("created_at")
                if newest:
                    state.last_seen_created_at = newest

            sent = 0
            for a in fresh:
                x = contract_fields_from_any(a)
                tier = get_tier(x["ticker"])

                ok_trig, tags = trigger_unusual_volume_oi(x)
                if not ok_trig:
                    continue

                ok_q, qtags = quality_ok(x, tier)
                if not ok_q:
                    continue

                key = alert_hash(["flow", x["ticker"], x["type"], x["expiry"], x["strike"], x["premium"], x["volume"], x["open_interest"]])
                if not cooldown_ok(key):
                    continue

                msg = fmt_msg("FLOW", x, tier, tags + qtags, extra=[])
                await telegram_send(msg, client)
                sent += 1

            state.last_sent_count = sent
            state.last_error = None
        except Exception as e:
            state.last_error = repr(e)
            log.exception("flow_loop error: %s", e)

        await asyncio.sleep(POLL_FLOW_SECONDS)

async def scan_loop(client: httpx.AsyncClient):
    """
    Slower scan loop:
    - expiry-breakdown (cached)
    - option-contracts scan
    - optional oi-change scan
    """
    sem = asyncio.Semaphore(MAX_INFLIGHT)

    while state.running:
        state.last_scan_at = datetime.now(timezone.utc).isoformat()
        try:
            tickers = TICKERS[:] if TICKERS else list(DEFAULT_TIERS.keys())
            if not tickers:
                # if no tickers provided, do nothing (avoid scanning whole market by accident)
                await asyncio.sleep(SCAN_SECONDS)
                continue

            sent = 0

            for ticker in tickers:
                ticker = ticker.upper()
                tier = get_tier(ticker)

                expiries = await fetch_expiries(client, ticker)
                if not expiries:
                    continue

                # 1) contract scan (unusual vol/oi)
                rows = await scan_option_contracts(client, ticker, expiries, sem=sem, limit_per_expiry=200)

                for row in rows:
                    x = contract_fields_from_any(row)
                    if x["ticker"] != ticker:
                        x["ticker"] = ticker

                    ok_trig, tags = trigger_unusual_volume_oi(x)
                    if not ok_trig:
                        continue

                    ok_q, qtags = quality_ok(x, tier)
                    if not ok_q:
                        continue

                    key = alert_hash(["contract", x["ticker"], x["type"], x["expiry"], x["strike"], x["volume"], x["open_interest"]])
                    if not cooldown_ok(key):
                        continue

                    msg = fmt_msg("CONTRACT_SCAN", x, tier, tags + qtags, extra=[])
                    await telegram_send(msg, client)
                    sent += 1

                # 2) optional OI-change scan (better “unusual OI”)
                if ENABLE_OI_CHANGE_SCAN:
                    oi_rows = await fetch_oi_change(client, ticker, sem=sem)
                    for row in oi_rows:
                        x = contract_fields_from_any(row)
                        if x["ticker"] != ticker:
                            x["ticker"] = ticker

                        ok_oi, oi_tags = trigger_oi_change(x)
                        if not ok_oi:
                            continue

                        # For oi-change, still apply (optional) quality filters if fields exist
                        ok_q, qtags = quality_ok(x, tier)
                        if not ok_q:
                            continue

                        key = alert_hash(["oi_change", x["ticker"], x.get("contract"), x.get("expiry"), x.get("strike"), x.get("oi_change")])
                        if not cooldown_ok(key):
                            continue

                        msg = fmt_msg("OI_CHANGE", x, tier, tags=[], extra=oi_tags + qtags)
                        await telegram_send(msg, client)
                        sent += 1

            state.last_sent_count += sent
            state.last_error = None

        except Exception as e:
            state.last_error = repr(e)
            log.exception("scan_loop error: %s", e)

        await asyncio.sleep(SCAN_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Options: Flow + Contract Scan → Telegram")

@app.on_event("startup")
async def startup():
    require_env()
    state.running = True
    async with httpx.AsyncClient() as client:
        # warm start: ensure token works
        try:
            _ = await fetch_flow_alerts(client)
        except Exception as e:
            log.warning("Token test failed (flow endpoint). err=%s", e)

    # new client per task (safer)
    asyncio.create_task(_run_flow_task())
    asyncio.create_task(_run_scan_task())

async def _run_flow_task():
    async with httpx.AsyncClient() as client:
        await flow_loop(client)

async def _run_scan_task():
    async with httpx.AsyncClient() as client:
        await scan_loop(client)

@app.on_event("shutdown")
async def shutdown():
    state.running = False

@app.get("/health")
def health():
    return {
        "ok": True,
        "running": state.running,
        "tickers": TICKERS,
        "poll_flow_seconds": POLL_FLOW_SECONDS,
        "scan_seconds": SCAN_SECONDS,
        "last_poll_flow_at": state.last_poll_flow_at,
        "last_scan_at": state.last_scan_at,
        "last_seen_created_at": state.last_seen_created_at,
        "last_fetch_flow_count": state.last_fetch_flow_count,
        "last_sent_count": state.last_sent_count,
        "last_error": state.last_error,
        "triggers": {
            "min_unusual_volume": MIN_UNUSUAL_VOLUME,
            "min_unusual_oi": MIN_UNUSUAL_OI,
            "require_vol_gt_oi": REQUIRE_VOL_GT_OI,
            "min_volume_oi_ratio": MIN_VOLUME_OI_RATIO,
            "enable_oi_change_scan": ENABLE_OI_CHANGE_SCAN,
            "min_abs_oi_change": MIN_ABS_OI_CHANGE,
        },
        "filters": {
            "ask_only": ASK_ONLY,
            "require_min_premium": REQUIRE_MIN_PREMIUM,
            "min_premium": MIN_PREMIUM,
            "require_delta_band": REQUIRE_DELTA_BAND,
            "delta_band": [DELTA_MIN, DELTA_MAX],
            "require_dte_band": REQUIRE_DTE_BAND,
        },
        "limits": {"max_inflight": MAX_INFLIGHT, "cooldown_seconds": COOLDOWN_SECONDS},
    }

@app.post("/test/telegram")
async def test_telegram(body: TestMessage):
    require_env()
    async with httpx.AsyncClient() as client:
        await telegram_send(h(body.text), client)
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
    asyncio.create_task(_run_flow_task())
    asyncio.create_task(_run_scan_task())
    return {"running": True}

@app.post("/control/reset_cursor")
def reset_cursor():
    state.last_seen_created_at = None
    return {"last_seen_created_at": None}

@app.post("/simulate/contract")
async def simulate_contract(body: SimulateContract):
    """
    Simulate a contract scan record and run triggers/filters/cooldown.
    """
    require_env()
    x = {
        "ticker": body.ticker.upper(),
        "type": normalize_type(body.option_type),
        "expiry": body.expiry,
        "strike": body.strike,
        "volume": body.volume,
        "open_interest": body.open_interest,
        "premium": body.premium,
        "delta": body.delta,
        "underlying": body.underlying_price,
        "voloi": (body.volume / body.open_interest) if body.open_interest else None,
        "side": "",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "contract": None,
        "oi_change": None,
        "raw": {},
    }
    tier = get_tier(x["ticker"])

    ok_trig, tags = trigger_unusual_volume_oi(x)
    if not ok_trig:
        return {"sent": False, "stage": "trigger", "reason": tags}

    ok_q, qtags = quality_ok(x, tier)
    if not ok_q:
        return {"sent": False, "stage": "quality", "reason": qtags}

    key = alert_hash(["simulate_contract", x["ticker"], x["type"], x["expiry"], x["strike"], x["volume"], x["open_interest"]])
    if not cooldown_ok(key):
        return {"sent": False, "stage": "cooldown", "reason": "duplicate"}

    async with httpx.AsyncClient() as client:
        await telegram_send(fmt_msg("SIM_CONTRACT", x, tier, tags + qtags, extra=[]), client)
    return {"sent": True, "ticker": x["ticker"], "tier": tier, "passed": tags + qtags}
