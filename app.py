"""
app.py — Unusual Whales Flow Alerts → Telegram (Unusual Volume / Open Interest Trigger)

What changed vs prior version:
✅ REMOVED: bias, call_min, put_max, zone logic completely
✅ NEW trigger: alert when CALL/PUT has unusual contract VOLUME or unusual OPEN INTEREST
   - volume >= MIN_UNUSUAL_VOLUME  OR
   - open_interest >= MIN_UNUSUAL_OI OR
   - volume > open_interest (optional toggle)
   - vol/oi ratio >= MIN_VOLUME_OI_RATIO (optional, still supported)
✅ Still supports (optional) filters: premium, ask-only, delta band, tier-based DTE windows
✅ Logging + no silent failures
✅ Cursor advances even when nothing passes (no reprocessing loop)
✅ Improved cooldown id (prefer UW id; avoid created_at)
✅ Telegram uses HTML (escaped; avoids Markdown parse failures)
✅ /debug/eval endpoint to show pass/fail reasons

NOTE:
- This continues to use UW Flow Alerts endpoint: /api/option-trades/flow-alerts
- We trigger based on fields commonly present in flow alerts (volume/open_interest/volume_oi_ratio/etc).
"""

import os
import asyncio
import json
import hashlib
import html
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("uw-unusual")

# ----------------------------
# Config
# ----------------------------
UW_BASE_URL = "https://api.unusualwhales.com"
UW_FLOW_ALERTS_PATH = "/api/option-trades/flow-alerts"

UW_TOKEN = os.getenv("UW_TOKEN", "").strip()
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

# Optional UW request params
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

# Cooldown (seconds) per unique alert hash
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

# Auth mode (some APIs require Bearer)
UW_AUTH_BEARER = os.getenv("UW_AUTH_BEARER", "1").strip() == "1"

# First-run behavior
FIRST_RUN_NO_HISTORY = os.getenv("FIRST_RUN_NO_HISTORY", "1").strip() == "1"
FIRST_RUN_SEND_N = int(os.getenv("FIRST_RUN_SEND_N", "10"))

# ----------------------------
# Unusual Activity Triggers (MAIN CHANGE)
# ----------------------------
# Trigger if volume >= this
MIN_UNUSUAL_VOLUME = int(os.getenv("MIN_UNUSUAL_VOLUME", "500"))          # contracts
# Trigger if open interest >= this
MIN_UNUSUAL_OI = int(os.getenv("MIN_UNUSUAL_OI", "2000"))                # contracts
# Optional trigger: volume > open interest
REQUIRE_VOL_GT_OI = os.getenv("REQUIRE_VOL_GT_OI", "0").strip() == "1"
# Optional trigger: require vol/oi ratio >= this (set 0 to disable)
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "0"))

# Optional additional “quality” filters (can be disabled via env)
REQUIRE_MIN_PREMIUM = os.getenv("REQUIRE_MIN_PREMIUM", "1").strip() == "1"
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))

ASK_ONLY = os.getenv("ASK_ONLY", "1").strip() == "1"

REQUIRE_DELTA_BAND = os.getenv("REQUIRE_DELTA_BAND", "1").strip() == "1"
DELTA_MIN = float(os.getenv("DELTA_MIN", "0.30"))
DELTA_MAX = float(os.getenv("DELTA_MAX", "0.65"))

REQUIRE_DTE_BAND = os.getenv("REQUIRE_DTE_BAND", "1").strip() == "1"

# ----------------------------
# Ticker Config (ONLY tier now)
# You can still override via TICKER_CONFIG_JSON='{"NVDA":{"tier":"T1"}, ... }'
# ----------------------------
TICKER_CONFIG_JSON = os.getenv("TICKER_CONFIG_JSON", "").strip()

DEFAULT_TICKER_CONFIG: Dict[str, Dict[str, Any]] = {
    # Keep your watchlist here (tier only)
    "NVDA": {"tier": "T1"},
    "AMD":  {"tier": "T1"},
    "MU":   {"tier": "T1"},
    "PLTR": {"tier": "T2"},
    "MSFT": {"tier": "T2"},
    "META": {"tier": "T2"},
    "CRWD": {"tier": "T2"},
    "SNOW": {"tier": "T3"},
    "CRM":  {"tier": "T3"},
    "TTD":  {"tier": "T3"},
    "OKLO": {"tier": "T4"},
    "RGTI": {"tier": "T4"},

    "PG":   {"tier": "T2"},
    "MCD":  {"tier": "T2"},
    "HD":   {"tier": "T2"},
    "LULU": {"tier": "T3"},
    "NKE":  {"tier": "T3"},
    "DKNG": {"tier": "T3"},
    "RKT":  {"tier": "T2"},
    "PDD":  {"tier": "T3"},
    "PLNT": {"tier": "T3"},
    "OPEN": {"tier": "T4"},
    "F":    {"tier": "T2"},
    "PFE":  {"tier": "T2"},

    "XOM":  {"tier": "T1"},
    "CVX":  {"tier": "T1"},
    "OXY":  {"tier": "T2"},
    "GE":   {"tier": "T1"},
    "BA":   {"tier": "T2"},
    "UNH":  {"tier": "T2"},
    "UPS":  {"tier": "T2"},
    "V":    {"tier": "T2"},
    "MSTR": {"tier": "T3"},
    "CIFR": {"tier": "T3"},
    "SMR":  {"tier": "T4"},
    "ARM":  {"tier": "T3"},
    "ACHR": {"tier": "T4"},
}

class SimulateAlert(BaseModel):
    ticker: str
    side: str            # CALL or PUT
    strike: float
    expiry: str
    premium: float
    volume: int
    oi: int
    price: float         # underlying price (display only)
    delta: Optional[float] = 0.45
    volume_oi_ratio: Optional[float] = None
    reason: Optional[str] = "manual simulation"

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
    auth = f"Bearer {UW_TOKEN}" if UW_AUTH_BEARER and not UW_TOKEN.lower().startswith("bearer ") else UW_TOKEN
    return {"Accept": "application/json, text/plain", "Authorization": auth}

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"

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

def load_ticker_config() -> Dict[str, Dict[str, Any]]:
    cfg = dict(DEFAULT_TICKER_CONFIG)
    if TICKER_CONFIG_JSON:
        try:
            override = json.loads(TICKER_CONFIG_JSON)
            for k, v in override.items():
                ku = k.upper()
                cfg[ku] = {**cfg.get(ku, {}), **v}
                # strip any accidental keys we no longer use
                cfg[ku].pop("bias", None)
                cfg[ku].pop("call_min", None)
                cfg[ku].pop("put_max", None)
        except Exception as e:
            log.warning("Bad TICKER_CONFIG_JSON; using defaults. err=%s", e)
    return cfg

TICKER_CONFIG = load_ticker_config()

def tier_dte_range(tier: str) -> Tuple[int, int]:
    t = (tier or "T2").upper()
    if t == "T1":
        return (14, 45)
    if t == "T2":
        return (7, 30)
    if t == "T3":
        return (7, 21)
    return (3, 14)  # T4

def normalize_type(opt_type: Any) -> str:
    s = str(opt_type or "").lower()
    if "call" in s:
        return "CALL"
    if "put" in s:
        return "PUT"
    if s in ("c",):
        return "CALL"
    if s in ("p",):
        return "PUT"
    return s.upper() if s else "UNK"

def get_underlying_price(a: Dict[str, Any]) -> Optional[float]:
    for k in ("underlying_price", "underlying", "stock_price", "spot_price"):
        v = safe_float(a.get(k))
        if v is not None:
            return v
    return None

def get_premium(a: Dict[str, Any]) -> Optional[float]:
    for k in ("total_premium", "premium", "notional", "total_notional"):
        v = safe_float(a.get(k))
        if v is not None:
            return v
    return None

def get_vol_oi(a: Dict[str, Any]) -> Optional[float]:
    for k in ("volume_oi_ratio", "vol_oi_ratio", "volume_oi", "vol_oi"):
        v = safe_float(a.get(k))
        if v is not None:
            return v
    # fallback compute if possible
    vol = safe_int(a.get("volume"))
    oi = safe_int(a.get("open_interest") or a.get("oi"))
    if vol is not None and oi is not None and oi > 0:
        return vol / oi
    return None

def get_side(a: Dict[str, Any]) -> str:
    return str(a.get("side") or a.get("trade_side") or "").lower()

def get_delta(a: Dict[str, Any]) -> Optional[float]:
    return safe_float(a.get("delta") or a.get("option_delta"))

def get_expiry(a: Dict[str, Any]) -> Optional[str]:
    return a.get("expiry") or a.get("expiration") or a.get("exp_date")

def get_strike(a: Dict[str, Any]) -> Optional[float]:
    return safe_float(a.get("strike"))

def get_ticker(a: Dict[str, Any]) -> str:
    return str(a.get("ticker") or a.get("symbol") or "UNK").upper()

def get_rule(a: Dict[str, Any]) -> str:
    return a.get("alert_rule") or a.get("rule_name") or a.get("rule") or "FLOW"

def get_volume(a: Dict[str, Any]) -> Optional[int]:
    return safe_int(a.get("volume") or a.get("total_volume") or a.get("contracts"))

def get_open_interest(a: Dict[str, Any]) -> Optional[int]:
    return safe_int(a.get("open_interest") or a.get("oi") or a.get("openInt"))

def alert_id(a: Dict[str, Any]) -> str:
    """
    Stable-ish id for cooldown + dedupe.
    Prefer UW's own id if present.
    Avoid created_at so repeated emissions dedupe better.
    """
    if a.get("id"):
        return f"uw_{a['id']}"
    parts = [
        get_ticker(a),
        normalize_type(a.get("type")),
        str(get_strike(a) or ""),
        str(get_expiry(a) or ""),
        str(get_premium(a) or ""),
        str(get_volume(a) or ""),
        str(get_open_interest(a) or ""),
        str(get_side(a) or ""),
    ]
    raw = "|".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]

# ----------------------------
# Trigger + Filters
# ----------------------------
def unusual_trigger_ok(a: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Main trigger:
    - CALL/PUT only
    - unusual if:
        volume >= MIN_UNUSUAL_VOLUME OR
        open_interest >= MIN_UNUSUAL_OI OR
        (optional) volume > open_interest OR
        (optional) vol/oi ratio >= MIN_VOLUME_OI_RATIO
    """
    reasons: List[str] = []

    opt_type = normalize_type(a.get("type"))
    if opt_type not in ("CALL", "PUT"):
        return False, ["type_not_call_put"]

    vol = get_volume(a)
    oi = get_open_interest(a)
    voloi = get_vol_oi(a)

    hit = False

    if vol is not None and vol >= MIN_UNUSUAL_VOLUME:
        hit = True
        reasons.append(f"vol>={MIN_UNUSUAL_VOLUME}")

    if oi is not None and oi >= MIN_UNUSUAL_OI:
        hit = True
        reasons.append(f"oi>={MIN_UNUSUAL_OI}")

    if REQUIRE_VOL_GT_OI and vol is not None and oi is not None and vol > oi:
        hit = True
        reasons.append("vol_gt_oi")

    if MIN_VOLUME_OI_RATIO and MIN_VOLUME_OI_RATIO > 0 and voloi is not None and voloi >= MIN_VOLUME_OI_RATIO:
        hit = True
        reasons.append(f"voloi>={MIN_VOLUME_OI_RATIO:g}")

    if not hit:
        return False, ["no_unusual_trigger"]

    return True, reasons

def quality_filters_ok(a: Dict[str, Any], tier: str) -> Tuple[bool, List[str]]:
    """
    Optional quality filters (toggleable by env):
    - premium >= MIN_PREMIUM (if REQUIRE_MIN_PREMIUM=1)
    - ask-only (if ASK_ONLY=1)
    - delta band (if REQUIRE_DELTA_BAND=1)
    - DTE tier band (if REQUIRE_DTE_BAND=1)
    """
    reasons: List[str] = []

    if REQUIRE_MIN_PREMIUM:
        premium = get_premium(a)
        if premium is None or premium < MIN_PREMIUM:
            return False, ["premium_low"]

    side = get_side(a)
    if ASK_ONLY and side and side != "ask":
        return False, [f"side_{side}"]

    if REQUIRE_DELTA_BAND:
        delta = get_delta(a)
        if delta is None:
            # strict if enabled
            return False, ["delta_missing"]
        if abs(delta) < DELTA_MIN or abs(delta) > DELTA_MAX:
            return False, ["delta_out_of_band"]

    if REQUIRE_DTE_BAND:
        now = datetime.now(timezone.utc)
        dte = days_to_expiry(get_expiry(a), now)
        if dte is None:
            return False, ["expiry_missing"]
        dmin, dmax = tier_dte_range(tier)
        if dte < dmin or dte > dmax:
            return False, [f"dte_{dte}_out_{dmin}-{dmax}"]

    reasons.append("quality_ok")
    return True, reasons

def score_alert(a: Dict[str, Any], tier: str, trigger_tags: List[str]) -> Tuple[int, List[str]]:
    """
    Score favors bigger/cleaner unusual triggers:
    - tier weight
    - premium / vol / oi / voloi boosts (when available)
    - ask-side bonus
    """
    score = 45
    why: List[str] = []

    t = (tier or "T2").upper()
    if t == "T1":
        score += 12; why.append("tier_T1")
    elif t == "T2":
        score += 6; why.append("tier_T2")
    elif t == "T3":
        why.append("tier_T3")
    else:
        score -= 6; why.append("tier_T4")

    if get_side(a) == "ask":
        score += 6; why.append("ask_side")

    prem = get_premium(a) or 0
    if prem >= 1_000_000:
        score += 10; why.append("prem_1m+")
    elif prem >= 500_000:
        score += 6; why.append("prem_500k+")
    elif prem >= 200_000:
        score += 3; why.append("prem_200k+")

    vol = get_volume(a) or 0
    if vol >= 5000:
        score += 10; why.append("vol_5k+")
    elif vol >= 2000:
        score += 7; why.append("vol_2k+")
    elif vol >= 500:
        score += 4; why.append("vol_500+")

    oi = get_open_interest(a) or 0
    if oi >= 20000:
        score += 8; why.append("oi_20k+")
    elif oi >= 5000:
        score += 5; why.append("oi_5k+")
    elif oi >= 2000:
        score += 3; why.append("oi_2k+")

    voloi = get_vol_oi(a)
    if voloi is not None:
        if voloi >= 3.0:
            score += 8; why.append("voloi_3+")
        elif voloi >= 2.0:
            score += 5; why.append("voloi_2+")
        elif voloi >= 1.2:
            score += 3; why.append("voloi_1.2+")

    # Reward having multiple trigger hits
    if len(trigger_tags) >= 2:
        score += 4; why.append("multi_trigger")

    score = max(0, min(100, score))
    return score, why

# ----------------------------
# Telegram (HTML mode)
# ----------------------------
def h(x: Any) -> str:
    return html.escape("" if x is None else str(x))

async def telegram_send(text: str, client: httpx.AsyncClient) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = await client.post(url, json=payload, timeout=20)
    r.raise_for_status()

def fmt_alert(a: Dict[str, Any], tier: str, trigger_tags: List[str], quality_tags: List[str]) -> str:
    ticker = get_ticker(a)
    opt_type = normalize_type(a.get("type"))
    strike = get_strike(a)
    expiry = get_expiry(a)
    rule = get_rule(a)
    created_at = a.get("created_at")

    prem = get_premium(a)
    vol = get_volume(a)
    oi = get_open_interest(a)
    voloi = get_vol_oi(a)
    delta = get_delta(a)
    side = get_side(a)
    underlying = get_underlying_price(a)
    chain = a.get("option_chain") or a.get("contract") or a.get("symbol")

    score, why = score_alert(a, tier, trigger_tags)

    passed = ", ".join(trigger_tags + quality_tags) if (trigger_tags or quality_tags) else ""

    lines = [
        f"🐳 <b>UW Unusual Alert</b> — <b>{h(ticker)}</b> | <b>{h(opt_type)}</b> | <b>Score:</b> <code>{score}/100</code>",
        f"• Tier: <code>{h(tier)}</code> | Rule: <code>{h(rule)}</code> | Side: <code>{h(side or 'n/a')}</code>",
        f"• Contract: <code>{h(chain)}</code>" if chain else f"• {h(opt_type)} {h(strike)} @ {h(expiry)}",
        f"• Underlying: <code>{h(underlying)}</code> | Δ: <code>{h(delta)}</code> | Vol/OI: <code>{h(voloi)}</code>",
        f"• Volume: <code>{h(vol)}</code> | OI: <code>{h(oi)}</code> | Premium: <b>{h(money(prem))}</b>",
        f"• Trigger: <code>{h(', '.join(trigger_tags))}</code>",
        f"• Filters: <code>{h(', '.join(quality_tags))}</code>",
        f"• Why: <code>{h(', '.join(why[:10]))}</code>",
        f"• Time: <code>{h(created_at)}</code>",
        f"• Passed: <code>{h(passed)}</code>" if passed else "• Passed: <code>n/a</code>",
    ]
    return "\n".join(lines)

# ----------------------------
# Core poller
# ----------------------------
class State:
    running: bool = False
    last_seen_created_at: Optional[str] = None
    cooldown: Dict[str, datetime] = {}
    last_error: Optional[str] = None
    last_fetch_count: int = 0
    last_fresh_count: int = 0
    last_sent_count: int = 0
    last_poll_at: Optional[str] = None

state = State()

async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}

    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    # Keep UW endpoint filters light; do logic locally
    if RULE_NAMES:
        params["rule_name[]"] = RULE_NAMES
    if TICKERS:
        params["tickers"] = ",".join(TICKERS)

    url = f"{UW_BASE_URL}{UW_FLOW_ALERTS_PATH}"
    r = await client.get(url, headers=uw_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    if isinstance(data, list):
        return data
    return []

def _created_key(a: Dict[str, Any]) -> datetime:
    ca = a.get("created_at") or ""
    try:
        return parse_iso(ca)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

def dedupe_and_sort(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    alerts_sorted = sorted(alerts, key=_created_key)

    if not state.last_seen_created_at:
        if FIRST_RUN_NO_HISTORY:
            if alerts_sorted:
                newest = alerts_sorted[-1].get("created_at")
                if newest:
                    state.last_seen_created_at = newest
            return []
        return alerts_sorted[-FIRST_RUN_SEND_N:]

    last_dt = parse_iso(state.last_seen_created_at)
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
    return fresh

def cooldown_ok(a: Dict[str, Any]) -> bool:
    hsh = alert_id(a)
    now = datetime.now(timezone.utc)
    last = state.cooldown.get(hsh)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return False
    state.cooldown[hsh] = now

    if len(state.cooldown) > 5000:
        cutoff = now - timedelta(seconds=COOLDOWN_SECONDS * 3)
        state.cooldown = {k: v for k, v in state.cooldown.items() if v >= cutoff}
    return True

async def poll_loop():
    require_env()
    state.running = True
    log.info(
        "Starting poll_loop poll_seconds=%s tickers_filter=%s triggers(vol=%s oi=%s vol>oi=%s voloi>=%s)",
        POLL_SECONDS, (TICKERS or "ALL(config-only)"),
        MIN_UNUSUAL_VOLUME, MIN_UNUSUAL_OI, REQUIRE_VOL_GT_OI, MIN_VOLUME_OI_RATIO
    )

    async with httpx.AsyncClient() as client:
        while state.running:
            state.last_poll_at = datetime.now(timezone.utc).isoformat()
            try:
                alerts = await fetch_flow_alerts(client)
                fresh = dedupe_and_sort(alerts)

                state.last_fetch_count = len(alerts)
                state.last_fresh_count = len(fresh)
                sent = 0

                # Advance cursor even if none pass
                if fresh:
                    newest = fresh[-1].get("created_at")
                    if newest:
                        state.last_seen_created_at = newest

                for a in fresh:
                    ticker = get_ticker(a)
                    cfg = TICKER_CONFIG.get(ticker)

                    # If user didn't restrict tickers, only alert for tickers in config (noise control)
                    if cfg is None and not TICKERS:
                        continue

                    tier = (cfg.get("tier") if cfg else "T2") or "T2"

                    ok_trig, trig_tags = unusual_trigger_ok(a)
                    if not ok_trig:
                        continue

                    ok_q, q_tags = quality_filters_ok(a, tier)
                    if not ok_q:
                        continue

                    if not cooldown_ok(a):
                        continue

                    msg = fmt_alert(a, tier=tier, trigger_tags=trig_tags, quality_tags=q_tags)
                    await telegram_send(msg, client)
                    sent += 1

                state.last_sent_count = sent
                state.last_error = None

                log.info(
                    "Poll ok fetched=%d fresh=%d sent=%d cursor=%s",
                    state.last_fetch_count, state.last_fresh_count, state.last_sent_count, state.last_seen_created_at
                )

            except Exception as e:
                state.last_error = repr(e)
                log.exception("poll_loop error: %s", e)

            await asyncio.sleep(POLL_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Unusual Volume/OI → Telegram")

@app.on_event("startup")
async def startup():
    asyncio.create_task(poll_loop())

@app.on_event("shutdown")
async def shutdown():
    state.running = False

class TestMessage(BaseModel):
    text: str

@app.get("/health")
def health():
    return {
        "ok": True,
        "running": state.running,
        "poll_seconds": POLL_SECONDS,
        "last_poll_at": state.last_poll_at,
        "last_seen_created_at": state.last_seen_created_at,
        "tickers_env_filter": TICKERS,
        "config_count": len(TICKER_CONFIG),
        "cooldown_seconds": COOLDOWN_SECONDS,
        "last_fetch_count": state.last_fetch_count,
        "last_fresh_count": state.last_fresh_count,
        "last_sent_count": state.last_sent_count,
        "last_error": state.last_error,
        "uw_auth_bearer": UW_AUTH_BEARER,
        "first_run_no_history": FIRST_RUN_NO_HISTORY,
        "triggers": {
            "min_unusual_volume": MIN_UNUSUAL_VOLUME,
            "min_unusual_oi": MIN_UNUSUAL_OI,
            "require_vol_gt_oi": REQUIRE_VOL_GT_OI,
            "min_volume_oi_ratio": MIN_VOLUME_OI_RATIO,
        },
        "filters": {
            "require_min_premium": REQUIRE_MIN_PREMIUM,
            "min_premium": MIN_PREMIUM,
            "ask_only": ASK_ONLY,
            "require_delta_band": REQUIRE_DELTA_BAND,
            "delta_band": [DELTA_MIN, DELTA_MAX],
            "require_dte_band": REQUIRE_DTE_BAND,
        }
    }

@app.get("/config")
def config():
    return {"tickers": TICKER_CONFIG}

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
    asyncio.create_task(poll_loop())
    return {"running": True}

@app.post("/control/reset_cursor")
def reset_cursor():
    state.last_seen_created_at = None
    return {"last_seen_created_at": None}

@app.get("/debug/uw")
async def debug_uw():
    require_env()
    async with httpx.AsyncClient() as client:
        alerts = await fetch_flow_alerts(client)
    return {"count": len(alerts), "sample": alerts[:3], "last_error": state.last_error}

@app.get("/debug/eval")
async def debug_eval(limit: int = 10):
    """
    Evaluate latest alerts and show pass/fail reasons (no Telegram send).
    """
    require_env()
    async with httpx.AsyncClient() as client:
        alerts = await fetch_flow_alerts(client)

    alerts_sorted = sorted(alerts, key=_created_key)
    sample = alerts_sorted[-max(1, min(50, limit)):]

    out = []
    for a in sample:
        ticker = get_ticker(a)
        cfg = TICKER_CONFIG.get(ticker)

        if cfg is None and not TICKERS:
            out.append({"ticker": ticker, "created_at": a.get("created_at"), "decision": "skip", "reason": ["not_in_config"]})
            continue

        tier = (cfg.get("tier") if cfg else "T2") or "T2"

        ok_trig, trig_tags = unusual_trigger_ok(a)
        if not ok_trig:
            out.append({"ticker": ticker, "created_at": a.get("created_at"), "decision": "fail", "stage": "trigger", "reason": trig_tags})
            continue

        ok_q, q_tags = quality_filters_ok(a, tier)
        if not ok_q:
            out.append({"ticker": ticker, "created_at": a.get("created_at"), "decision": "fail", "stage": "quality", "reason": q_tags, "trigger": trig_tags})
            continue

        out.append({"ticker": ticker, "created_at": a.get("created_at"), "decision": "pass", "passed": trig_tags + q_tags})

    return {"count": len(out), "results": out}

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "UW Unusual Volume/OI → Telegram",
        "endpoints": [
            "/docs",
            "/health",
            "/config",
            "/debug/uw",
            "/debug/eval",
            "/test/telegram",
            "/control/start",
            "/control/stop",
            "/control/reset_cursor",
            "/simulate/alert",
        ],
    }

@app.post("/simulate/alert")
async def simulate_alert(body: SimulateAlert):
    """
    Simulates a UW flow alert and runs it through trigger + filters + cooldown + Telegram.
    """
    require_env()

    fake_alert = {
        "ticker": body.ticker.upper(),
        "type": body.side.lower(),
        "strike": body.strike,
        "expiry": body.expiry,
        "total_premium": body.premium,
        "volume": body.volume,
        "open_interest": body.oi,
        "volume_oi_ratio": body.volume_oi_ratio or (body.volume / max(body.oi, 1)),
        "delta": body.delta,
        "side": "ask",
        "underlying_price": body.price,
        "alert_rule": "SIMULATED",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    ticker = fake_alert["ticker"]
    cfg = TICKER_CONFIG.get(ticker)
    if not cfg:
        return {"sent": False, "reason": "ticker not in config"}

    tier = (cfg.get("tier") or "T2").upper()

    ok_trig, trig_tags = unusual_trigger_ok(fake_alert)
    if not ok_trig:
        return {"sent": False, "stage": "trigger", "reason": trig_tags}

    ok_q, q_tags = quality_filters_ok(fake_alert, tier)
    if not ok_q:
        return {"sent": False, "stage": "quality", "reason": q_tags, "trigger": trig_tags}

    if not cooldown_ok(fake_alert):
        return {"sent": False, "stage": "cooldown", "reason": "duplicate within cooldown window"}

    async with httpx.AsyncClient() as client:
        await telegram_send(fmt_alert(fake_alert, tier=tier, trigger_tags=trig_tags, quality_tags=q_tags), client)

    return {"sent": True, "ticker": ticker, "tier": tier, "trigger": trig_tags, "filters": q_tags, "reason": body.reason}
