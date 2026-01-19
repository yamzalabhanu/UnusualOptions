"""
app.py — Unusual Whales Flow Alerts → Telegram (Upgraded)

What’s new (based on your “table + logic” request):
✅ Per-ticker config (Tier/Bias + CALL/PUT price zones)
✅ Mandatory filters:
   - Flow quality: premium, ask-side, vol/OI, delta band, DTE band (by tier)
   - Price zone filter: CALL requires underlying >= call_min, PUT requires underlying <= put_max
✅ Tier modes (DTE ranges differ for T1/T2/T3/T4)
✅ Cooldown / de-dupe (prevents spam)
✅ Scoring (0–100) + “why passed” details in Telegram
✅ Safe defaults even if you don’t supply a config JSON

Notes:
- UW flow payload may not include VWAP/ORB. So this update uses what UW reliably provides:
  underlying_price + contract + premium + side + volume_oi_ratio + delta + expiry/created_at.
- If your UW payload includes extra fields (like "vwap_ok" or "trend_ok"), you can wire them in.
"""

import os
import asyncio
import json
import hashlib
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
# Config
# ----------------------------
UW_BASE_URL = "https://api.unusualwhales.com"
UW_FLOW_ALERTS_PATH = "/api/option-trades/flow-alerts"

UW_TOKEN = os.getenv("UW_TOKEN", "").strip()
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

# Global filters (baseline; tier + config can override)
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "1.2"))
ASK_ONLY = os.getenv("ASK_ONLY", "1").strip() == "1"

# Delta band (common scalp band; you can tune)
DELTA_MIN = float(os.getenv("DELTA_MIN", "0.30"))
DELTA_MAX = float(os.getenv("DELTA_MAX", "0.65"))

# Optional UW params
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

# Cooldown (seconds) per unique alert hash
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))  # 10 min default

# If you want to supply full config via env:
# TICKER_CONFIG_JSON='{"NVDA":{"tier":"T1","bias":"CALL","call_min":190,"put_max":180}}'
TICKER_CONFIG_JSON = os.getenv("TICKER_CONFIG_JSON", "").strip()

# ----------------------------
# Default Ticker Config (from your master table)
# - call_min: underlying must be >= this for CALL alerts
# - put_max : underlying must be <= this for PUT alerts
# - bias    : preferred direction (CALL/PUT/NEUTRAL/SPEC)
# - tier    : T1..T4 affects DTE band and strictness
# ----------------------------
DEFAULT_TICKER_CONFIG: Dict[str, Dict[str, Any]] = {
    # Tech / AI / Growth
    "NVDA": {"tier": "T1", "bias": "CALL", "call_min": 190.0, "put_max": 180.0},
    "AMD":  {"tier": "T1", "bias": "CALL", "call_min": 225.0, "put_max": 210.0},
    "MU":   {"tier": "T1", "bias": "CALL", "call_min": 350.0, "put_max": 330.0},
    "PLTR": {"tier": "T2", "bias": "NEUTRAL", "call_min": 185.0, "put_max": 175.0},
    "MSFT": {"tier": "T2", "bias": "PUT", "call_min": 480.0, "put_max": 470.0},
    "META": {"tier": "T2", "bias": "MIXED", "call_min": 650.0, "put_max": 630.0},
    "CRWD": {"tier": "T2", "bias": "CALL", "call_min": 460.0, "put_max": 440.0},
    "SNOW": {"tier": "T3", "bias": "PUT", "call_min": 235.0, "put_max": 215.0},
    "CRM":  {"tier": "T3", "bias": "PUT", "call_min": 250.0, "put_max": 235.0},
    "TTD":  {"tier": "T3", "bias": "PUT", "call_min": 45.0, "put_max": 40.0},
    "OKLO": {"tier": "T4", "bias": "SPEC", "call_min": 100.0, "put_max": 90.0},
    "RGTI": {"tier": "T4", "bias": "SPEC", "call_min": 28.0, "put_max": 24.0},

    # Consumer / Retail / Defensive
    "PG":   {"tier": "T2", "bias": "NEUTRAL", "call_min": 148.0, "put_max": 142.0},
    "MCD":  {"tier": "T2", "bias": "NEUTRAL", "call_min": 310.0, "put_max": 300.0},
    "HD":   {"tier": "T2", "bias": "CALL", "call_min": 380.0, "put_max": 360.0},
    "LULU": {"tier": "T3", "bias": "PUT", "call_min": 215.0, "put_max": 200.0},
    "NKE":  {"tier": "T3", "bias": "PUT", "call_min": 68.0, "put_max": 66.0},
    "DKNG": {"tier": "T3", "bias": "PUT", "call_min": 36.0, "put_max": 33.0},
    "RKT":  {"tier": "T2", "bias": "CALL", "call_min": 23.0, "put_max": 21.0},
    "PDD":  {"tier": "T3", "bias": "PUT", "call_min": 120.0, "put_max": 110.0},
    "PLNT": {"tier": "T3", "bias": "PUT", "call_min": 104.0, "put_max": 102.0},
    "OPEN": {"tier": "T4", "bias": "SPEC", "call_min": 7.5, "put_max": 6.0},
    "F":    {"tier": "T2", "bias": "CALL", "call_min": 13.2, "put_max": 12.8},
    "PFE":  {"tier": "T2", "bias": "NEUTRAL", "call_min": 26.5, "put_max": 25.0},

    # Energy / Industrial / Macro
    "XOM":  {"tier": "T1", "bias": "CALL", "call_min": 122.0, "put_max": 115.0},
    "CVX":  {"tier": "T1", "bias": "CALL", "call_min": 158.0, "put_max": 150.0},
    "OXY":  {"tier": "T2", "bias": "CALL", "call_min": 42.0, "put_max": 40.0},
    "GE":   {"tier": "T1", "bias": "CALL", "call_min": 315.0, "put_max": 300.0},
    "BA":   {"tier": "T2", "bias": "CALL", "call_min": 245.0, "put_max": 230.0},
    "UNH":  {"tier": "T2", "bias": "NEUTRAL", "call_min": 345.0, "put_max": 330.0},
    "UPS":  {"tier": "T2", "bias": "CALL", "call_min": 102.0, "put_max": 100.0},
    "V":    {"tier": "T2", "bias": "PUT", "call_min": 350.0, "put_max": 345.0},
    "MSTR": {"tier": "T3", "bias": "BTC", "call_min": 200.0, "put_max": 175.0},
    "CIFR": {"tier": "T3", "bias": "SPEC", "call_min": 19.0, "put_max": 16.0},
    "SMR":  {"tier": "T4", "bias": "SPEC", "call_min": 22.0, "put_max": 18.0},
    "ARM":  {"tier": "T3", "bias": "PUT", "call_min": 132.0, "put_max": 120.0},
    "ACHR": {"tier": "T4", "bias": "SPEC", "call_min": 9.0, "put_max": 8.0},
}

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
    return {
        "Accept": "application/json, text/plain",
        "Authorization": UW_TOKEN,
    }

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
    # expiry may be "2026-01-17" or ISO; handle both
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
            # merge override
            for k, v in override.items():
                cfg[k.upper()] = {**cfg.get(k.upper(), {}), **v}
        except Exception:
            # if bad json, keep defaults
            pass
    return cfg

TICKER_CONFIG = load_ticker_config()

def tier_dte_range(tier: str) -> Tuple[int, int]:
    # Tier-based expiry windows (from your logic)
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
    # UW sometimes uses "c"/"p"
    if s in ("c",):
        return "CALL"
    if s in ("p",):
        return "PUT"
    return s.upper() if s else "UNK"

def get_underlying_price(a: Dict[str, Any]) -> Optional[float]:
    # Common UW field names:
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
    return None

def get_side(a: Dict[str, Any]) -> str:
    # common values: ask/bid/mid/unknown
    s = str(a.get("side") or a.get("trade_side") or "").lower()
    return s

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

def alert_id(a: Dict[str, Any]) -> str:
    """
    Build a stable-ish hash for cooldown + dedupe.
    Uses ticker + type + strike + expiry + created_at + premium + size.
    """
    parts = [
        get_ticker(a),
        normalize_type(a.get("type")),
        str(get_strike(a) or ""),
        str(get_expiry(a) or ""),
        str(a.get("created_at") or ""),
        str(get_premium(a) or ""),
        str(a.get("total_size") or a.get("size") or ""),
    ]
    raw = "|".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]

# ----------------------------
# Filters + Scoring
# ----------------------------
def allow_alert(ticker: str, opt_type: str, underlying: float, tier: str) -> Tuple[bool, List[str]]:
    """
    Mandatory zone filter based on your table:
    - CALL requires underlying >= call_min
    - PUT  requires underlying <= put_max
    - Also enforces: no puts on T4 by default (noise suppression)
    """
    reasons = []
    cfg = TICKER_CONFIG.get(ticker, {"tier": tier, "bias": "NEUTRAL", "call_min": None, "put_max": None})
    t = (cfg.get("tier") or tier or "T2").upper()
    call_min = safe_float(cfg.get("call_min"))
    put_max = safe_float(cfg.get("put_max"))

    if t == "T4" and opt_type == "PUT":
        return False, ["T4_puts_blocked"]

    if opt_type == "CALL" and call_min is not None and underlying < call_min:
        return False, [f"underlying<{call_min:g}"]
    if opt_type == "PUT" and put_max is not None and underlying > put_max:
        return False, [f"underlying>{put_max:g}"]

    reasons.append("zone_ok")
    return True, reasons

def flow_ok(a: Dict[str, Any], tier: str) -> Tuple[bool, List[str]]:
    """
    Flow quality filter:
    - premium >= MIN_PREMIUM
    - ask side (if ASK_ONLY enabled)
    - vol/oi >= MIN_VOLUME_OI_RATIO
    - delta within band
    - dte within tier range
    """
    reasons = []
    now = datetime.now(timezone.utc)

    premium = get_premium(a)
    if premium is None or premium < MIN_PREMIUM:
        return False, ["premium_low"]

    side = get_side(a)
    if ASK_ONLY and side and side != "ask":
        return False, [f"side_{side}"]

    vol_oi = get_vol_oi(a)
    if vol_oi is None or vol_oi < MIN_VOLUME_OI_RATIO:
        return False, ["voloi_low"]

    delta = get_delta(a)
    if delta is not None:
        if abs(delta) < DELTA_MIN or abs(delta) > DELTA_MAX:
            return False, ["delta_out_of_band"]
    else:
        # if missing delta, be stricter for T1/T2, lenient for T3/T4
        if (tier or "").upper() in ("T1", "T2"):
            return False, ["delta_missing"]

    dte = days_to_expiry(get_expiry(a), now)
    if dte is not None:
        dmin, dmax = tier_dte_range(tier)
        if dte < dmin or dte > dmax:
            return False, [f"dte_{dte}_out_{dmin}-{dmax}"]
    else:
        # if missing DTE/expiry, be strict for T1/T2
        if (tier or "").upper() in ("T1", "T2"):
            return False, ["expiry_missing"]

    reasons.append("flow_ok")
    return True, reasons

def score_alert(ticker: str, opt_type: str, a: Dict[str, Any]) -> Tuple[int, List[str]]:
    """
    Simple 0–100 score. Higher = better.
    Scoring uses premium, vol/oi, side=ask, delta closeness, tier.
    """
    cfg = TICKER_CONFIG.get(ticker, {})
    tier = (cfg.get("tier") or "T2").upper()
    bias = (cfg.get("bias") or "NEUTRAL").upper()

    score = 50
    why = []

    # Tier weight
    if tier == "T1":
        score += 15; why.append("tier_T1")
    elif tier == "T2":
        score += 8; why.append("tier_T2")
    elif tier == "T3":
        score += 0; why.append("tier_T3")
    else:
        score -= 8; why.append("tier_T4")

    # Bias alignment
    if bias in ("CALL", "PUT") and bias == opt_type:
        score += 10; why.append("bias_aligned")
    elif bias == "NEUTRAL":
        score += 0; why.append("bias_neutral")
    else:
        score -= 5; why.append("bias_not_aligned")

    # Ask-side bonus
    if get_side(a) == "ask":
        score += 8; why.append("ask_side")

    # vol/oi bonus
    vol_oi = get_vol_oi(a)
    if vol_oi is not None:
        if vol_oi >= 3.0:
            score += 12; why.append("voloi_3+")
        elif vol_oi >= 2.0:
            score += 8; why.append("voloi_2+")
        elif vol_oi >= 1.2:
            score += 4; why.append("voloi_1.2+")

    # premium bonus
    prem = get_premium(a) or 0
    if prem >= 1_000_000:
        score += 12; why.append("prem_1m+")
    elif prem >= 500_000:
        score += 8; why.append("prem_500k+")
    elif prem >= 200_000:
        score += 4; why.append("prem_200k+")

    # delta preference around ~0.40–0.55 for scalps
    d = get_delta(a)
    if d is not None:
        ad = abs(d)
        if 0.40 <= ad <= 0.55:
            score += 8; why.append("delta_sweetspot")
        elif 0.30 <= ad < 0.40 or 0.55 < ad <= 0.65:
            score += 3; why.append("delta_ok")

    # clamp
    score = max(0, min(100, score))
    return score, why

# ----------------------------
# Telegram
# ----------------------------
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

def fmt_alert(a: Dict[str, Any]) -> str:
    ticker = get_ticker(a)
    cfg = TICKER_CONFIG.get(ticker, {"tier": "T2", "bias": "NEUTRAL"})
    tier = (cfg.get("tier") or "T2").upper()
    bias = (cfg.get("bias") or "NEUTRAL").upper()

    opt_type = normalize_type(a.get("type"))
    strike = get_strike(a)
    expiry = get_expiry(a)
    rule = get_rule(a)
    created_at = a.get("created_at")

    prem = get_premium(a)
    size = a.get("total_size") or a.get("size")
    vol = a.get("volume")
    oi = a.get("open_interest")
    voloi = get_vol_oi(a)
    delta = get_delta(a)
    side = get_side(a)
    underlying = get_underlying_price(a)
    chain = a.get("option_chain") or a.get("contract") or a.get("symbol")

    score, why = score_alert(ticker, opt_type, a)

    # Zones (for display)
    call_min = cfg.get("call_min")
    put_max = cfg.get("put_max")
    zone_line = f"• Zones: CALL ≥ *{call_min}* | PUT ≤ *{put_max}*" if (call_min is not None and put_max is not None) else "• Zones: n/a"

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}*  |  *Score:* `{score}/100`",
        f"• Tier/Bias: `{tier}` / `{bias}`",
        f"• Rule: `{rule}` | Side: `{side or 'n/a'}`",
        zone_line,
        f"• Contract: `{chain}`" if chain else f"• {opt_type} {strike} @ {expiry}",
        f"• Underlying: `{underlying}` | Δ: `{delta}` | Vol/OI: `{voloi}`",
        f"• Premium: *{money(prem)}* | Size: `{size}` | Vol: `{vol}` | OI: `{oi}`",
        f"• Why: `{', '.join(why[:6])}`",
        f"• Time: `{created_at}`",
    ]
    return "\n".join(lines)

# ----------------------------
# Core poller
# ----------------------------
class State:
    running: bool = False
    last_seen_created_at: Optional[str] = None
    cooldown: Dict[str, datetime] = {}  # alert_hash -> last_sent_time

state = State()

async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}

    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    # Keep UW endpoint filters light; do most logic locally
    params["min_premium"] = MIN_PREMIUM
    params["min_volume_oi_ratio"] = MIN_VOLUME_OI_RATIO
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

def dedupe_and_sort(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(a: Dict[str, Any]):
        ca = a.get("created_at") or ""
        try:
            return parse_iso(ca)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    alerts_sorted = sorted(alerts, key=key)

    if not state.last_seen_created_at:
        # first run: avoid spamming old history
        return alerts_sorted[-10:]

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
    h = alert_id(a)
    now = datetime.now(timezone.utc)
    last = state.cooldown.get(h)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return False
    state.cooldown[h] = now
    # prune occasionally
    if len(state.cooldown) > 5000:
        cutoff = now - timedelta(seconds=COOLDOWN_SECONDS * 3)
        state.cooldown = {k: v for k, v in state.cooldown.items() if v >= cutoff}
    return True

async def poll_loop():
    require_env()
    state.running = True

    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                alerts = await fetch_flow_alerts(client)
                fresh = dedupe_and_sort(alerts)

                for a in fresh:
                    ticker = get_ticker(a)
                    cfg = TICKER_CONFIG.get(ticker)

                    # If user restricted tickers, we already filtered; if not, you can choose to only alert
                    # on tickers we have config for:
                    if cfg is None and not TICKERS:
                        # Skip unknown tickers to reduce noise
                        continue

                    tier = (cfg.get("tier") if cfg else "T2") or "T2"
                    opt_type = normalize_type(a.get("type"))
                    underlying = get_underlying_price(a)

                    # Must have underlying for zone logic
                    if underlying is None:
                        continue

                    ok_zone, why_zone = allow_alert(ticker, opt_type, underlying, tier)
                    if not ok_zone:
                        continue

                    ok_flow, why_flow = flow_ok(a, tier)
                    if not ok_flow:
                        continue

                    if not cooldown_ok(a):
                        continue

                    # Passed all filters -> send
                    msg = fmt_alert(a) + f"\n• Passed: `{', '.join(why_zone + why_flow)}`"
                    await telegram_send(msg, client)

                    if a.get("created_at"):
                        state.last_seen_created_at = a["created_at"]

            except Exception:
                # keep running; don't spam TG on transient errors
                pass

            await asyncio.sleep(POLL_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Flow Alerts → Telegram (Tier+Zones+Scoring)")

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
        "last_seen_created_at": state.last_seen_created_at,
        "tickers_env_filter": TICKERS,
        "min_premium": MIN_PREMIUM,
        "min_volume_oi_ratio": MIN_VOLUME_OI_RATIO,
        "ask_only": ASK_ONLY,
        "delta_band": [DELTA_MIN, DELTA_MAX],
        "cooldown_seconds": COOLDOWN_SECONDS,
        "config_count": len(TICKER_CONFIG),
    }

@app.get("/config")
def config():
    # Helpful to verify your table/config loaded correctly
    return {"tickers": TICKER_CONFIG}

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
    sample = alerts[:3]
    return {"count": len(alerts), "sample": sample}

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "UW Flow Alerts → Telegram (Tier+Zones+Scoring)",
        "endpoints": ["/docs", "/health", "/config", "/debug/uw", "/test/telegram", "/control/start", "/control/stop", "/control/reset_cursor"],
    }
