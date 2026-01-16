import os
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from math import fabs
from zoneinfo import ZoneInfo

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

# UW-side filters (server-side filtering)
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))
MIN_DTE = int(os.getenv("MIN_DTE", "1"))
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "1.0"))
RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

# Local quality upgrades (client-side scoring/gating)
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "70"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "10"))

MIN_OI = int(os.getenv("MIN_OI", "300"))
MIN_CONTRACT_VOL = int(os.getenv("MIN_CONTRACT_VOL", "200"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "10"))  # only if bid/ask present
MAX_STRIKE_DISTANCE_PCT = float(os.getenv("MAX_STRIKE_DISTANCE_PCT", "3.0"))
MAX_DTE = int(os.getenv("MAX_DTE", "7"))

MAX_ALERTS_PER_TICKER_WINDOW = int(os.getenv("MAX_ALERTS_PER_TICKER_WINDOW", "3"))
RATE_WINDOW_SECONDS = int(os.getenv("RATE_WINDOW_SECONDS", "120"))

RULE_WEIGHTS = {
    "Sweep": 25,
    "Sweeps": 25,
    "RepeatedHits": 18,
    "RepeatedHitsAscendingFill": 20,
    "RepeatedHitsDescendingFill": 20,
    "Block": 12,
}

# Trend confirmation (Polygon)
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
CONFIRM_STRICT = os.getenv("CONFIRM_STRICT", "true").lower() == "true"
CONFIRM_TTL_SECONDS = int(os.getenv("CONFIRM_TTL_SECONDS", "20"))
ORB_MINUTES = int(os.getenv("ORB_MINUTES", "15"))

CONFIRM_VWAP_REQUIRED = os.getenv("CONFIRM_VWAP_REQUIRED", "true").lower() == "true"
CONFIRM_ORB_REQUIRED = os.getenv("CONFIRM_ORB_REQUIRED", "true").lower() == "true"

NY_TZ = ZoneInfo("America/New_York")

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
    if CONFIRM_STRICT and not POLYGON_API_KEY:
        missing.append("POLYGON_API_KEY (required when CONFIRM_STRICT=true)")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def uw_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain",
        "Authorization": UW_TOKEN,
    }


def parse_iso(dt_str: str) -> datetime:
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)


def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    try:
        return f"${float(x):,.0f}"
    except Exception:
        return "n/a"


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v, default=None):
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def contract_key(a: Dict[str, Any]) -> str:
    if a.get("option_chain"):
        return str(a["option_chain"])
    return f'{a.get("ticker")}:{a.get("expiry")}:{a.get("strike")}:{a.get("type")}'


def calc_dte(a: Dict[str, Any]) -> Optional[int]:
    exp = a.get("expiry")
    if not exp:
        return None
    try:
        exp_dt = datetime.fromisoformat(str(exp)).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0, (exp_dt.date() - now.date()).days)
    except Exception:
        return None


def strike_distance_pct(a: Dict[str, Any]) -> Optional[float]:
    strike = safe_float(a.get("strike"))
    und = safe_float(a.get("underlying_price"))
    if strike is None or und is None or und <= 0:
        return None
    return 100.0 * fabs(strike - und) / und


def spread_pct(a: Dict[str, Any]) -> Optional[float]:
    bid = safe_float(a.get("bid"))
    ask = safe_float(a.get("ask"))
    if bid is None or ask is None or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return 100.0 * (ask - bid) / mid


def time_of_day_bonus() -> int:
    hour = datetime.now(timezone.utc).hour
    if 14 <= hour <= 16:
        return 8
    if 19 <= hour <= 21:
        return 6
    return 0


def liquidity_gate(a: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons = []

    oi = safe_int(a.get("open_interest"))
    vol = safe_int(a.get("volume"))

    if oi is not None and oi < MIN_OI:
        reasons.append(f"OI<{MIN_OI}")
    if vol is not None and vol < MIN_CONTRACT_VOL:
        reasons.append(f"VOL<{MIN_CONTRACT_VOL}")

    dte = calc_dte(a)
    if dte is not None and dte > MAX_DTE:
        reasons.append(f"DTE>{MAX_DTE}")

    dist = strike_distance_pct(a)
    if dist is not None and dist > MAX_STRIKE_DISTANCE_PCT:
        reasons.append(f"StrikeDist>{MAX_STRIKE_DISTANCE_PCT:.1f}%")

    sp = spread_pct(a)
    if sp is not None and sp > MAX_SPREAD_PCT:
        reasons.append(f"Spread>{MAX_SPREAD_PCT:.0f}%")

    return (len(reasons) == 0, reasons)


def score_alert(a: Dict[str, Any]) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    score = 0.0

    premium = safe_float(a.get("total_premium"), 0.0) or 0.0
    voi = safe_float(a.get("volume_oi_ratio"), 0.0) or 0.0
    rule = (a.get("alert_rule") or a.get("rule_name") or a.get("rule") or "").strip()

    if premium >= 1_000_000:
        score += 28
        reasons.append("Premium>=1M")
    elif premium >= 500_000:
        score += 22
        reasons.append("Premium>=500k")
    elif premium >= 200_000:
        score += 16
        reasons.append("Premium>=200k")
    elif premium >= 75_000:
        score += 10
        reasons.append("Premium>=75k")
    else:
        score += 4

    if voi >= 5:
        score += 28
        reasons.append("Vol/OI>=5")
    elif voi >= 3:
        score += 22
        reasons.append("Vol/OI>=3")
    elif voi >= 2:
        score += 16
        reasons.append("Vol/OI>=2")
    elif voi >= 1:
        score += 10
        reasons.append("Vol/OI>=1")
    else:
        score += 2

    rw = 0
    for k, w in RULE_WEIGHTS.items():
        if k.lower() in rule.lower():
            rw = max(rw, w)
    if rw:
        score += rw
        reasons.append(f"Rule:{rule}")

    dte = calc_dte(a)
    if dte is not None:
        if dte <= 2:
            score += 12
            reasons.append("DTE<=2")
        elif dte <= 7:
            score += 8
            reasons.append("DTE<=7")
        else:
            score -= 10
            reasons.append("DTE>7")

    dist = strike_distance_pct(a)
    if dist is not None:
        if dist <= 1.0:
            score += 10
            reasons.append("NearATM<=1%")
        elif dist <= 3.0:
            score += 6
            reasons.append("NearATM<=3%")
        else:
            score -= 8
            reasons.append("FarOTM")

    sp = spread_pct(a)
    if sp is not None:
        if sp <= 5:
            score += 6
            reasons.append("TightSpread<=5%")
        elif sp <= 10:
            score += 2
        else:
            score -= 12
            reasons.append("WideSpread")

    tb = time_of_day_bonus()
    if tb:
        score += tb
        reasons.append("TimeBonus")

    return clamp(score, 0, 100), reasons


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


def fmt_alert(
    a: Dict[str, Any],
    score: Optional[float] = None,
    reasons: Optional[List[str]] = None,
    bias: Optional[str] = None,
    confirm: Optional[Dict[str, Any]] = None,
    confirm_notes: Optional[List[str]] = None,
) -> str:
    ticker = a.get("ticker", "UNK")
    opt_type = str(a.get("type", "")).upper()
    strike = a.get("strike")
    expiry = a.get("expiry")
    rule = a.get("alert_rule") or a.get("rule_name") or a.get("rule") or "FLOW"
    created_at = a.get("created_at")

    total_premium = a.get("total_premium")
    total_size = a.get("total_size")
    vol = a.get("volume")
    oi = a.get("open_interest")
    voi = a.get("volume_oi_ratio")

    underlying = a.get("underlying_price")
    opt_price = a.get("price")
    chain = a.get("option_chain")

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}*",
        f"• Rule: `{rule}`",
        f"• Contract: `{chain}`" if chain else f"• {opt_type} {strike} @ {expiry}",
        f"• Option Px: `{opt_price}` | Underlying: `{underlying}`",
        f"• Premium: *{money(total_premium)}* | Size: `{total_size}`",
        f"• Vol: `{vol}` | OI: `{oi}` | Vol/OI: `{voi}`",
        f"• Time: `{created_at}`",
    ]

    if bias:
        lines.append(f"\n🧭 *Bias:* `{bias.upper()}`")

    if score is not None:
        lines.append(f"⭐ *Score:* `{score:.0f}/100`")
    if reasons:
        lines.append("✅ *Why:* " + ", ".join([f"`{r}`" for r in reasons[:6]]))

    if confirm is not None:
        lines.append("\n📈 *Trend Confirm*")
        if confirm.get("ok"):
            lines.append(
                f"• Last: `{confirm.get('last')}` | VWAP: `{confirm.get('vwap')}`"
            )
            lines.append(
                f"• ORB{ORB_MINUTES} High: `{confirm.get('orb_high')}` | ORB{ORB_MINUTES} Low: `{confirm.get('orb_low')}`"
            )
        else:
            lines.append(f"• Unavailable: `{confirm.get('reason')}`")
        if confirm_notes:
            lines.append("• Notes: " + ", ".join([f"`{x}`" for x in confirm_notes[:3]]))

    return "\n".join(lines)

# ----------------------------
# Bias + Trend Confirmation (Polygon VWAP/ORB)
# ----------------------------
_confirm_cache: Dict[str, Any] = {}

def _now_ny() -> datetime:
    return datetime.now(timezone.utc).astimezone(NY_TZ)

def _session_day_ny() -> str:
    return _now_ny().date().isoformat()

def _market_window_ny() -> Tuple[datetime, datetime]:
    now = _now_ny()
    start = now.replace(hour=9, minute=30, second=0, microsecond=0)
    end = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return start, end

async def polygon_get_minute_bars(ticker: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    day = _session_day_ny()
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{day}/{day}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": POLYGON_API_KEY,
    }
    r = await client.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data.get("results") or []

def compute_vwap_and_orb(bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not bars:
        return {"ok": False, "reason": "no_bars"}

    start, end = _market_window_ny()
    now = _now_ny()

    session_bars: List[Tuple[datetime, Dict[str, Any]]] = []
    for b in bars:
        ts = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc).astimezone(NY_TZ)
        if ts < start or ts > end:
            continue
        if ts > now:
            continue
        session_bars.append((ts, b))

    if len(session_bars) < ORB_MINUTES:
        return {"ok": False, "reason": "insufficient_session_bars", "count": len(session_bars)}

    orb_end = start + timedelta(minutes=ORB_MINUTES)
    orb_bars = [b for (ts, b) in session_bars if start <= ts < orb_end]
    if not orb_bars:
        return {"ok": False, "reason": "no_orb_bars"}

    orb_high = max(float(b.get("h", 0) or 0) for b in orb_bars)
    orb_low = min(float(b.get("l", 0) or 0) for b in orb_bars)

    pv_sum = 0.0
    v_sum = 0.0
    for _, b in session_bars:
        v = float(b.get("v", 0) or 0)
        if v <= 0:
            continue
        h = float(b.get("h", 0) or 0)
        l = float(b.get("l", 0) or 0)
        c = float(b.get("c", 0) or 0)
        tp = (h + l + c) / 3.0
        pv_sum += tp * v
        v_sum += v

    vwap = (pv_sum / v_sum) if v_sum > 0 else None
    last = float(session_bars[-1][1].get("c", 0) or 0)

    return {
        "ok": True,
        "last": last,
        "vwap": vwap,
        "orb_high": orb_high,
        "orb_low": orb_low,
        "session_bars": len(session_bars),
    }

async def get_confirmation_metrics(ticker: str, client: httpx.AsyncClient) -> Dict[str, Any]:
    now_epoch = datetime.now(timezone.utc).timestamp()
    cached = _confirm_cache.get(ticker)
    if cached and cached["exp"] > now_epoch:
        return cached["val"]

    if not POLYGON_API_KEY:
        val = {"ok": False, "reason": "missing_polygon_key"}
        _confirm_cache[ticker] = {"exp": now_epoch + CONFIRM_TTL_SECONDS, "val": val}
        return val

    bars = await polygon_get_minute_bars(ticker, client)
    metrics = compute_vwap_and_orb(bars)
    _confirm_cache[ticker] = {"exp": now_epoch + CONFIRM_TTL_SECONDS, "val": metrics}
    return metrics

def infer_bias(a: Dict[str, Any]) -> str:
    t = str(a.get("type", "")).lower()
    if "call" in t or t == "c":
        return "bull"
    if "put" in t or t == "p":
        return "bear"
    return "unknown"

def confirmation_passes(bias: str, m: Dict[str, Any]) -> Tuple[bool, List[str]]:
    if not m.get("ok"):
        return (False, [f"confirm_unavailable:{m.get('reason')}"])

    last = m.get("last")
    vwap = m.get("vwap")
    orb_high = m.get("orb_high")
    orb_low = m.get("orb_low")

    if last is None:
        return (False, ["no_last"])

    reasons: List[str] = []

    if CONFIRM_VWAP_REQUIRED:
        if vwap is None:
            return (False, ["no_vwap"])
        if bias == "bull" and not (last > vwap):
            reasons.append("below_vwap")
        if bias == "bear" and not (last < vwap):
            reasons.append("above_vwap")

    if CONFIRM_ORB_REQUIRED:
        if orb_high is None or orb_low is None:
            return (False, ["no_orb"])
        if bias == "bull" and not (last > orb_high):
            reasons.append("below_orb_high")
        if bias == "bear" and not (last < orb_low):
            reasons.append("above_orb_low")

    return (len(reasons) == 0, reasons)

# ----------------------------
# Core poller state + dedupe
# ----------------------------
class State:
    running: bool = False
    last_seen_created_at: Optional[str] = None

state = State()

_seen_contracts: Dict[str, datetime] = {}
_ticker_recent: Dict[str, deque] = defaultdict(deque)

def cooldown_ok(a: Dict[str, Any]) -> bool:
    key = contract_key(a)
    now = datetime.now(timezone.utc)
    last = _seen_contracts.get(key)
    if last and (now - last).total_seconds() < COOLDOWN_MINUTES * 60:
        return False
    _seen_contracts[key] = now
    return True

def ticker_rate_limit_ok(ticker: str) -> bool:
    now = datetime.now(timezone.utc)
    q = _ticker_recent[ticker]
    while q and (now - q[0]).total_seconds() > RATE_WINDOW_SECONDS:
        q.popleft()
    if len(q) >= MAX_ALERTS_PER_TICKER_WINDOW:
        return False
    q.append(now)
    return True

async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}

    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    params["min_dte"] = MIN_DTE
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

async def poll_loop():
    require_env()
    state.running = True

    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                alerts = await fetch_flow_alerts(client)
                fresh = dedupe_and_sort(alerts)

                for a in fresh:
                    if not cooldown_ok(a):
                        continue

                    ticker = (a.get("ticker") or "UNK").upper()
                    if not ticker_rate_limit_ok(ticker):
                        continue

                    ok, _gate_reasons = liquidity_gate(a)
                    if not ok:
                        continue

                    sc, sc_reasons = score_alert(a)
                    if sc < SCORE_THRESHOLD:
                        continue

                    bias = infer_bias(a)

                    # Trend confirmation (VWAP + ORB)
                    confirm = None
                    confirm_notes: List[str] = []
                    confirm_ok = True

                    if bias in ("bull", "bear"):
                        confirm = await get_confirmation_metrics(ticker, client)
                        confirm_ok, confirm_notes = confirmation_passes(bias, confirm)

                        if not confirm_ok and CONFIRM_STRICT:
                            continue

                        # Score adjust based on confirmation
                        if confirm_ok:
                            sc = min(100, sc + 10)
                            sc_reasons = sc_reasons + [f"CONFIRM:{bias.upper()}"]
                        else:
                            sc = max(0, sc - 10)
                            sc_reasons = sc_reasons + [f"NO_CONFIRM:{','.join(confirm_notes[:2])}"]

                    msg = fmt_alert(
                        a,
                        score=sc,
                        reasons=sc_reasons,
                        bias=bias if bias != "unknown" else None,
                        confirm=confirm,
                        confirm_notes=confirm_notes if confirm_notes else None,
                    )
                    await telegram_send(msg, client)

                    if a.get("created_at"):
                        state.last_seen_created_at = a["created_at"]

            except Exception:
                pass

            await asyncio.sleep(POLL_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Flow Alerts → Telegram (Scored + VWAP/ORB Confirmed)")

@app.on_event("startup")
async def startup():
    asyncio.create_task(poll_loop())

@app.on_event("shutdown")
async def shutdown():
    state.running = False

class TestMessage(BaseModel):
    text: str

class ConfirmReq(BaseModel):
    ticker: str

@app.get("/")
def root():
    return {"ok": True, "hint": "Use /docs or /health"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "running": state.running,
        "tickers": TICKERS,
        "poll_seconds": POLL_SECONDS,
        "last_seen_created_at": state.last_seen_created_at,
        "score_threshold": SCORE_THRESHOLD,
        "cooldown_minutes": COOLDOWN_MINUTES,
        "confirm_strict": CONFIRM_STRICT,
        "orb_minutes": ORB_MINUTES,
        "confirm_ttl_seconds": CONFIRM_TTL_SECONDS,
    }

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
    return {"count": len(alerts), "sample": alerts[:2]}

@app.post("/debug/send_best_now")
async def send_best_now():
    require_env()
    async with httpx.AsyncClient() as client:
        alerts = await fetch_flow_alerts(client)

        best = None
        best_score = -1.0
        best_reasons: List[str] = []
        best_bias = None
        best_confirm = None
        best_confirm_notes: List[str] = []

        for a in alerts:
            ok, _ = liquidity_gate(a)
            if not ok:
                continue

            sc, rs = score_alert(a)
            bias = infer_bias(a)

            confirm = None
            confirm_ok = True
            confirm_notes: List[str] = []

            if bias in ("bull", "bear"):
                confirm = await get_confirmation_metrics((a.get("ticker") or "").upper(), client)
                confirm_ok, confirm_notes = confirmation_passes(bias, confirm)
                if CONFIRM_STRICT and not confirm_ok:
                    continue
                if confirm_ok:
                    sc = min(100, sc + 10)
                    rs = rs + [f"CONFIRM:{bias.upper()}"]
                else:
                    sc = max(0, sc - 10)
                    rs = rs + [f"NO_CONFIRM:{','.join(confirm_notes[:2])}"]

            if sc > best_score:
                best_score = sc
                best = a
                best_reasons = rs
                best_bias = bias
                best_confirm = confirm
                best_confirm_notes = confirm_notes

        if not best:
            return {"sent": False, "reason": "No alert passed gates/confirmation"}

        msg = fmt_alert(
            best,
            score=best_score,
            reasons=best_reasons,
            bias=best_bias if best_bias != "unknown" else None,
            confirm=best_confirm,
            confirm_notes=best_confirm_notes if best_confirm_notes else None,
        )
        await telegram_send(msg, client)
        return {"sent": True, "score": best_score, "ticker": best.get("ticker")}

@app.post("/debug/confirm")
async def debug_confirm(body: ConfirmReq):
    require_env()
    ticker = body.ticker.strip().upper()
    async with httpx.AsyncClient() as client:
        m = await get_confirmation_metrics(ticker, client)
    return m
