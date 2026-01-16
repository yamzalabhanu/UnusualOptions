import os
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
from collections import defaultdict, deque
from math import fabs

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
    # Only works if UW includes bid/ask in response (some plans/fields do)
    bid = safe_float(a.get("bid"))
    ask = safe_float(a.get("ask"))
    if bid is None or ask is None or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return 100.0 * (ask - bid) / mid


def time_of_day_bonus() -> int:
    # Simple UTC-based bonus; refine later using NY timezone + market calendar
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

    # Premium bucket score
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

    # Vol/OI unusualness
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

    # Rule weight
    rw = 0
    for k, w in RULE_WEIGHTS.items():
        if k.lower() in rule.lower():
            rw = max(rw, w)
    if rw:
        score += rw
        reasons.append(f"Rule:{rule}")

    # DTE bonus/penalty
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

    # Near-ATM bonus
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

    # Spread bonus/penalty if available
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

    # Time-of-day bonus
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


def fmt_alert(a: Dict[str, Any], score: Optional[float] = None, reasons: Optional[List[str]] = None) -> str:
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

    if score is not None:
        lines.append(f"\n⭐ *Score:* `{score:.0f}/100`")
    if reasons:
        lines.append("✅ *Why:* " + ", ".join([f"`{r}`" for r in reasons[:6]]))

    return "\n".join(lines)

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
                    # 1) contract cooldown
                    if not cooldown_ok(a):
                        continue

                    ticker = (a.get("ticker") or "UNK").upper()

                    # 2) per-ticker rate limit
                    if not ticker_rate_limit_ok(ticker):
                        continue

                    # 3) liquidity gate
                    ok, gate_reasons = liquidity_gate(a)
                    if not ok:
                        continue

                    # 4) score gate
                    sc, sc_reasons = score_alert(a)
                    if sc < SCORE_THRESHOLD:
                        continue

                    # 5) send
                    msg = fmt_alert(a, score=sc, reasons=sc_reasons)
                    await telegram_send(msg, client)

                    # cursor update
                    if a.get("created_at"):
                        state.last_seen_created_at = a["created_at"]

            except Exception:
                # Keep running; avoid spamming Telegram on transient errors
                pass

            await asyncio.sleep(POLL_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Flow Alerts → Telegram (Scored)")

@app.on_event("startup")
async def startup():
    asyncio.create_task(poll_loop())

@app.on_event("shutdown")
async def shutdown():
    state.running = False

class TestMessage(BaseModel):
    text: str

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

        for a in alerts:
            ok, _ = liquidity_gate(a)
            if not ok:
                continue
            sc, rs = score_alert(a)
            if sc > best_score:
                best_score = sc
                best = a
                best_reasons = rs

        if not best:
            return {"sent": False, "reason": "No alert passed liquidity gate"}

        msg = fmt_alert(best, score=best_score, reasons=best_reasons)
        await telegram_send(msg, client)
        return {"sent": True, "score": best_score, "ticker": best.get("ticker")}
