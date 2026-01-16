import os
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ----------------------------
# Config
# ----------------------------
UW_BASE_URL = "https://api.unusualwhales.com"
UW_FLOW_ALERTS_PATH = "/api/option-trades/flow-alerts"  # from UW public example :contentReference[oaicite:2]{index=2}

UW_TOKEN = os.getenv("UW_TOKEN", "").strip()
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))
MIN_DTE = int(os.getenv("MIN_DTE", "1"))
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "1.0"))

RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

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
    # Matches UW public example: Accept + Authorization token :contentReference[oaicite:3]{index=3}
    return {
        "Accept": "application/json, text/plain",
        "Authorization": UW_TOKEN,
    }

def parse_iso(dt_str: str) -> datetime:
    # UW example shows created_at like "2024-10-04T19:59:41.509377Z" :contentReference[oaicite:4]{index=4}
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"

def fmt_alert(a: Dict[str, Any]) -> str:
    """
    Expected keys (based on UW example output columns):
      ticker, type (call/put), strike, expiry, price, underlying_price,
      total_premium, total_size, volume, open_interest, volume_oi_ratio,
      alert_rule (rule), created_at, option_chain
    :contentReference[oaicite:5]{index=5}
    """
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

    # Telegram supports Markdown/HTML formatting (sendMessage formatting is supported) :contentReference[oaicite:6]{index=6}
    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}*",
        f"• Rule: `{rule}`",
        f"• Contract: `{chain}`" if chain else f"• {opt_type} {strike} @ {expiry}",
        f"• Option Px: `{opt_price}` | Underlying: `{underlying}`",
        f"• Premium: *{money(total_premium)}* | Size: `{total_size}`",
        f"• Vol: `{vol}` | OI: `{oi}` | Vol/OI: `{voi}`",
        f"• Time: `{created_at}`",
    ]
    return "\n".join(lines)

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
# Core poller
# ----------------------------
class State:
    running: bool = False
    last_seen_created_at: Optional[str] = None

state = State()

async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}

    # From UW example: issue_types[] / min_dte / min_premium / min_volume_oi_ratio / rule_name[] :contentReference[oaicite:7]{index=7}
    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    params["min_dte"] = MIN_DTE
    params["min_premium"] = MIN_PREMIUM
    params["min_volume_oi_ratio"] = MIN_VOLUME_OI_RATIO
    if RULE_NAMES:
        params["rule_name[]"] = RULE_NAMES

    # Optional tickers filter (not shown in the snippet, but commonly supported for flow endpoints)
    if TICKERS:
        params["tickers"] = ",".join(TICKERS)

    url = f"{UW_BASE_URL}{UW_FLOW_ALERTS_PATH}"
    r = await client.get(url, headers=uw_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # UW responses can be either {"data":[...]} or direct list depending on endpoint/version
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    if isinstance(data, list):
        return data
    return []

def dedupe_and_sort(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Sort oldest -> newest by created_at for clean sending order
    def key(a: Dict[str, Any]):
        ca = a.get("created_at") or ""
        try:
            return parse_iso(ca)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    alerts_sorted = sorted(alerts, key=key)

    if not state.last_seen_created_at:
        # first run: don't spam old history; only keep newest few
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
                    msg = fmt_alert(a)
                    await telegram_send(msg, client)
                    # Update last seen cursor as we go
                    if a.get("created_at"):
                        state.last_seen_created_at = a["created_at"]

            except Exception as e:
                # Keep running; optionally send an error message or log
                # (Avoid spamming Telegram on transient errors)
                pass

            await asyncio.sleep(POLL_SECONDS)

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="UW Flow Alerts → Telegram")

@app.on_event("startup")
async def startup():
    # Start background poller
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
        "tickers": TICKERS,
        "poll_seconds": POLL_SECONDS,
        "last_seen_created_at": state.last_seen_created_at,
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
