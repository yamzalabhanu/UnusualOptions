"""
app.py — Unusual Whales Public API → Telegram (Updated auth + includes /api/market/market-tide)

You confirmed this works:
curl https://api.unusualwhales.com/api/market/market-tide \
  -H "Accept: application/json" \
  -H "Authorization: Bearer <TOKEN>"

So we update the script to match curl EXACTLY:
✅ Always sends:
   Accept: application/json
   Authorization: Bearer <UW_TOKEN>

Also adds:
✅ /debug/market-tide  (calls the same endpoint you tested)
✅ /debug/headers      (shows header presence safely, no token leak)
✅ Better error logging (no silent pass)

The rest of the script keeps the “unusual options volume/OI” logic using UW public APIs:
- /api/option-trades/flow-alerts
- /api/stock/{ticker}/expiry-breakdown
- /api/stock/{ticker}/option-contracts
"""

import os
import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger("uw_public")

# ----------------------------
# Public API base + paths
# ----------------------------
UW_BASE_URL = "https://api.unusualwhales.com"

FLOW_ALERTS_PATH = "/api/option-trades/flow-alerts"
EXPIRY_BREAKDOWN_PATH = "/api/stock/{ticker}/expiry-breakdown"
OPTION_CONTRACTS_PATH = "/api/stock/{ticker}/option-contracts"
MARKET_TIDE_PATH = "/api/market/market-tide"  # curl-tested endpoint

# ----------------------------
# Env
# ----------------------------
UW_TOKEN = os.getenv("UW_TOKEN", "").strip()              # must be RAW token, no "Bearer "
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "").split(",") if t.strip()]
RULE_NAMES = [x.strip() for x in os.getenv("RULE_NAMES", "").split(",") if x.strip()]
ISSUE_TYPES = [x.strip() for x in os.getenv("ISSUE_TYPES", "").split(",") if x.strip()]

POLL_FLOW_SECONDS = int(os.getenv("POLL_FLOW_SECONDS", "15"))
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "180"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "600"))

# Triggers
MIN_UNUSUAL_VOLUME = int(os.getenv("MIN_UNUSUAL_VOLUME", "500"))
MIN_UNUSUAL_OI = int(os.getenv("MIN_UNUSUAL_OI", "2000"))
MIN_VOLUME_OI_RATIO = float(os.getenv("MIN_VOLUME_OI_RATIO", "0"))  # 0 disables
REQUIRE_VOL_GREATER_OI = os.getenv("REQUIRE_VOL_GREATER_OI", "0").strip() == "1"

OPTION_TYPES = [x.strip().upper() for x in os.getenv("OPTION_TYPES", "CALL,PUT").split(",") if x.strip()]

# Flow quality (optional)
ASK_ONLY = os.getenv("ASK_ONLY", "0").strip() == "1"
MIN_PREMIUM = float(os.getenv("MIN_PREMIUM", "200000"))

# Chain scan filters
EXCLUDE_ZERO_VOL = os.getenv("EXCLUDE_ZERO_VOL", "1").strip() == "1"
EXCLUDE_ZERO_OI = os.getenv("EXCLUDE_ZERO_OI", "1").strip() == "1"
EXCLUDE_ZERO_DTE = os.getenv("EXCLUDE_ZERO_DTE", "1").strip() == "1"
MAYBE_OTM_ONLY = os.getenv("MAYBE_OTM_ONLY", "0").strip() == "1"

MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "8"))
FIRST_RUN_SKIP_HISTORY = os.getenv("FIRST_RUN_SKIP_HISTORY", "1").strip() == "1"

# ----------------------------
# Models
# ----------------------------
class TestMessage(BaseModel):
    text: str

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
    """
    Match curl exactly:
      Accept: application/json
      Authorization: Bearer <token>
    """
    token = UW_TOKEN.strip()
    # allow user to accidentally include "Bearer " and still work
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()
    if not token:
        raise RuntimeError("UW_TOKEN is empty after stripping")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

def safe_int(x, default=None):
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def normalize_type(s: Any) -> str:
    v = str(s or "").lower()
    if "call" in v or v == "c":
        return "CALL"
    if "put" in v or v == "p":
        return "PUT"
    return v.upper() if v else "UNK"

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def alert_hash(parts: List[Any]) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:16]

def money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"${x:,.0f}"

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

# ----------------------------
# State + caching
# ----------------------------
@dataclass
class CacheItem:
    value: Any
    expires_at: datetime

class State:
    running: bool = False
    last_seen_created_at: Optional[str] = None
    cooldown: Dict[str, datetime] = {}
    expiries: Dict[str, CacheItem] = {}
    last_error: Optional[str] = None
    last_flow_poll_at: Optional[str] = None
    last_scan_at: Optional[str] = None
    last_sent: int = 0

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

# ----------------------------
# UW requests
# ----------------------------
async def uw_get(client: httpx.AsyncClient, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{UW_BASE_URL}{path}"
    r = await client.get(url, headers=uw_headers(), params=params or {}, timeout=30)
    # If auth fails, log header presence (without leaking token)
    if r.status_code in (401, 403):
        log.error("UW auth failed status=%s url=%s token_len=%s", r.status_code, url, len(UW_TOKEN or ""))
    r.raise_for_status()
    return r.json()

async def fetch_market_tide(client: httpx.AsyncClient) -> Any:
    return await uw_get(client, MARKET_TIDE_PATH, params={})

async def fetch_flow_alerts_for_ticker(client: httpx.AsyncClient, ticker: str, newer_than: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "limit": 200,
        "min_premium": MIN_PREMIUM,
        "ticker_symbol": ticker,
    }
    if RULE_NAMES:
        params["rule_name[]"] = RULE_NAMES
    if ISSUE_TYPES:
        params["issue_types[]"] = ISSUE_TYPES
    if ASK_ONLY:
        params["is_ask_side"] = True
    if REQUIRE_VOL_GREATER_OI:
        params["vol_greater_oi"] = True
    if newer_than:
        params["newer_than"] = newer_than

    data = await uw_get(client, FLOW_ALERTS_PATH, params=params)
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return [x for x in data["data"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []

async def fetch_expiry_breakdown(client: httpx.AsyncClient, ticker: str) -> List[str]:
    now = datetime.now(timezone.utc)
    c = state.expiries.get(ticker)
    if c and c.expires_at > now:
        return c.value

    data = await uw_get(client, EXPIRY_BREAKDOWN_PATH.format(ticker=ticker), params={})
    expiries: List[str] = []

    if isinstance(data, dict) and isinstance(data.get("data"), list):
        for row in data["data"]:
            if isinstance(row, dict):
                e = row.get("expiry") or row.get("expiration")
                if e:
                    expiries.append(str(e))

    expiries = sorted(list({e for e in expiries if e}))
    state.expiries[ticker] = CacheItem(expiries, now + timedelta(hours=6))
    return expiries

async def fetch_option_contracts(
    client: httpx.AsyncClient,
    ticker: str,
    expiry: Optional[str],
    option_type: str,
    page: int = 0,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "page": page,
        "limit": min(500, max(1, limit)),
        "option_type": option_type,
    }
    if expiry:
        params["expiry"] = expiry
    if REQUIRE_VOL_GREATER_OI:
        params["vol_greater_oi"] = True
    if EXCLUDE_ZERO_VOL:
        params["exclude_zero_vol_chains"] = True
    if EXCLUDE_ZERO_OI:
        params["exclude_zero_oi_chains"] = True
    if EXCLUDE_ZERO_DTE:
        params["exclude_zero_dte"] = True
    if MAYBE_OTM_ONLY:
        params["maybe_otm_only"] = True

    data = await uw_get(client, OPTION_CONTRACTS_PATH.format(ticker=ticker), params=params)
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return [x for x in data["data"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []

# ----------------------------
# Normalizers
# ----------------------------
def normalize_flow_row(a: Dict[str, Any]) -> Dict[str, Any]:
    ticker = str(a.get("ticker") or a.get("ticker_symbol") or a.get("symbol") or "UNK").upper()
    opt_type = normalize_type(a.get("type"))
    expiry = a.get("expiry") or a.get("expiration") or a.get("exp_date")
    strike = safe_float(a.get("strike"))
    premium = safe_float(a.get("total_premium") or a.get("premium") or a.get("notional") or a.get("total_notional"))
    vol = safe_int(a.get("volume"))
    oi = safe_int(a.get("open_interest"))
    voloi = safe_float(a.get("volume_oi_ratio") or a.get("vol_oi_ratio"))
    side = str(a.get("side") or a.get("trade_side") or "").lower()
    created_at = a.get("created_at")

    if voloi is None and vol is not None and oi and oi > 0:
        voloi = vol / oi

    return {
        "src": "FLOW",
        "ticker": ticker,
        "type": opt_type,
        "expiry": expiry,
        "strike": strike,
        "premium": premium,
        "volume": vol,
        "open_interest": oi,
        "voloi": voloi,
        "side": side,
        "created_at": created_at,
        "raw": a,
    }

def normalize_contract_row(a: Dict[str, Any], ticker_override: str) -> Dict[str, Any]:
    ticker = ticker_override.upper()
    opt_type = normalize_type(a.get("option_type") or a.get("type") or a.get("right"))
    expiry = a.get("expiry") or a.get("expiration")
    strike = safe_float(a.get("strike") or a.get("strike_price"))
    vol = safe_int(a.get("volume") or a.get("total_volume"))
    oi = safe_int(a.get("open_interest") or a.get("oi"))
    voloi = safe_float(a.get("volume_oi_ratio") or a.get("vol_oi_ratio"))
    if voloi is None and vol is not None and oi and oi > 0:
        voloi = vol / oi
    contract = a.get("option_symbol") or a.get("symbol") or a.get("contract")

    return {
        "src": "CHAIN",
        "ticker": ticker,
        "type": opt_type,
        "expiry": expiry,
        "strike": strike,
        "premium": safe_float(a.get("premium") or a.get("notional")),
        "volume": vol,
        "open_interest": oi,
        "voloi": voloi,
        "contract": contract,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "raw": a,
    }

# ----------------------------
# Triggers
# ----------------------------
def is_unusual(x: Dict[str, Any]) -> Tuple[bool, List[str]]:
    tags: List[str] = []

    if x.get("type") not in ("CALL", "PUT"):
        return False, ["not_call_put"]
    if x.get("type") not in OPTION_TYPES:
        return False, ["type_filtered"]

    vol = x.get("volume")
    oi = x.get("open_interest")
    voloi = x.get("voloi")

    hit = False
    if vol is not None and vol >= MIN_UNUSUAL_VOLUME:
        hit = True; tags.append(f"vol>={MIN_UNUSUAL_VOLUME}")
    if oi is not None and oi >= MIN_UNUSUAL_OI:
        hit = True; tags.append(f"oi>={MIN_UNUSUAL_OI}")
    if MIN_VOLUME_OI_RATIO and MIN_VOLUME_OI_RATIO > 0 and voloi is not None and voloi >= MIN_VOLUME_OI_RATIO:
        hit = True; tags.append(f"voloi>={MIN_VOLUME_OI_RATIO:g}")
    if REQUIRE_VOL_GREATER_OI and vol is not None and oi is not None and vol > oi:
        hit = True; tags.append("vol_gt_oi")

    return hit, (tags or ["no_trigger"])

def fmt_msg(x: Dict[str, Any], tags: List[str]) -> str:
    t = x["ticker"]
    typ = x["type"]
    src = x.get("src", "SRC")
    expiry = x.get("expiry")
    strike = x.get("strike")
    vol = x.get("volume")
    oi = x.get("open_interest")
    voloi = x.get("voloi")
    prem = x.get("premium")
    side = x.get("side", "n/a")
    contract = x.get("contract")
    created = x.get("created_at", "n/a")

    line_contract = f"`{contract}`" if contract else f"`{typ} {strike} @ {expiry}`"
    return "\n".join([
        f"🐳 *UW Unusual Options* — *{t}*  |  *{typ}*  |  *{src}*",
        f"• Contract: {line_contract}",
        f"• Vol: `{vol}` | OI: `{oi}` | Vol/OI: `{voloi}`",
        f"• Premium: *{money(prem)}* | Side: `{side}`",
        f"• Trigger: `{', '.join(tags)}`",
        f"• Time: `{created}`",
    ])

# ----------------------------
# Loops
# ----------------------------
async def flow_loop():
    require_env()
    async with httpx.AsyncClient() as client:
        while state.running:
            state.last_flow_poll_at = datetime.now(timezone.utc).isoformat()
            sent = 0
            try:
                if not TICKERS:
                    await asyncio.sleep(POLL_FLOW_SECONDS)
                    continue

                newer_than = state.last_seen_created_at
                sem = asyncio.Semaphore(MAX_INFLIGHT)

                async def fetch_one(tk: str) -> List[Dict[str, Any]]:
                    async with sem:
                        return await fetch_flow_alerts_for_ticker(client, tk, newer_than)

                results = await asyncio.gather(*[fetch_one(tk) for tk in TICKERS], return_exceptions=True)

                rows: List[Dict[str, Any]] = []
                for r in results:
                    if isinstance(r, Exception):
                        log.warning("flow fetch error: %s", repr(r))
                        continue
                    rows.extend(r)

                rows_sorted = sorted(rows, key=lambda a: parse_iso(a.get("created_at") or ""))

                if not state.last_seen_created_at and FIRST_RUN_SKIP_HISTORY and rows_sorted:
                    state.last_seen_created_at = rows_sorted[-1].get("created_at")
                    await asyncio.sleep(POLL_FLOW_SECONDS)
                    continue

                last_dt = parse_iso(state.last_seen_created_at) if state.last_seen_created_at else datetime(1970,1,1,tzinfo=timezone.utc)
                fresh = []
                for a in rows_sorted:
                    ca = a.get("created_at")
                    if not ca:
                        continue
                    try:
                        if parse_iso(ca) > last_dt:
                            fresh.append(a)
                    except Exception:
                        continue

                if fresh:
                    state.last_seen_created_at = fresh[-1].get("created_at") or state.last_seen_created_at

                for a in fresh:
                    x = normalize_flow_row(a)
                    ok, tags = is_unusual(x)
                    if not ok:
                        continue

                    key = alert_hash(["flow", x["ticker"], x["type"], x.get("expiry"), x.get("strike"), x.get("premium"), x.get("volume"), x.get("open_interest")])
                    if not cooldown_ok(key):
                        continue

                    await telegram_send(fmt_msg(x, tags), client)
                    sent += 1

                state.last_sent = sent
                state.last_error = None

            except Exception as e:
                state.last_error = repr(e)
                log.exception("flow_loop error: %s", e)

            await asyncio.sleep(POLL_FLOW_SECONDS)

async def chain_scan_loop():
    require_env()
    async with httpx.AsyncClient() as client:
        sem = asyncio.Semaphore(MAX_INFLIGHT)

        while state.running:
            state.last_scan_at = datetime.now(timezone.utc).isoformat()
            sent = 0
            try:
                if not TICKERS:
                    await asyncio.sleep(SCAN_SECONDS)
                    continue

                for ticker in TICKERS:
                    expiries = await fetch_expiry_breakdown(client, ticker)
                    if not expiries:
                        continue

                    expiries_to_scan = expiries[:6]

                    async def scan_expiry(exp: str, opt_type: str) -> List[Dict[str, Any]]:
                        async with sem:
                            rows = []
                            rows += await fetch_option_contracts(client, ticker, expiry=exp, option_type=opt_type, page=0, limit=500)
                            return rows

                    tasks = []
                    for exp in expiries_to_scan:
                        for opt_type in OPTION_TYPES:
                            tasks.append(scan_expiry(exp, opt_type))

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for r in results:
                        if isinstance(r, Exception):
                            log.warning("chain scan error: %s", repr(r))
                            continue
                        for row in r:
                            x = normalize_contract_row(row, ticker)
                            ok, tags = is_unusual(x)
                            if not ok:
                                continue

                            key = alert_hash(["chain", x["ticker"], x["type"], x.get("expiry"), x.get("strike"), x.get("volume"), x.get("open_interest")])
                            if not cooldown_ok(key):
                                continue

                            await telegram_send(fmt_msg(x, tags), client)
                            sent += 1

                state.last_sent += sent
                state.last_error = None

            except Exception as e:
                state.last_error = repr(e)
                log.exception("chain_scan_loop error: %s", e)

            await asyncio.sleep(SCAN_SECONDS)

# ----------------------------
# FastAPI
# ----------------------------
app = FastAPI(title="UW Public API → Telegram (Unusual Volume/OI)")

@app.on_event("startup")
async def startup():
    require_env()
    state.running = True
    asyncio.create_task(flow_loop())
    asyncio.create_task(chain_scan_loop())

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
        "last_seen_created_at": state.last_seen_created_at,
        "last_flow_poll_at": state.last_flow_poll_at,
        "last_scan_at": state.last_scan_at,
        "last_sent": state.last_sent,
        "last_error": state.last_error,
        "auth": {"token_present": bool(UW_TOKEN), "token_len": len(UW_TOKEN or "")},
    }

@app.get("/debug/headers")
def debug_headers():
    # Safe preview: don't show token
    require_env()
    hdr = uw_headers()
    return {
        "accept": hdr.get("Accept"),
        "authorization_prefix": hdr.get("Authorization", "").split(" ")[0],
        "token_len": len((hdr.get("Authorization", "").split(" ", 1)[1] if " " in hdr.get("Authorization","") else "")),
    }

@app.get("/debug/market-tide")
async def debug_market_tide():
    require_env()
    async with httpx.AsyncClient() as client:
        data = await fetch_market_tide(client)
    return {"ok": True, "data": data}

@app.get("/debug/uw-auth-test")
async def uw_auth_test():
    require_env()
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{UW_BASE_URL}{MARKET_TIDE_PATH}",
            headers=uw_headers(),
            timeout=20,
        )
    return {"status": r.status_code, "preview": r.text[:300]}

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
    asyncio.create_task(chain_scan_loop())
    return {"running": True}

@app.post("/control/reset_cursor")
def reset_cursor():
    state.last_seen_created_at = None
    return {"last_seen_created_at": None}

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "UW Public API → Telegram (Unusual Volume/OI)",
        "endpoints": [
            "/docs",
            "/health",
            "/debug/headers",
            "/debug/market-tide",
            "/debug/uw-auth-test",
            "/test/telegram",
            "/control/start",
            "/control/stop",
            "/control/reset_cursor",
        ],
    }
