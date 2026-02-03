import asyncio
import contextlib
import os
import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from uw_core import (
    require_env, state, UW_BASE_URL, WATCHLIST, DEFAULT_WATCHLIST,
    MIN_HARD_VOLUME, MIN_HARD_OI, MIN_SCORE_TO_ALERT,
    FLOW_POLL_SECONDS, MIN_FLOW_PREMIUM, MIN_FLOW_VOL_OI_RATIO, FLOW_ASK_ONLY, FLOW_LIMIT,
    CHAIN_POLL_SECONDS, CHAIN_MIN_PREMIUM, CHAIN_MIN_OI_CHANGE, CHAIN_VOL_OI_RATIO, CHAIN_VOL_GREATER_OI_ONLY,
    COOLDOWN_SECONDS, ENABLE_CUSTOM_ALERTS_FEED, now_et, market_hours_ok_now,
    get_market_tide, get_darkpool_for_ticker
)
from uw_handlers import flow_loop, chains_loop, custom_alerts_loop
from chatgpt import gpt_rewrite_alert, telegram_send, send_via_gpt_formatter

ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN", "") or "").strip()  # optional


def require_admin(x_admin_token: str | None):
    if not ADMIN_TOKEN:
        return  # if unset, no auth (dev mode)
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def _cancel_task(task: asyncio.Task | None):
    if not task:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def start_loops():
    """Idempotent start: will not start duplicates."""
    if getattr(state, "tasks", None) is None:
        state.tasks = {}

    if state.running:
        # already running; ensure tasks exist (in case of partial state)
        pass
    state.running = True

    # Only create if missing or done
    def ensure_task(name: str, coro):
        t = state.tasks.get(name)
        if t is None or t.done():
            state.tasks[name] = asyncio.create_task(coro, name=name)

    ensure_task("flow_loop", flow_loop())
    ensure_task("chains_loop", chains_loop())
    if ENABLE_CUSTOM_ALERTS_FEED:
        ensure_task("custom_alerts_loop", custom_alerts_loop())


async def stop_loops():
    """Stop flag + cancel tasks."""
    state.running = False
    tasks = getattr(state, "tasks", {}) or {}
    # cancel in parallel
    await asyncio.gather(*[_cancel_task(t) for t in tasks.values()], return_exceptions=True)
    tasks.clear()


async def lifespan(app: FastAPI):
    require_env()
    # shared client (optional)
    app.state.http = httpx.AsyncClient(timeout=20.0)
    await start_loops()
    try:
        yield
    finally:
        await stop_loops()
        await app.state.http.aclose()


app = FastAPI(
    title="Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide + Darkpool)",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    et = now_et()
    return {
        "ok": True,
        "running": state.running,
        "base_url": UW_BASE_URL,
        "watchlist_count": len(WATCHLIST or DEFAULT_WATCHLIST),
        "market_time_et": et.isoformat(),
        "market_hours_ok_now": market_hours_ok_now(),
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
        "cooldown_seconds": COOLDOWN_SECONDS,
        "custom_alerts_feed_enabled": ENABLE_CUSTOM_ALERTS_FEED,
        "tasks": list(getattr(state, "tasks", {}).keys()),
    }


@app.get("/debug/market-tide")
async def debug_market_tide():
    require_env()
    client: httpx.AsyncClient = app.state.http
    tide = await get_market_tide(client)
    return {"tide": tide}


@app.get("/debug/darkpool/{ticker}")
async def debug_darkpool(ticker: str):
    require_env()
    t = ticker.upper()
    client: httpx.AsyncClient = app.state.http
    dp = await get_darkpool_for_ticker(client, t)
    return {"ticker": t, "darkpool": dp}


class TestMessage(BaseModel):
    text: str


@app.post("/test/telegram")
async def test_telegram(body: TestMessage):
    require_env()
    client: httpx.AsyncClient = app.state.http
    await send_via_gpt_formatter(body.text, client)
    return {"sent": True}


@app.post("/control/stop")
async def stop(x_admin_token: str | None = Header(default=None)):
    require_admin(x_admin_token)
    await stop_loops()
    return {"running": state.running}


@app.post("/control/start")
async def start(x_admin_token: str | None = Header(default=None)):
    require_admin(x_admin_token)
    await start_loops()
    return {"running": state.running}


@app.post("/control/reset_cursors")
def reset_cursors(x_admin_token: str | None = Header(default=None)):
    require_admin(x_admin_token)
    state.flow_newer_than = None
    state.alerts_newer_than = None
    return {"flow_newer_than": None, "alerts_newer_than": None}


class InboundAlert(BaseModel):
    raw_text: str
    send_to_telegram: bool = True


@app.post("/format")
async def format_only(body: InboundAlert):
    require_env()
    formatted = await gpt_rewrite_alert(body.raw_text)
    if body.send_to_telegram:
        client: httpx.AsyncClient = app.state.http
        await telegram_send(formatted, client)
    return {"formatted": formatted, "sent": body.send_to_telegram}


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Unusual Whales Public API → Telegram",
        "endpoints": [
            "/docs", "/health",
            "/debug/market-tide", "/debug/darkpool/{ticker}",
            "/test/telegram",
            "/control/start", "/control/stop", "/control/reset_cursors",
        ],
    }
