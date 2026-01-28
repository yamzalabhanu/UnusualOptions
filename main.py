# main.py
import logging
import asyncio
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

from chatgpt import start_gpt_worker, gpt_rewrite_alert, telegram_send, send_via_gpt_formatter
from uw_core import require_env, state, UW_BASE_URL, WATCHLIST, DEFAULT_WATCHLIST, CHAIN_TOP_N
from uw_alerts import flow_loop, chains_loop

log = logging.getLogger("uw_app")

app = FastAPI(title="Unusual Whales Public API → Telegram (Flow + Unusual Chains + Market Tide + Darkpool)")

@app.on_event("startup")
async def startup():
    require_env()
    start_gpt_worker()
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
        "chain_top_n": CHAIN_TOP_N,
    }

@app.head("/health")
def health_head():
    return None

@app.get("/")
def root():
    return {"ok": True, "service": "unusualwhales → telegram"}

@app.head("/")
def root_head():
    return None


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
    if body.send_to_telegram:
        async with httpx.AsyncClient() as client:
            await send_via_gpt_formatter(body.raw_text, client)
    return {"ok": True, "sent": body.send_to_telegram}
