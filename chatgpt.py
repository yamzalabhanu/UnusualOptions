# chatgpt.py
import os
import re
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from openai import OpenAI

log = logging.getLogger("uw_app.chatgpt")

# ----------------------------
# ENV / CONFIG
# ----------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5").strip()
OPENAI_REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "low").strip()
OPENAI_MAX_OUTPUT_TOKENS = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "350"))

# Rate limits (applies in worker)
GPT_MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("GPT_MIN_SECONDS_BETWEEN_CALLS", "2.0"))
GPT_MAX_CALLS_PER_MINUTE = int(os.getenv("GPT_MAX_CALLS_PER_MINUTE", "20"))

# Queue controls (NEW)
GPT_QUEUE_MAX = int(os.getenv("GPT_QUEUE_MAX", "200"))     # buffer alerts instead of dropping
GPT_QUEUE_DROP_OLDEST = os.getenv("GPT_QUEUE_DROP_OLDEST", "1") == "1"  # if full, drop oldest

# If OpenAI fails, do we send raw to Telegram?
GPT_FALLBACK_DIRECT_ON_ERROR = os.getenv("GPT_FALLBACK_DIRECT_ON_ERROR", "1") == "1"

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

_openai_client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Worker + queue state
_gpt_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=GPT_QUEUE_MAX)
_gpt_worker_task: Optional[asyncio.Task] = None

# Rate-limit state (kept in this module)
_gpt_last_call_at: Optional[datetime] = None
_gpt_window_start: Optional[datetime] = None
_gpt_calls_in_window: int = 0


# ----------------------------
# Helpers
# ----------------------------
def require_openai_env() -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing env var: OPENAI_API_KEY")


def require_telegram_env() -> None:
    missing = []
    if not TG_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TG_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def sanitize_for_prompt(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\n{4,}", "\n\n", text)
    return text[:12000]


def strip_markdown_risky(text: str) -> str:
    return (text or "").replace("```", "`")


def formatter_instructions() -> str:
    return (
        "Rewrite stock/options alerts into a user-readable, accurate, actionable summary.\n"
        "Rules:\n"
        "- ONLY use facts present in the input alert text. Do NOT invent numbers, news, levels, targets, or catalysts.\n"
        "- If a field is missing (e.g., delta), say 'unknown' (do not guess).\n"
        "- Keep it short: 6–12 lines.\n"
        "- Use Telegram-friendly Markdown. No code blocks.\n"
        "- Highlight: ticker, alert type (Flow/Chain), direction (CALL/PUT), contract, premium, vol, OI, vol/OI, score, side, rule.\n"
        "- Include a 'Risk notes' section with max 2 bullets.\n"
    )


def _rate_allow_now() -> bool:
    """
    Returns True if we can call OpenAI *right now*.
    (Worker uses this + sleeps until allowed.)
    """
    global _gpt_last_call_at, _gpt_window_start, _gpt_calls_in_window

    now = now_utc()

    # min spacing
    if _gpt_last_call_at is not None:
        if (now - _gpt_last_call_at).total_seconds() < GPT_MIN_SECONDS_BETWEEN_CALLS:
            return False

    # rolling 60s window
    if _gpt_window_start is None or (now - _gpt_window_start).total_seconds() >= 60:
        _gpt_window_start = now
        _gpt_calls_in_window = 0

    if _gpt_calls_in_window >= GPT_MAX_CALLS_PER_MINUTE:
        return False

    _gpt_last_call_at = now
    _gpt_calls_in_window += 1
    return True


async def _wait_for_rate_slot() -> None:
    """
    Sleeps until a GPT call is allowed by both spacing and per-minute window.
    """
    while True:
        if _rate_allow_now():
            return
        await asyncio.sleep(0.25)


async def telegram_send(text: str, client: httpx.AsyncClient) -> None:
    require_telegram_env()
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    r = await client.post(url, json=payload, timeout=20)
    r.raise_for_status()


def _sync_openai_rewrite(raw_text: str) -> str:
    """
    Runs in a thread via asyncio.to_thread to avoid blocking the event loop.
    """
    require_openai_env()
    if _openai_client is None:
        raise RuntimeError("OpenAI client not initialized")

    resp = _openai_client.responses.create(
        model=OPENAI_MODEL,
        reasoning={"effort": OPENAI_REASONING_EFFORT},
        instructions=formatter_instructions(),
        input=f"Rewrite this alert:\n\n{sanitize_for_prompt(raw_text)}",
        max_output_tokens=OPENAI_MAX_OUTPUT_TOKENS,
    )
    out = getattr(resp, "output_text", "") or ""
    out = strip_markdown_risky(out.strip())
    return ("🧠 *GPT Summary*\n" + out) if out else raw_text


async def gpt_rewrite_alert(raw_text: str) -> str:
    # call in thread to avoid blocking
    return await asyncio.to_thread(_sync_openai_rewrite, raw_text)


async def _gpt_worker_loop() -> None:
    """
    Single worker that drains the queue and calls OpenAI at the permitted rate.
    """
    async with httpx.AsyncClient() as client:
        while True:
            raw_text = await _gpt_queue.get()
            try:
                await _wait_for_rate_slot()
                formatted = await gpt_rewrite_alert(raw_text)
                await telegram_send(formatted, client)
            except Exception as e:
                log.warning("GPT worker failed: %s", repr(e))
                if GPT_FALLBACK_DIRECT_ON_ERROR:
                    try:
                        await telegram_send(raw_text, client)
                    except Exception:
                        pass
            finally:
                _gpt_queue.task_done()


def start_gpt_worker() -> None:
    """
    Call once on FastAPI startup to ensure worker is running.
    """
    global _gpt_worker_task
    if _gpt_worker_task and not _gpt_worker_task.done():
        return
    _gpt_worker_task = asyncio.create_task(_gpt_worker_loop())
    log.info("GPT worker started (queue max=%s)", GPT_QUEUE_MAX)


async def send_via_gpt_formatter(raw_text: str, _client_unused: httpx.AsyncClient | None = None) -> None:
    """
    Enqueue message for GPT formatting + Telegram send.
    NOTE: We ignore passed client now; worker owns its own AsyncClient.
    """
    if not raw_text:
        return

    if _gpt_queue.full():
        if GPT_QUEUE_DROP_OLDEST:
            try:
                _ = _gpt_queue.get_nowait()
                _gpt_queue.task_done()
            except Exception:
                pass
        else:
            log.info("GPT queue full: dropping newest")
            return

    await _gpt_queue.put(raw_text)
