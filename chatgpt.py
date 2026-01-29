# chatgpt.py (updated)
import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from openai import OpenAI

log = logging.getLogger("uw_app.chatgpt")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY", "") or "").strip()
OPENAI_MODEL = (os.getenv("OPENAI_MODEL", "gpt-5") or "").strip()
OPENAI_REASONING_EFFORT = (os.getenv("OPENAI_REASONING_EFFORT", "low") or "").strip()
OPENAI_MAX_OUTPUT_TOKENS = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "350"))

GPT_FORMATTER_TIMEOUT = int(os.getenv("GPT_FORMATTER_TIMEOUT", "20"))
GPT_FORMATTER_FALLBACK_DIRECT = os.getenv("GPT_FORMATTER_FALLBACK_DIRECT", "0").strip() == "1"

# Throttling
GPT_MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("GPT_MIN_SECONDS_BETWEEN_CALLS", "2.0"))
GPT_MAX_CALLS_PER_MINUTE = int(os.getenv("GPT_MAX_CALLS_PER_MINUTE", "20"))

# IMPORTANT: If throttled, do NOT drop by default. Prefer fallback raw so you see more alerts.
# Set env GPT_DROP_WHEN_THROTTLED=1 if you truly want to drop.
GPT_DROP_WHEN_THROTTLED = os.getenv("GPT_DROP_WHEN_THROTTLED", "0") == "1"

# Telegram
TG_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()

_openai_client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# simple module-local throttler
_gpt_last_call_at: Optional[datetime] = None
_gpt_window_start: Optional[datetime] = None
_gpt_calls_in_window: int = 0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def require_openai_env() -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing env var: OPENAI_API_KEY")


def sanitize_for_prompt(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\n{4,}", "\n\n", text)
    return text[:12000]


def strip_markdown_risky(text: str) -> str:
    """
    You asked for a plain-text style output like:
    'COIN Flow – Quick Summary ...'
    So we strip Telegram-Markdown control chars to avoid formatting surprises.
    """
    if not text:
        return text
    return (
        text.replace("```", "`")
        .replace("*", "")
        .replace("_", "")
    )


def formatter_instructions() -> str:
    # Matches your requested format, while ensuring we don't invent levels/targets/news.
    return (
        "You rewrite options alerts into a concise trader-style summary.\n"
        "Output MUST be Telegram-friendly plain text (no code blocks).\n"
        "\n"
        "CRITICAL RULES:\n"
        "- Use ONLY facts present in the input alert text for numeric values and fields.\n"
        "- You MAY add careful interpretation (e.g., 'likely hedge' vs 'speculative'), "
        "but do NOT invent catalysts, support/resistance levels, targets, or price zones.\n"
        "- If a field is missing, write 'unknown' or omit that line.\n"
        "- Keep it 10–18 lines.\n"
        "\n"
        "FORMAT EXACTLY LIKE THIS (use the ticker and alert type):\n"
        "<TICKER> <Flow|Chain> – Quick Summary\n"
        "\n"
        "Trade: <expiry> <strike> <CALL/PUT>\n"
        "Size: <premium> premium (<brief size label>)\n"
        "Type: <vol/oi + side + rule + any positioning hints using ONLY given data>\n"
        "Meaning: <1 sentence interpretation, conservative>\n"
        "\n"
        "Why it matters\n"
        "- <bullet 1>\n"
        "- <bullet 2>\n"
        "\n"
        "Bias for <TICKER>\n"
        "- Near-term: <bullish/bearish/neutral/unknown>\n"
        "- Longer-term: <bullish/bearish/neutral/unknown>\n"
        "\n"
        "Practical takeaway (general, not financial advice)\n"
        "- <1–3 bullets; strategies are allowed but must be generic>\n"
        "\n"
        "Signal strength: <weak/medium/strong/very strong> (<0–10 estimate>)\n"
    )


def _gpt_allow_call() -> bool:
    global _gpt_last_call_at, _gpt_window_start, _gpt_calls_in_window
    now = _now_utc()

    # min spacing between GPT calls
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


async def telegram_send(text: str, client: httpx.AsyncClient) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        # Plain text output: no Markdown parsing needed.
        # If you still want Markdown, set parse_mode="Markdown" and remove strip_markdown_risky stripping of * and _.
        "disable_web_page_preview": True,
    }
    r = await client.post(url, json=payload, timeout=20)
    r.raise_for_status()


async def gpt_rewrite_alert(raw_text: str) -> str:
    require_openai_env()
    if _openai_client is None:
        raise RuntimeError("OpenAI client not initialized")

    # NOTE: OpenAI SDK call is sync; this is fine in async contexts for small volume,
    # but if you want fully async, we can switch to httpx to call the API directly.
    resp = _openai_client.responses.create(
        model=OPENAI_MODEL,
        reasoning={"effort": OPENAI_REASONING_EFFORT},
        instructions=formatter_instructions(),
        input=f"Rewrite this alert:\n\n{sanitize_for_prompt(raw_text)}",
        max_output_tokens=OPENAI_MAX_OUTPUT_TOKENS,
    )

    out = getattr(resp, "output_text", "") or ""
    out = strip_markdown_risky(out.strip())

    # Your requested output is already titled; we won't add "🧠 GPT Summary" anymore.
    return out if out else raw_text


async def send_via_gpt_formatter(raw_text: str, client: httpx.AsyncClient) -> None:
    """
    Throttle GPT calls to avoid 429 spam.
    If throttled:
      - default behavior (recommended): send raw alert to Telegram (so you see more alerts)
      - optional: set GPT_DROP_WHEN_THROTTLED=1 to drop
    """
    if not _gpt_allow_call():
        if GPT_DROP_WHEN_THROTTLED:
            log.info("GPT throttled: dropping alert")
            return
        await telegram_send(raw_text, client)
        return

    try:
        formatted = await gpt_rewrite_alert(raw_text)
        await telegram_send(formatted, client)
        return
    except Exception as e:
        log.warning("GPT formatter failed: %s", repr(e))
        if GPT_FORMATTER_FALLBACK_DIRECT:
            await telegram_send(raw_text, client)
        return
