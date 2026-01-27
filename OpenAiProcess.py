"""
gpt_telegram_formatter.py — OpenAI → “human-friendly” Telegram alerts

Purpose
- Takes the raw alert text produced by your UW script (Flow Alert / Unusual Chain).
- Sends it to OpenAI (Responses API) to rewrite into a clean, readable, accurate summary.
- Posts the rewritten message to Telegram.

How to use
1) pip install openai httpx fastapi uvicorn
2) Set env vars (below)
3) Run: uvicorn gpt_telegram_formatter:app --host 0.0.0.0 --port 8000

Env vars
REQUIRED:
- OPENAI_API_KEY=...
- TELEGRAM_BOT_TOKEN=...
- TELEGRAM_CHAT_ID=...

Optional:
- OPENAI_MODEL=gpt-5
- OPENAI_REASONING_EFFORT=low   # low|medium|high
- OPENAI_MAX_OUTPUT_TOKENS=350
- TELEGRAM_PARSE_MODE=Markdown
- TG_PREFIX_HEADER=1            # 1 to include "🧠 GPT Summary" header
"""

import os
import re
import httpx
from typing import Optional, Dict, Any

from fastapi import FastAPI
from pydantic import BaseModel

from openai import OpenAI

# ----------------------------
# ENV
# ----------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5").strip()
OPENAI_REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "low").strip()
OPENAI_MAX_OUTPUT_TOKENS = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "350"))

TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_PARSE_MODE = os.getenv("TELEGRAM_PARSE_MODE", "Markdown").strip()
TG_PREFIX_HEADER = os.getenv("TG_PREFIX_HEADER", "1").strip() == "1"

# ----------------------------
# Clients
# ----------------------------
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ----------------------------
# Helpers
# ----------------------------
def require_env():
    missing = []
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if not TG_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TG_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

def sanitize_for_prompt(text: str) -> str:
    """
    Keep prompt safe + compact. Remove excessive whitespace, keep the payload intact.
    """
    text = (text or "").strip()
    text = re.sub(r"\n{4,}", "\n\n", text)
    return text[:12000]  # prevent massive prompts

def strip_markdown_risky(text: str) -> str:
    """
    Telegram Markdown can break if model outputs unmatched backticks etc.
    We keep it minimal and safe. (You can change parse_mode to HTML if you prefer.)
    """
    if not text:
        return text
    # Avoid triple-backtick code blocks (often breaks formatting)
    text = text.replace("```", "`")
    return text

async def telegram_send(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": TG_PARSE_MODE,
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, timeout=20)
        r.raise_for_status()

def build_instructions() -> str:
    return (
        "You rewrite stock/options alerts into a user-readable, accurate, actionable summary.\n"
        "Rules:\n"
        "- ONLY use the information present in the input alert text. Do NOT invent prices, levels, news, catalysts, IV rank, etc.\n"
        "- If a field is missing (e.g., delta), say 'unknown' (do not guess).\n"
        "- Keep it short: 6–12 lines.\n"
        "- Use clear headings and bullets.\n"
        "- Highlight why it matters: premium, contract, type (CALL/PUT), volume, OI, vol/OI, score, side, rule.\n"
        "- If it’s a Flow Alert, mention Score and whether it meets a high-quality threshold (>=80) if score exists.\n"
        "- If it’s an Unusual Chain, emphasize premium + vol/OI + OI change triggers.\n"
        "- Include 'Risk notes' with 2 bullets maximum (e.g., 'Low OI', 'Delta missing', 'Bid-side').\n"
        "- Output must be Telegram-friendly Markdown without code blocks.\n"
    )

def build_user_prompt(raw_alert_text: str) -> str:
    return (
        "Rewrite this alert for a trader:\n\n"
        f"{sanitize_for_prompt(raw_alert_text)}"
    )

def extract_output_text(resp: Any) -> str:
    """
    Extract text from Responses API result.
    The OpenAI Python SDK provides resp.output_text in many examples.
    Fall back to best-effort parsing if needed.
    """
    if hasattr(resp, "output_text") and isinstance(resp.output_text, str):
        return resp.output_text
    # Best-effort fallback:
    try:
        out = []
        for item in (resp.output or []):
            for c in getattr(item, "content", []) or []:
                t = getattr(c, "text", None)
                if t:
                    out.append(t)
        return "\n".join(out).strip()
    except Exception:
        return ""

async def gpt_rewrite_alert(raw_alert_text: str) -> str:
    require_env()
    if openai_client is None:
        raise RuntimeError("OpenAI client not initialized")

    resp = openai_client.responses.create(
        model=OPENAI_MODEL,
        reasoning={"effort": OPENAI_REASONING_EFFORT},
        instructions=build_instructions(),
        input=build_user_prompt(raw_alert_text),
        max_output_tokens=OPENAI_MAX_OUTPUT_TOKENS,
    )
    txt = extract_output_text(resp).strip()
    txt = strip_markdown_risky(txt)

    if TG_PREFIX_HEADER:
        txt = "🧠 *GPT Summary*\n" + txt

    return txt

# ----------------------------
# FastAPI
# ----------------------------
app = FastAPI(title="GPT Telegram Alert Formatter")

class InboundAlert(BaseModel):
    """
    raw_text: the exact message your UW script currently sends (Flow or Chain).
    """
    raw_text: str
    send_to_telegram: bool = True

@app.get("/health")
def health():
    return {
        "ok": True,
        "openai_key_present": bool(OPENAI_API_KEY),
        "openai_model": OPENAI_MODEL,
        "telegram_ready": bool(TG_BOT_TOKEN and TG_CHAT_ID),
        "parse_mode": TG_PARSE_MODE,
    }

@app.post("/format")
async def format_only(body: InboundAlert):
    """
    Returns the formatted message. Optionally sends to Telegram.
    """
    formatted = await gpt_rewrite_alert(body.raw_text)
    if body.send_to_telegram:
        await telegram_send(formatted)
    return {"formatted": formatted, "sent": body.send_to_telegram}

@app.post("/format-and-send")
async def format_and_send(body: InboundAlert):
    """
    Alias endpoint always sending to Telegram (unless you set send_to_telegram=false).
    """
    formatted = await gpt_rewrite_alert(body.raw_text)
    if body.send_to_telegram:
        await telegram_send(formatted)
    return {"ok": True, "sent": body.send_to_telegram}

"""
Integration options with your existing UW app.py:

Option A (simple): In your UW script, instead of telegram_send(msg), do:
  POST http://<this-service>/format-and-send with {"raw_text": msg}

Option B (direct import): import gpt_rewrite_alert and call it before telegram_send.

Recommended: Option A (keeps OpenAI key isolated in this service).
"""
