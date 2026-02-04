# daily_report.py
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

import httpx

from uw_core import state, now_utc, money
from chatgpt import telegram_send

CT = ZoneInfo("America/Chicago")

# Telegram hard limit is ~4096 chars; keep buffer for safety
TELEGRAM_MAX_CHARS = 3800


def _now_ct() -> datetime:
    return now_utc().astimezone(CT)


def _next_run_ct(hour: int, minute: int) -> datetime:
    now_ct = _now_ct()
    run = now_ct.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run <= now_ct:
        run = run + timedelta(days=1)
    return run


def _clip(text: str, limit: int = TELEGRAM_MAX_CHARS) -> str:
    text = (text or "")
    if len(text) <= limit:
        return text
    return text[: limit - 80] + "\n...\n(truncated to fit Telegram)\n"


def build_daily_summary(alerts: List[Dict[str, Any]]) -> str:
    """
    Plain-text daily summary.
    (Your telegram_send currently sends plain text, no Markdown parse_mode.)
    """
    today_ct = _now_ct().date().isoformat()

    if not alerts:
        return f"Daily Alert Summary — {today_ct} (2:55pm CT)\n- No alerts sent today."

    total = len(alerts)
    flows = [a for a in alerts if a.get("kind") == "FLOW"]
    chains = [a for a in alerts if a.get("kind") == "CHAIN"]
    customs = [a for a in alerts if a.get("kind") == "CUSTOM"]

    # Bucket by ticker
    by_ticker: Dict[str, Dict[str, Any]] = {}
    for a in alerts:
        t = (a.get("ticker") or "UNK").upper()
        by_ticker.setdefault(t, {"count": 0, "prem": 0.0})
        by_ticker[t]["count"] += 1
        by_ticker[t]["prem"] += float(a.get("premium") or 0.0)

    # Top tickers by premium
    top = sorted(by_ticker.items(), key=lambda kv: kv[1]["prem"], reverse=True)[:10]

    # Tier breakdown
    tier_counts: Dict[str, int] = {}
    for a in alerts:
        tier = str(a.get("tier") or "n/a")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    tier_line = " | ".join([f"{k}:{v}" for k, v in sorted(tier_counts.items(), key=lambda x: -x[1])])

    # Notable alerts (top by premium)
    notable = sorted(alerts, key=lambda a: float(a.get("premium") or 0.0), reverse=True)[:10]

    lines: List[str] = []
    lines.append(f"Daily Alert Summary — {today_ct} (2:55pm CT)")
    lines.append(f"- Total alerts: {total} | Flow: {len(flows)} | Chains: {len(chains)} | Custom: {len(customs)}")
    if tier_line:
        lines.append(f"- Tier mix: {tier_line}")

    lines.append("")
    lines.append("Top tickers by total premium:")
    for t, info in top:
        lines.append(f"- {t}: {money(info['prem'])} across {info['count']} alerts")

    lines.append("")
    lines.append("Notable alerts (top premium):")
    for a in notable:
        t = (a.get("ticker") or "UNK").upper()
        k = a.get("kind") or "UNK"
        ot = a.get("opt_type") or "UNK"
        prem = money(float(a.get("premium") or 0.0))
        tier = a.get("tier") or "n/a"
        s10 = a.get("strength10")
        score = a.get("score")

        if score is not None:
            lines.append(f"- [{k}] {t} {ot} | {prem} | {tier} | {s10}/10 score {score}")
        else:
            lines.append(f"- [{k}] {t} {ot} | {prem} | {tier} | {s10}/10")

    return _clip("\n".join(lines))


async def daily_report_loop(client: Optional[httpx.AsyncClient] = None) -> None:
    """
    Runs forever; sends summary at 2:55pm CT every day.

    Recommended: pass your shared app http client from main.py:
      asyncio.create_task(daily_report_loop(app.state.http))
    """
    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=20.0)
        own_client = True

    try:
        while True:
            run_ct = _next_run_ct(14, 55)  # 2:55pm CT
            now_ct = _now_ct()
            sleep_s = (run_ct - now_ct).total_seconds()
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

            # Snapshot today's alerts
            alerts = list(getattr(state, "daily_alerts", []) or [])

            # Reset AFTER snapshot
            state.daily_alerts = []
            state.daily_alert_date = _now_ct().date()

            msg = build_daily_summary(alerts)
            await telegram_send(msg, client)

            # Guard so it won't double-send if loop wakes twice quickly
            await asyncio.sleep(2)

    finally:
        if own_client:
            await client.aclose()
