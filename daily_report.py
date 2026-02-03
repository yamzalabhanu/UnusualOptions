# daily_report.py
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List

import httpx

from uw_core import state, now_utc, money
from chatgpt import telegram_send

CT = ZoneInfo("America/Chicago")

def _next_run_ct(hour: int, minute: int) -> datetime:
    now_ct = now_utc().astimezone(CT)
    run = now_ct.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run <= now_ct:
        run = run + timedelta(days=1)
    return run

def build_daily_summary(alerts: List[Dict[str, Any]]) -> str:
    if not alerts:
        return "🧾 *Daily Alert Summary* (2:55pm CT)\n• No alerts sent today."

    total = len(alerts)
    flows = [a for a in alerts if a.get("kind") == "FLOW"]
    chains = [a for a in alerts if a.get("kind") == "CHAIN"]

    # Bucket by ticker
    by_ticker: Dict[str, Dict[str, Any]] = {}
    for a in alerts:
        t = (a.get("ticker") or "UNK").upper()
        by_ticker.setdefault(t, {"count": 0, "prem": 0.0, "items": []})
        by_ticker[t]["count"] += 1
        by_ticker[t]["prem"] += float(a.get("premium") or 0.0)
        by_ticker[t]["items"].append(a)

    # Top tickers by premium
    top = sorted(by_ticker.items(), key=lambda kv: kv[1]["prem"], reverse=True)[:10]

    lines = []
    today_ct = now_utc().astimezone(CT).date().isoformat()
    lines.append(f"🧾 *Daily Alert Summary* — `{today_ct}` (2:55pm CT)")
    lines.append(f"• Total alerts: `{total}` | Flow: `{len(flows)}` | Chains: `{len(chains)}`")

    # Optional: tier breakdown
    tier_counts: Dict[str, int] = {}
    for a in alerts:
        tier = str(a.get("tier") or "n/a")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    tier_line = " | ".join([f"{k}:{v}" for k, v in sorted(tier_counts.items(), key=lambda x: -x[1])])
    lines.append(f"• Tier mix: `{tier_line}`")

    lines.append("")
    lines.append("*Top tickers by total premium*")
    for t, info in top:
        lines.append(f"• {t}: {money(info['prem'])} across `{info['count']}` alerts")

    # Include a compact “notable alerts” list (top 10 by premium)
    notable = sorted(alerts, key=lambda a: float(a.get("premium") or 0.0), reverse=True)[:10]
    lines.append("")
    lines.append("*Notable alerts* (top premium)")
    for a in notable:
        t = (a.get("ticker") or "UNK").upper()
        k = a.get("kind")
        ot = a.get("opt_type") or "UNK"
        prem = money(float(a.get("premium") or 0.0))
        tier = a.get("tier") or "n/a"
        s10 = a.get("strength10")
        score = a.get("score")
        if score is not None:
            lines.append(f"• [{k}] {t} {ot} | {prem} | {tier} | `{s10}/10` score `{score}`")
        else:
            lines.append(f"• [{k}] {t} {ot} | {prem} | {tier} | `{s10}/10`")

    return "\n".join(lines)

async def daily_report_loop() -> None:
    while True:
        run_ct = _next_run_ct(14, 55)  # 2:55pm CT
        sleep_s = (run_ct - now_utc().astimezone(CT)).total_seconds()
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

        # snapshot + reset
        alerts = list(getattr(state, "daily_alerts", []) or [])
        # reset AFTER snapshot
        state.daily_alerts = []
        state.daily_alert_date = now_utc().astimezone(CT).date()

        msg = build_daily_summary(alerts)

        async with httpx.AsyncClient() as client:
            await telegram_send(msg, client)

        # small guard so it won't double-send in same minute
        await asyncio.sleep(2)
