
import asyncio
import json
import logging
from typing import Any, Dict, List, Tuple

import httpx

from uw_core import (
    WATCHLIST,
    DEFAULT_WATCHLIST,
    FLOW_POLL_SECONDS,
    CHAIN_POLL_SECONDS,
    ALERTS_POLL_SECONDS,
    MIN_FLOW_PREMIUM,
    MIN_FLOW_VOL_OI_RATIO,
    FLOW_ASK_ONLY,
    FLOW_LIMIT,
    CHAIN_VOL_OI_RATIO,
    CHAIN_MIN_VOLUME,
    CHAIN_MIN_OI,
    CHAIN_MIN_PREMIUM,
    CHAIN_MIN_OI_CHANGE,
    CHAIN_VOL_GREATER_OI_ONLY,
    MIN_HARD_VOLUME,
    MIN_HARD_OI,
    MIN_SCORE_TO_ALERT,
    COOLDOWN_SECONDS,
    state,
    now_utc,
    parse_iso,
    safe_float,
    safe_int,
    money,
    sha16,
    uw_get,
    get_market_tide,
    get_darkpool_for_ticker,
    summarize_market_tide,
    summarize_darkpool,
    cooldown_ok,
    dedupe_event,
    suppress_contract,
    stable_fingerprint,
    market_hours_ok_now,
)

from chatgpt import send_via_gpt_formatter, telegram_send

log = logging.getLogger("uw_app.handlers")


# ----------------------------
# Flow helpers
# ----------------------------
def get_flow_fields(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": str(a.get("ticker_symbol") or a.get("ticker") or a.get("symbol") or "UNK").upper(),
        "opt_type": "CALL" if bool(a.get("is_call")) else ("PUT" if bool(a.get("is_put")) else str(a.get("type") or "").upper()),
        "premium": safe_float(a.get("total_premium") or a.get("premium") or a.get("notional") or a.get("total_notional")),
        "vol_oi": safe_float(a.get("min_volume_oi_ratio") or a.get("volume_oi_ratio") or a.get("vol_oi_ratio") or a.get("volume_oi")),
        "side": "ask" if bool(a.get("is_ask_side")) else ("bid" if bool(a.get("is_bid_side")) else str(a.get("side") or a.get("trade_side") or "")),
        "strike": safe_float(a.get("strike")),
        "expiry": a.get("expiry") or a.get("expiration") or a.get("exp_date"),
        "delta": safe_float(a.get("delta") or a.get("option_delta")),
        "oi": safe_int(a.get("open_interest")),
        "volume": safe_int(a.get("volume")),
        "created_at": a.get("created_at") or a.get("tape_time"),
        "rule": a.get("alert_rule") or a.get("rule_name") or a.get("rule") or "FLOW",
        "contract": a.get("option_symbol") or a.get("contract") or a.get("option_chain") or "",
        "underlying": safe_float(a.get("underlying_price") or a.get("underlying") or a.get("stock_price") or a.get("spot_price")),
    }


def score_flow_alert(f: Dict[str, Any]) -> int:
    score = 50
    prem = f.get("premium") or 0.0
    if prem >= 1_000_000:
        score += 18
    elif prem >= 500_000:
        score += 12
    elif prem >= 200_000:
        score += 6

    voloi = f.get("vol_oi")
    if voloi is not None:
        if voloi >= 3.0:
            score += 14
        elif voloi >= 2.0:
            score += 10
        elif voloi >= 1.2:
            score += 6

    if str(f.get("side") or "").lower() == "ask":
        score += 8

    d = f.get("delta")
    if d is not None:
        ad = abs(float(d))
        if 0.40 <= ad <= 0.55:
            score += 8
        elif 0.30 <= ad <= 0.65:
            score += 3

    return max(0, min(100, score))


async def fetch_flow_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "min_premium": MIN_FLOW_PREMIUM,
        "min_volume_oi_ratio": MIN_FLOW_VOL_OI_RATIO,
        "limit": FLOW_LIMIT,
    }
    if FLOW_ASK_ONLY:
        params["is_ask_side"] = "true"
    if state.flow_newer_than:
        params["newer_than"] = state.flow_newer_than

    tickers = WATCHLIST or DEFAULT_WATCHLIST
    params["ticker_symbol"] = ",".join(tickers)

    data = await uw_get(client, "/api/option-trades/flow-alerts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


def flow_alert_key(f: Dict[str, Any]) -> str:
    raw = "|".join(
        [
            f.get("ticker", ""),
            str(f.get("opt_type", "")),
            str(f.get("strike", "")),
            str(f.get("expiry", "")),
            str(f.get("premium", "")),
            str(f.get("created_at", "")),
            str(f.get("contract", "")),
        ]
    )
    return sha16(raw)


async def handle_flow_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> None:
    if not market_hours_ok_now():
        return

    f = get_flow_fields(a)

    vol = f.get("volume") or 0
    oi = f.get("oi") or 0
    if vol < MIN_HARD_VOLUME or oi < MIN_HARD_OI:
        return

    score = score_flow_alert(f)
    if score < MIN_SCORE_TO_ALERT:
        return

    contract = f.get("contract") or ""
    if not suppress_contract(contract):
        return

    fp = stable_fingerprint(
        [
            "FLOW",
            f.get("ticker", ""),
            f.get("opt_type", ""),
            contract,
            str(int((f.get("premium") or 0) / 1000)),  # bucket premium $1k
            str(f.get("side") or ""),
            str(f.get("rule") or ""),
            str(f.get("strike") or ""),
            str(f.get("expiry") or ""),
        ]
    )
    if not dedupe_event("evt:" + fp):
        return

    key = "flow:" + flow_alert_key(f)
    if not cooldown_ok(key):
        return

    ticker = f["ticker"]
    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}* | *Score:* `{score}/100`",
        f"• Type/Side: `{f.get('opt_type')}` / `{str(f.get('side') or 'n/a')}` | Rule: `{f.get('rule')}`",
        f"• Contract: `{contract or 'n/a'}`",
        f"• Premium: *{money(f.get('premium'))}* | Vol/OI: `{f.get('vol_oi')}` | Δ: `{f.get('delta')}`",
        f"• Vol: `{vol}` | OI: `{oi}` | Underlying: `{f.get('underlying')}`",
        f"• MarketTide: {summarize_market_tide(tide)}",
        f"• Darkpool: {summarize_darkpool(dp)}",
        f"• Time: `{f.get('created_at')}`",
    ]

    await send_via_gpt_formatter("\n".join(lines), client)

    if f.get("created_at"):
        state.flow_newer_than = f["created_at"]


# ----------------------------
# Chain scanner
# ----------------------------
def chain_row_fields(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "option_symbol": r.get("option_symbol") or r.get("option_contract") or r.get("symbol") or "",
        "volume": safe_int(r.get("volume")),
        "open_interest": safe_int(r.get("open_interest")),
        "prev_oi": safe_int(r.get("prev_oi")),
        "total_premium": safe_float(r.get("total_premium") or r.get("premium") or r.get("notional")),
        "avg_price": safe_float(r.get("avg_price")),
        "iv": safe_float(r.get("implied_volatility")),
        "ask_volume": safe_int(r.get("ask_volume")),
        "bid_volume": safe_int(r.get("bid_volume")),
        "sweep_volume": safe_int(r.get("sweep_volume")),
        "multi_leg_volume": safe_int(r.get("multi_leg_volume")),
    }


def chain_is_unusual(f: Dict[str, Any]) -> Tuple[bool, List[str]]:
    vol = f.get("volume") or 0
    oi = f.get("open_interest") or 0
    prev_oi = f.get("prev_oi")
    prem = f.get("total_premium") or 0.0

    if vol < MIN_HARD_VOLUME:
        return False, [f"hard_vol<{MIN_HARD_VOLUME}"]
    if oi < MIN_HARD_OI:
        return False, [f"hard_oi<{MIN_HARD_OI}"]

    voloi = (vol / oi) if oi > 0 else None
    oi_chg = (oi - prev_oi) if (prev_oi is not None) else None

    if CHAIN_VOL_GREATER_OI_ONLY and not (oi > 0 and vol > oi):
        return False, ["need_vol>oi"]

    reasons: List[str] = []
    if prem >= CHAIN_MIN_PREMIUM:
        reasons.append(f"prem>={int(CHAIN_MIN_PREMIUM)}")
    if voloi is not None and voloi >= CHAIN_VOL_OI_RATIO:
        reasons.append(f"vol/oi>={CHAIN_VOL_OI_RATIO:g}")
    if oi_chg is not None and abs(oi_chg) >= CHAIN_MIN_OI_CHANGE:
        reasons.append(f"oi_chg>={CHAIN_MIN_OI_CHANGE}")

    if vol >= CHAIN_MIN_VOLUME:
        reasons.append(f"vol>={CHAIN_MIN_VOLUME}")
    if oi >= CHAIN_MIN_OI:
        reasons.append(f"oi>={CHAIN_MIN_OI}")

    real_triggers = [r for r in reasons if r.startswith(("prem>=", "vol/oi>=", "oi_chg>="))]
    return (len(real_triggers) > 0), reasons


async def fetch_option_contracts(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "limit": 500,
        "exclude_zero_vol_chains": "true",
        "exclude_zero_oi_chains": "true",
    }
    if CHAIN_VOL_GREATER_OI_ONLY:
        params["vol_greater_oi"] = "true"

    data = await uw_get(client, f"/api/stock/{ticker}/option-contracts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


async def scan_unusual_chains_for_ticker(client: httpx.AsyncClient, ticker: str) -> None:
    if not market_hours_ok_now():
        return

    try:
        rows = await fetch_option_contracts(client, ticker)
    except Exception:
        return

    scored: List[Tuple[float, Dict[str, Any], List[str]]] = []
    for r in rows:
        f = chain_row_fields(r)
        ok, reasons = chain_is_unusual(f)
        if not ok:
            continue
        prem = f.get("total_premium") or 0.0
        scored.append((prem, f, reasons))

    if not scored:
        return

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:3]

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)

    for prem, f, reasons in top:
        contract = f.get("option_symbol") or ""
        if not contract:
            continue

        if not suppress_contract(contract):
            continue

        fp = stable_fingerprint(
            [
                "CHAIN",
                ticker,
                contract,
                str(int((prem or 0) / 1000)),
                str(f.get("volume") or 0),
                str(f.get("open_interest") or 0),
            ]
        )
        if not dedupe_event("evt:" + fp):
            continue

        unique = f"chain:{ticker}:{contract}"
        last = state.chain_seen.get(unique)
        if last and (now_utc() - last).total_seconds() < COOLDOWN_SECONDS:
            continue
        state.chain_seen[unique] = now_utc()

        key = "chain:" + sha16(unique + ":" + "|".join(sorted(reasons)))
        if not cooldown_ok(key):
            continue

        vol = f.get("volume") or 0
        oi = f.get("open_interest") or 0
        prev_oi = f.get("prev_oi")
        voloi = (vol / oi) if (oi > 0) else None
        oi_chg = (oi - prev_oi) if (prev_oi is not None) else None

        lines = [
            f"🔥 *UW Unusual Chain* — *{ticker}*",
            f"• Contract: `{contract}`",
            f"• Premium: *{money(prem)}* | AvgPx: `{f.get('avg_price')}` | IV: `{f.get('iv')}`",
            f"• Vol: `{vol}` | OI: `{oi}` | PrevOI: `{prev_oi}` | OIΔ: `{oi_chg}` | Vol/OI: `{voloi}`",
            f"• AskVol: `{f.get('ask_volume')}` | BidVol: `{f.get('bid_volume')}` | Sweeps: `{f.get('sweep_volume')}` | MultiLeg: `{f.get('multi_leg_volume')}`",
            f"• Triggers: `{', '.join(reasons)}`",
            f"• MarketTide: {summarize_market_tide(tide)}",
            f"• Darkpool: {summarize_darkpool(dp)}",
            f"• Time: `{now_utc().isoformat()}`",
        ]

        await send_via_gpt_formatter("\n".join(lines), client)


# ----------------------------
# Optional custom alerts feed
# ----------------------------
async def fetch_custom_alerts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": 100}
    if state.alerts_newer_than:
        params["newer_than"] = state.alerts_newer_than
    data = await uw_get(client, "/api/alerts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


def alert_key(a: Dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(a.get("id") or ""),
            str(a.get("name") or ""),
            str(a.get("noti_type") or ""),
            str(a.get("tape_time") or a.get("created_at") or ""),
            str(a.get("symbol") or a.get("ticker") or ""),
        ]
    )
    return sha16(raw)


async def handle_custom_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> None:
    if not market_hours_ok_now():
        return

    key = "alert:" + alert_key(a)
    if not cooldown_ok(key):
        return

    fp = stable_fingerprint(
        ["CUSTOM", str(a.get("noti_type") or ""), str(a.get("symbol") or ""), str(a.get("id") or "")]
    )
    if not dedupe_event("evt:" + fp):
        return

    name = a.get("name") or "Custom Alert"
    noti_type = a.get("noti_type") or "unknown"
    sym = a.get("symbol") or a.get("ticker") or ""
    ts = a.get("tape_time") or a.get("created_at") or ""

    lines = [
        f"🔔 *UW Custom Alert* — `{noti_type}`",
        f"• Name: `{name}`",
        f"• Symbol: `{sym}`",
        f"• Payload: `{json.dumps(a)[:1500]}`",
        f"• Time: `{ts}`",
    ]
    await telegram_send("\n".join(lines), client)

    if ts:
        state.alerts_newer_than = ts


# ----------------------------
# Background loops  (EXPORT THESE)
# ----------------------------
async def flow_loop() -> None:
    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                rows = await fetch_flow_alerts(client)

                def _k(x: Dict[str, Any]):
                    return parse_iso(str(x.get("created_at") or "1970-01-01T00:00:00+00:00"))

                for a in sorted(rows, key=_k):
                    await handle_flow_alert(client, a)
            except Exception:
                pass

            await asyncio.sleep(FLOW_POLL_SECONDS)


async def chains_loop() -> None:
    async with httpx.AsyncClient() as client:
        while state.running:
            tickers = WATCHLIST or DEFAULT_WATCHLIST
            try:
                for t in tickers:
                    await scan_unusual_chains_for_ticker(client, t)
            except Exception:
                pass

            await asyncio.sleep(CHAIN_POLL_SECONDS)


async def custom_alerts_loop() -> None:
    async with httpx.AsyncClient() as client:
        while state.running:
            try:
                rows = await fetch_custom_alerts(client)

                def _k(x: Dict[str, Any]):
                    return parse_iso(str(x.get("tape_time") or x.get("created_at") or "1970-01-01T00:00:00+00:00"))

                for a in sorted(rows, key=_k):
                    await handle_custom_alert(client, a)
            except Exception:
                pass

            await asyncio.sleep(ALERTS_POLL_SECONDS)


__all__ = ["flow_loop", "chains_loop", "custom_alerts_loop"]
