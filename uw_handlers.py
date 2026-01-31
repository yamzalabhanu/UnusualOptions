import asyncio
import json
import logging
import math
import re
from datetime import date
from typing import Any, Dict, List, Tuple, Optional

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
    FLOW_MAX_SEND_PER_CYCLE,
    CHAINS_MAX_SEND_PER_CYCLE,
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
    summarize_darkpool,
    stable_fingerprint,
    market_hours_ok_now,

    # ✅ async/persistent gates
    cooldown_ok_async,
    cooldown_ok_ticker_dir_async,
    dedupe_event_async,
    suppress_contract_async,
)

from chatgpt import send_via_gpt_formatter, telegram_send

log = logging.getLogger("uw_app.handlers")

# ============================================================
# PATCH CONFIG
# ============================================================

MIN_STRENGTH_10 = 8
VERY_LARGE_PREMIUM_USD = 2_000_000
MARKET_TIDE_NOTIONAL_THRESHOLD = 25_000_000
DAY_TRADE_MAX_DTE = 7
TICKER_DIR_COOLDOWN_SECONDS = 900


# ============================================================
# Helpers
# ============================================================

def _as_float(x: Any, default: Any = 0.0) -> Any:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def strength_10_from_score(score_100: int) -> int:
    score_100 = int(score_100 or 0)
    return max(0, min(10, int(math.ceil(score_100 / 10.0))))


def parse_occ_expiry_from_contract(contract: str) -> Optional[date]:
    if not contract:
        return None
    m = re.search(r"(\d{6})([CP])", contract)
    if not m:
        return None
    yymmdd = m.group(1)
    try:
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        yyyy = 2000 + yy
        return date(yyyy, mm, dd)
    except Exception:
        return None


def dte_from_contract(contract: str) -> Optional[int]:
    exp = parse_occ_expiry_from_contract(contract)
    if not exp:
        return None
    today = now_utc().date()
    return (exp - today).days


def trade_horizon_label(contract: str) -> str:
    dte = dte_from_contract(contract)
    if dte is None:
        return "unknown horizon"
    return "day trade" if dte <= DAY_TRADE_MAX_DTE else "swing trade"


def premium_bucket_label(prem: float) -> str:
    if prem >= 10_000_000:
        return "EXTREMELY LARGE"
    if prem >= 5_000_000:
        return "HUGE"
    if prem >= VERY_LARGE_PREMIUM_USD:
        return "very large"
    if prem >= 500_000:
        return "large"
    return "small"


def market_bias_from_tide(tide: Any) -> str:
    if not isinstance(tide, dict):
        return "MIXED"
    rows = tide.get("data")
    if not isinstance(rows, list) or not rows:
        return "MIXED"
    last = rows[-1] if isinstance(rows[-1], dict) else None
    if not last:
        return "MIXED"

    call_net = _as_float(last.get("net_call_premium"), 0.0)
    put_net = _as_float(last.get("net_put_premium"), 0.0)

    net = call_net - put_net
    if net >= MARKET_TIDE_NOTIONAL_THRESHOLD:
        return "UP"
    if net <= -MARKET_TIDE_NOTIONAL_THRESHOLD:
        return "DOWN"
    return "MIXED"


def summarize_market_tide_compact(tide: Any) -> str:
    if not isinstance(tide, dict) or not isinstance(tide.get("data"), list) or not tide["data"]:
        return "bias=MIXED"
    last = tide["data"][-1]
    call_net = _as_float(last.get("net_call_premium"), 0.0)
    put_net = _as_float(last.get("net_put_premium"), 0.0)
    net = call_net - put_net
    ts = last.get("timestamp") or "n/a"
    bias = market_bias_from_tide(tide)
    return f"bias={bias} | net={money(net)} call={money(call_net)} put={money(put_net)} | t={ts}"


def multileg_ratio(multileg_vol: Optional[int], vol: int) -> Optional[float]:
    if multileg_vol is None or not vol or vol <= 0:
        return None
    return float(multileg_vol) / float(vol)


def downgrade_steps_from_complexity(
    vol: int,
    oi: int,
    oi_chg: Optional[int],
    multileg_vol: Optional[int],
    ask_vol: Optional[int] = None,
    bid_vol: Optional[int] = None,
) -> Tuple[int, str]:
    steps = 0
    notes: List[str] = []

    mr = multileg_ratio(multileg_vol, vol)
    if mr is not None:
        if mr >= 0.60:
            steps += 2
            notes.append(f"multileg_ratio={mr:.2f} (very high)")
        elif mr >= 0.35:
            steps += 1
            notes.append(f"multileg_ratio={mr:.2f} (high)")

    if oi_chg is not None:
        if oi_chg <= -5000:
            steps += 2
            notes.append(f"oiΔ={oi_chg} (sharp drop)")
        elif oi_chg <= -1500:
            steps += 1
            notes.append(f"oiΔ={oi_chg} (drop)")

    if ask_vol is not None and bid_vol is not None and (ask_vol + bid_vol) > 0:
        total = ask_vol + bid_vol
        ask_ratio = ask_vol / total
        if 0.42 <= ask_ratio <= 0.58:
            steps += 1
            notes.append(f"prints_mixed ask%={ask_ratio:.2f}")

    steps = int(_clamp(float(steps), 0.0, 3.0))

    if steps == 0:
        flowtype = "directional"
    elif steps == 1:
        flowtype = "mixed"
    else:
        flowtype = "roll/complex"

    expl = ", ".join(notes) if notes else "no complexity flags"
    return steps, f"{flowtype} | {expl}"


def apply_tier_downgrade(tier: str, steps: int) -> str:
    order = ["Strong Buy", "Buy", "Moderate Buy", "Watch"]
    if tier not in order:
        return tier
    idx = order.index(tier)
    idx2 = min(len(order) - 1, idx + max(0, int(steps)))
    return order[idx2]


def recommendation_tier(
    opt_type: str,
    strength10: int,
    premium_usd: float,
    vol: int,
    oi: int,
    voloi: Optional[float],
    oi_chg: Optional[int],
    market_bias: str,
) -> Tuple[str, str]:
    score = 0
    score += max(0, strength10 - 5) * 3

    if premium_usd >= 10_000_000:
        score += 8
    elif premium_usd >= 5_000_000:
        score += 6
    elif premium_usd >= VERY_LARGE_PREMIUM_USD:
        score += 4
    elif premium_usd >= 500_000:
        score += 2

    if voloi is not None:
        if voloi >= 3.0:
            score += 5
        elif voloi >= 2.0:
            score += 3
        elif voloi >= 1.2:
            score += 1

    if oi_chg is not None:
        if oi_chg >= 500:
            score += 3
        elif oi_chg >= 100:
            score += 1
        elif oi_chg <= -500:
            score -= 2

    if market_bias == "UP" and opt_type == "CALL":
        score += 2
    elif market_bias == "DOWN" and opt_type == "PUT":
        score += 2
    elif market_bias == "UP" and opt_type == "PUT":
        score -= 2
    elif market_bias == "DOWN" and opt_type == "CALL":
        score -= 2

    if vol >= 50_000:
        score += 2
    if oi >= 50_000:
        score += 2

    if score >= 20:
        tier = "Strong Buy"
    elif score >= 14:
        tier = "Buy"
    elif score >= 9:
        tier = "Moderate Buy"
    else:
        tier = "Watch"

    reason = f"strength={strength10}/10 prem={premium_bucket_label(premium_usd)} vol={vol} oi={oi}"
    if voloi is not None:
        reason += f" vol/oi={voloi:.3f}"
    if oi_chg is not None:
        reason += f" oiΔ={oi_chg}"
    reason += f" market={market_bias}"
    return tier, reason


# ============================================================
# Flow helpers
# ============================================================

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


async def handle_flow_alert(client: httpx.AsyncClient, a: Dict[str, Any]) -> bool:
    if not market_hours_ok_now():
        return False

    f = get_flow_fields(a)

    vol = f.get("volume") or 0
    oi = f.get("oi") or 0
    if vol < MIN_HARD_VOLUME or oi < MIN_HARD_OI:
        return False

    score = score_flow_alert(f)
    strength10 = strength_10_from_score(score)

    if strength10 < MIN_STRENGTH_10:
        return False

    prem = float(f.get("premium") or 0.0)
    if prem < VERY_LARGE_PREMIUM_USD:
        return False

    if score < MIN_SCORE_TO_ALERT:
        return False

    ticker = f["ticker"]
    opt_type = str(f.get("opt_type") or "").upper()

    # ✅ persistent ticker-direction cooldown
    if not await cooldown_ok_ticker_dir_async(ticker, opt_type, TICKER_DIR_COOLDOWN_SECONDS):
        return False

    contract = f.get("contract") or ""

    # ✅ persistent cross-stream suppression
    if not await suppress_contract_async(contract):
        return False

    fp = stable_fingerprint(
        [
            "FLOW",
            ticker,
            opt_type,
            contract,
            str(int((prem or 0) / 1000)),  # bucket premium $1k
            str(f.get("side") or ""),
            str(f.get("rule") or ""),
            str(f.get("strike") or ""),
            str(f.get("expiry") or ""),
        ]
    )

    # ✅ persistent event dedupe
    if not await dedupe_event_async("evt:" + fp):
        return False

    # ✅ persistent cooldown per unique flow
    key = "flow:" + flow_alert_key(f)
    if not await cooldown_ok_async(key, COOLDOWN_SECONDS):
        return False

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)
    bias = market_bias_from_tide(tide)

    tier, tier_reason = recommendation_tier(
        opt_type=opt_type,
        strength10=strength10,
        premium_usd=prem,
        vol=int(vol),
        oi=int(oi),
        voloi=_as_float(f.get("vol_oi"), None) if f.get("vol_oi") is not None else None,
        oi_chg=None,
        market_bias=bias,
    )

    # complexity downgrade (best effort)
    flow_multileg = safe_int(a.get("multi_leg_volume") or a.get("multi_leg") or a.get("multileg_volume"))
    flow_ask = safe_int(a.get("ask_volume") or a.get("askVol") or a.get("ask_volume_total"))
    flow_bid = safe_int(a.get("bid_volume") or a.get("bidVol") or a.get("bid_volume_total"))
    prev_oi = safe_int(a.get("prev_oi") or a.get("previous_open_interest") or a.get("prev_open_interest"))
    oi_chg = (int(oi) - int(prev_oi)) if (prev_oi is not None and oi is not None) else None

    downgrade_steps, flowtype_reason = downgrade_steps_from_complexity(
        vol=int(vol),
        oi=int(oi),
        oi_chg=oi_chg,
        multileg_vol=flow_multileg,
        ask_vol=flow_ask,
        bid_vol=flow_bid,
    )
    tier2 = apply_tier_downgrade(tier, downgrade_steps)

    horizon = trade_horizon_label(contract)

    lines = [
        f"🚨 *UW Flow Alert* — *{ticker}* | *Strength:* `{strength10}/10` | *Score:* `{score}/100`",
        f"• Recommendation: *{tier2}* (base={tier}, -{downgrade_steps}) | BiasPref: `{('CALL' if bias=='UP' else 'PUT') if bias in ('UP','DOWN') else 'MIXED'}` | Horizon: *{horizon}*",
        f"• FlowType: `{flowtype_reason}`",
        f"• Type/Side: `{opt_type}` / `{str(f.get('side') or 'n/a')}` | Rule: `{f.get('rule')}`",
        f"• Contract: `{contract or 'n/a'}`",
        f"• Premium: *{money(prem)}* ({premium_bucket_label(prem)}) | Vol/OI: `{f.get('vol_oi')}` | Δ: `{f.get('delta')}`",
        f"• Vol: `{vol}` | OI: `{oi}` | Underlying: `{f.get('underlying')}`",
        f"• MarketTide: {summarize_market_tide_compact(tide)}",
        f"• Darkpool: {summarize_darkpool(dp)}",
        f"• Model: `{tier_reason}`",
        f"• Time: `{f.get('created_at')}`",
    ]

    await send_via_gpt_formatter("\n".join(lines), client)

    if f.get("created_at"):
        state.flow_newer_than = f["created_at"]

    return True


# ============================================================
# Chain scanner
# ============================================================

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


def chain_strength_10(prem: float, vol: int, oi: int, voloi: Optional[float], oi_chg: Optional[int]) -> int:
    s = 4
    if prem >= 10_000_000:
        s += 4
    elif prem >= 5_000_000:
        s += 3
    elif prem >= VERY_LARGE_PREMIUM_USD:
        s += 2
    elif prem >= 500_000:
        s += 1

    if voloi is not None:
        if voloi >= 3.0:
            s += 3
        elif voloi >= 2.0:
            s += 2
        elif voloi >= 1.2:
            s += 1

    if oi_chg is not None:
        if oi_chg >= 500:
            s += 2
        elif oi_chg >= 100:
            s += 1
        elif oi_chg <= -500:
            s -= 1

    if vol >= 50_000:
        s += 1
    if oi >= 50_000:
        s += 1
    return max(0, min(10, s))


def opt_type_from_contract(contract: str) -> str:
    if not contract:
        return "UNK"
    m = re.search(r"\d{6}([CP])", contract)
    if not m:
        return "UNK"
    return "CALL" if m.group(1) == "C" else "PUT"


async def fetch_option_contracts(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": 500, "exclude_zero_vol_chains": "true", "exclude_zero_oi_chains": "true"}
    if CHAIN_VOL_GREATER_OI_ONLY:
        params["vol_greater_oi"] = "true"

    data = await uw_get(client, f"/api/stock/{ticker}/option-contracts", params=params)
    rows = data.get("data") if isinstance(data, dict) else data
    return rows if isinstance(rows, list) else []


async def scan_unusual_chains_for_ticker(client: httpx.AsyncClient, ticker: str) -> int:
    if not market_hours_ok_now():
        return 0

    try:
        rows = await fetch_option_contracts(client, ticker)
    except Exception:
        return 0

    scored: List[Tuple[float, Dict[str, Any], List[str]]] = []
    for r in rows:
        f = chain_row_fields(r)
        ok, reasons = chain_is_unusual(f)
        if not ok:
            continue
        prem = float(f.get("total_premium") or 0.0)
        scored.append((prem, f, reasons))

    if not scored:
        return 0

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:3]

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)
    bias = market_bias_from_tide(tide)

    sent = 0

    for prem, f, reasons in top:
        contract = f.get("option_symbol") or ""
        if not contract:
            continue
        if prem < VERY_LARGE_PREMIUM_USD:
            continue

        opt_type = opt_type_from_contract(contract)

        if not await cooldown_ok_ticker_dir_async(ticker, opt_type, TICKER_DIR_COOLDOWN_SECONDS):
            continue

        if not await suppress_contract_async(contract):
            continue

        fp = stable_fingerprint(
            ["CHAIN", ticker, contract, str(int((prem or 0) / 1000)), str(f.get("volume") or 0), str(f.get("open_interest") or 0)]
        )
        if not await dedupe_event_async("evt:" + fp):
            continue

        unique = f"chain:{ticker}:{contract}"
        key = "chain:" + sha16(unique + ":" + "|".join(sorted(reasons)))
        if not await cooldown_ok_async(key, COOLDOWN_SECONDS):
            continue

        vol = int(f.get("volume") or 0)
        oi = int(f.get("open_interest") or 0)
        prev_oi = f.get("prev_oi")
        voloi = (vol / oi) if (oi > 0) else None
        oi_chg = (oi - prev_oi) if (prev_oi is not None) else None

        strength10 = chain_strength_10(prem, vol, oi, voloi, oi_chg)
        if strength10 < MIN_STRENGTH_10:
            continue

        tier, tier_reason = recommendation_tier(opt_type, strength10, prem, vol, oi, voloi, oi_chg, bias)

        downgrade_steps, flowtype_reason = downgrade_steps_from_complexity(
            vol=vol,
            oi=oi,
            oi_chg=oi_chg,
            multileg_vol=safe_int(f.get("multi_leg_volume")),
            ask_vol=safe_int(f.get("ask_volume")),
            bid_vol=safe_int(f.get("bid_volume")),
        )
        tier2 = apply_tier_downgrade(tier, downgrade_steps)

        horizon = trade_horizon_label(contract)

        lines = [
            f"🔥 *UW Unusual Chain* — *{ticker}* | *Strength:* `{strength10}/10`",
            f"• Recommendation: *{tier2}* (base={tier}, -{downgrade_steps}) | BiasPref: `{('CALL' if bias=='UP' else 'PUT') if bias in ('UP','DOWN') else 'MIXED'}` | Horizon: *{horizon}*",
            f"• FlowType: `{flowtype_reason}`",
            f"• Contract: `{contract}` | Type: `{opt_type}`",
            f"• Premium: *{money(prem)}* ({premium_bucket_label(prem)}) | AvgPx: `{f.get('avg_price')}` | IV: `{f.get('iv')}`",
            f"• Vol: `{vol}` | OI: `{oi}` | PrevOI: `{prev_oi}` | OIΔ: `{oi_chg}` | Vol/OI: `{voloi}`",
            f"• AskVol: `{f.get('ask_volume')}` | BidVol: `{f.get('bid_volume')}` | Sweeps: `{f.get('sweep_volume')}` | MultiLeg: `{f.get('multi_leg_volume')}`",
            f"• Triggers: `{', '.join(reasons)}`",
            f"• MarketTide: {summarize_market_tide_compact(tide)}",
            f"• Darkpool: {summarize_darkpool(dp)}",
            f"• Model: `{tier_reason}`",
            f"• Time: `{now_utc().isoformat()}`",
        ]

        await send_via_gpt_formatter("\n".join(lines), client)
        sent += 1

    return sent


# ============================================================
# Optional custom alerts feed
# ============================================================

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

    # (Custom alerts can remain simple; add redis gates later if you want)
    key = "alert:" + alert_key(a)

    # keep old in-memory cooldown if you want (or upgrade later)
    # NOTE: we intentionally didn't force redis here because payloads vary a lot.
    now = now_utc()
    last = state.cooldown.get(key)
    if last and (now - last).total_seconds() < COOLDOWN_SECONDS:
        return
    state.cooldown[key] = now

    fp = stable_fingerprint(["CUSTOM", str(a.get("noti_type") or ""), str(a.get("symbol") or ""), str(a.get("id") or "")])
    if not await dedupe_event_async("evt:" + fp):
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


# ============================================================
# Background loops  (EXPORT THESE)
# ============================================================

async def flow_loop() -> None:
    async with httpx.AsyncClient() as client:
        while state.running:
            sent = 0
            try:
                rows = await fetch_flow_alerts(client)

                def _k(x: Dict[str, Any]):
                    return parse_iso(str(x.get("created_at") or "1970-01-01T00:00:00+00:00"))

                for a in sorted(rows, key=_k):
                    if sent >= FLOW_MAX_SEND_PER_CYCLE:
                        break
                    if await handle_flow_alert(client, a):
                        sent += 1
            except Exception:
                pass

            await asyncio.sleep(FLOW_POLL_SECONDS)


async def chains_loop() -> None:
    async with httpx.AsyncClient() as client:
        while state.running:
            tickers = WATCHLIST or DEFAULT_WATCHLIST
            total_sent = 0
            try:
                for t in tickers:
                    if total_sent >= CHAINS_MAX_SEND_PER_CYCLE:
                        break
                    total_sent += await scan_unusual_chains_for_ticker(client, t)
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
