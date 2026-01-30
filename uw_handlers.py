import asyncio
import json
import logging
import os
from datetime import datetime, date, timezone
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
# Alert Quality Gates
# ----------------------------
MIN_SIGNAL_STRENGTH_0_10 = float(os.getenv("MIN_SIGNAL_STRENGTH_0_10", "8.0"))
MIN_SCORE_0_100 = int(round(MIN_SIGNAL_STRENGTH_0_10 * 10))  # 8/10 => 80/100
VERY_LARGE_PREMIUM_USD = float(os.getenv("VERY_LARGE_PREMIUM_USD", "250000"))

# Market bias threshold: if calls notional exceeds puts by this amount => "UP", vice versa => "DOWN"
MARKET_TIDE_NOTIONAL_THRESHOLD = float(os.getenv("MARKET_TIDE_NOTIONAL_THRESHOLD", "10000000"))  # $10M

# Recommendation tuning
STRONG_BUY_MIN_SCORE = int(os.getenv("STRONG_BUY_MIN_SCORE", "90"))
BUY_MIN_SCORE = int(os.getenv("BUY_MIN_SCORE", "85"))
MODERATE_BUY_MIN_SCORE = int(os.getenv("MODERATE_BUY_MIN_SCORE", "80"))

STRONG_BUY_MIN_PREM = float(os.getenv("STRONG_BUY_MIN_PREM", "1000000"))   # $1M
BUY_MIN_PREM = float(os.getenv("BUY_MIN_PREM", "500000"))                  # $500k
MODERATE_BUY_MIN_PREM = float(os.getenv("MODERATE_BUY_MIN_PREM", "250000"))# $250k (matches "very large" gate)


def premium_label(p: float) -> str:
    try:
        v = float(p or 0.0)
    except Exception:
        v = 0.0
    return "very large" if v >= VERY_LARGE_PREMIUM_USD else "large"


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


# ----------------------------
# Market regime helpers
# ----------------------------
def market_bias_from_tide(tide: Any) -> str:
    """
    Uses Unusual Whales /api/market/market-tide response:
      tide["data"] = list of intervals, each has:
        net_call_premium (string number)
        net_put_premium  (string number)
    We take the latest interval and compare net_call_premium vs net_put_premium.
    """
    if not isinstance(tide, dict):
        return "MIXED"

    rows = tide.get("data")
    if not isinstance(rows, list) or not rows:
        return "MIXED"

    last = rows[-1] if isinstance(rows[-1], dict) else None
    if not last:
        return "MIXED"

    call_net = _as_float(last.get("net_call_premium"), 0.0)
    put_net  = _as_float(last.get("net_put_premium"), 0.0)

    # Net difference (calls minus puts)
    net = call_net - put_net

    if net >= MARKET_TIDE_NOTIONAL_THRESHOLD:
        return "UP"
    if net <= -MARKET_TIDE_NOTIONAL_THRESHOLD:
        return "DOWN"
    return "MIXED"



def prefer_side_for_market(mkt_bias: str) -> str:
    if mkt_bias == "UP":
        return "CALL"
    if mkt_bias == "DOWN":
        return "PUT"
    return "EITHER"


# ----------------------------
# Expiry / DTE helpers
# ----------------------------
def _parse_date_any(s: Any) -> Optional[date]:
    if not s:
        return None
    if isinstance(s, date) and not isinstance(s, datetime):
        return s
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, str):
        s = s.strip()
        # Try ISO
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(s[:10], fmt).date()
            except Exception:
                pass
    return None


def parse_occ_expiry(contract: str) -> Optional[date]:
    """
    Parse OCC option symbol: underlying + yymmdd + C/P + strike...
    Example: NVDA260130C00187500 (yymmdd = 260130)
    """
    if not contract or not isinstance(contract, str):
        return None
    c = contract.strip().upper()

    # Find the yymmdd + [CP] pattern.
    # Commonly appears as ...YYMMDDC... or ...YYMMDDP...
    for i in range(0, len(c) - 7):
        chunk = c[i : i + 7]  # yymmdd + letter
        yymmdd = chunk[:6]
        cp = chunk[6:7]
        if cp not in ("C", "P"):
            continue
        if not yymmdd.isdigit():
            continue
        try:
            yy = int(yymmdd[0:2])
            mm = int(yymmdd[2:4])
            dd = int(yymmdd[4:6])
            # 20xx assumption
            yyyy = 2000 + yy
            return date(yyyy, mm, dd)
        except Exception:
            continue
    return None


def dte_and_horizon(expiry: Optional[date]) -> Tuple[Optional[int], str]:
    if not expiry:
        return None, "unknown"
    today = now_utc().date()
    dte = (expiry - today).days
    if dte <= 0:
        return dte, "day trade"
    if dte <= 7:
        return dte, "day trade"
    if dte <= 30:
        return dte, "swing trade"
    return dte, "swing/position"


# ----------------------------
# Recommendation logic
# ----------------------------
def recommendation_from_metrics(
    *,
    score_0_100: Optional[int],
    premium: float,
    volume: int,
    oi: int,
    vol_oi: Optional[float],
    opt_type: str,
    market_bias: str,
    ask_vol: Optional[int] = None,
    bid_vol: Optional[int] = None,
    sweeps: Optional[int] = None,
    multileg: Optional[int] = None,
) -> Tuple[str, str]:
    """
    Returns (recommendation, notes)
    recommendation: Strong Buy / Buy / Moderate Buy / Watch
    notes: short explanation + market alignment note
    """
    s = score_0_100 if isinstance(score_0_100, int) else None
    prem = float(premium or 0.0)
    vol = int(volume or 0)
    oii = int(oi or 0)
    vratio = vol_oi if (vol_oi is not None) else (vol / oii if oii > 0 else None)

    # Tape aggression (if available)
    aggression = None
    if ask_vol is not None and bid_vol is not None and (ask_vol + bid_vol) > 0:
        aggression = (ask_vol - bid_vol) / float(ask_vol + bid_vol)

    # Baseline tier by score+premium first
    rec = "Watch"
    if s is not None:
        if s >= STRONG_BUY_MIN_SCORE and prem >= STRONG_BUY_MIN_PREM:
            rec = "Strong Buy"
        elif s >= BUY_MIN_SCORE and prem >= BUY_MIN_PREM:
            rec = "Buy"
        elif s >= MODERATE_BUY_MIN_SCORE and prem >= MODERATE_BUY_MIN_PREM:
            rec = "Moderate Buy"
        else:
            rec = "Watch"
    else:
        # Chain path (no score): use premium + vol/oi + volume as proxy
        if prem >= STRONG_BUY_MIN_PREM and (vratio or 0) >= 1.5 and vol >= max(MIN_HARD_VOLUME, 1000):
            rec = "Buy"
        elif prem >= BUY_MIN_PREM and (vratio or 0) >= 1.2:
            rec = "Moderate Buy"
        elif prem >= MODERATE_BUY_MIN_PREM and (vratio or 0) >= 1.0:
            rec = "Moderate Buy"
        else:
            rec = "Watch"

    notes_bits: List[str] = []

    # Explain drivers
    if s is not None:
        notes_bits.append(f"strength={round(s/10,1)}/10")
    notes_bits.append(f"prem={money(prem)}")
    notes_bits.append(f"vol={vol}, oi={oii}")
    if vratio is not None:
        notes_bits.append(f"vol/oi={vratio:.2f}")
    if aggression is not None:
        notes_bits.append(f"aggr={aggression:+.2f}")
    if sweeps is not None:
        notes_bits.append(f"sweeps={sweeps}")
    if multileg is not None:
        notes_bits.append(f"multileg={multileg}")

    # Market alignment: prefer CALL when UP, PUT when DOWN
    preferred = prefer_side_for_market(market_bias)
    aligns = (preferred == "EITHER") or (preferred == opt_type)
    if not aligns and preferred != "EITHER":
        # Downgrade one notch if fighting tape
        if rec == "Strong Buy":
            rec = "Buy"
        elif rec == "Buy":
            rec = "Moderate Buy"
        elif rec == "Moderate Buy":
            rec = "Watch"
        notes_bits.append(f"⚠️ market_bias={market_bias} prefers {preferred}")
    else:
        notes_bits.append(f"market_bias={market_bias}")

    return rec, "; ".join(notes_bits)


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

    prem = f.get("premium") or 0.0
    # Premium must be "very large"
    if prem < VERY_LARGE_PREMIUM_USD:
        return

    score = score_flow_alert(f)
    if score < MIN_SCORE_TO_ALERT:
        return

    # Signal strength >= 8/10 (score >= 80/100)
    if score < MIN_SCORE_0_100:
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
            str(int((prem or 0) / 1000)),  # bucket premium $1k
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

    mkt_bias = market_bias_from_tide(tide)
    expiry_dt = _parse_date_any(f.get("expiry")) or parse_occ_expiry(contract)
    dte, horizon = dte_and_horizon(expiry_dt)

    rec, rec_notes = recommendation_from_metrics(
        score_0_100=score,
        premium=prem,
        volume=vol,
        oi=oi,
        vol_oi=f.get("vol_oi"),
        opt_type=str(f.get("opt_type") or "").upper(),
        market_bias=mkt_bias,
        # flow alerts generally don't have ask/bid split; keep None
    )

    header = f"🚨 *UW Flow Alert* — *{ticker}* | *Score:* `{score}/100` | *Rec:* *{rec}*"
    trade_line = f"• Type/Side: `{f.get('opt_type')}` / `{str(f.get('side') or 'n/a')}` | Rule: `{f.get('rule')}`"
    horizon_line = f"• Horizon: *{horizon}*" + (f" | DTE: `{dte}`" if dte is not None else "")
    market_line = f"• Market bias: *{mkt_bias}* (prefers {prefer_side_for_market(mkt_bias)})"
    premium_line = f"• Premium: *{money(prem)}* ({premium_label(prem)}) | Vol/OI: `{f.get('vol_oi')}` | Δ: `{f.get('delta')}`"
    stats_line = f"• Vol: `{vol}` | OI: `{oi}` | Underlying: `{f.get('underlying')}`"
    notes_line = f"• Notes: `{rec_notes}`"

    lines = [
        header,
        trade_line,
        f"• Contract: `{contract or 'n/a'}`",
        premium_line,
        stats_line,
        horizon_line,
        market_line,
        notes_line,
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

        # Premium must be "very large"
        if prem < VERY_LARGE_PREMIUM_USD:
            continue

        scored.append((prem, f, reasons))

    if not scored:
        return

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:3]

    tide = await get_market_tide(client)
    dp = await get_darkpool_for_ticker(client, ticker)
    mkt_bias = market_bias_from_tide(tide)

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

        # Infer type from OCC symbol if possible
        opt_type = "CALL" if "C" in contract[-9:-8] or "C" in contract else "PUT" if "P" in contract else "UNK"

        expiry_dt = parse_occ_expiry(contract)
        dte, horizon = dte_and_horizon(expiry_dt)

        rec, rec_notes = recommendation_from_metrics(
            score_0_100=None,  # chain has no score
            premium=prem,
            volume=vol,
            oi=oi,
            vol_oi=voloi,
            opt_type=opt_type,
            market_bias=mkt_bias,
            ask_vol=f.get("ask_volume"),
            bid_vol=f.get("bid_volume"),
            sweeps=f.get("sweep_volume"),
            multileg=f.get("multi_leg_volume"),
        )

        header = f"🔥 *UW Unusual Chain* — *{ticker}* | *Rec:* *{rec}*"
        horizon_line = f"• Horizon: *{horizon}*" + (f" | DTE: `{dte}`" if dte is not None else "")
        market_line = f"• Market bias: *{mkt_bias}* (prefers {prefer_side_for_market(mkt_bias)})"
        notes_line = f"• Notes: `{rec_notes}`"

        lines = [
            header,
            f"• Contract: `{contract}`",
            f"• Premium: *{money(prem)}* ({premium_label(prem)}) | AvgPx: `{f.get('avg_price')}` | IV: `{f.get('iv')}`",
            f"• Vol: `{vol}` | OI: `{oi}` | PrevOI: `{prev_oi}` | OIΔ: `{oi_chg}` | Vol/OI: `{voloi}`",
            f"• AskVol: `{f.get('ask_volume')}` | BidVol: `{f.get('bid_volume')}` | Sweeps: `{f.get('sweep_volume')}` | MultiLeg: `{f.get('multi_leg_volume')}`",
            horizon_line,
            market_line,
            notes_line,
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

    fp = stable_fingerprint(["CUSTOM", str(a.get("noti_type") or ""), str(a.get("symbol") or ""), str(a.get("id") or "")])
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
