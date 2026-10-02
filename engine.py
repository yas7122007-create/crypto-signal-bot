"""Pure, testable market rules. All timestamps are UTC milliseconds."""
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_CEILING
import hashlib
import json
import math
from statistics import mean

VERSION = "mtf-sweep-breakout-2"
INTERVALS = {"1m": 60_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}


@dataclass(frozen=True)
class Rules:
    relative_volume: float = 1.2
    taker_imbalance: float = 0.05
    max_spread_bps: float = 8
    max_funding_rate: float = 0.001
    reward_risk: float = 2
    fee_bps: float = 5
    slippage_bps: float = 3
    entry_minutes: int = 10
    hold_hours: int = 24
    evaluation_samples: int = 20

    def __post_init__(self):
        for value in asdict(self).values():
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError("Semua batas strategi harus positif dan finite")
        if any(type(value) is not int for value in (self.entry_minutes, self.hold_hours, self.evaluation_samples)):
            raise ValueError("Durasi dan jumlah sampel harus bilangan bulat")
        if self.taker_imbalance >= 1 or self.reward_risk < 1:
            raise ValueError("Imbalance harus < 1 dan reward/risk >= 1")


def number(value):
    if isinstance(value, bool):
        raise ValueError("Boolean bukan angka pasar")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Angka pasar non-finite")
    return result


def timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Timestamp harus bilangan bulat milidetik UTC")
    result = int(value)
    if result < 0:
        raise ValueError("Timestamp tidak boleh negatif")
    return result


def candles(raw, interval, asof, minimum=60, require_latest=True):
    asof = timestamp(asof)
    if not isinstance(raw, list):
        raise ValueError("Candle harus berupa daftar baris kline")
    step = INTERVALS[interval]
    result = []
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 11:
            raise ValueError("Format candle tidak lengkap")
        start, end = timestamp(row[0]), timestamp(row[6])
        if end >= asof:
            continue
        o, h, l, c, v, q, buy = [number(row[i]) for i in (1, 2, 3, 4, 5, 7, 10)]
        if (start % step or end != start + step - 1 or min(o, h, l, c) <= 0
                or l > min(o, c) or h < max(o, c) or min(v, q, buy) < 0
                or buy > q + max(1e-8, q * 1e-8)):
            raise ValueError("Candle tidak valid")
        if result and start != result[-1]["time"] + step:
            raise ValueError("Candle hilang, duplikat, atau tidak berurutan")
        result.append(dict(time=start, end=end, open=o, high=h, low=l, close=c,
                           volume=v, quote_volume=q, taker_buy=buy))
    if len(result) < minimum:
        raise ValueError("Riwayat candle belum cukup")
    if require_latest and (not result or result[-1]["time"] != (asof // step - 1) * step):
        raise ValueError("Candle kedaluwarsa")
    return result


def ema(values, period):
    value = values[0]
    alpha = 2 / (period + 1)
    for item in values[1:]:
        value += alpha * (item - value)
    return value


def features(bars):
    if len(bars) < 60:
        raise ValueError("Indikator memerlukan sedikitnya 60 candle")
    close = [b["close"] for b in bars]
    fast, slow = ema(close, 20), ema(close, 50)
    last, prev = bars[-1], bars[-21:-1]
    atr = mean(max(b["high"] - b["low"], abs(b["high"] - a["close"]),
                   abs(b["low"] - a["close"])) for a, b in zip(bars[-15:-1], bars[-14:]))
    support, resistance = min(b["low"] for b in prev), max(b["high"] for b in prev)
    volume = mean(b["volume"] for b in prev)
    delta = lambda b: 2 * b["taker_buy"] - b["quote_volume"]
    recent, earlier = bars[-10:], bars[-50:-10]
    recent_range = max(b["high"] for b in recent) - min(b["low"] for b in recent)
    earlier_range = max(b["high"] for b in earlier) - min(b["low"] for b in earlier)
    accumulation = (recent_range < earlier_range * 0.7
                    and abs(last["close"] / recent[0]["open"] - 1) < 0.03
                    and sum(delta(b) for b in recent) > 0)
    return dict(close=close[-1], ema20=fast, ema50=slow, atr=atr,
                trend="LONG" if close[-1] > fast > slow else "SHORT" if close[-1] < fast < slow else "FLAT",
                support=support, resistance=resistance,
                relative_volume=last["volume"] / volume if volume else 0,
                taker_imbalance=delta(last) / last["quote_volume"] if last["quote_volume"] else 0,
                delta3=sum(delta(b) for b in bars[-3:]),
                delta20=sum(delta(b) for b in bars[-20:]),
                sweep_long=last["low"] < support < last["close"],
                sweep_short=last["high"] > resistance > last["close"],
                breakout_long=last["close"] > resistance,
                breakout_short=last["close"] < support,
                fvg_long=last["low"] > bars[-3]["high"],
                fvg_short=last["high"] < bars[-3]["low"],
                accumulation_candidate=accumulation)


def round_price(value, tick, up=False):
    try:
        quantum, price = Decimal(str(tick)), Decimal(str(value))
        if not quantum.is_finite() or quantum <= 0 or not price.is_finite():
            raise ValueError("Harga atau tickSize tidak valid")
        return number((price / quantum).to_integral_value(
            rounding=ROUND_CEILING if up else ROUND_FLOOR) * quantum)
    except InvalidOperation:
        raise ValueError("Harga atau tickSize tidak valid") from None


def analyze(symbol, frames, tick, rules):
    if number(tick) <= 0:
        raise ValueError("tickSize tidak valid")
    f = {tf: features(frames[tf]) for tf in ("15m", "1h", "4h")}
    revision = VERSION + ":" + hashlib.sha256(json.dumps(asdict(rules), sort_keys=True).encode()).hexdigest()[:10]
    result = dict(symbol=symbol, version=revision, action="HOLD", reason="", features=f,
                  candle_ms=frames["15m"][-1]["time"], rules=asdict(rules))
    def hold(reason):
        result["reason"] = reason
        return result
    side = f["4h"]["trend"]
    if side == "FLAT" or f["1h"]["trend"] != side:
        return hold("Arah 4h dan 1h belum selaras")
    fast = f["15m"]
    sign = 1 if side == "LONG" else -1
    if sign * (fast["close"] - fast["ema20"]) <= 0:
        return hold("Harga 15m belum mengonfirmasi EMA20")
    key = side.lower()
    if not (fast[f"sweep_{key}"] or fast[f"breakout_{key}"]):
        return hold("Belum ada liquidity sweep/reclaim atau breakout 20 candle")
    if fast["relative_volume"] < rules.relative_volume:
        return hold("Volume pemicu belum cukup")
    if sign * fast["taker_imbalance"] < rules.taker_imbalance or sign * fast["delta3"] <= 0:
        return hold("Tekanan transaksi taker belum mendukung arah entry")
    atr = fast["atr"]
    if atr <= 0:
        return hold("ATR tidak valid")
    entry = round_price(fast["close"], tick, up=side == "SHORT")
    recent = frames["15m"][-5:]
    pivot = min(b["low"] for b in recent) if side == "LONG" else max(b["high"] for b in recent)
    stop = round_price(pivot - sign * atr * 0.15, tick, up=side == "SHORT")
    risk = sign * (entry - stop)
    if not atr * 0.5 <= risk <= atr * 3:
        return hold("Jarak stop di luar 0.5–3 ATR")
    target = round_price(entry + sign * risk * rules.reward_risk, tick, up=side == "LONG")
    if min(entry, stop, target) <= 0:
        return hold("Entry/SL/TP harus positif setelah pembulatan")
    obstacles = [f[tf]["resistance" if side == "LONG" else "support"] for tf in ("1h", "4h")]
    if any(0 < sign * (level - entry) < sign * (target - entry) for level in obstacles):
        return hold("Support/resistance timeframe besar membatasi ruang target")
    setup = "sweep" if fast[f"sweep_{key}"] else "breakout"
    result.update(action=side, entry=entry, stop=stop, target=target, atr=atr,
                  setup=setup, regime=f"{setup}:{side}:trend", tick=str(tick),
                  reason=f"4h/1h selaras; {setup} 15m, volume dan delta terkonfirmasi",
                  rank=fast["relative_volume"] * abs(fast["taker_imbalance"]))
    return result


def validate_market(candidate, book, premium, now, rules):
    now = timestamp(now)
    candle_ms = timestamp(candidate["candle_ms"])
    if candidate["action"] not in ("LONG", "SHORT"):
        raise ValueError("Kandidat harus LONG atau SHORT")
    e, s, t, atr = (number(candidate[k]) for k in ("entry", "stop", "target", "atr"))
    direction = 1 if candidate["action"] == "LONG" else -1
    if min(e, s, t, atr) <= 0 or direction * (e - s) <= 0 or direction * (t - e) <= 0:
        raise ValueError("Urutan entry/SL/TP atau ATR tidak valid")
    bid, ask = number(book["bidPrice"]), number(book["askPrice"])
    funding = number(premium["lastFundingRate"])
    if bid <= 0 or ask < bid:
        raise ValueError("Bid/ask tidak valid")
    if abs(now - timestamp(book["time"])) > 30_000 or abs(now - timestamp(premium["time"])) > 120_000:
        raise ValueError("Data quote/funding kedaluwarsa")
    age = now - (candle_ms + INTERVALS["15m"])
    if candle_ms % INTERVALS["15m"] or age < 0:
        raise ValueError("Candle kandidat belum ditutup atau timestamp tidak selaras")
    if age > 10 * 60_000:
        raise ValueError("Kandidat terlalu lama setelah candle close")
    spread = (ask - bid) / ((ask + bid) / 2) * 10_000
    if spread > rules.max_spread_bps:
        raise ValueError("Spread terlalu lebar")
    if abs(funding) > rules.max_funding_rate:
        raise ValueError("Funding terlalu ekstrem")
    if abs((bid + ask) / 2 - e) > atr * 0.25:
        raise ValueError("Harga telah bergerak lebih dari 0.25 ATR dari entry")
    # Costs are estimates; actual funding is charged during paper evaluation.
    costs = (e + t) * (rules.fee_bps + rules.slippage_bps) / 10_000
    net_rr = (abs(t - e) - costs) / (abs(e - s) + costs)
    if net_rr < 1:
        raise ValueError("Reward/risk setelah estimasi biaya di bawah 1")
    return dict(bid=bid, ask=ask, spread_bps=spread, funding_rate=funding, net_rr_estimate=net_rr)


def signal_id(candidate):
    key = f"{candidate['version']}:{candidate['symbol']}:{candidate['candle_ms']}:{candidate['action']}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def new_signal(candidate, now, rules):
    signal = dict(candidate)
    signal.update(id=signal_id(candidate), status="PENDING", created_ms=now,
                  expires_ms=now + rules.entry_minutes * 60_000,
                  next_bar_ms=(now // 60_000 + 1) * 60_000,
                  delivery="DISABLED", paper=True)
    return signal


def paper_update(signal, bars, now):
    """Conservative 1m simulation, replayable and independent of polling time."""
    s = dict(signal)
    if s["status"] not in ("PENDING", "OPEN"):
        return s
    sign = 1 if s["action"] == "LONG" else -1
    slip = s["rules"]["slippage_bps"] / 10_000
    for b in bars:
        if b["time"] < s["next_bar_ms"] or b["end"] >= now:
            continue
        if b["time"] != s["next_bar_ms"]:
            raise ValueError("Data evaluasi berlubang; hasil belum dapat ditentukan")
        s["next_bar_ms"] = b["time"] + 60_000
        just_filled = False
        if s["status"] == "PENDING":
            if b["end"] >= s["expires_ms"]:
                s.update(status="EXPIRED", exit_ms=s["expires_ms"], outcome="NOT_FILLED")
                break
            touched = b["low"] <= s["entry"] if sign == 1 else b["high"] >= s["entry"]
            if not touched:
                continue
            at_open = sign * (b["open"] - s["entry"]) <= 0
            price = b["open"] if at_open else s["entry"]
            s.update(status="OPEN", fill_price=price,
                     fill_ms=b["time"] if at_open else b["end"],
                     exit_deadline_ms=(b["time"] if at_open else b["end"]) + s["rules"]["hold_hours"] * 3_600_000)
            just_filled = not at_open
        if b["time"] >= s["exit_deadline_ms"]:
            s.update(status="SETTLING", outcome="TIMEOUT", exit_ms=b["time"],
                     exit_price=b["open"] * (1 - sign * slip))
            break
        stop_hit = b["low"] <= s["stop"] if sign == 1 else b["high"] >= s["stop"]
        target_hit = b["high"] >= s["target"] if sign == 1 else b["low"] <= s["target"]
        outcome, exit_price = None, None
        if stop_hit:
            outcome = "SL_AMBIGUOUS" if target_hit else "SL"
            exit_price = min(b["open"], s["stop"]) if sign == 1 else max(b["open"], s["stop"])
        elif target_hit and not just_filled:
            outcome, exit_price = "TP", s["target"]
        if outcome:
            # ponytail: intrabar ordering is unknowable from OHLC; SL wins ties.
            s.update(status="SETTLING", outcome=outcome, exit_ms=b["end"],
                     exit_price=exit_price * (1 - sign * slip),
                     execution_assumption="closed 1m; SL first on ambiguity; no TP on intrabar fill candle")
            break
    return s


def settle(signal, funding):
    s = dict(signal)
    if s["status"] != "SETTLING":
        raise ValueError("Sinyal belum ditutup")
    sign = 1 if s["action"] == "LONG" else -1
    paid = 0.0
    for event in funding:
        if s["fill_ms"] < int(event["fundingTime"]) <= s["exit_ms"]:
            paid += sign * number(event["fundingRate"]) * number(event["markPrice"])
    fees = (s["fill_price"] + s["exit_price"]) * s["rules"]["fee_bps"] / 10_000
    # Reserve entry slippage as an additional conservative cost for limit fills.
    entry_slip = s["fill_price"] * s["rules"]["slippage_bps"] / 10_000
    pnl = sign * (s["exit_price"] - s["fill_price"]) - fees - entry_slip - paid
    risk = abs(s["entry"] - s["stop"])
    if risk <= 0:
        raise ValueError("Risiko awal tidak valid")
    s.update(status="CLOSED", net_r=pnl / risk, net_return=pnl / s["fill_price"],
             funding_per_unit=paid, costs_per_unit=fees + entry_slip)
    return s


def ai_decision(text):
    obj = json.loads(text)
    if not isinstance(obj, dict) or set(obj) != {"decision", "reason"}:
        raise ValueError("Respons AI tidak sesuai schema")
    if obj["decision"] not in ("CONFIRM", "HOLD"):
        raise ValueError("Keputusan AI tidak valid")
    if not isinstance(obj["reason"], str) or not 1 <= len(obj["reason"].strip()) <= 800:
        raise ValueError("Alasan AI tidak valid")
    return obj
