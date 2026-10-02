"""Analisis engine: python analyze.py --symbol BTCUSDT | --input snapshot.json."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import re
import sys

from engine import Rules, analyze, candles, timestamp


def analyze_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        raise ValueError("Snapshot harus berupa objek JSON")
    symbol = snapshot["symbol"]
    if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]{1,28}USDT", symbol):
        raise ValueError("Gunakan simbol kontrak USDT seperti BTCUSDT")
    asof = timestamp(snapshot["asof_ms"])
    rules = Rules(**snapshot.get("rules", {}))
    frames = {tf: candles(snapshot["frames"][tf], tf, asof) for tf in ("15m", "1h", "4h")}
    result = analyze(symbol, frames, snapshot["tick"], rules)
    result.update(mode="ANALYSIS_ONLY", asof_ms=asof, market_checks="NOT_RUN")
    return result


def fetch_snapshot(symbol):
    # Offline analysis uses only the standard library; live mode reuses the bot's client.
    from services import Binance, rules_from_env
    api = Binance()
    try:
        info = api.get("/fapi/v1/exchangeInfo")
        contract = next((s for s in info["symbols"] if s["symbol"] == symbol
                         and s["status"] == "TRADING" and s["contractType"] == "PERPETUAL"
                         and s["quoteAsset"] == "USDT" and s.get("underlyingType") == "COIN"), None)
        if contract is None:
            raise ValueError("Kontrak crypto perpetual USDT aktif tidak ditemukan")
        tick = next(f["tickSize"] for f in contract["filters"] if f["filterType"] == "PRICE_FILTER")
        asof = api.now()
        frames = {tf: api.get("/fapi/v1/klines", symbol=symbol, interval=tf, limit=200, endTime=asof)
                  for tf in ("15m", "1h", "4h")}
        return dict(symbol=symbol, tick=tick, asof_ms=asof, frames=frames, rules=asdict(rules_from_env()))
    finally:
        api.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--symbol", type=str.upper, help="Ambil candle Binance Futures, misalnya BTCUSDT")
    source.add_argument("--input", type=Path, help="Baca snapshot JSON lokal")
    parser.add_argument("--save-snapshot", type=Path, help="Simpan input tervalidasi ke file baru untuk replay")
    args = parser.parse_args(argv)
    try:
        if args.symbol and not re.fullmatch(r"[A-Z0-9]{1,28}USDT", args.symbol):
            raise ValueError("Gunakan simbol kontrak USDT seperti BTCUSDT")
        snapshot = (json.loads(args.input.read_text(encoding="utf-8-sig")) if args.input
                    else fetch_snapshot(args.symbol))
        result = analyze_snapshot(snapshot)
        result["source"] = "snapshot" if args.input else "binance"
        output = json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2)
        if args.save_snapshot:
            encoded = json.dumps(snapshot, ensure_ascii=False, allow_nan=False)
            with args.save_snapshot.open("x", encoding="utf-8") as target:
                target.write(encoded + "\n")
        print(output)
        return 0
    except Exception as exc:
        # HTTP exceptions may include URLs; expose only status/type, never credentials.
        if isinstance(exc, ValueError):
            reason = str(exc)[:180]
        elif isinstance(exc, KeyError):
            reason = "Field wajib tidak tersedia"
        else:
            response = getattr(exc, "response", None)
            reason = (f"HTTP {response.status_code} pada {response.request.url.path}"
                      if response is not None else type(exc).__name__)
        print("Analisis gagal: " + reason, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
