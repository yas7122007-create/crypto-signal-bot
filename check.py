"""Runnable checks: python check.py [--db] [--hermes] [--scheduler]. No orders/messages."""
from dataclasses import asdict
from contextlib import redirect_stderr
from io import StringIO
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from unittest.mock import patch
import sys

from engine import (INTERVALS, Rules, ai_decision, analyze, candles, features,
                    new_signal, paper_update, round_price, settle, validate_market)


def expect_error(fn):
    try:
        fn()
    except (ValueError, KeyError, TypeError):
        return
    raise AssertionError("Input tidak valid seharusnya ditolak")


def bar(t, o=100, h=101, l=99, c=100, volume=100, buy_ratio=0.6):
    quote = volume * c
    return dict(time=t, end=t + 59_999, open=o, high=h, low=l, close=c,
                volume=volume, quote_volume=quote, taker_buy=quote * buy_ratio)


def fixture_frames(short=False):
    frames = {}
    asof = 1200 * INTERVALS["4h"]
    for tf in ("15m", "1h", "4h"):
        step = INTERVALS[tf]
        items = []
        for i in range(200):
            c = 100 + i * 0.1
            item = bar(asof - (200 - i) * step, c - 0.1, c + 0.3, c - 0.3, c)
            item["end"] = item["time"] + step - 1
            items.append(item)
        if tf == "15m":
            items[-1].update(open=119.8, high=120.7, low=119.4, close=120.6,
                             volume=200, quote_volume=24120, taker_buy=18090)
        if short:
            for b in items:
                b.update(open=300-b["open"], high=300-b["low"], low=300-b["high"], close=300-b["close"],
                         taker_buy=b["quote_volume"]-b["taker_buy"])
        frames[tf] = items
    return frames


def candidate(short=False):
    return dict(symbol="TESTUSDT", version="__self_check__", candle_ms=0,
                action="SHORT" if short else "LONG", entry=100,
                stop=105 if short else 95, target=90 if short else 110,
                atr=2, setup="breakout", regime="test", reason="synthetic test",
                rules=asdict(Rules()))


def main():
    rules = Rules()
    frames = fixture_frames()
    long = analyze("DEMOUSDT", frames, "0.01", rules)
    short = analyze("DEMOUSDT", fixture_frames(True), "0.01", rules)
    assert long["action"] == "LONG", long["reason"]
    assert short["action"] == "SHORT", short["reason"]
    assert long["stop"] < long["entry"] < long["target"]
    assert short["target"] < short["entry"] < short["stop"]
    frames["1h"] = fixture_frames(True)["1h"]
    assert analyze("DEMOUSDT", frames, "0.01", rules)["action"] == "HOLD"
    frames = fixture_frames()
    frames["15m"][-1]["taker_buy"] = 0
    assert analyze("DEMOUSDT", frames, "0.01", rules)["action"] == "HOLD"
    frames = fixture_frames()
    frames["15m"][-1]["volume"] = 1
    assert analyze("DEMOUSDT", frames, "0.01", rules)["action"] == "HOLD"
    print("PASS: LONG, SHORT, MTF conflict, weak volume, opposing orderflow")

    bars = fixture_frames()["15m"]
    raw = [[b["time"], b["open"], b["high"], b["low"], b["close"], b["volume"],
            b["end"], b["quote_volume"], 1, 1, b["taker_buy"]] for b in bars]
    asof = bars[-1]["end"] + 1
    partial = list(raw[-1])
    partial[0] += 900_000
    partial[6] += 900_000
    assert len(candles(raw + [partial], "15m", asof)) == 200
    expect_error(lambda: candles(raw[:-2] + raw[-1:], "15m", asof))
    expect_error(lambda: candles(raw[:-1], "15m", asof))
    corrupt = [list(r) for r in raw]
    corrupt[-1][4] = float("nan")
    expect_error(lambda: candles(corrupt, "15m", asof))
    assert round_price(1.234, "0.05") == 1.2
    assert round_price(1.234, "0.05", up=True) == 1.25
    for invalid in ("NaN", "Infinity", "0", "-0.01", "broken", True):
        expect_error(lambda tick=invalid: round_price(1.234, tick))
    expect_error(lambda: round_price(float("nan"), "0.01"))
    expect_error(lambda: candles([], "15m", asof, minimum=0))
    assert candles([], "15m", asof, minimum=0, require_latest=False) == []
    corrupt = [list(r) for r in raw]
    corrupt[-1][0] += 0.5
    expect_error(lambda: candles(corrupt, "15m", asof))
    for changes in ({"entry_minutes": 0.5}, {"hold_hours": True}, {"fee_bps": "5"}):
        expect_error(lambda changes=changes: Rules(**changes))
    print("PASS: closed candles, missing/stale/non-finite data, exchange tick rounding")

    quote = dict(bidPrice="99.99", askPrice="100.01", time=901_000)
    premium = dict(lastFundingRate="0.0001", time=901_000)
    assert validate_market(candidate(), quote, premium, 901_000, rules)["net_rr_estimate"] > 1
    expect_error(lambda: validate_market(candidate(), {**quote, "askPrice": "103"}, premium, 901_000, rules))
    expect_error(lambda: validate_market(candidate(), quote, premium, 1_001_000, rules))
    expect_error(lambda: validate_market(candidate(), quote, {**premium, "lastFundingRate": "0.1"}, 901_000, rules))
    for key in ("entry", "stop", "target", "atr"):
        for invalid in (float("nan"), float("inf"), 0, -1, True):
            expect_error(lambda key=key, invalid=invalid: validate_market(
                {**candidate(), key: invalid}, quote, premium, 901_000, rules))
    for changes in ({"candle_ms": 1_800_000}, {"candle_ms": 1}, {"candle_ms": -900_000},
                    {"candle_ms": 0.5}, {"action": "HOLD"}):
        expect_error(lambda changes=changes: validate_market(
            {**candidate(), **changes}, quote, premium, 901_000, rules))
    print("PASS: spread, stale quote, excessive funding gates")
    print("PASS: reject future/misaligned candles, invalid prices/ATR, ticks and rules")

    from analyze import analyze_snapshot, fetch_snapshot, main as analyze_main
    snapshot = dict(symbol="DEMOUSDT", tick="0.01", asof_ms=asof, rules=asdict(rules),
                    frames={tf: [[b["time"], b["open"], b["high"], b["low"], b["close"], b["volume"],
                                  b["end"], b["quote_volume"], 1, 1, b["taker_buy"], "0"] for b in items]
                            for tf, items in fixture_frames().items()})
    result = analyze_snapshot(snapshot)
    assert result["action"] == "LONG" and result["mode"] == "ANALYSIS_ONLY"
    assert result["market_checks"] == "NOT_RUN" and "id" not in result
    expect_error(lambda: analyze_snapshot({**snapshot, "asof_ms": asof + INTERVALS["15m"]}))
    expect_error(lambda: analyze_snapshot({**snapshot, "symbol": "../BTCUSDT"}))
    expect_error(lambda: analyze_snapshot({**snapshot, "frames": {"15m": raw}}))
    contract = dict(symbol="DEMOUSDT", status="TRADING", contractType="PERPETUAL", quoteAsset="USDT",
                    underlyingType="COIN", filters=[dict(filterType="PRICE_FILTER", tickSize="0.01")])
    with patch("services.Binance") as api_class, patch("services.rules_from_env", return_value=rules):
        api = api_class.return_value
        api.now.return_value = asof
        api.get.side_effect = [{"symbols": [contract]}, *snapshot["frames"].values()]
        assert analyze_snapshot(fetch_snapshot("DEMOUSDT"))["action"] == "LONG"
        assert all(call.kwargs["endTime"] == asof for call in api.get.call_args_list[1:])
        api.close.assert_called_once()
        api.get.side_effect = ValueError("Data gagal")
        expect_error(lambda: fetch_snapshot("DEMOUSDT"))
        assert api.close.call_count == 2
    with TemporaryDirectory() as directory:
        source, saved = Path(directory) / "input.json", Path(directory) / "saved.json"
        source.write_text(json.dumps(snapshot), encoding="utf-8-sig")
        command = [sys.executable, "-S", str(Path(__file__).with_name("analyze.py")), "--input", str(source)]
        run = subprocess.run(command + ["--save-snapshot", str(saved)], capture_output=True, text=True)
        assert run.returncode == 0, run.stderr
        assert json.loads(run.stdout)["action"] == "LONG"
        assert json.loads(saved.read_text(encoding="utf-8")) == snapshot
        original = saved.read_bytes()
        run = subprocess.run(command + ["--save-snapshot", str(saved)], capture_output=True, text=True)
        assert run.returncode == 1 and saved.read_bytes() == original
        source.write_text('{"symbol":"DEMOUSDT"}', encoding="utf-8")
        run = subprocess.run(command, capture_output=True, text=True)
        assert run.returncode == 1 and not run.stdout and "Traceback" not in run.stderr
    stderr = StringIO()
    with patch("analyze.fetch_snapshot", side_effect=RuntimeError("SECRET_TOKEN")), redirect_stderr(stderr):
        assert analyze_main(["--symbol", "BTCUSDT"]) == 1
    assert "SECRET_TOKEN" not in stderr.getvalue()
    import httpx
    response = httpx.Response(403, request=httpx.Request("GET", "https://fapi.binance.com/fapi/v1/exchangeInfo"))
    stderr = StringIO()
    with patch("analyze.fetch_snapshot", side_effect=httpx.HTTPStatusError(
            "SECRET_TOKEN", request=response.request, response=response)), redirect_stderr(stderr):
        assert analyze_main(["--symbol", "BTCUSDT"]) == 1
    assert "HTTP 403 pada /fapi/v1/exchangeInfo" in stderr.getvalue()
    assert "SECRET_TOKEN" not in stderr.getvalue()
    print("PASS: standalone JSON analysis without dependencies, snapshot replay, live adapter, safe CLI errors")

    base = new_signal(candidate(), 1000, rules)
    assert base["next_bar_ms"] == 60_000  # Never evaluate a candle opened before publication.
    ambiguous = paper_update(base, [bar(60_000, 100, 111, 94, 101)], 120_000)
    assert ambiguous["outcome"] == "SL_AMBIGUOUS"
    assert settle(ambiguous, [])["net_r"] < -1
    opened = paper_update(base, [bar(60_000, 102, 112, 99, 105)], 120_000)
    assert opened["status"] == "OPEN"  # TP might have happened before entry.
    won = paper_update(opened, [bar(120_000, 105, 111, 104, 110)], 180_000)
    assert won["outcome"] == "TP"
    without_funding = settle(won, [])
    with_funding = settle(won, [{"fundingTime": 130_000, "fundingRate": "0.001", "markPrice": "100"}])
    assert with_funding["net_r"] < without_funding["net_r"] < 2
    assert paper_update(without_funding, [], 999_999) == without_funding
    short_base = new_signal(candidate(True), 1000, rules)
    short_win = paper_update(short_base, [bar(60_000, 100, 101, 89, 90)], 120_000)
    assert settle(short_win, [{"fundingTime": 70_000, "fundingRate": "0.001", "markPrice": "100"}])["net_r"] > settle(short_win, [])["net_r"]
    expired = paper_update(base, [bar(t, 102, 103, 101, 102) for t in range(60_000, 660_000, 60_000)], 700_000)
    assert expired["status"] == "EXPIRED" and "net_r" not in expired
    expect_error(lambda: paper_update(base, [bar(120_000)], 180_000))
    gapped = paper_update(opened, [bar(120_000, 90, 93, 89, 92)], 180_000)
    assert gapped["exit_price"] < 90
    print("PASS: ambiguous SL/TP, entry ordering, long/short costs and funding, gaps, expiry, replay")

    assert ai_decision('{"decision":"CONFIRM","reason":"Bukti cukup"}')["decision"] == "CONFIRM"
    for text in ('{}', '[]', 'not json', '{"decision":"LONG","reason":"ok"}',
                 '{"decision":"CONFIRM","reason":"ok","entry":1}'):
        expect_error(lambda t=text: ai_decision(t))
    from services import confirm, deliver, signal_text
    with patch.dict("os.environ", {"AI_PROVIDER": "ollama"}), patch("services.httpx.post", side_effect=httpx.ReadTimeout("SECRET_TOKEN")):
        result = confirm(candidate(), {})
    assert result["decision"] == "HOLD" and "SECRET_TOKEN" not in result["reason"]
    # Hermes JSONL must contain exactly one successful, schema-valid final result.
    with patch.dict("os.environ", {"AI_PROVIDER": "hermes"}), patch("services.Path.is_file", return_value=True):
        for event, code, expected in (
                ({"type": "result", "exit_code": 0, "text": '{"decision":"CONFIRM","reason":"Bukti cukup"}'}, 0, "CONFIRM"),
                ({"type": "result", "exit_code": 1, "text": '{"decision":"CONFIRM","reason":"partial"}'}, 0, "HOLD"),
                ({"type": "text", "text": '{"decision":"CONFIRM","reason":"partial"}'}, 0, "HOLD"),
                ([], 0, "HOLD"),
                ({"type": "result", "exit_code": 0, "text": 'not json'}, 0, "HOLD"),
                ({"type": "result", "exit_code": 0, "text": '{}'}, 1, "HOLD")):
            with patch("services.subprocess.Popen") as launch:
                def finish(timeout):
                    options = launch.call_args.kwargs
                    assert options.get("shell", False) is False
                    assert options["env"]["HERMES_HOME"].endswith("hermes")
                    assert "--query-file" in launch.call_args.args[0]
                    options["stdout"].write((json.dumps(event) + "\n").encode())
                    return code
                launch.return_value.__enter__.return_value.wait.side_effect = finish
                assert confirm(candidate(), {})["decision"] == expected
        with patch("services.subprocess.Popen", side_effect=FileNotFoundError("SECRET_TOKEN")):
            result = confirm(candidate(), {})
            assert result["decision"] == "HOLD" and "SECRET_TOKEN" not in result["reason"]
        with patch("services.subprocess.Popen") as launch, patch("services.subprocess.run") as terminate:
            process = launch.return_value.__enter__.return_value
            process.pid = 12345
            process.wait.side_effect = subprocess.TimeoutExpired("hermes", 180)
            assert confirm(candidate(), {})["decision"] == "HOLD"
            process.kill.assert_called_once()
            if sys.platform == "win32":
                assert terminate.call_args.args[0] == ["taskkill.exe", "/PID", "12345", "/T", "/F"]
    with patch.dict("os.environ", {"AI_PROVIDER": "invalid"}):
        assert confirm(candidate(), {})["decision"] == "HOLD"
    print("PASS: Hermes final JSON validation, failed/partial responses, missing executable, provider selection")
    with patch.dict("os.environ", {"TELEGRAM_ENABLED": "false"}), patch("services.httpx.post") as send:
        deliver(None, base)
        send.assert_not_called()
    print("PASS: AI schema and timeout fail closed; dry-run never sends Telegram")

    if "--hermes" in sys.argv:
        from services import hermes_confirm
        result = hermes_confirm('Uji integrasi bot. Data pasar belum tersedia. Balas hanya JSON: '
                                '{"decision":"HOLD","reason":"Data pasar belum tersedia"}')
        assert result["decision"] == "HOLD", result
        print("PASS: Hermes Agent dengan model lokal menghasilkan HOLD untuk data yang tidak tersedia")

    if "--scheduler" in sys.argv:
        import os
        url = os.getenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
        response = httpx.get(url + "/deployments/name/crypto-signal-bot/local-crypto-signals", timeout=10)
        response.raise_for_status()
        deployment = response.json()
        assert not deployment["paused"] and any(s["active"] for s in deployment["schedules"])
        response = httpx.post(url + "/flow_runs/filter", timeout=10, json={
            "deployments": {"id": {"any_": [deployment["id"]]}}, "limit": 1})
        response.raise_for_status()
        assert response.json(), "Scheduler belum membuat flow run; periksa runtime/prefect-server.log"
        print("PASS: scheduler aktif dan telah membuat flow run (pemeriksaan read-only)")

    if "--db" in sys.argv:
        from services import database, query, save_signal, active_signals, save_candles, save_analysis
        with database() as db:
            before = query(db, "SELECT COUNT(*) AS n FROM signals")[0]["n"]
            db.begin()
            try:
                assert save_signal(db, base, new=True)
                assert not save_signal(db, base, new=True)
                assert any(s["id"] == base["id"] for s in active_signals(db))
                save_candles(db, "TESTUSDT", "15m", bars[-2:])
                save_analysis(db, long)
                save_analysis(db, long)
            finally:
                db.rollback()
            assert query(db, "SELECT COUNT(*) AS n FROM signals")[0]["n"] == before
        print("PASS: MariaDB JSON/upsert, duplicate prevention, transaction rollback")
    print("ALL CHECKS PASSED. Fixture data is synthetic; this is not a profitability backtest.")


if __name__ == "__main__":
    main()
