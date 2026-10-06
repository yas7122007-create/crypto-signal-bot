"""Runnable checks: python check.py [--db] [--hermes] [--scheduler] [--nemotron-live]. No orders/messages."""
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


def nemotron_checks(candidate, rules):
    import logging
    import httpx
    import reasoning
    from engine import signal_id
    from services import signal_text
    key = "nvapi-SECRET_TEST_KEY"
    env = {"AI_PROVIDER": "nemotron", "NVIDIA_API_KEY": key, "NEMOTRON_TIMEOUT_SECONDS": "30",
           "NVIDIA_BASE_URL": reasoning.DEFAULT_URL, "NEMOTRON_MODEL": reasoning.DEFAULT_MODEL,
           "NEMOTRON_MAX_RETRIES": "2", "NEMOTRON_MAX_AGE_SECONDS": "600", "NEMOTRON_THINKING": "false"}
    url = "https://integrate.api.nvidia.com/v1/chat/completions"
    request = httpx.Request("POST", url)
    answer = dict(thesis="Tren 4h/1h dan breakout 15m selaras", bullish_evidence=["Volume relatif tinggi"],
                  bearish_evidence=["Jurnal paper masih sedikit"], contradictions=[],
                  forecast_consistency="NOT_AVAILABLE", uncertainty_summary="Tanpa forecast",
                  risk_summary="SL di bawah pivot", operator_explanation="Setup LONG sesuai aturan engine")

    def reply(content=None, status=200, finish="stop", headers=None, body=None):
        if body is None:
            body = {"model": "nvidia/nemotron-3-super-120b-a12b", "choices": [
                {"finish_reason": finish, "message": {"content": content if content is not None else json.dumps(answer)}}]}
        return httpx.Response(status, json=body, headers=headers, request=request)

    def run(responses, extra=None, polls=(), subject=None):
        subject = subject or candidate
        frozen = json.dumps(subject, sort_keys=True)
        with patch.dict("os.environ", {**env, **(extra or {})}), \
                patch("reasoning.httpx.post", side_effect=responses) as post, \
                patch("reasoning.httpx.get", side_effect=list(polls)) as get, \
                patch("reasoning.time.sleep") as sleep:
            result = reasoning.explain(subject, {"sample_count": 0, "average_net_r": None, "recent_cases": []})
        # The explanation layer must never touch the deterministic decision (action, entry, stop, target).
        assert json.dumps(subject, sort_keys=True) == frozen
        run.get = get
        return result, post, sleep

    # Target provider needs no Ollama, Hermes or Qwen settings.
    clean = {"NVIDIA_API_KEY": key}
    with patch.dict("os.environ", clean, clear=True), patch("reasoning.httpx.post", return_value=reply()) as post:
        assert reasoning.provider_name() == "nemotron"
        result = reasoning.explain(candidate, {})
    assert result["status"] == "OK" and result["source"] == "nemotron"
    assert result["model_name"] == "nvidia/nemotron-3-super-120b-a12b"
    assert result["signal_id"] == signal_id(candidate) and result["decision_authority"] == "quant_engine"
    sent = post.call_args
    assert sent.args[0] == url and sent.kwargs["headers"]["Authorization"] == "Bearer " + key
    body = sent.kwargs["json"]
    assert body["chat_template_kwargs"] == {"enable_thinking": False} and body["stream"] is False
    assert "NOT_AVAILABLE" in body["messages"][1]["content"] and key not in json.dumps(body)
    print("PASS: Nemotron default provider, model id, auth header, structured evidence, no Qwen config")

    result, post, _ = run([reply("```json\n" + json.dumps(answer) + "\n```")])
    assert result["status"] == "OK"
    result, post, _ = run([reply("<think>internal</think>\n" + json.dumps(answer))])
    assert result["status"] == "OK" and result["thesis"] == answer["thesis"]
    for broken in ("not json", "[]", json.dumps({**answer, "decision": "CONFIRM"}),
                   json.dumps({**answer, "forecast_consistency": "BULLISH"}),
                   json.dumps({**answer, "bullish_evidence": ["x"] * 7}),
                   json.dumps({**answer, "thesis": " "}),
                   json.dumps({**answer, "contradictions": [1]})):
        result, post, _ = run([reply(broken)])
        assert result["status"] == "DEGRADED" and result["source"] == "deterministic", broken
    result, _, _ = run([reply(finish="length")])
    assert result["status"] == "DEGRADED"
    result, _, _ = run([reply(body={"choices": []})])
    assert result["status"] == "DEGRADED"
    print("PASS: Nemotron schema validation, fenced/think output, malformed and truncated responses degrade")

    result, post, sleep = run([reply(status=429, headers={"Retry-After": "3"}), reply()])
    assert result["status"] == "OK" and post.call_count == 2 and sleep.call_args.args[0] == 3
    result, post, _ = run([reply(status=429)] * 3)
    assert result["status"] == "DEGRADED" and result["error"] == "HTTP 429" and post.call_count == 3
    result, post, _ = run([reply(status=503), reply()])
    assert result["status"] == "OK" and post.call_count == 2
    result, post, _ = run([reply(status=401)])
    assert result["status"] == "DEGRADED" and post.call_count == 1  # Bad credentials are never retried.
    result, post, _ = run([reply(status=429, headers={"Retry-After": "120"})])
    assert result["status"] == "DEGRADED" and post.call_count == 1  # Wait would exceed the deadline.
    result, _, _ = run(httpx.ReadTimeout("timeout " + key))
    assert result["status"] == "DEGRADED" and result["error"] == "ReadTimeout"
    result, _, _ = run(RuntimeError("boom " + key))
    assert result["status"] == "DEGRADED"
    result, post, _ = run([reply(status=403)])
    assert result["status"] == "DEGRADED" and result["error"] == "HTTP 403" and post.call_count == 1
    result, post, _ = run([reply(status=204, body={})])
    assert result["status"] == "DEGRADED"
    print("PASS: Nemotron rate limit backoff, 5xx retry, invalid credentials, timeout, unexpected errors")

    pending = lambda rid="req-123", wait=None: reply(status=202, body={}, headers={
        **({"NVCF-REQID": rid} if rid is not None else {}), **({"NVCF-POLL-SECONDS": wait} if wait else {})})
    result, post, sleep = run([pending()], polls=[pending(), reply()])
    assert result["status"] == "OK" and post.call_count == 1 and run.get.call_count == 2
    assert run.get.call_args.args[0] == "https://integrate.api.nvidia.com/v1/status/req-123"
    assert run.get.call_args.kwargs["headers"]["Authorization"] == "Bearer " + key
    result, _, _ = run([pending()], polls=[pending()] * 200)
    assert result["status"] == "DEGRADED" and run.get.call_count <= 120  # Bounded polling.
    for rid in (None, "../../v2/x", ""):
        result, _, _ = run([pending(rid)])
        assert result["status"] == "DEGRADED" and not run.get.called
    result, _, _ = run([pending()], polls=[reply(status=422, body={"detail": "bad"})])
    assert result["status"] == "DEGRADED" and result["error"] == "HTTP 422"
    result, _, _ = run([pending(wait="60")], polls=[reply()])
    assert result["status"] == "OK" and run.get.called  # Server poll hint is capped at 5 s.
    print("PASS: Nemotron 202 async polling, bounded wait, request id validation, poll errors")

    with patch.dict("os.environ", {**env, "NVIDIA_API_KEY": ""}), patch("reasoning.httpx.post") as post:
        result = reasoning.explain(candidate, {})
    post.assert_not_called()
    assert result["status"] == "DISABLED" and result["operator_explanation"]
    with patch.dict("os.environ", env), patch("reasoning.httpx.post") as post:
        result = reasoning.explain(candidate, {}, skip="Nemotron dilewati")
    post.assert_not_called()
    assert result["status"] == "DEGRADED" and result["error"] == "Nemotron dilewati"
    with patch.dict("os.environ", {"AI_PROVIDER": "mock"}), patch("reasoning.httpx.post") as post:
        result = reasoning.explain(candidate, {})
    post.assert_not_called()
    assert result["status"] == "OK" and result["model_name"] == "mock"
    assert any("volume" in item.lower() for item in result["bullish_evidence"])
    for invalid in ("http://example.com/v1", "ftp://x", "https://"):
        with patch.dict("os.environ", {**env, "NVIDIA_BASE_URL": invalid}), patch("reasoning.httpx.post") as post:
            assert reasoning.explain(candidate, {})["status"] == "DEGRADED"
        post.assert_not_called()  # Never send the key over plain HTTP to a remote host.
    expect_error(lambda: reasoning.check_provider("qwen"))
    for name in ("nemotron", "mock", "ollama", "hermes"):
        reasoning.check_provider(name)
    print("PASS: Nemotron missing key, skipped, mock provider, unsafe base URL, provider validation")

    signal = new_signal(candidate, candidate["candle_ms"] + 960_000, rules)
    fresh, _, _ = run([reply()])
    signal["reasoning"] = fresh
    now = fresh["generated_at_ms"] + 1000
    assert reasoning.mark_stale(fresh, signal, now)["status"] == "OK"
    assert reasoning.mark_stale(fresh, signal, now + 3_600_000)["status"] == "STALE"
    assert reasoning.mark_stale(fresh, signal, now - 30_000)["status"] == "OK"  # Small clock skew.
    assert reasoning.mark_stale(fresh, signal, now - 120_000)["status"] == "STALE"
    assert reasoning.mark_stale(fresh, {**signal, "candle_ms": 900_000}, now)["status"] == "STALE"
    assert signal["action"] == candidate["action"] and signal["entry"] == candidate["entry"]
    text = signal_text(signal)
    assert "Nemotron (penjelasan, bukan keputusan): Setup LONG" in text
    signal["reasoning"] = reasoning.mark_stale(fresh, signal, now + 3_600_000)
    text = signal_text(signal)
    assert "Nemotron STALE" in text and "Setup LONG sesuai" not in text
    with patch.dict("os.environ", {"NEMOTRON_MAX_AGE_SECONDS": "broken"}):
        assert reasoning.mark_stale(fresh, signal, now)["status"] == "STALE"
    degraded, _, _ = run(httpx.ReadTimeout("x"))
    text = signal_text({**signal, "reasoning": degraded})
    assert "Ringkasan engine (Nemotron DEGRADED)" in text and key not in text
    with patch.dict("os.environ", {**env, "NVIDIA_API_KEY": ""}):
        disabled = reasoning.explain(candidate, {})
    assert "Nemotron DISABLED" in signal_text({**signal, "reasoning": disabled})
    legacy = {k: v for k, v in signal.items() if k != "reasoning"}
    assert "AI: Model lokal" in signal_text({**legacy, "ai": {"decision": "CONFIRM", "reason": "Model lokal setuju"}})
    short = {**candidate, "action": "SHORT", "entry": 100.0, "stop": 105.0, "target": 90.0}
    assert run([reply()], subject=short)[0]["status"] == "OK"
    assert run(RuntimeError("x"), subject={"symbol": "BROKENUSDT"})[0]["status"] == "DEGRADED"
    print("PASS: stale Nemotron result suppressed, deterministic signal unchanged, Telegram text")

    logs = StringIO()
    handler = logging.StreamHandler(logs)
    root = logging.getLogger()
    root.addHandler(handler)
    level = root.level
    root.setLevel(logging.DEBUG)
    try:
        outputs = [run([reply()])[0], run([reply(status=401)])[0],
                   run(httpx.ConnectError("SECRET " + key))[0], run(ValueError("bad " + key))[0]]
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
    assert key not in logs.getvalue() and key not in json.dumps(outputs) and "SECRET" not in json.dumps(outputs)
    print("PASS: NVIDIA API key never appears in logs or stored reasoning results")
    scan_checks(candidate, rules)


def scan_checks(candidate, rules):
    import time
    import httpx
    import bot
    coins = [dict(symbol=s, group="volume", tick="0.01") for s in ("AAAUSDT", "BBBUSDT")]

    def scan(env, gate=None, **post_options):
        with patch.dict("os.environ", env), patch("bot.Binance") as api_class, patch("bot.database"), \
                patch("bot.work_lock") as lock, patch("bot.query", return_value=[]), \
                patch("bot.rules_from_env", return_value=rules), patch("bot.active_signals", return_value=[]), \
                patch("bot.save_candles"), patch("bot.save_analysis"), \
                patch("bot.analyze", side_effect=lambda symbol, *a: {**candidate, "symbol": symbol}), \
                patch("bot.memories", return_value={"sample_count": 0, "average_net_r": None, "recent_cases": []}), \
                patch("bot.validate_market", **(gate or {"return_value": candidate["market"]})), \
                patch("bot.save_signal", return_value=True) as save, patch("bot.deliver") as deliver, \
                patch("reasoning.httpx.post", **post_options) as post, patch("bot.confirm") as legacy:
            lock.return_value.__enter__.return_value = True
            api = api_class.return_value
            api.now.return_value = int(time.time() * 1000)  # Exchange clock, as in production.
            api.universe.return_value = (coins, {})
            legacy.return_value = {"decision": "HOLD", "reason": "Model lokal ragu"}
            summary = bot.scan.fn(force=True)
            issued = [c.args[1] for c in save.call_args_list if c.kwargs.get("new")]
            return summary, issued, post, deliver, legacy

    import reasoning
    env = {"AI_PROVIDER": "nemotron", "NVIDIA_API_KEY": "nvapi-x", "MAX_OPEN_SIGNALS": "3",
           "NVIDIA_BASE_URL": reasoning.DEFAULT_URL, "NEMOTRON_MAX_RETRIES": "2",
           "NEMOTRON_TIMEOUT_SECONDS": "30", "NEMOTRON_MAX_AGE_SECONDS": "600"}
    summary, issued, post, deliver, legacy = scan(env, side_effect=httpx.ReadTimeout("down"))
    assert summary["paper_signals"] == 2 and deliver.call_count == 2 and not legacy.called
    assert post.call_count == 1  # Circuit breaker: the second candidate skips the failed provider.
    assert [s["reasoning"]["status"] for s in issued] == ["DEGRADED", "DEGRADED"]
    assert all(s["action"] == candidate["action"] and s["entry"] == candidate["entry"] for s in issued)
    ok = httpx.Response(200, request=httpx.Request("POST", "https://integrate.api.nvidia.com/v1/chat/completions"),
                        json={"model": "m", "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(dict(
                            thesis="t", bullish_evidence=[], bearish_evidence=["Harga bisa turun"],
                            contradictions=["15m melawan 4h"], forecast_consistency="CONFLICTING",
                            uncertainty_summary="u", risk_summary="r", operator_explanation="e"))}}]})
    summary, issued, post, _, _ = scan(env, return_value=ok)
    assert summary["paper_signals"] == 2 and post.call_count == 2
    assert all(s["reasoning"]["status"] == "OK" and s["action"] == candidate["action"] for s in issued)
    assert all((s["entry"], s["stop"], s["target"]) == (candidate["entry"], candidate["stop"], candidate["target"])
               for s in issued)  # Contradictions are reported, never acted on.
    summary, issued, post, _, _ = scan(env, gate={"side_effect": ValueError("Spread terlalu lebar")})
    assert summary["paper_signals"] == 0 and not post.called  # Risk gate failure: Nemotron is never asked.
    summary, issued, post, _, legacy = scan({**env, "AI_PROVIDER": "ollama"})
    assert summary["paper_signals"] == 0 and legacy.call_count == 2 and not post.called
    expect_error(lambda: scan({**env, "AI_PROVIDER": "qwen"}))
    print("PASS: scan issues deterministic signals during Nemotron outage, risk gate first, legacy veto flag")


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
    reply = {"done": True, "message": {"content": '{"decision":"CONFIRM","reason":"Bukti cukup"}'}}
    with patch.dict("os.environ", {"AI_PROVIDER": " Ollama "}), patch("services.httpx.post") as post:
        post.return_value.json.return_value = reply
        assert confirm(candidate(), {})["decision"] == "CONFIRM"  # Same normalization as the scan loop.
    print("PASS: Hermes final JSON validation, failed/partial responses, missing executable, provider selection")
    with patch.dict("os.environ", {"TELEGRAM_ENABLED": "false"}), patch("services.httpx.post") as send:
        deliver(None, base)
        send.assert_not_called()
    print("PASS: AI schema and timeout fail closed; dry-run never sends Telegram")
    nemotron_checks({**long, "market": dict(bid=120.59, ask=120.61, spread_bps=1.6,
                                            funding_rate=0.0001, net_rr_estimate=1.8)}, rules)

    if "--nemotron-live" in sys.argv:
        # Optional smoke test against NVIDIA's hosted API; needs NVIDIA_API_KEY in .env. Never prints the key.
        import os
        import httpx
        import reasoning
        from services import load_dotenv, ROOT
        load_dotenv(ROOT / ".env")
        if not os.getenv("NVIDIA_API_KEY", "").strip():
            print("SKIP: NVIDIA_API_KEY kosong; uji live Nemotron tidak dijalankan")
        else:
            sizes, statuses = [], []
            real_post, real_get = httpx.post, httpx.get

            def measure(call):
                def wrapped(*args, **kwargs):
                    response = call(*args, **kwargs)
                    sizes.append(len(response.content))
                    statuses.append(response.status_code)
                    return response
                return wrapped
            sample = {**long, "market": dict(bid=120.59, ask=120.61, spread_bps=1.6, funding_rate=0.0001,
                                             net_rr_estimate=1.8)}
            with patch.dict("os.environ", {"AI_PROVIDER": "nemotron"}), \
                    patch("reasoning.httpx.post", measure(real_post)), patch("reasoning.httpx.get", measure(real_get)):
                result = reasoning.explain(sample, {"sample_count": 0, "average_net_r": None, "recent_cases": []})
            print(json.dumps({"status": result["status"], "error": result["error"], "latency_ms": result["latency_ms"],
                              "model_version": result["model_version"], "http_statuses": statuses,
                              "response_bytes": sizes, "requests": len(statuses),
                              "schema_ok": result["source"] == "nemotron"}))
            assert result["status"] == "OK", "Nemotron live gagal; lihat error di atas"
            print("PASS: Nemotron live menghasilkan ReasoningResult yang valid")

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
