"""Run: python bot.py doctor | once | serve | report | init-db."""
import argparse
import json
import logging
import os

from engine import (VERSION, analyze, validate_market, new_signal, paper_update,
                    settle, signal_id)
from services import (ROOT, LOG, Binance, active_signals, confirm, database, deliver,
                      dump, init_schema, memories, query, rules_from_env, safe_error,
                      save_analysis, save_candles, save_signal, record_outcome, work_lock)

# .env is loaded by services before Prefect imports its settings.
from prefect import flow, task
from prefect.cache_policies import NO_CACHE


@task(name="Evaluasi-paper-trading", cache_policy=NO_CACHE, persist_result=False)
def evaluate():
    api = Binance()
    completed = 0
    try:
        with database() as db, work_lock(db) as acquired:
            if not acquired:
                return 0
            now = api.now()
            for s in active_signals(db):
                try:
                    if s["status"] != "SETTLING":
                        end = min(now, s["expires_ms"] + s["rules"]["hold_hours"] * 3_600_000 + 120_000)
                        bars = api.minute_history(s["symbol"], s["next_bar_ms"], end)
                        save_candles(db, s["symbol"], "1m", bars)
                        s = paper_update(s, bars, end)
                        save_signal(db, s)
                    if s["status"] == "SETTLING":
                        funding = api.get("/fapi/v1/fundingRate", symbol=s["symbol"],
                                          startTime=s["fill_ms"], endTime=s["exit_ms"], limit=1000)
                        if len(funding) >= 1000:
                            raise ValueError("Riwayat funding memerlukan pagination tambahan")
                        s = settle(s, funding)
                        record_outcome(db, s)
                        completed += 1
                        LOG.info("Paper %s %s: %s %.3f R", s["id"], s["symbol"], s["outcome"], s["net_r"])
                except (ValueError, KeyError, TypeError, OSError) as exc:
                    LOG.warning("Evaluasi %s tertunda: %s", s["id"], safe_error(exc))
                except Exception as exc:
                    # Preserve active state and original evidence on API/DB failure.
                    LOG.warning("Evaluasi %s tertunda: %s", s["id"], safe_error(exc))
    finally:
        api.close()
    return completed


@task(name="Scan-50-coin", cache_policy=NO_CACHE, persist_result=False)
def scan(force=False):
    api = Binance()
    rules = rules_from_env()
    try:
        with database() as db, work_lock(db) as acquired:
            if not acquired:
                return {"state": "BUSY"}
            now = api.now()
            bucket = (now // 900_000 - 1) * 900_000
            last = query(db, "SELECT value FROM bot_state WHERE name='last_scan_candle'")
            if not force and (now % 900_000 < 120_000 or (last and int(last[0]["value"]) >= bucket)):
                return {"state": "WAITING_CANDLE"}
            chosen, hourly = api.universe(now, db)
            if not chosen:
                raise ValueError("Tidak ada kontrak yang memenuhi filter universe")
            query(db, "INSERT INTO bot_state VALUES ('universe',%s) ON DUPLICATE KEY UPDATE value=VALUES(value)",
                  (dump({"asof": now, "coins": chosen}),))
            candidates = []
            for item in chosen:
                symbol = item["symbol"]
                try:
                    frames = {"1h": hourly[symbol] if symbol in hourly else api.bars(symbol, "1h", now),
                              "15m": api.bars(symbol, "15m", now),
                              "4h": api.bars(symbol, "4h", now)}
                    for timeframe, bars in frames.items():
                        save_candles(db, symbol, timeframe, bars)
                    result = analyze(symbol, frames, item["tick"], rules)
                    result["universe_group"] = item["group"]
                    if result["action"] != "HOLD":
                        candidates.append(result)
                    else:
                        save_analysis(db, result)
                except (ValueError, KeyError, TypeError) as exc:
                    save_analysis(db, dict(symbol=symbol, candle_ms=bucket, action="HOLD", reason=safe_error(exc)))
                except Exception as exc:
                    if isinstance(exc, RuntimeError):
                        raise
                    save_analysis(db, dict(symbol=symbol, candle_ms=bucket, action="HOLD", reason="Data gagal: " + safe_error(exc)))
            open_signals = active_signals(db)
            busy_symbols = {s["symbol"] for s in open_signals}
            max_open = int(os.getenv("MAX_OPEN_SIGNALS", "3"))
            if not 1 <= max_open <= 50:
                raise ValueError("MAX_OPEN_SIGNALS harus 1–50")
            ai_calls, issued = 0, 0
            for result in sorted(candidates, key=lambda c: c["rank"], reverse=True):
                try:
                    if result["symbol"] in busy_symbols:
                        raise ValueError("Sudah ada sinyal aktif pada coin ini")
                    if len(open_signals) + issued >= max_open:
                        raise ValueError("Batas sinyal aktif tercapai")
                    if query(db, "SELECT id FROM signals WHERE id=%s", (signal_id(result),)):
                        raise ValueError("Kandidat candle ini sudah pernah diterbitkan")
                    # ponytail: bound local LLM work to 5 candidates per scan; increase after measuring latency.
                    if ai_calls >= 5:
                        raise ValueError("Batas 5 konfirmasi AI per siklus tercapai")
                    history = memories(db, result["regime"], result["version"])
                    result["journal"] = history
                    if history["sample_count"] >= rules.evaluation_samples and history["average_net_r"] <= 0:
                        raise ValueError("Setup ditahan: rata-rata hasil paper terbaru tidak positif")
                    quote = api.get("/fapi/v1/ticker/bookTicker", symbol=result["symbol"])
                    premium = api.get("/fapi/v1/premiumIndex", symbol=result["symbol"])
                    result["market"] = validate_market(result, quote, premium, api.now(), rules)
                    result["ai"] = confirm(result, history)
                    ai_calls += 1
                    if result["ai"]["decision"] != "CONFIRM":
                        raise ValueError(result["ai"]["reason"])
                    quote = api.get("/fapi/v1/ticker/bookTicker", symbol=result["symbol"])
                    premium = api.get("/fapi/v1/premiumIndex", symbol=result["symbol"])
                    fresh_now = api.now()
                    result["market"] = validate_market(result, quote, premium, fresh_now, rules)
                    signal = new_signal(result, fresh_now, rules)
                    if save_signal(db, signal, new=True):
                        issued += 1
                        busy_symbols.add(signal["symbol"])
                        deliver(db, signal)
                        LOG.info("Sinyal PAPER %s %s %s", signal["id"], signal["symbol"], signal["action"])
                except Exception as exc:
                    result["proposed_action"] = result["action"]
                    result["action"] = "HOLD"
                    result["reason"] = safe_error(exc)
                    LOG.info("%s HOLD: %s", result["symbol"], result["reason"])
                save_analysis(db, result)
            summary = {"state": "DONE", "coins": len(chosen), "candidates": len(candidates),
                       "ai_reviews": ai_calls, "paper_signals": issued, "asof_ms": now}
            query(db, "INSERT INTO bot_state VALUES ('last_scan_candle',%s) ON DUPLICATE KEY UPDATE value=VALUES(value)", (str(bucket),))
            query(db, "INSERT INTO bot_state VALUES ('last_scan_summary',%s) ON DUPLICATE KEY UPDATE value=VALUES(value)", (dump(summary),))
            LOG.info("Scan selesai: %s", summary)
            return summary
    finally:
        api.close()


@flow(name="crypto-signal-bot", log_prints=True, persist_result=False)
def cycle(force=False):
    try:
        done = evaluate()
        result = scan(force)
        return {"evaluated": done, "scan": result}
    except Exception as exc:
        raise RuntimeError("Workflow dihentikan: " + safe_error(exc)) from None


def report():
    with database() as db:
        summary = query(db, "SELECT value FROM bot_state WHERE name='last_scan_summary'")
        print("Scan terakhir:", summary[0]["value"] if summary else "belum ada")
        print("Sinyal:", dump(query(db, "SELECT status,COUNT(*) AS jumlah FROM signals GROUP BY status")))
        rows = query(db, """SELECT regime,COUNT(*) AS samples,AVG(net_r) AS average_net_r,
                            SUM(net_r>0)/COUNT(*) AS positive_fraction
                            FROM outcomes GROUP BY regime""")
        print("Hasil PAPER (bukan transaksi akun):", rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["doctor", "once", "serve", "report", "init-db"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # HTTP request logs can include Telegram credentials in the path.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if args.command == "doctor":
        from setup_local import doctor
        raise SystemExit(doctor())
    if args.command == "init-db":
        with database() as db:
            init_schema(db)
        print("Tabel MariaDB siap.")
    elif args.command == "report":
        report()
    elif args.command == "once":
        print(dump(cycle(force=True)))
    else:
        from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
        cycle.serve(name="local-crypto-signals", cron="* * * * *", limit=1,
                    global_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW),
                    pause_on_shutdown=True)


if __name__ == "__main__":
    main()
