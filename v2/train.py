"""Offline training and evaluation: python -m v2.train --bars-dir DIR --out DIR.

Builds the dataset from recorded bars, fits the baselines and PatchTST on the training split
only, and reports every model on the same held-out test split (plus walk-forward folds for
the baselines, and for PatchTST with --walk-forward-patchtst). Nothing is live: no network,
no orders. A bars directory that contains a SYNTHETIC marker file is reported as synthetic,
and such results say nothing about real markets.
"""
import argparse
import json
from pathlib import Path
import sys
import time

import numpy as np

from v2 import bars as bars_mod
from v2.baselines import all_baselines
from v2.dataset import DatasetSpec, build, time_split, walk_forward
from v2.metrics import evaluate

MIN_TRAIN = 200


def log(**fields):
    print(json.dumps(dict(component="v2.train", **fields), sort_keys=True), file=sys.stderr)


def describe(meta, idx):
    if len(idx) == 0:
        return dict(n=0)
    asof = [meta[i]["asof_ms"] for i in idx]
    return dict(n=int(len(idx)), first_asof_ms=min(asof), last_asof_ms=max(asof))


def score_baselines(x, y, spec, train, test):
    out = {}
    for model in all_baselines():
        model.fit(x[train], y[train], spec)
        out[model.name] = dict(evaluate(y[test], *model.predict(x[test])), model=model.describe())
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bars-dir", action="append", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--window", type=int, default=32)
    ap.add_argument("--horizon", type=int, default=15)
    ap.add_argument("--obi-levels", type=int, default=10)
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--folds", type=int, default=4)
    ap.add_argument("--skip-patchtst", action="store_true")
    ap.add_argument("--walk-forward-patchtst", action="store_true")
    args = ap.parse_args(argv)
    spec = DatasetSpec(window=args.window, horizon=args.horizon, obi_levels=args.obi_levels)
    out = Path(args.out)

    raw = []
    synthetic = False
    for d in args.bars_dir:
        raw.extend(bars_mod.read_dir(d))
        synthetic |= (Path(d) / "SYNTHETIC").exists()
    groups = bars_mod.series(raw)
    x, y, meta = build(groups, spec)
    train, val, test = time_split(meta, spec)
    log(event="dataset", bars=len(raw), samples=len(y), train=len(train), val=len(val),
        test=len(test), synthetic=synthetic)
    if len(train) < MIN_TRAIN or len(test) == 0:
        log(event="error", reason="not_enough_samples", train=len(train), test=len(test))
        return 2

    report = dict(
        report="v2.train.v1", synthetic_data=synthetic, spec=spec.as_dict(),
        sources=[str(Path(d).resolve()) for d in args.bars_dir],
        dataset=dict(bars=len(raw), complete_bars=sum(b["complete"] for b in raw),
                     samples=len(y), symbols=sorted({k[0] for k in groups}),
                     sessions=len(groups), target_mean_bps=float(y.mean()),
                     target_std_bps=float(y.std()),
                     split=dict(train=describe(meta, train), val=describe(meta, val),
                                test=describe(meta, test))),
        test=score_baselines(x, y, spec, train, test),
    )
    folds = walk_forward(meta, spec, args.folds)
    report["walk_forward"] = [dict(fold=k, train=describe(meta, tr), test=describe(meta, te),
                                   models=score_baselines(x, y, spec, tr, te))
                              for k, (tr, te) in enumerate(folds)]

    if not args.skip_patchtst:
        from v2.patchtst import Forecaster, configure
        configure(args.threads, args.seed)
        started = time.perf_counter()
        model = Forecaster(spec).fit(x[train], y[train], x[val], y[val], epochs=args.epochs,
                                     seed=args.seed, log=lambda h: log(event="epoch", **h))
        train_s = time.perf_counter() - started
        version = model.save(out / "model")
        started = time.perf_counter()
        mu, sigma = model.predict(x[test])
        infer_s = time.perf_counter() - started
        report["test"]["patchtst"] = dict(evaluate(y[test], mu, sigma), model_version=version,
                                          trained={k: v for k, v in model.trained.items()
                                                   if k != "history"})
        report["benchmark"] = dict(threads=args.threads, train_seconds=round(train_s, 3),
                                   epochs=model.trained["epochs_run"],
                                   inference_ms_per_sample=round(1e3 * infer_s / len(test), 4),
                                   parameters=sum(p.numel() for p in model.net.parameters()))
        if args.walk_forward_patchtst:
            for fold, (tr, te) in zip(report["walk_forward"], folds):
                cut = int(len(tr) * 0.85)  # Last 15 % of the fold's train set for early stop.
                inner_tr, inner_val = tr[:cut], tr[cut:]
                m = Forecaster(spec).fit(x[inner_tr], y[inner_tr], x[inner_val], y[inner_val],
                                         epochs=args.epochs, seed=args.seed)
                fold["models"]["patchtst"] = evaluate(y[te], *m.predict(x[te]))

    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True))
    summary = {name: {k: r.get(k) for k in ("mae_bps", "directional_accuracy", "nll", "brier")}
               for name, r in report["test"].items()}
    log(event="done", out=str(out), synthetic=synthetic, test=summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
