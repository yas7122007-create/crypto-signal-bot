"""Forecast metrics (requires numpy). Gaussian forecasts: mean `mu` and std `sigma` in bps."""
import math

import numpy as np

SQRT2 = math.sqrt(2.0)


def p_up(mu, sigma):
    """P(return > 0) under N(mu, sigma)."""
    z = np.asarray(mu, dtype=np.float64) / np.asarray(sigma, dtype=np.float64)
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / SQRT2)) if z.size else z


def calibration(prob, outcome, bins=10):
    """Reliability table: per probability bin, count, mean forecast and observed frequency."""
    prob, outcome = np.asarray(prob, float), np.asarray(outcome, float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    index = np.clip(np.digitize(prob, edges[1:-1]), 0, bins - 1)
    table = []
    for b in range(bins):
        mask = index == b
        if mask.any():
            table.append(dict(lo=float(edges[b]), hi=float(edges[b + 1]), n=int(mask.sum()),
                              forecast=float(prob[mask].mean()), observed=float(outcome[mask].mean())))
    n = len(prob)
    ece = sum(row["n"] / n * abs(row["forecast"] - row["observed"]) for row in table) if n else None
    return table, ece


def evaluate(y, mu, sigma):
    """Point, direction and probabilistic metrics. Zero returns are excluded from direction."""
    y, mu, sigma = (np.asarray(a, dtype=np.float64) for a in (y, mu, sigma))
    n = len(y)
    if n == 0:
        return dict(n=0)
    err = mu - y
    moved = y != 0
    up = (y > 0).astype(float)
    prob = p_up(mu, sigma)
    nll = 0.5 * np.log(2 * math.pi * sigma ** 2) + 0.5 * (err / sigma) ** 2
    table, ece = calibration(prob[moved], up[moved])
    return dict(
        n=n,
        mae_bps=float(np.abs(err).mean()),
        rmse_bps=float(np.sqrt((err ** 2).mean())),
        directional_accuracy=float((np.sign(mu[moved]) == np.sign(y[moved])).mean()) if moved.any() else None,
        nll=float(nll.mean()),
        brier=float(((prob[moved] - up[moved]) ** 2).mean()) if moved.any() else None,
        ece=ece,
        calibration=table,
        mean_sigma_bps=float(sigma.mean()),
    )
