"""Reference forecasters (requires numpy). A model is only worth using if it beats these.

Each is fitted on training samples only and returns (mu, sigma) in bps for the target, the
log mid return over the next `horizon` bars. sigma is the training residual RMS, so every
baseline is also a probabilistic forecast and can be scored with NLL and Brier.
"""
import numpy as np

from v2.dataset import CHANNELS

RET = CHANNELS.index("ret_bps")


class Baseline:
    name = "baseline"

    def fit(self, x, y, spec):
        self.horizon = spec.horizon
        resid = y - self.point(x)
        self.sigma = float(max(np.sqrt(np.mean(resid ** 2)), 1e-3)) if len(y) else 1.0
        return self

    def predict(self, x):
        mu = self.point(x)
        return mu, np.full(len(mu), self.sigma)

    def describe(self):
        return dict(name=self.name, sigma_bps=self.sigma)


class Zero(Baseline):
    """Random walk: the expected log return is zero."""
    name = "zero"

    def point(self, x):
        return np.zeros(len(x))


class Persistence(Baseline):
    """The last `horizon` minutes of return continue (capped by the window)."""
    name = "persistence"

    def point(self, x):
        k = min(self.horizon, x.shape[1])
        return x[:, -k:, RET].sum(axis=1).astype(np.float64)


class MovingAverage(Baseline):
    """Mean one-minute return over the window, scaled to the horizon."""
    name = "moving_average"

    def point(self, x):
        return x[:, :, RET].mean(axis=1).astype(np.float64) * self.horizon


class Ridge(Baseline):
    """Linear model on per-channel summaries (last value, window mean), standardized on train."""
    name = "ridge"

    def __init__(self, alpha=10.0):
        self.alpha = alpha

    @staticmethod
    def summary(x):
        return np.concatenate([x[:, -1, :], x.mean(axis=1)], axis=1).astype(np.float64)

    def fit(self, x, y, spec):
        f = self.summary(x)
        self.mean, self.scale = f.mean(axis=0), f.std(axis=0) + 1e-9
        z = np.c_[np.ones(len(f)), (f - self.mean) / self.scale]
        penalty = self.alpha * np.eye(z.shape[1])
        penalty[0, 0] = 0.0  # The intercept is not shrunk.
        self.coef = np.linalg.solve(z.T @ z + penalty, z.T @ y.astype(np.float64))
        return super().fit(x, y, spec)

    def point(self, x):
        if len(x) == 0:
            return np.zeros(0)
        z = np.c_[np.ones(len(x)), (self.summary(x) - self.mean) / self.scale]
        return z @ self.coef

    def describe(self):
        names = ["intercept"] + [f"last_{c}" for c in CHANNELS] + [f"mean_{c}" for c in CHANNELS]
        return dict(super().describe(), alpha=self.alpha,
                    coefficients={n: float(c) for n, c in zip(names, self.coef)})


def all_baselines():
    return [Zero(), Persistence(), MovingAverage(), Ridge()]
