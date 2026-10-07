"""PatchTST forecaster (requires torch and numpy), CPU only, small and deterministic.

Architecture (Nie et al., "A Time Series is Worth 64 Words", ICLR 2023, simplified):
each input channel is cut into overlapping patches, every patch is embedded by one shared
linear layer plus a learned position, and a small Transformer encoder runs over the patches
of each channel independently. The flattened encodings of all channels feed one linear head
that outputs the mean and log standard deviation of the target (the next `horizon` bars'
log mid return in bps), trained with Gaussian negative log likelihood. P(up) = Phi(mu/sigma).

Inputs and target are standardized with statistics fitted on the training split only and
stored with the weights. A saved model is a directory with `weights.pt` (tensors only, loaded
with weights_only=True) and `model.json`; the model version is the SHA-256 of both, verified
on load.
"""
import hashlib
import io
import json
import math
from pathlib import Path

import numpy as np
import torch
from torch import nn

from v2.dataset import CHANNELS, DatasetSpec

MODEL_NAME = "patchtst"
SIGMA_FLOOR = 1e-3
LOG_SIGMA_MAX = 6.0  # Standardized units: sigma at most ~400 target std.


def configure(threads=1, seed=0):
    """Deterministic CPU execution with a bounded thread count."""
    torch.set_num_threads(max(1, int(threads)))
    torch.manual_seed(seed)
    np.random.seed(seed)
    torch.use_deterministic_algorithms(True)


class PatchTST(nn.Module):
    def __init__(self, channels, window, patch=8, stride=4, d_model=32, heads=4, layers=2,
                 ff=64, dropout=0.1):
        super().__init__()
        if window < patch or (window - patch) % stride:
            raise ValueError("window must tile into patches")
        self.patch, self.stride = patch, stride
        self.patches = (window - patch) // stride + 1
        self.embed = nn.Linear(patch, d_model)
        self.position = nn.Parameter(torch.randn(1, self.patches, d_model) * 0.02)
        layer = nn.TransformerEncoderLayer(d_model, heads, ff, dropout, batch_first=True,
                                           norm_first=True)
        self.encoder = nn.TransformerEncoder(layer, layers, enable_nested_tensor=False)
        self.head = nn.Sequential(nn.Dropout(dropout),
                                  nn.Linear(channels * self.patches * d_model, 2))

    def forward(self, x):
        """x: (batch, window, channels), standardized -> (mu, log_sigma), standardized."""
        b, _, c = x.shape
        series = x.transpose(1, 2).reshape(b * c, -1)              # channel independent
        patches = series.unfold(1, self.patch, self.stride)          # (b*c, patches, patch)
        h = self.encoder(self.embed(patches) + self.position)
        out = self.head(h.reshape(b, -1))
        return out[:, 0], out[:, 1].clamp(-LOG_SIGMA_MAX, LOG_SIGMA_MAX)


class Forecaster:
    """Trained network plus its scalers and spec: the unit that is saved and versioned."""

    def __init__(self, spec, hyper=None):
        self.spec = spec
        self.hyper = {**dict(patch=8, stride=4, d_model=32, heads=4, layers=2, ff=64,
                             dropout=0.1), **(hyper or {})}
        self.net = PatchTST(len(CHANNELS), spec.window, **self.hyper)
        self.x_mean = self.x_std = None
        self.y_mean, self.y_std = 0.0, 1.0
        self.version = None
        self.trained = {}

    def fit_scalers(self, x, y):
        flat = x.reshape(-1, x.shape[-1]).astype(np.float64)
        self.x_mean = flat.mean(axis=0)
        self.x_std = flat.std(axis=0) + 1e-6
        self.y_mean, self.y_std = float(y.mean()), float(y.std() + 1e-6)

    def _x(self, x):
        return torch.from_numpy(((x - self.x_mean) / self.x_std).astype(np.float32))

    def _y(self, y):
        return torch.from_numpy(((y - self.y_mean) / self.y_std).astype(np.float32))

    @staticmethod
    def _nll(mu, log_sigma, y):
        return (log_sigma + 0.5 * ((y - mu) * torch.exp(-log_sigma)) ** 2).mean() \
            + 0.5 * math.log(2 * math.pi)

    def fit(self, x, y, x_val, y_val, epochs=30, batch=256, lr=1e-3, patience=5, seed=0,
            log=None):
        """Adam with early stopping on validation NLL; keeps the best epoch's weights."""
        self.fit_scalers(x, y)
        gen = torch.Generator().manual_seed(seed)
        xt, yt = self._x(x), self._y(y)
        xv, yv = self._x(x_val), self._y(y_val)
        opt = torch.optim.AdamW(self.net.parameters(), lr=lr, weight_decay=1e-4)
        best, best_state, stale, history = math.inf, None, 0, []
        for epoch in range(epochs):
            self.net.train()
            order = torch.randperm(len(xt), generator=gen)
            total = 0.0
            for i in range(0, len(order), batch):
                idx = order[i:i + batch]
                mu, ls = self.net(xt[idx])
                loss = self._nll(mu, ls, yt[idx])
                opt.zero_grad()
                loss.backward()
                nn.utils.clip_grad_norm_(self.net.parameters(), 1.0)
                opt.step()
                total += loss.item() * len(idx)
            val = self._loss(xv, yv) if len(xv) else total / len(xt)
            history.append(dict(epoch=epoch, train_nll=total / len(xt), val_nll=val))
            if log:
                log(history[-1])
            if val < best - 1e-4:
                best, stale = val, 0
                best_state = {k: v.detach().clone() for k, v in self.net.state_dict().items()}
            else:
                stale += 1
                if stale >= patience:
                    break
        if best_state is not None:
            self.net.load_state_dict(best_state)
        self.trained = dict(epochs_run=len(history), best_val_nll=best, history=history,
                            samples=int(len(x)), val_samples=int(len(x_val)), seed=seed)
        return self

    @torch.no_grad()
    def _loss(self, xv, yv):
        self.net.eval()
        mu, ls = self.net(xv)
        return float(self._nll(mu, ls, yv))

    @torch.no_grad()
    def predict(self, x):
        """(mu_bps, sigma_bps) as float64 arrays."""
        self.net.eval()
        if len(x) == 0:
            return np.zeros(0), np.zeros(0)
        mu, ls = self.net(self._x(x))
        mu = mu.double().numpy() * self.y_std + self.y_mean
        sigma = np.maximum(np.exp(ls.double().numpy()) * self.y_std, SIGMA_FLOOR)
        return mu, sigma

    # Persistence -----------------------------------------------------------------------

    def metadata(self):
        return dict(model=MODEL_NAME, spec=self.spec.as_dict(), hyper=self.hyper,
                    x_mean=[float(v) for v in self.x_mean], x_std=[float(v) for v in self.x_std],
                    y_mean=self.y_mean, y_std=self.y_std, trained=self.trained,
                    torch=torch.__version__)

    def save(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        weights = directory / "weights.pt"
        torch.save(self.net.state_dict(), weights)
        meta = json.dumps(self.metadata(), sort_keys=True, indent=1).encode()
        (directory / "model.json").write_bytes(meta)
        self.version = version_of(weights.read_bytes(), meta)
        (directory / "VERSION").write_text(self.version + "\n")
        return self.version

    @classmethod
    def load(cls, directory):
        directory = Path(directory)
        weights, meta = (directory / "weights.pt").read_bytes(), (directory / "model.json").read_bytes()
        version = version_of(weights, meta)
        expected = (directory / "VERSION").read_text().strip()
        if version != expected:
            raise ValueError("model files do not match their VERSION")
        info = json.loads(meta)
        spec_d = info["spec"]
        if info.get("model") != MODEL_NAME or spec_d.get("channels") != list(CHANNELS):
            raise ValueError("model was trained for another input contract")
        spec = DatasetSpec(window=spec_d["window"], horizon=spec_d["horizon"],
                           obi_levels=spec_d["obi_levels"])
        if spec.as_dict() != spec_d:
            raise ValueError("model was trained for another dataset schema")
        model = cls(spec, {k: v for k, v in info["hyper"].items()})
        model.x_mean, model.x_std = np.asarray(info["x_mean"]), np.asarray(info["x_std"])
        model.y_mean, model.y_std = info["y_mean"], info["y_std"]
        model.trained = info.get("trained", {})
        # Load the bytes that were hashed, not the file again.
        state = torch.load(io.BytesIO(weights), weights_only=True, map_location="cpu")
        model.net.load_state_dict(state)
        model.net.eval()
        model.version = version
        return model


def version_of(weights, meta):
    return hashlib.sha256(hashlib.sha256(weights).digest() + hashlib.sha256(meta).digest()).hexdigest()[:16]
