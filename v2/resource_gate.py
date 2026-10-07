"""ResourceGate: bounds the host CPU/RAM the optional V2 model calls (PatchTST, Toto) may use.
Stdlib only (threading.Lock, os.getloadavg(), /proc/meminfo); no scheduler, no queue, no new
infrastructure. Never imported when V2_MODE="off": reasoning.patchtst_forecast() and
reasoning.toto_evidence() return before importing anything from v2/ in that mode.

Heavy inference is optional; market data and the deterministic engine are authoritative. So
this gate runs a job only when it can positively confirm the host is healthy, and fails closed
on everything else:

- One heavy job at a time, process-wide. A job arriving while another holds the mutex is
  skipped (BUSY), never queued: nothing can accumulate.
- `check()` returns one status. Only OK runs both jobs.
    OK              both thresholds satisfied.
    PRESSURE        load per core above V2_RESOURCE_MAX_LOAD_PER_CORE (default 1.5) or free
                    RAM below V2_RESOURCE_MIN_FREE_MB (default 512): Toto, the secondary
                    opinion, is skipped; PatchTST still runs.
    SEVERE          load above twice the limit or free RAM below half of it: PatchTST is
                    skipped too.
    INVALID_CONFIG  a threshold that is not a plain ASCII decimal above zero (bogus, NaN, inf,
                    0, negative, blank, "1_5", exponents). Never replaced by a default.
    TELEMETRY_ERROR on Linux, any failure to obtain or parse a measurement, or a non-finite or
                    negative reading.
    UNSUPPORTED     not Linux: the gate only knows how to measure on Linux, so on any other
                    platform (including the Windows laptop setup in README.md) it never runs a
                    job. Reported separately from TELEMETRY_ERROR so the two are not confused.
- A bug in the gate itself (GATE_ERROR) also skips the job. An exception raised by the job
  itself propagates unchanged; both callers already turn it into "NOT_AVAILABLE".
"""
import math
import os
import re
import sys
import threading
from types import MappingProxyType

_LOCK = threading.Lock()
ACQUIRE_TIMEOUT_S = 0.05
MEMINFO = "/proc/meminfo"
SEVERE_FACTOR = 2.0
DEFAULTS = MappingProxyType({"V2_RESOURCE_MAX_LOAD_PER_CORE": "1.5",
                             "V2_RESOURCE_MIN_FREE_MB": "512"})
JOBS = ("patchtst", "toto")
# A plain ASCII decimal: rejects what float() would also accept ("1_5", full-width digits,
# exponents, hex, surrounding blanks), so a typo cannot silently loosen a threshold.
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?|\.[0-9]+")

OK = "ok"
PRESSURE = "resource_pressure"
SEVERE = "resource_pressure_severe"
INVALID_CONFIG = "invalid_config"
TELEMETRY_ERROR = "telemetry_error"
UNSUPPORTED = "resource_unsupported"
BUSY = "model_job_in_progress"
GATE_ERROR = "gate_error"

# The worst status each job tolerates: Toto only runs on a healthy host, PatchTST also under
# ordinary pressure.
_ALLOWED = MappingProxyType({"toto": (OK,), "patchtst": (OK, PRESSURE)})


def _threshold(name):
    raw = os.environ.get(name, DEFAULTS[name])
    if not _DECIMAL.fullmatch(raw):
        raise ValueError(f"{name} must be a plain decimal number")
    value = float(raw)
    if not math.isfinite(value * SEVERE_FACTOR) or value <= 0:
        raise ValueError(f"{name} must be a finite number above zero")
    return value


def _linux():
    return sys.platform.startswith("linux")


def _load_per_core():
    return os.getloadavg()[0] / (os.cpu_count() or 1)


def _free_mb():
    with open(MEMINFO) as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024
    raise ValueError("MemAvailable missing from " + MEMINFO)


def check():
    """The host's current status (see the module docstring). Never raises."""
    try:
        max_load = _threshold("V2_RESOURCE_MAX_LOAD_PER_CORE")
        min_free = _threshold("V2_RESOURCE_MIN_FREE_MB")
    except (TypeError, ValueError, OverflowError):
        return INVALID_CONFIG
    if not _linux():
        return UNSUPPORTED
    try:
        load, free = float(_load_per_core()), float(_free_mb())
    except Exception:
        return TELEMETRY_ERROR
    if not (math.isfinite(load) and math.isfinite(free) and load >= 0 and free >= 0):
        return TELEMETRY_ERROR
    if load > max_load * SEVERE_FACTOR or free < min_free / SEVERE_FACTOR:
        return SEVERE
    if load > max_load or free < min_free:
        return PRESSURE
    return OK


def guarded(job, fn):
    """Runs `fn()` for `job` ("patchtst" or "toto") under the mutex if the host allows it.
    Returns (True, fn()'s result) or (False, reason). Never raises on its own account."""
    acquired = reached_fn = False
    try:
        if job not in _ALLOWED:
            return False, GATE_ERROR
        acquired = _LOCK.acquire(timeout=ACQUIRE_TIMEOUT_S)
        if not acquired:
            return False, BUSY
        status = check()
        if status not in _ALLOWED[job]:
            return False, status
        reached_fn = True
        return True, fn()
    except Exception:
        if reached_fn:
            raise
        return False, GATE_ERROR
    finally:
        if acquired:
            _LOCK.release()
