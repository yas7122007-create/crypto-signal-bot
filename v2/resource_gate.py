"""Fix 6 (adversarial-audit corrective pass): a minimal, auditable gate on how much of the
host's CPU/RAM the V2 model calls (PatchTST, Toto) may use at once. Stdlib only
(threading.Lock, os.getloadavg(), /proc/meminfo); no new infrastructure -- no Redis, no
Celery, no external queue. Inert whenever this module is never imported, which is the case
under V2_MODE="off" (patchtst_forecast()/toto_evidence() in reasoning.py return before
touching anything V2-related at all when V2_MODE is off, so this gate is never reached).

Policy:
- At most one heavy job (a PatchTST forecast or a Toto validation) runs at a time,
  process-wide, via a plain `threading.Lock` acquired with a short timeout. A second job
  arriving while one is in flight is SKIPPED, never queued: there is no queue, so there is
  nothing for stale work to accumulate in.
- Before running, pressure is checked against two independently configurable thresholds:
  load average per CPU core (`V2_RESOURCE_MAX_LOAD_PER_CORE`, default 1.5) and free RAM in
  MB (`V2_RESOURCE_MIN_FREE_MB`, default 512). Under pressure, Toto -- the secondary,
  confirm/reject opinion -- is skipped before even attempting the mutex; PatchTST -- the
  primary forecast everything else depends on -- is only skipped if pressure is still
  present once the mutex is actually held (checked a second time deliberately: pressure can
  appear in the time between the two checks).
- A missing or unreadable pressure *source* (no `os.getloadavg()` on this platform, no
  `/proc/meminfo`) is an expected, normal condition on some hosts, not a bug: it is treated
  as "no pressure detected" from that source, so it alone never blocks a call.
- An unexpected internal failure of the gate's own control flow (not a model/bridge
  failure, which the caller already handles) fails CLOSED: `guarded()` skips running the
  job rather than let a bug in this small module either crash the caller or silently bypass
  the one-job-at-a-time guarantee it exists to enforce. This matches the project's existing
  fail-closed convention (patchtst_forecast()/toto_evidence()/bridge.py all degrade to
  "unavailable" rather than raise); `guarded()` never raises on its own account, though an
  exception from `fn()` itself still propagates unchanged, exactly as if this gate did not
  wrap it.
"""
import os
import threading

_LOCK = threading.Lock()

DEFAULT_MAX_LOAD_PER_CORE = 1.5
DEFAULT_MIN_FREE_MB = 512
DEFAULT_ACQUIRE_TIMEOUT_S = 0.05

PRESSURE = "resource_pressure"
BUSY = "model_job_in_progress"


def _max_load_per_core():
    return float(os.getenv("V2_RESOURCE_MAX_LOAD_PER_CORE", DEFAULT_MAX_LOAD_PER_CORE))


def _min_free_mb():
    return float(os.getenv("V2_RESOURCE_MIN_FREE_MB", DEFAULT_MIN_FREE_MB))


def _load_pressure():
    load1 = os.getloadavg()[0]
    cores = os.cpu_count() or 1
    return (load1 / cores) > _max_load_per_core()


def _memory_pressure():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                free_kb = int(line.split()[1])
                return (free_kb / 1024) < _min_free_mb()
    return False  # No MemAvailable line: can't tell, so don't claim pressure.


def under_pressure():
    """True if either configured threshold is currently exceeded. Never raises."""
    try:
        if _load_pressure():
            return True
    except Exception:
        pass
    try:
        if _memory_pressure():
            return True
    except Exception:
        pass
    return False


GATE_ERROR = "gate_error"


def guarded(job, fn):
    """Runs `fn()` (a zero-argument callable) under the single-heavy-job mutex, or skips it.

    `job` is "patchtst" or "toto": under pressure, "toto" is skipped first, before even
    attempting the mutex; "patchtst" is skipped only if pressure remains once the mutex is
    held. Returns `(True, fn()'s return value)` when it ran, or `(False, reason)` --
    `PRESSURE`, `BUSY`, or `GATE_ERROR` -- when it was skipped.

    Fails closed on its own account: if anything in this function's own control flow raises
    before `fn()` is reached (not `under_pressure()`, which already never raises, but a
    defensive catch-all for this module's own bugs), the job is skipped rather than run
    un-gated or left to crash the caller. Once `fn()` is actually called, though, its own
    exception propagates unchanged -- that is the caller's exception to handle, exactly as
    before this gate existed (every current caller already catches and degrades its own
    model/bridge failures)."""
    acquired, reached_fn = False, False
    try:
        if job == "toto" and under_pressure():
            return False, PRESSURE
        acquired = _LOCK.acquire(timeout=DEFAULT_ACQUIRE_TIMEOUT_S)
        if not acquired:
            return False, BUSY
        if job == "patchtst" and under_pressure():
            return False, PRESSURE
        reached_fn = True
        return True, fn()
    except Exception:
        if reached_fn:
            raise  # fn() itself raised; not this gate's failure to swallow.
        return False, GATE_ERROR
    finally:
        if acquired:
            _LOCK.release()
