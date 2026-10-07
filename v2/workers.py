"""Subprocess worker runner (stdlib only): the mechanism Toto needs, since the real
`toto-ts` package pins a torch version that conflicts with PatchTST's. A worker is a fixed
argv (never a shell string), fed one JSON object on stdin and returning one JSON object on
stdout; this process never passes it unparsed data and never retries on failure -- a timeout
or bad output is the caller's signal to fail closed.
"""
import json
import os
import signal
import subprocess


class WorkerError(RuntimeError):
    pass


def run(argv, payload, timeout_s):
    """argv: a fixed list, no shell. Raises WorkerError on anything but a clean JSON object
    on stdout; never raises on the subprocess's own stderr, which is discarded other than
    for the error message (observability, not a value)."""
    if not isinstance(argv, list) or not argv or any(not isinstance(a, str) for a in argv):
        raise WorkerError("argv must be a non-empty list of strings")
    # The worker gets its own process group on POSIX so a timeout kills everything it started
    # (e.g. a wrapper script's model process), not just the direct child: an orphaned model
    # would keep running after ResourceGate released its mutex.
    posix = os.name == "posix"
    try:
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, shell=False,
                                start_new_session=posix)
    except OSError as exc:
        raise WorkerError(f"worker could not start: {exc}") from None
    try:
        stdout, stderr = proc.communicate(json.dumps(payload, default=str), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        _kill(proc, posix)
        raise WorkerError("worker timed out") from None
    except BaseException:
        _kill(proc, posix)
        raise
    if proc.returncode != 0:
        raise WorkerError(f"worker exited {proc.returncode}: {stderr[-500:]}")
    try:
        out = json.loads(stdout)
    except json.JSONDecodeError:
        raise WorkerError("worker did not return valid JSON") from None
    if not isinstance(out, dict):
        raise WorkerError("worker output must be a JSON object")
    return out


def _kill(proc, posix):
    """Kill the worker and, on POSIX, its whole process group; then reap it."""
    try:
        if posix:
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError):
        pass
    proc.communicate()
