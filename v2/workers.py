"""Subprocess worker runner (stdlib only): the mechanism Toto needs, since the real
`toto-ts` package pins a torch version that conflicts with PatchTST's. A worker is a fixed
argv (never a shell string), fed one JSON object on stdin and returning one JSON object on
stdout; this process never passes it unparsed data and never retries on failure -- a timeout
or bad output is the caller's signal to fail closed.
"""
import json
import subprocess


class WorkerError(RuntimeError):
    pass


def run(argv, payload, timeout_s):
    """argv: a fixed list, no shell. Raises WorkerError on anything but a clean JSON object
    on stdout; never raises on the subprocess's own stderr, which is discarded other than
    for the error message (observability, not a value)."""
    if not isinstance(argv, list) or not argv or any(not isinstance(a, str) for a in argv):
        raise WorkerError("argv must be a non-empty list of strings")
    try:
        result = subprocess.run(argv, input=json.dumps(payload, default=str), capture_output=True,
                                text=True, timeout=timeout_s, shell=False)
    except subprocess.TimeoutExpired:
        raise WorkerError("worker timed out") from None
    except OSError as exc:
        raise WorkerError(f"worker could not start: {exc}") from None
    if result.returncode != 0:
        raise WorkerError(f"worker exited {result.returncode}: {result.stderr[-500:]}")
    try:
        out = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise WorkerError("worker did not return valid JSON") from None
    if not isinstance(out, dict):
        raise WorkerError("worker output must be a JSON object")
    return out
