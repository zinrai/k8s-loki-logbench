#!/usr/bin/env python3
"""k8s-loki-logbench -- measure the Loki log pipeline for Kubernetes pods.

Given pods (discovered by namespace prefix, or fed as JSON records on stdin),
report how long their log lines take to become queryable in Loki (`latency`)
and whether any are missing (`verify`). Thin glue over kubectl (read-only) and
logcli; no client libraries, stdlib only.
"""

import argparse
import json
import select
import subprocess
import sys
import time
from datetime import datetime, timezone

# --- shell ----------------------------------------------------------------
# The only place this glue shells out, so the logcli/kubectl surface stays in
# one auditable section.


def call(cmd, *, stdin=None):
    """Run cmd without raising; return the CompletedProcess."""
    return subprocess.run(cmd, input=stdin, capture_output=True, text=True)


def run(cmd, *, stdin=None, check=True):
    """Run cmd and return stdout. Raise on non-zero exit when check is set."""
    p = call(cmd, stdin=stdin)
    if check and p.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({p.returncode}): {p.stderr.strip()}")
    return p.stdout


def run_json(cmd, **kw):
    return json.loads(run(cmd, **kw))


def first_jsonl_line(cmd, *, timeout):
    """Start cmd, return its first non-empty JSONL line parsed, then terminate.

    Used for `logcli query --tail`, which streams and never exits on its own.
    Returns None on timeout or if the stream ends without a line.
    """
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            ready, _, _ = select.select([p.stdout], [], [], remaining)
            if not ready:
                return None
            line = p.stdout.readline()
            if not line:  # EOF
                return None
            line = line.strip()
            if line:
                return json.loads(line)
    finally:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def emit(obj):
    """Write one JSON object as a line to stdout (pipeline-friendly)."""
    print(json.dumps(obj))


# --- pod targeting --------------------------------------------------------
# Every stage speaks the same record: {"namespace", "pod", "podStartTime", ...}.
# A producer (e.g. k8s-logload) emits it on stdin; latency/verify consume it,
# or discover pods directly by namespace prefix.


def _normalize(rec):
    out = {
        "namespace": rec.get("namespace") or rec.get("targetNamespace"),
        "pod": rec.get("pod") or rec.get("podName"),
        "podStartTime": rec.get("podStartTime"),
    }
    for key, value in rec.items():
        if key not in ("namespace", "targetNamespace", "pod", "podName"):
            out[key] = value
    return out


def _read_stdin_pods():
    """Accept either a single JSON object or a JSON array on stdin."""
    data = json.loads(sys.stdin.read())
    items = data if isinstance(data, list) else [data]
    pods = []
    for rec in items:
        pods.append(_normalize(rec))
    return pods


def _pods_by_prefix(prefix):
    data = run_json(["kubectl", "get", "pods", "-A", "-o", "json"])
    out = []
    for item in data.get("items", []):
        ns = item["metadata"]["namespace"]
        if not ns.startswith(prefix):
            continue
        start = item.get("status", {}).get("startTime")
        if not start:
            continue
        out.append(
            {
                "namespace": ns,
                "pod": item["metadata"]["name"],
                "podStartTime": start,
                "annotations": item["metadata"].get("annotations", {}),
            }
        )
    return out


def collect_pods(args):
    """Target pods come from stdin (pipeline) or from a namespace-prefix scan."""
    if args.stdin:
        return _read_stdin_pods()
    return _pods_by_prefix(args.namespace_prefix)


# --- latency --------------------------------------------------------------
# latency = now - pod .status.startTime at the moment the first line is
# observed; a seconds-grained benchmark figure.
#   range : poll `logcli query --from/--to` until a line lands (query_range).
#   tail  : `logcli query --tail`, measure the first streamed line.


def _now_utc():
    return datetime.now(timezone.utc)


def _parse_rfc3339(s):
    # Python 3.11+ datetime.fromisoformat accepts the trailing 'Z'.
    return datetime.fromisoformat(s)


def _query(pod):
    return '{pod_name="%s"}' % pod["pod"]


def _measure_range(pod, *, poll_interval, timeout):
    start = _parse_rfc3339(pod["podStartTime"])
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cmd = [
            "logcli",
            "query",
            _query(pod),
            "--from",
            start.isoformat(),
            "--to",
            _now_utc().isoformat(),
            "--org-id",
            pod["namespace"],
            "--limit",
            "1",
            "--quiet",
            "-o",
            "jsonl",
        ]
        # With -o jsonl --quiet, stdout has a line only when results exist.
        if run(cmd).strip():
            return (_now_utc() - start).total_seconds()
        time.sleep(poll_interval)
    return None


def _measure_tail(pod, *, timeout):
    start = _parse_rfc3339(pod["podStartTime"])
    cmd = [
        "logcli",
        "query",
        _query(pod),
        "--tail",
        "--org-id",
        pod["namespace"],
        "--quiet",
        "-o",
        "jsonl",
    ]
    line = first_jsonl_line(cmd, timeout=timeout)
    if line is None:
        return None
    return (_now_utc() - start).total_seconds()


def _summary(latencies):
    vals = []
    for x in latencies:
        if x is not None:
            vals.append(x)
    if not vals:
        return {"measured": 0}
    return {
        "measured": len(vals),
        "max_seconds": max(vals),
        "min_seconds": min(vals),
        "mean_seconds": sum(vals) / len(vals),
    }


def cmd_latency(args):
    targets = collect_pods(args)
    latencies = []
    for pod in targets:
        if args.mode == "tail":
            latency = _measure_tail(pod, timeout=args.timeout)
        else:
            latency = _measure_range(
                pod, poll_interval=args.poll_interval, timeout=args.timeout
            )
        latencies.append(latency)
        emit(
            {
                "namespace": pod["namespace"],
                "pod": pod["pod"],
                "latency_seconds": latency,
            }
        )
    emit({"summary": _summary(latencies)})
    return 0


# --- verify ---------------------------------------------------------------
# Compare each pod's total_log_lines annotation (the load generator's source
# of truth) against what Loki holds; emit only the mismatches.


def _annotation_lines(pod):
    # The pod record carries annotations (the discovery path always sets them).
    # A pod without the total_log_lines annotation is not ours to verify.
    raw = pod["annotations"].get("total_log_lines")
    if raw in (None, ""):
        return None
    return int(raw)


def _loki_count(pod):
    # Scope the count to this pod's lifetime [startTime, now]. A fixed [1h]
    # window would also count earlier runs that reused the same pod_name.
    # logcli instant-query prints a JSON vector: [{"metric": {...},
    # "value": [<ts>, "<count>"]}, ...]. One pod_name can split into several
    # streams (e.g. by container), so sum the sample value across results.
    start = _parse_rfc3339(pod["podStartTime"])
    window = int((_now_utc() - start).total_seconds()) + 5  # +5s slack
    if window < 1:
        window = 1
    expr = 'count_over_time({pod_name="%s"}[%ds])' % (pod["pod"], window)
    out = run(
        ["logcli", "instant-query", expr, "--org-id", pod["namespace"], "--quiet"]
    )
    results = json.loads(out)
    total = 0
    for r in results:
        total += int(float(r["value"][1]))
    return total


def cmd_verify(args):
    targets = collect_pods(args)
    mismatches = 0
    for pod in targets:
        expected = _annotation_lines(pod)
        if expected is None:
            continue
        actual = _loki_count(pod)
        if expected != actual:
            mismatches += 1
            emit(
                {
                    "namespace": pod["namespace"],
                    "pod": pod["pod"],
                    "total_log_lines": expected,
                    "loki_count": actual,
                }
            )
    emit({"summary": {"mismatches": mismatches}})
    return 1 if mismatches else 0


# --- cli ------------------------------------------------------------------


def _add_target(p):
    """Target selection shared by latency/verify: stdin pipe or namespace scan."""
    p.add_argument(
        "--stdin",
        action="store_true",
        help="read pod records (JSON object or array) from stdin",
    )
    p.add_argument(
        "--namespace-prefix",
        default="logger-ns",
        help="scan pods whose namespace starts with this",
    )


def build_parser():
    p = argparse.ArgumentParser(prog="k8s-loki-logbench.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    lat = sub.add_parser("latency", help="measure pod-start -> queryable latency")
    lat.add_argument("--mode", choices=["range", "tail"], default="range")
    lat.add_argument("--timeout", type=float, default=60.0)
    lat.add_argument("--poll-interval", type=float, default=2.0)
    _add_target(lat)
    lat.set_defaults(func=cmd_latency)

    ver = sub.add_parser("verify", help="compare annotated line count against Loki")
    _add_target(ver)
    ver.set_defaults(func=cmd_verify)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
