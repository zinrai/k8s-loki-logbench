# k8s-loki-logbench

Measure the Grafana Loki log pipeline for Kubernetes pods: report how long
their log lines take to become queryable and whether any are missing.

A single self-contained Python script (`k8s-loki-logbench.py`, stdlib only). It
owns no Kubernetes or Loki client library — it shells out to `kubectl`
(read-only) and `logcli` and moves JSON between them. No `go.mod`, no PyPI
dependency.

It only **observes**; it creates no workloads. Generating the load is a separate
concern handled by [k8s-logload](../k8s-logload), which emits pod records this
tool consumes by pipe. The `k8s-` prefix marks that it reads from the cluster
(pod discovery, start time, annotations); the `loki` part, that it queries Loki.

## Requirements

- Python 3.11+ (`datetime.fromisoformat` with `Z`)
- `kubectl`, `logcli` on `PATH`
- A reachable Loki, with the search endpoint deployed on the cluster

Connection details are **not** configured here:

- Loki endpoint and auth are owned by `logcli` (`LOKI_ADDR`, etc.).
- kubeconfig / context is owned by `kubectl` (`KUBECONFIG`, current-context).
- The Loki tenant id is the Kubernetes namespace (`logcli --org-id <namespace>`).

## Subcommands

Each subcommand prints JSON; they compose with pipes. Run
`./k8s-loki-logbench.py <cmd> --help` for flags — that output is the source of
truth, not this file.

| command | what it does |
| --- | --- |
| `latency --mode range` | poll `logcli query` until a line lands; report start→queryable latency |
| `latency --mode tail` | measure the first line streamed over `logcli query --tail` |
| `verify` | compare each pod's `total_log_lines` annotation against Loki's count |

`latency` and `verify` take their target pods either from a namespace scan
(`--namespace-prefix`) or from a pod record on stdin (`--stdin`), so a producer
can feed a measurement directly.

## Usage

```bash
# scan an existing workload by namespace prefix:
./k8s-loki-logbench.py latency --mode range --namespace-prefix logger-ns
./k8s-loki-logbench.py verify --namespace-prefix logger-ns

# or measure exactly what a producer just created:
k8s-logload.py task-run | ./k8s-loki-logbench.py latency --mode tail --stdin
```

## Notes on the measurement

- Latency is `now - pod .status.startTime` at the moment the first line is
  observed. It is a **seconds-grained** figure for a benchmark; subprocess
  startup jitter sits below that floor and is not corrected for.
- `verify` counts via `count_over_time` scoped to each pod's lifetime
  (`[startTime, now]`) through `logcli instant-query` — so a re-run that reuses a
  pod name is not double-counted. Loki's metric-query output format is the one
  place this glue depends on a specific `logcli` version; if a count looks wrong,
  check that line first.

## License

This project is licensed under the [MIT License](./LICENSE).
