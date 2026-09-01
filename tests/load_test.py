#!/usr/bin/env python3
"""Load test for the metrics remote_write receiver.

Finds the max sustained samples/sec the receiver+TimescaleDB can absorb
without dropping samples or crashing, verified against actual DB row counts
(not just HTTP status codes) — a request timing out on the client is not the
same thing as the server actually losing the data, and this test tells them
apart.

Methodology: for each concurrency level, N worker threads hammer the
receiver back-to-back (no throttling) for --duration seconds, each sending
batches of SERIES_PER_BATCH x SAMPLES_PER_SERIES samples tagged with a
unique run_id label. After each level:
  - compare sum(samples acknowledged with 204) vs. actual COUNT(*) in
    metrics_raw for this run_id -> a positive "dropped" count means the
    server genuinely lost data.
  - compare container restart counts before/after -> any change means a
    crash happened.
The ramp stops at the first level that isn't clean (HTTP failures, restart,
or genuine drop) and reports the best clean level's throughput.

Requires: SSH access to the deployment host (for exact-count verification
and crash detection via `podman exec`/`podman inspect`), and the receiver's
bearer token (see group_vars/all/vault.yml -> vault_receiver_bearer_token).

Usage:
    ansible-vault view group_vars/all/vault.yml | grep bearer_token
    python3 tests/load_test.py --token <RECEIVER_BEARER_TOKEN>

    # narrower/wider ramp, longer duration, keep data for inspection:
    python3 tests/load_test.py --token ... --levels 1,2,4,8 --duration 15 --no-cleanup
"""
import argparse
import shlex
import statistics
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

# reuse the receiver's manual protobuf wire-format decoder's sibling
# encoder logic isn't importable directly (receiver.py only decodes), so we
# just need cramjam from the same app dir for raw snappy compression.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "roles/receiver/files/app"))
import cramjam  # noqa: E402


# --- minimal protobuf WriteRequest encoder (mirrors receiver.py's decoder) ---

def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(field_no, wire_type):
    return _varint((field_no << 3) | wire_type)


def _len_delim(field_no, payload):
    return _tag(field_no, 2) + _varint(len(payload)) + payload


def _label(name, value):
    return _len_delim(1, name.encode()) + _len_delim(2, value.encode())


def _sample(value, ts_ms):
    return (
        _tag(1, 1)
        + struct.pack("<Q", struct.unpack("<Q", struct.pack("<d", value))[0])
        + _tag(2, 0)
        + _varint(ts_ms)
    )


def _timeseries(labels, samples):
    buf = b""
    for name, value in labels.items():
        buf += _len_delim(1, _label(name, value))
    for value, ts_ms in samples:
        buf += _len_delim(2, _sample(value, ts_ms))
    return buf


def _write_request(series_list):
    buf = b""
    for labels, samples in series_list:
        buf += _len_delim(1, _timeseries(labels, samples))
    return buf


METRIC_NAME = "perf_test_ingest"
SERIES_PER_BATCH = 4
SAMPLES_PER_SERIES = 500
SAMPLES_PER_BATCH = SERIES_PER_BATCH * SAMPLES_PER_SERIES  # 2000, matches Prometheus's default max_samples_per_send


def build_batch(run_id, worker_id):
    now_ms = int(time.time() * 1000)
    series = []
    for s in range(SERIES_PER_BATCH):
        pts = [(float(i), now_ms + i) for i in range(SAMPLES_PER_SERIES)]
        labels = {
            "__name__": METRIC_NAME,
            "run_id": run_id,
            "worker": str(worker_id),
            "series": str(s),
        }
        series.append((labels, pts))
    wr = _write_request(series)
    return bytes(cramjam.snappy.compress_raw(wr))


def send_batch(url, token, payload, timeout):
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
            "Authorization": f"Bearer {token}",
        },
    )
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, time.monotonic() - t0
    except urllib.error.HTTPError as e:
        return e.code, time.monotonic() - t0
    except Exception as e:
        return f"EXC:{e}", time.monotonic() - t0


class WorkerStats:
    def __init__(self):
        self.ok = 0
        self.fail = 0
        self.fail_reasons = []
        self.latencies = []


def worker_loop(url, token, run_id, worker_id, stop_at, req_timeout, stats: WorkerStats):
    payload = build_batch(run_id, worker_id)  # content doesn't need to vary across sends within a level
    while time.monotonic() < stop_at:
        status, latency = send_batch(url, token, payload, req_timeout)
        stats.latencies.append(latency)
        if status == 204:
            stats.ok += 1
        else:
            stats.fail += 1
            stats.fail_reasons.append(str(status))


def ssh_run(ssh_target, remote_cmd, timeout=30):
    return subprocess.run(["ssh", ssh_target, remote_cmd], capture_output=True, text=True, timeout=timeout)


def db_count(ssh_target, pg_container, run_id):
    sql = f"SELECT count(*) FROM metrics_raw WHERE metric_name='{METRIC_NAME}' AND labels->>'run_id' = '{run_id}';"
    remote_cmd = f"podman exec -i {pg_container} psql -U postgres -d metrics -tA -c " + shlex.quote(sql)
    out = ssh_run(ssh_target, remote_cmd)
    if out.returncode != 0 or not out.stdout.strip():
        print("db_count error:", out.returncode, out.stdout, out.stderr, file=sys.stderr)
        raise RuntimeError("db_count failed")
    return int(out.stdout.strip())


def cleanup(ssh_target, pg_container, run_id):
    sql = f"DELETE FROM metrics_raw WHERE metric_name='{METRIC_NAME}' AND labels->>'run_id' = '{run_id}';"
    remote_cmd = f"podman exec -i {pg_container} psql -U postgres -d metrics -c " + shlex.quote(sql)
    ssh_run(ssh_target, remote_cmd)


def container_restart_counts(ssh_target, container_names):
    out = {}
    for name in container_names:
        remote_cmd = f"podman inspect {name} --format '{{{{.RestartCount}}}}'"
        r = ssh_run(ssh_target, remote_cmd, timeout=15)
        out[name] = r.stdout.strip()
    return out


def run_level(url, token, run_id, concurrency, duration_s, req_timeout):
    stop_at = time.monotonic() + duration_s
    stats_list = [WorkerStats() for _ in range(concurrency)]
    threads = [
        threading.Thread(target=worker_loop, args=(url, token, run_id, i, stop_at, req_timeout, stats_list[i]))
        for i in range(concurrency)
    ]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    total_ok = sum(s.ok for s in stats_list)
    total_fail = sum(s.fail for s in stats_list)
    all_latencies = [l for s in stats_list for l in s.latencies]
    fail_reasons = [r for s in stats_list for r in s.fail_reasons]

    samples_sent_ok = total_ok * SAMPLES_PER_BATCH
    samples_per_sec = samples_sent_ok / elapsed if elapsed > 0 else 0

    return {
        "concurrency": concurrency,
        "elapsed": elapsed,
        "requests_ok": total_ok,
        "requests_fail": total_fail,
        "samples_sent_ok": samples_sent_ok,
        "samples_per_sec": samples_per_sec,
        "fail_reasons": fail_reasons[:5],
        "p50_latency_ms": statistics.median(all_latencies) * 1000 if all_latencies else None,
        "p99_latency_ms": (sorted(all_latencies)[int(len(all_latencies) * 0.99)] * 1000
                           if len(all_latencies) >= 5 else None),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--token", required=True, help="RECEIVER_BEARER_TOKEN (see group_vars/all/vault.yml)")
    ap.add_argument("--host", default="192.168.122.151", help="receiver host")
    ap.add_argument("--port", type=int, default=9201, help="receiver port")
    ap.add_argument("--ssh-target", default="xzizka@192.168.122.151", help="user@host for DB verification / crash detection over SSH")
    ap.add_argument("--pg-container", default="timescaledb", help="podman container name for TimescaleDB")
    ap.add_argument("--receiver-container", default="receiver", help="podman container name for the receiver")
    ap.add_argument("--levels", default="1,2,4,8,16,32", help="comma-separated concurrency levels to ramp through")
    ap.add_argument("--duration", type=float, default=10, help="seconds to hammer at each level")
    ap.add_argument("--request-timeout", type=float, default=15, help="client-side HTTP timeout per batch (seconds)")
    ap.add_argument("--no-cleanup", action="store_true", help="keep test rows in metrics_raw for manual inspection")
    args = ap.parse_args()

    url = f"http://{args.host}:{args.port}/api/v1/write"
    run_id = uuid.uuid4().hex[:12]
    levels = [int(x) for x in args.levels.split(",")]
    containers = (args.receiver_container, args.pg_container)

    print(f"url={url}  run_id={run_id}  batch={SAMPLES_PER_BATCH} samples ({SERIES_PER_BATCH}x{SAMPLES_PER_SERIES})  duration={args.duration}s/level")

    baseline_restarts = container_restart_counts(args.ssh_target, containers)
    print("baseline restart counts:", baseline_restarts)

    cumulative_expected = 0
    results = []
    for c in levels:
        r = run_level(url, args.token, run_id, c, args.duration, args.request_timeout)
        cumulative_expected += r["samples_sent_ok"]
        time.sleep(1.5)  # let in-flight inserts settle before counting
        actual = db_count(args.ssh_target, args.pg_container, run_id)
        restarts = container_restart_counts(args.ssh_target, containers)
        crashed = restarts != baseline_restarts
        dropped = cumulative_expected - actual  # negative = some acked-late requests still landed, not real loss

        print(
            f"conc={c:>3}  ok={r['requests_ok']:>4} fail={r['requests_fail']:>3}  "
            f"sent={r['samples_sent_ok']:>7}  {r['samples_per_sec']:>8.0f} samples/s  "
            f"p50={r['p50_latency_ms'] or 0:>6.0f}ms p99={r['p99_latency_ms'] or 0:>6.0f}ms  "
            f"db_total={actual:>7} expected={cumulative_expected:>7} dropped={dropped:>5}  "
            f"restarts={restarts}{'  <<< CRASH DETECTED' if crashed else ''}"
        )
        if r["fail_reasons"]:
            print(f"         fail samples: {r['fail_reasons']}")

        results.append({**r, "db_total": actual, "expected": cumulative_expected, "dropped": dropped, "crashed": crashed})

        # dropped > 0 is real loss; dropped < 0 just means some slow-but-successful
        # requests hadn't been counted as "ok" yet when we measured — not a failure.
        if crashed or dropped > 0 or r["requests_fail"] > 0:
            print(f"\n>>> stopping ramp: level {c} was not clean (crash={crashed}, dropped={dropped}, http_fail={r['requests_fail']})")
            break

    print("\n=== summary ===")
    clean_levels = [x for x in results if not x["crashed"] and x["dropped"] <= 0 and x["requests_fail"] == 0]
    if clean_levels:
        best = max(clean_levels, key=lambda x: x["samples_per_sec"])
        print(f"max clean sustained throughput: {best['samples_per_sec']:.0f} samples/sec at concurrency={best['concurrency']}")
    else:
        print("no clean level found (failed even at the lowest concurrency)")

    if args.no_cleanup:
        print(f"\ntest rows kept (metric_name='{METRIC_NAME}', run_id='{run_id}') — clean up manually when done.")
    else:
        cleanup(args.ssh_target, args.pg_container, run_id)
        print("\ntest rows cleaned up.")


if __name__ == "__main__":
    main()
