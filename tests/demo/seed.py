#!/usr/bin/env python3
"""Seed the demo InfluxDB instances with backdated, self-verifying points.

The trick that makes this demo assertable: every point's value IS its own
bucket identity — `value = floor(epoch_seconds / 60)`, the epoch-minute the
point falls in. Six points are written per minute, all carrying that same
integer, so a `GROUP BY time(1m)` / `aggregateWindow(every: 1m)` mean comes
back as exactly that integer with no floating-point slop.

Consequence: after the import, this must hold for every seeded row in
TimescaleDB —

    value == floor(extract(epoch from time) / 60)

which is a single SQL check that catches a wrong value, a shifted timestamp,
a mis-aligned bucket (a partial bucket would average two different minutes
and land between integers) and a timezone error all at once. See verify.py.

Points are backdated rather than written "now" so the importer has real
history to back-fill on its very first tick, instead of the demo having to
sit and wait for data to accumulate.
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request

#: Points every 10s -> 6 per 1-minute bucket.
POINT_INTERVAL_S = 10
BUCKET_S = 60


def build_lines(measurement, field_map, tag_sets, hours_back, now_s):
    """Generate line-protocol points on a minute-aligned grid.

    Aligning the whole range to a minute boundary matters: a range that
    started mid-minute would give the first and last buckets fewer than six
    points. Their mean would still be the same integer here, but the demo is
    also checking bucket *completeness*, so the grid is kept honest.
    """
    end_s = (int(now_s) // BUCKET_S) * BUCKET_S
    start_s = end_s - hours_back * 3600
    lines = []
    for ts in range(start_s, end_s, POINT_INTERVAL_S):
        minute_id = ts // BUCKET_S
        for tags in tag_sets:
            tag_str = ",".join("%s=%s" % (k, v) for k, v in sorted(tags.items()))
            fields = ",".join(
                "%s=%s" % (name, fn(minute_id, tags))
                for name, fn in sorted(field_map.items())
            )
            lines.append("%s,%s %s %d" % (measurement, tag_str, fields, ts * 10**9))
    return lines, start_s, end_s


def post(url, body, headers, label):
    req = urllib.request.Request(url, data=body.encode("utf-8"), headers=headers,
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            resp.read()
            return
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise SystemExit("%s: HTTP %d writing points: %s" % (label, exc.code, detail))
    except urllib.error.URLError as exc:
        raise SystemExit("%s: cannot reach %s: %s" % (label, url, exc))


def chunked(lines, size=5000):
    for i in range(0, len(lines), size):
        yield "\n".join(lines[i:i + size])


def seed_influx1(base_url, database, user, password, hours_back, now_s):
    # cpu has two fields on purpose: it proves the importer turns one
    # measurement with several fields into several metric names rather than
    # collapsing them.
    lines, start_s, end_s = build_lines(
        "cpu",
        {"load": lambda m, t: m, "temp": lambda m, t: m},
        [{"host": "srv1"}, {"host": "srv2"}],
        hours_back, now_s,
    )
    import base64
    token = base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
    url = "%s/write?db=%s&precision=ns" % (base_url.rstrip("/"), database)
    for chunk in chunked(lines):
        post(url, chunk, {"Authorization": "Basic " + token,
                          "Content-Type": "text/plain"}, "influxdb1")
    return len(lines), start_s, end_s


def seed_influx2(base_url, org, bucket, token, hours_back, now_s):
    lines, start_s, end_s = build_lines(
        "mem",
        {"used_percent": lambda m, t: m},
        [{"host": "srv1"}, {"host": "srv2"}],
        hours_back, now_s,
    )
    url = "%s/api/v2/write?org=%s&bucket=%s&precision=ns" % (
        base_url.rstrip("/"), org, bucket)
    for chunk in chunked(lines):
        post(url, chunk, {"Authorization": "Token " + token,
                          "Content-Type": "text/plain"}, "influxdb2")
    return len(lines), start_s, end_s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--influx1-url", default="http://127.0.0.1:18086")
    ap.add_argument("--influx2-url", default="http://127.0.0.1:28086")
    ap.add_argument("--hours-back", type=int, default=2)
    args = ap.parse_args()

    now_s = time.time()

    n1, s1, e1 = seed_influx1(args.influx1_url, "demo", "demo_writer",
                              "demo-writer-pw", args.hours_back, now_s)
    print("influxdb1: wrote %d points (cpu: load+temp, hosts srv1/srv2)" % n1)

    n2, s2, e2 = seed_influx2(args.influx2_url, "demo-org", "demo-bucket",
                              "demo-influx2-token-0123456789",
                              args.hours_back, now_s)
    print("influxdb2: wrote %d points (mem: used_percent, hosts srv1/srv2)" % n2)

    buckets = (e1 - s1) // BUCKET_S
    print("range: %d complete 1-minute buckets, value == epoch_minute in each"
          % buckets)
    # What verify.py should find once the importer has run:
    #   influx1: buckets * 2 hosts * 2 fields
    #   influx2: buckets * 2 hosts * 1 field
    print("expected rows once imported: influx1=%d influx2=%d"
          % (buckets * 4, buckets * 2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
