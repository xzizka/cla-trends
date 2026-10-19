#!/usr/bin/env python3
"""Dummy Prometheus exporter for the Mimir import test: 20 self-verifying gauges.

Every gauge's value is `unix_seconds + index`, so a single arithmetic identity
checks a stored row end to end: `value - index` must equal the epoch second the
sample was taken at. That catches a wrong value, a shifted timestamp, a
timezone error and a mis-joined series with one expression — the same trick
tests/demo/seed.py plays with epoch-minutes, adapted to a scrape-based source.

Note what the identity is NOT: an exact match against the row's timestamp. The
importer reads through `query_range` at a 30s step, and a range query answers
each grid point with the last sample at or before it — so a row stamped at grid
point `g` carries the value of a scrape that happened up to one scrape interval
earlier. The invariant is therefore `0 <= g - (value - index) < scrape_interval`
(plus a second of jitter), which is exactly what verify_mimir.py asserts.

The 4-hour run length lives here rather than in a host-side timer: the exporter
exits on its own deadline, scrapes start failing, and no further samples reach
Mimir. Nothing external has to remember to stop it.
"""
from __future__ import annotations

import argparse
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

#: Number of distinct gauges. Also the modulus of the value identity: gauge
#: `demo_gauge_NN` reads `unix_seconds + NN`.
GAUGE_COUNT = 20
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def render():
    now = int(time.time())
    lines = []
    for i in range(GAUGE_COUNT):
        name = "demo_gauge_%02d" % i
        lines.append("# HELP %s Self-verifying demo gauge: value == unix_seconds + %d"
                     % (name, i))
        lines.append("# TYPE %s gauge" % name)
        lines.append('%s{idx="%02d"} %d' % (name, i, now + i))
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.split("?")[0] not in ("/metrics", "/"):
            self.send_error(404)
            return
        body = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # One line per scrape is enough to see the generator is alive without
        # the default noisy access log.
        pass


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=9300)
    ap.add_argument("--duration", type=float, default=4 * 3600,
                    help="seconds to serve before exiting (0 = forever)")
    args = ap.parse_args()

    started = time.time()
    print("exporter: %d gauges on :%d, value == unix_seconds + idx"
          % (GAUGE_COUNT, args.port), flush=True)
    if args.duration > 0:
        deadline = started + args.duration
        print("exporter: will exit at %s (in %.1f h)"
              % (time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(deadline)),
                 args.duration / 3600.0), flush=True)

        def stopper():
            # os._exit, not sys.exit: this runs in a non-main thread, where
            # SystemExit would only unwind this thread and leave the server
            # serving forever — the one failure mode that would silently turn
            # a 4-hour test into an indefinite one.
            time.sleep(max(0.0, deadline - time.time()))
            print("exporter: duration reached, exiting", flush=True)
            os._exit(0)

        threading.Thread(target=stopper, daemon=True).start()

    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
