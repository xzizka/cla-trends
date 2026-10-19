"""Durations, schedules, and time-window arithmetic.

Kept free of any I/O so it can be unit-tested offline — see
`tests/importer_unit_test.py`. `now` is always passed in, never read from the
clock in here, for the same reason.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$", re.I)
_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}

UTC = timezone.utc


def parse_duration(spec, where="duration"):
    # type: (object, str) -> timedelta
    """Parse "30s" / "5m" / "1h" / "7d" / "2w" into a timedelta.

    Bare numbers are rejected on purpose: `step: 60` reads like 60 seconds to
    a human but is exactly the kind of ambiguity that silently produces a
    60-*something*-else window, so the unit is mandatory.
    """
    if isinstance(spec, timedelta):
        return spec
    if not isinstance(spec, str):
        raise ValueError(
            "%s: expected a duration string with a unit like '1h' or '30s', got %r"
            % (where, spec)
        )
    m = _DURATION_RE.match(spec)
    if not m:
        raise ValueError(
            "%s: cannot parse %r as a duration — use <number><unit> with unit "
            "one of ms/s/m/h/d/w (e.g. '30s', '1h', '7d')" % (where, spec)
        )
    return timedelta(seconds=float(m.group(1)) * _UNITS[m.group(2).lower()])


def format_duration(td):
    # type: (timedelta) -> str
    total = td.total_seconds()
    for unit, size in (("d", 86400.0), ("h", 3600.0), ("m", 60.0), ("s", 1.0)):
        if total >= size and total % size == 0:
            return "%d%s" % (int(total / size), unit)
    return "%gs" % total


def align_down(dt, step):
    # type: (datetime, timedelta) -> datetime
    """Floor `dt` onto the epoch-anchored `step` grid.

    Why this matters: Prometheus `query_range` places its samples at
    start, start+step, start+2*step, ... So if the window start moved with
    whenever the importer happened to tick, the same series would get
    timestamps on a different grid every run — three runs producing
    10:00:03, 10:01:47, 10:02:11 instead of a clean 10:00/10:01/10:02.
    Anchoring both bounds to the epoch grid makes the stored timestamps
    reproducible and the rollup buckets clean.
    """
    if not step:
        return dt
    step_s = step.total_seconds()
    epoch_s = (dt - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        seconds=(epoch_s // step_s) * step_s
    )


def plan_windows(last_ts, now, max_lookback, end_lag, max_span, step=None):
    # type: (Optional[datetime], datetime, timedelta, timedelta, timedelta, Optional[timedelta]) -> List[Tuple[datetime, datetime]]
    """Work out which half-open [start, end) windows still need importing.

    `last_ts` is the exclusive watermark ("everything before this is already
    in the database"), or None on first ever run for this stream.

    Rules, in order:
      * `end` is now minus `end_lag`. The lag exists because a source may
        still be writing samples for the current instant; importing right up
        to `now` and then advancing the watermark past it would permanently
        lose whatever landed a second later.
      * a first run starts `max_lookback` back, not at the epoch — otherwise
        pointing the importer at a Prometheus with years of history would
        hammer it with an unbounded backfill on the very first tick.
      * `max_lookback` also *caps* an existing watermark: after a long
        outage there is no point requesting data that the raw tier's 7-day
        retention would drop again anyway (and the source may not have it).
        This is the one case where the importer knowingly leaves a gap, and
        it is logged as such.
      * the result is chunked at `max_span` so one backfill turns into
        several bounded requests instead of a single huge one.
    """
    end = now - end_lag
    floor = end - max_lookback
    start = last_ts if last_ts is not None else floor
    if start < floor:
        start = floor

    if step:
        start = align_down(start, step)
        end = align_down(end, step)

    if start >= end:
        return []

    windows = []
    cur = start
    while cur < end:
        stop = min(cur + max_span, end)
        windows.append((cur, stop))
        cur = stop
    return windows


class Schedule(object):
    """When a source should next run. Subclasses below."""

    def next_after(self, after):
        # type: (datetime) -> datetime
        raise NotImplementedError

    def describe(self):
        # type: () -> str
        raise NotImplementedError


class IntervalSchedule(Schedule):
    """`every: 1h` — fixed interval, anchored to the epoch grid so an hourly
    source fires at :00 rather than at whatever minute the container started."""

    def __init__(self, every):
        # type: (timedelta) -> None
        if every.total_seconds() <= 0:
            raise ValueError("schedule.every must be positive, got %r" % (every,))
        self.every = every

    def next_after(self, after):
        return align_down(after, self.every) + self.every

    def describe(self):
        return "every %s" % format_duration(self.every)


class CronSchedule(Schedule):
    """`cron: "0 3 * * *"` — for "once a day at 03:00"-style requirements
    that a plain interval cannot express."""

    def __init__(self, expr, tz_name="UTC"):
        try:
            from croniter import croniter
        except ImportError:  # pragma: no cover - container always has it
            raise ValueError(
                "schedule.cron requires the croniter package; use schedule.every "
                "instead, or rebuild the importer image"
            )
        if not croniter.is_valid(expr):
            raise ValueError("schedule.cron: %r is not a valid cron expression" % expr)
        self._croniter = croniter
        self.expr = expr
        self.tz_name = tz_name
        self.tz = _resolve_tz(tz_name)

    def next_after(self, after):
        local = after.astimezone(self.tz)
        nxt = self._croniter(self.expr, local).get_next(datetime)
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=self.tz)
        return nxt.astimezone(UTC)

    def describe(self):
        return "cron %r (%s)" % (self.expr, self.tz_name)


def _resolve_tz(tz_name):
    if tz_name in (None, "", "UTC", "utc"):
        return UTC
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        raise ValueError(
            "schedule.timezone needs Python 3.9+ zoneinfo; use UTC and adjust "
            "the cron expression instead"
        )
    try:
        return ZoneInfo(tz_name)
    except Exception:
        raise ValueError("schedule.timezone: unknown timezone %r" % tz_name)


def build_schedule(cfg, where):
    # type: (dict, str) -> Schedule
    """Turn a `schedule:` config block into a Schedule.

    Exactly one of `every` / `cron` must be given — accepting both and
    picking one silently is how you end up debugging why a source runs at an
    interval you already told it not to.
    """
    if not isinstance(cfg, dict):
        raise ValueError(
            "%s: schedule must be a mapping with either `every: 1h` or "
            "`cron: \"0 3 * * *\"`" % where
        )
    has_every = cfg.get("every") not in (None, "")
    has_cron = cfg.get("cron") not in (None, "")
    if has_every and has_cron:
        raise ValueError("%s: schedule sets both `every` and `cron` — pick one" % where)
    if has_every:
        return IntervalSchedule(parse_duration(cfg["every"], where + ".schedule.every"))
    if has_cron:
        return CronSchedule(cfg["cron"], cfg.get("timezone", "UTC"))
    raise ValueError(
        "%s: schedule needs either `every: 1h` or `cron: \"0 3 * * *\"`" % where
    )
