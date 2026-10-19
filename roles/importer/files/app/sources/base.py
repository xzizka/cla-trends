"""Source-plugin contract.

A source plugin turns one configured external endpoint into a stream of
`Row`s for a requested half-open time window. Everything else — scheduling,
watermarks, chunking, deduplication, the database write — is handled by the
runner, so a plugin only has to answer one question: "what samples exist in
[start, end) ?".

To add a new technology (see README.md):

  1. Create `sources/<tech>.py`.
  2. Subclass `Source`, implement `streams()` and `fetch()`.
  3. Decorate the class with `@register("<tech>")`.
  4. Import the module in `sources/__init__.py` so the decorator runs.

Nothing outside `sources/` needs to change.
"""
from __future__ import annotations

import abc
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, NamedTuple


class Row(NamedTuple):
    """One sample, shaped exactly like a `metrics_raw` row.

    `labels` must NOT contain `__name__` (that is `metric_name`) and must not
    contain the `source` key either — the runner injects provenance labels
    itself so a plugin cannot accidentally forge them.
    """

    ts: datetime
    metric_name: str
    labels: Dict[str, str]
    value: float


class Stream(NamedTuple):
    """One independently-watermarked unit of work inside a source.

    A source with three configured queries has three streams, each resuming
    from its own position — so adding a fourth query later backfills only
    that one instead of re-importing everything.

    key       stable identifier, stored in `importer_state.stream_key`.
              Changing it orphans the old watermark row and re-bootstraps
              from `max_lookback`, so it is derived from the query's
              configured `name`, never from the query text.
    step      sampling grid. Non-zero for step-based sources (Prometheus):
              window boundaries get aligned to it so sample timestamps land
              on a stable grid across runs instead of drifting with whenever
              the importer happened to tick. Zero means "the source decides"
              (Influx, where the GROUP BY / aggregateWindow in the query
              already fixes the resolution).
    max_span  largest window handed to `fetch()` in one call; the runner
              splits anything longer. Keeps a multi-day backfill from
              becoming a single enormous request the endpoint would reject.
    """

    key: str
    step: timedelta
    max_span: timedelta
    query: dict


class SourceError(Exception):
    """Raised by a plugin for any failure the runner should treat as
    retryable: the watermark is left untouched, so the same window is simply
    re-attempted on the next tick. Never used for config errors — those are
    raised at startup as ValueError."""


class Source(abc.ABC):
    """Base class for all source plugins."""

    #: set by @register
    type_name = "?"

    def __init__(self, cfg: dict, http):
        self.cfg = cfg
        self.name = cfg["name"]
        self.http = http

    @abc.abstractmethod
    def streams(self) -> List[Stream]:
        """Return the independently-watermarked units of work, in run order."""

    @abc.abstractmethod
    def fetch(self, stream: Stream, start: datetime, end: datetime) -> Iterator[Row]:
        """Yield every sample in the half-open window [start, end).

        Both bounds are timezone-aware UTC. Samples outside the window are
        dropped by the runner rather than trusted, so being sloppy at the
        boundary is safe — but returning a *wider* window than asked costs
        needless work.

        Raise `SourceError` for anything transient (network, 5xx, malformed
        response). Do not swallow errors and return an empty iterator: that
        would advance the watermark past data that was never imported.
        """

    @staticmethod
    def validate(cfg: dict) -> None:
        """Optional startup validation hook. Raise ValueError with an
        actionable message; the importer refuses to start rather than run a
        misconfigured source silently."""

    def __repr__(self):
        return "<%s source %r>" % (self.type_name, self.name)


def require(cfg: dict, key: str, where: str):
    """Fetch a required config key or raise an actionable ValueError."""
    if key not in cfg or cfg[key] in (None, ""):
        raise ValueError("%s: missing required key %r" % (where, key))
    return cfg[key]

