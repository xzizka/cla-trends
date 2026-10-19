"""Load, validate and materialise sources.yml.

Validation is deliberately strict and happens once at startup, before any
scheduling begins: a typo in a duration or a Flux query that ignores the
import window is the kind of mistake that otherwise shows up as
"why does this metric have four copies of every point" a week later. Better
to refuse to start with an actionable message.
"""
from __future__ import annotations

import logging
import os
import re
from typing import List

import yaml

from httpclient import HttpClient
from timespec import build_schedule, parse_duration
import sources as source_plugins

log = logging.getLogger("importer.config")

#: Label key the runner always sets to the source name. Reserved so a source
#: cannot forge or overwrite the provenance of its own data — with several
#: sources writing into one metrics_raw, being able to tell them apart (and
#: to delete one source's data) matters more than the flexibility.
SOURCE_LABEL = "source"

#: `${NAME}` references in sources.yml are substituted from the environment.
#: This is how credentials stay out of the rendered config file: the file is
#: bind-mounted into the container and therefore has to be world-readable
#: (the container process runs as its own uid, not the host owner's — see
#: roles/timescaledb for the same constraint), so a token written into it
#: literally would sit in plaintext on the host. Passing them as container env
#: vars instead puts them in the same exposure class as the POSTGRES_PASSWORD
#: and receiver token this stack already handles that way.
_ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value, where="config", env=None):
    """Recursively substitute ${VAR} references from the environment.

    An unset variable is a hard error, never an empty string: silently
    expanding a missing token to "" would turn an auth misconfiguration into
    a puzzling 401 from the source instead of a clear startup failure.
    """
    env = os.environ if env is None else env
    if isinstance(value, str):
        def sub(m):
            name = m.group(1)
            if name not in env:
                raise ValueError(
                    "%s: references ${%s} but that environment variable is "
                    "not set on the container — add it to vault_importer_env "
                    "in group_vars/all/vault.yml and re-run the playbook"
                    % (where, name)
                )
            return env[name]
        return _ENV_REF_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v, "%s.%s" % (where, k), env)
                for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v, "%s[%d]" % (where, i), env)
                for i, v in enumerate(value)]
    return value


_DEFAULTS = {
    "timeout_seconds": 60,
    # First run for a stream, and the cap on how far back an outage may be
    # backfilled. 7d matches the raw tier's retention (group_vars retention_raw_days):
    # importing older-than-retention data would only be dropped again.
    "max_lookback": "7d",
    # Do not import right up to `now`: a source may still be writing samples
    # for the current instant, and the watermark moving past them would lose
    # them permanently. Raise this if a source's own scrape/write lag is high.
    "end_lag": "0s",
    "tls_verify": True,
    "retries": 3,
    "backoff": 2.0,
    # Import once immediately at startup rather than waiting for the first
    # scheduled tick. Safe because the window is watermark-driven, and it makes
    # a deploy verifiable straight away instead of "check back in an hour".
    "run_on_start": True,
}


class Job(object):
    """One source plus everything the runner needs to drive it."""

    def __init__(self, source, schedule, streams, max_lookback, end_lag,
                 run_on_start, extra_labels):
        self.source = source
        self.name = source.name
        self.schedule = schedule
        self.streams = streams
        self.max_lookback = max_lookback
        self.end_lag = end_lag
        self.run_on_start = run_on_start
        self.extra_labels = extra_labels


def load(path):
    # type: (str) -> List[Job]
    if not os.path.exists(path):
        raise ValueError(
            "config file %s does not exist — roles/importer renders it from "
            "templates/sources.yml.j2, check the bind mount" % path
        )
    with open(path, "r") as fh:
        doc = yaml.safe_load(fh) or {}
    if not isinstance(doc, dict):
        raise ValueError("%s: top level must be a mapping" % path)
    doc = expand_env(doc, os.path.basename(path))

    defaults = dict(_DEFAULTS)
    user_defaults = doc.get("defaults") or {}
    unknown = set(user_defaults) - set(_DEFAULTS)
    if unknown:
        raise ValueError(
            "%s: unknown key(s) in `defaults`: %s — valid keys are: %s"
            % (path, ", ".join(sorted(unknown)), ", ".join(sorted(_DEFAULTS)))
        )
    defaults.update(user_defaults)

    raw_sources = doc.get("sources") or []
    if not isinstance(raw_sources, list):
        raise ValueError("%s: `sources` must be a list" % path)

    jobs = []
    seen_names = set()
    for i, cfg in enumerate(raw_sources):
        if not isinstance(cfg, dict):
            raise ValueError("%s: sources[%d] must be a mapping" % (path, i))
        name = cfg.get("name")
        if not name:
            raise ValueError("%s: sources[%d] has no `name`" % (path, i))
        if name in seen_names:
            raise ValueError(
                "%s: duplicate source name %r — names identify watermark rows "
                "and must be unique" % (path, name)
            )
        seen_names.add(name)

        if not cfg.get("enabled", True):
            log.info("source %s: disabled in config, skipping", name)
            continue

        jobs.append(_build_job(cfg, defaults, "source %r" % name))

    return jobs


def _build_job(cfg, defaults, where):
    def pick(key):
        return cfg[key] if key in cfg and cfg[key] is not None else defaults[key]

    extra_labels = cfg.get("extra_labels") or {}
    if not isinstance(extra_labels, dict):
        raise ValueError("%s: extra_labels must be a mapping" % where)
    if SOURCE_LABEL in extra_labels:
        raise ValueError(
            "%s: extra_labels may not set %r — it is reserved and always set "
            "to the source name" % (where, SOURCE_LABEL)
        )
    extra_labels = {str(k): str(v) for k, v in extra_labels.items()}

    schedule = build_schedule(cfg.get("schedule") or {}, where)

    # An HttpClient per source is what gives every HTTP technology the same
    # auth/retry/scrubbing behaviour — but a non-HTTP source (postgres) has
    # no endpoint to call, and its `auth` block is libpq credentials that
    # HttpClient would misread (and reject, e.g. basic without a password).
    # So the client is only built for plugins that speak HTTP.
    cls = source_plugins.get(cfg.get("type"), where)
    http = None
    if getattr(cls, "uses_http", True):
        http = HttpClient(
            auth=cfg.get("auth"),
            timeout=pick("timeout_seconds"),
            tls_verify=pick("tls_verify"),
            retries=pick("retries"),
            backoff=pick("backoff"),
            source_name=cfg["name"],
        )

    source = source_plugins.build(cfg, http)
    # Resolved default, not the raw cfg lookup: for HTTP sources this value
    # already reached the endpoint via HttpClient; a non-HTTP source (and
    # any plugin that wants a timeout of its own) reads it from here instead
    # of re-implementing the defaults merge.
    source.timeout_seconds = pick("timeout_seconds")
    streams = source.streams()
    if not streams:
        raise ValueError("%s: produced no streams — nothing to import" % where)

    return Job(
        source=source,
        schedule=schedule,
        streams=streams,
        max_lookback=parse_duration(pick("max_lookback"), where + ".max_lookback"),
        end_lag=parse_duration(pick("end_lag"), where + ".end_lag"),
        run_on_start=bool(pick("run_on_start")),
        extra_labels=extra_labels,
    )
