"""Shared HTTP access for source plugins.

Centralised so every technology gets the same auth options, the same
timeouts, the same retry/backoff behaviour and — importantly — the same
guarantee that credentials never reach the log. A plugin just calls
`client.get(...)` / `client.post(...)`.
"""
from __future__ import annotations

import logging
import re
import time

import requests

from sources.base import SourceError

log = logging.getLogger("importer.http")

#: Retried with backoff. 429/503 are explicit "back off" signals; 500/502/504
#: are usually a momentarily overloaded endpoint. 4xx other than 429 are NOT
#: retried — a 401 or a malformed PromQL will fail identically next time, and
#: retrying just delays a clear error message.
_RETRY_STATUS = (429, 500, 502, 503, 504)


class HttpClient(object):
    def __init__(self, auth=None, timeout=60.0, tls_verify=True, retries=3,
                 backoff=2.0, source_name="?"):
        self.timeout = float(timeout)
        self.retries = int(retries)
        self.backoff = float(backoff)
        self.source_name = source_name
        self.session = requests.Session()
        self.session.verify = bool(tls_verify)
        self._apply_auth(auth or {})

    def _apply_auth(self, auth):
        kind = (auth.get("type") or "none").lower()
        if kind in ("none", ""):
            return
        if kind == "bearer":
            self.session.headers["Authorization"] = "Bearer %s" % _need(auth, "token")
        elif kind == "token":
            # InfluxDB 2.x uses its own scheme name rather than Bearer.
            self.session.headers["Authorization"] = "Token %s" % _need(auth, "token")
        elif kind == "basic":
            self.session.auth = (_need(auth, "username"), _need(auth, "password"))
        elif kind == "header":
            # escape hatch for anything else (X-API-Key, proprietary schemes)
            self.session.headers[_need(auth, "name")] = _need(auth, "value")
        else:
            raise ValueError(
                "auth.type %r not supported — use one of: none, bearer, token, "
                "basic, header" % kind
            )

    def get(self, url, params=None):
        return self._request("GET", url, params=params)

    def post(self, url, params=None, data=None, headers=None):
        return self._request("POST", url, params=params, data=data, headers=headers)

    def _request(self, method, url, params=None, data=None, headers=None):
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.session.request(
                    method, url, params=params, data=data, headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt > self.retries:
                    raise SourceError(
                        "%s %s failed after %d attempt(s): %s"
                        % (method, _safe(url), attempt, _scrub(exc))
                    )
                self._sleep(attempt, _scrub(exc))
                continue

            if resp.status_code in _RETRY_STATUS and attempt <= self.retries:
                self._sleep(attempt, "HTTP %d" % resp.status_code)
                continue

            if resp.status_code >= 400:
                raise SourceError(
                    "%s %s returned HTTP %d: %s"
                    % (method, _safe(url), resp.status_code,
                       _scrub(resp.text or "", limit=400))
                )
            return resp

    def _sleep(self, attempt, why):
        delay = self.backoff ** (attempt - 1)
        log.warning(
            "source %s: %s — retrying in %.1fs (attempt %d/%d)",
            self.source_name, why, delay, attempt, self.retries + 1,
        )
        time.sleep(delay)

    def close(self):
        try:
            self.session.close()
        except Exception:  # pragma: no cover
            pass


def _need(auth, key):
    val = auth.get(key)
    if not val:
        raise ValueError(
            "auth.type=%r requires a non-empty %r — set it in "
            "vault_importer_credentials, not in vars.yml"
            % (auth.get("type"), key)
        )
    return val


#: Elide the query string of anything URL- or path-shaped, and any
#: `//user:pass@` userinfo. Two reasons, one of them found the hard way while
#: running tests/demo: InfluxDB 1.x accepts `u=`/`p=` as query parameters and
#: a `url:` with embedded credentials is legal, so a leaked password in a
#: container log is a real possibility; and `requests` builds its exception
#: text out of the full URL, so scrubbing only the URLs *we* format leaves the
#: library's own messages untouched (they were dumping the entire InfluxQL
#: query into every retry warning).
_URL_QUERY_RE = re.compile(r"((?:https?://|/)[^\s\x22'()]*?)\?[^\s\x22'()]*")
_USERINFO_RE = re.compile(r"(https?://)[^/@\s\x22']*@")


def _scrub(text, limit=300):
    """Make an arbitrary string safe and readable in a log line."""
    text = _URL_QUERY_RE.sub(r"\1?<elided>", str(text))
    text = _USERINFO_RE.sub(r"\1<credentials-elided>@", text)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit] + "..."


def _safe(url):
    """Strip the query string and any embedded credentials from a URL."""
    return _scrub(url)
