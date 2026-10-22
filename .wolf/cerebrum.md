# Cerebrum — CLA-grafana

## User Preferences
- User explicitly invited clarifying questions on ambiguous infra decisions
  ("Pokud budeš potřebovat, tak se doptej") — asked about ingestion mechanism,
  credential handling, and Grafana network exposure before building; this was
  the right call for a multi-component infra deployment with real security/
  cost tradeoffs.
- Dev/test loop must run ON `fedora-podman`, not the local machine — local
  Ansible (2.13.13 / ansible-lint 4.2.0, tied to system Python 3.8) is too old
  and gives misleading/incomplete lint results. Always sync + lint/run on the
  target host.
- User wants all containers started as systemd Quadlet units, not `podman run`
  / docker-compose / plain `podman_container` ansible module with generated
  systemd — confirmed this mid-build, matches what was already built.

- 2026-09-09: When asked to design the importer the user again invited
  questions and then answered all four decisively (Influx: "don't know yet,
  do both 1.8 + 2.x"; Prometheus: PromQL range queries; state: new
  `importer_state` table; scope: both backends working now, not a skeleton).
  Pattern to reuse: for a new component, ask about (a) which external API
  flavour/version, (b) data-fidelity mode, (c) where bookkeeping state lives,
  (d) scope now vs. later — those are the four that actually change the
  deliverable. The user prefers the fuller option when offered (both Influx
  versions, both backends done) rather than a staged skeleton.

## Key Learnings
- TimescaleDB Docker tags with an `-oss` suffix are Apache-2.0-only builds.
  Continuous aggregates, `add_retention_policy`, and `add_compression_policy`
  are all Timescale License (TSL) features and are **not present** in `-oss`
  images — only core hypertable partitioning + manual `drop_chunks()` are
  Apache. TSL itself is still free for self-hosted use (not a paid tier),
  just not OSI-approved open source. Check this before ever picking an `-oss`
  tag for a design that uses continuous aggregates or policies.
- `group_vars/<name>.yml` only auto-loads if `<name>` matches an actual
  inventory group. A file named `group_vars/vault.yml` is silently never
  loaded (no group called "vault"). Correct pattern: `group_vars/all/` as a
  directory containing both `vars.yml` and `vault.yml` — every file inside is
  loaded for group "all".
- Ansible handler names are global to the whole play, not scoped per role.
  Two roles each defining a handler called "Reload systemd user" means
  notifying it from one role's tasks fires *both* roles' handlers, including
  ones for services that don't exist yet → cryptic failures. Prefix handler
  names per role.
- `ansible_user_uid` is a gathered fact — unusable in a play-level
  `environment:` block, because that block is evaluated for the "Gathering
  Facts" task itself, before the fact exists. Use
  `{{ lookup('ansible.builtin.pipe', 'id -u') }}` instead when the UID is
  needed before/during fact gathering.
- Fedora 44 has no `podman-plugins` package (netavark/aardvark-dns cover
  rootless container networking already, pulled in as podman deps).
- Rootless Podman Quadlet + systemd --user over Ansible: `loginctl
  enable-linger <user>` is required for the user manager to exist reliably
  independent of the SSH session, and `[privilege_escalation] become = True`
  set globally in ansible.cfg is a trap — it silently makes the `systemd
  scope: user` tasks run as root via sudo, which cannot reach another user's
  D-Bus session bus ("Operation not permitted... consider --machine=<user>@
  .host --user"). become must stay per-task (only dnf/firewalld/loginctl need
  root), never a global default, in any rootless-podman-via-Ansible project.
- First-time `podman pull` on this specific VM (fedora-podman) is very slow
  (Docker Hub blob pulls took >10+ min for the ~600MB timescaledb image) —
  default Quadlet `TimeoutStartSec` (~90s) is nowhere near enough for a first
  deploy. Always pre-pull images via `containers.podman.podman_image` with
  `async: 1800, poll: 15` as an explicit task before deploying the Quadlet
  unit, and set a generous `TimeoutStartSec` (1800s) in the unit itself as a
  second line of defense.

- The receiver image's Docker Hub vulnerability count (18 HIGH/CRITICAL as of
  2026-09-03, trivy) is a Debian-OS-package floor, not a fixable app issue:
  perl-base, ncurses, libsqlite3, gzip, libacl1, libsystemd0/libudev1 CVEs
  with no upstream Debian fix yet (`apt list --upgradable` is empty even on a
  freshly-pulled `python:3.14-slim`), and none of those binaries (perl, gzip)
  are even invoked by receiver.py — they're just part of the base Debian
  install. Bumping the base image tag or pip deps cannot move this number;
  only a non-Debian base (e.g. alpine/musl) would, and that's blocked by
  `psycopg-binary` having no musllinux wheels (would need a from-source
  compile with build deps, or dropping `[binary]`). Don't re-attempt a plain
  version-bump to chase this count down further — it won't move — without
  first confirming with the user whether a base-distro change is in scope.
- Tried `python:3.13-slim-bullseye` (Debian 11) hoping an older/more-settled
  Debian release would carry fewer CVEs than trixie — opposite happened: 63
  HIGH/CRITICAL (trivy) vs. 18 on `python:3.14-slim` (Debian 13 trixie,
  currently pushed). Debian 11 exited standard LTS support in Aug 2024, so
  its regular archive has stopped receiving security patches entirely —
  deficit spread across openssl (6), libgnutls30 (7), perl-base (8), Kerberos
  libs, PAM, etc. For this image, newer Debian codename = fewer CVEs, not
  more; don't try older codenames (bullseye, buster, ...) again for this
  reason.

- `metrics_raw` has **no unique constraint** and that is deliberate
  (append-only hypertable fed at high rate by the receiver). Consequence for
  any *new* write path: the database will not catch a duplicate insert and
  nothing downstream cleans up, so dedup is the writer's job. The importer
  solves it with an exclusive watermark + half-open `[start, end)` windows +
  rows-and-watermark-in-one-transaction. Any future ingestion path has to do
  something equivalent; don't assume "insert twice is harmless".
- Prometheus `query_range` refuses more than 11000 points per series, and it
  aligns samples to `start` (not to a global grid). So a range-query importer
  must (a) bound windows by *points* (`step * max_points`), not by wall time,
  and (b) align window boundaries to an epoch-anchored grid, otherwise the
  same series gets timestamps on a different grid every run.
- InfluxDB (both 1.x `GROUP BY time()` and 2.x `aggregateWindow`) returns
  buckets that only *partially* overlap the requested range, aggregated over
  just the overlap. Importing an unaligned window therefore stores a
  "1-minute average" computed from a few seconds at every window edge. The
  fix is to align window boundaries to the bucket interval — hence the
  `align:` option, which must match the query's own interval.
- InfluxDB 2.x reports query errors as **HTTP 200** with an error table in the
  annotated CSV (`,error,reference` header), so a 2xx status is not proof of
  success — the CSV has to be inspected. Its annotated-CSV format needs no
  client library: stdlib `csv` plus dropping the leading annotation column is
  enough, and `#datatype`/`#group`/`#default` annotations can be ignored
  entirely when columns are looked up by name.
- Debian `*-slim` images (incl. `python:3.14-slim`) ship **no**
  `/usr/share/zoneinfo`, so `zoneinfo.ZoneInfo("Europe/Prague")` fails inside
  the container even though the code is fine. Add the `tzdata` PyPI wheel to
  requirements whenever a containerised app takes a timezone name from config.
- Jinja `{% set %}` is evaluated **eagerly**, not lazily. A
  `{% set x = something_that_needs_var %}` before the `{% if var %}` that
  guards it will still blow up on the None/undefined case. Either move the
  `set` inside the branch or make it None-safe (`| default('', true)`).
- Ansible's `to_nice_yaml` + `dict2items | rejectattr | items2dict` is a clean
  way to emit a config file from a list-of-dicts var while stripping
  Ansible-only keys — much less error-prone than hand-writing the YAML
  emission in Jinja. Pipe through `| trim` before `| indent(n, true)` or you
  get a trailing whitespace-only line per block.
- Passing a DB password via `PGPASSWORD` instead of inlining it into a
  `postgresql://user:pw@host/db` DSN sidesteps URL-encoding entirely (libpq
  reads it when the conninfo has no password). Jinja's `urlencode` is *not* a
  safe substitute — its default `safe='/'` leaves slashes unescaped.

- **psycopg + jsonb: never hand a pre-serialised `str` to a column typed
  `jsonb`.** With `cp.set_types([... "jsonb" ...])` (COPY) or a `jsonb`
  parameter, psycopg's jsonb dumper JSON-encodes whatever it is given — so an
  already-`json.dumps()`'d string gets encoded twice and the column holds a
  JSON *string*, not an object. `jsonb_typeof()` returns 'string',
  `labels ? 'key'` matches nothing, and NOTHING errors: rows land with
  correct timestamps and values. Always wrap the dict:
  `psycopg.types.json.Json(d)`. Cost a whole demo run to diagnose (bug-021).
  Corollary for any future integration test: assert the *shape* of a jsonb
  column first, so this failure names itself instead of appearing as
  "no rows anywhere".
- **Flux `aggregateWindow()` stamps buckets with their END time**
  (`timeSrc: "_stop"` default), while InfluxQL `GROUP BY time()` and
  TimescaleDB's `time_bucket()` stamp the START. Mixing them in one table
  gives a silent one-interval skew between sources that no code can detect.
  Always pass `timeSrc: "_start"` in Flux queries feeding this stack
  (bug-022).
- A stubbed-DB unit test suite cannot catch serialisation or
  external-semantics bugs — 63 unit tests were green while every label in the
  database was malformed. For anything that crosses a wire or a driver, an
  integration run against the real image is not optional. The cheap way to
  make it assertable: seed data whose *value encodes its own timestamp*
  (`value = floor(epoch/60)`), then one SQL identity checks values,
  timestamps, bucket completeness and timezone at once.
- Library exception text bypasses your own log sanitising. `requests` builds
  its messages from the full URL, so scrubbing only the URLs the app formats
  itself leaves credentials in query strings exposed (InfluxDB 1.x takes
  `u=`/`p=` as query params). Scrub the text that actually gets logged, not
  the inputs you happen to control (bug-023).
- `refresh_continuous_aggregate()` cannot run inside a transaction block —
  needs autocommit. Relevant to any script that primes a cagg after inserting.

- **ubuntu-docker (the `docker` branch target) is a bare host.** Verified
  2026-09-09: no ansible, no pip, no `rsync`, `python3 -m venv` broken
  (no `python3-venv`/`ensurepip`), and — the one that actually blocks you —
  **`galaxy.ansible.com` is unreachable from it** (timeout), while pypi.org,
  files.pythonhosted.org, bootstrap.pypa.io and github.com all work.
  Bootstrap recipe that works, no sudo needed:
  `get-pip.py --user --break-system-packages`, then
  `pip install --user --break-system-packages ansible ansible-lint docker`.
  Install the **full `ansible`** distribution, NOT `ansible-core` — it bundles
  `community.docker`, which is the only way to get the collection without
  galaxy. Sync the repo with `tar czf - | ssh 'tar xzf -'`, not rsync.
- **"Resetting" ubuntu-docker restores a pre-deployment snapshot, it does not
  reinstall the VM.** Verified 2026-09-16: SSH host key unchanged (no
  known_hosts conflict), `~` dated 2026-09-02, and `~/.bash_history` ends at
  `sudo usermod -aG docker xzizka` + `reboot`. So after a reset expect exactly
  that baseline: Docker installed, enabled, active, `xzizka` already in the
  `docker` group, buildx + compose plugins present — and **nothing else**:
  no images, containers, volumes, custom networks, no repo in `~`, no pip /
  ansible / ansible-lint / rsync. Re-bootstrap = the recipe above, then sync
  the repo and run the playbook; you never have to re-install Docker or fix
  group membership.
  Full restore verified 2026-09-16 end to end: bootstrap ~2 min, tar sync
  seconds, `ansible-lint` 0 failures, `ansible-playbook` ok=33/changed=28/
  failed=0 in 4m29s with **no retry at all** (the CREATE-EXTENSION restart
  race did not even fire), second run changed=0. Budget ~10 minutes wall
  clock for a reset-to-verified-production cycle.
- `galaxy.ansible.com` from ubuntu-docker is **partially** reachable as of
  2026-09-16 — the homepage and the v3 collection endpoint answer 200 while
  `/api/` still times out. Do not treat "galaxy works now" as settled; the
  full-`ansible`-from-PyPI bootstrap avoids depending on it either way.
- `xzizka` on ubuntu-docker **is in the `sudo` group** — the old comments
  claiming "NO sudo" were wrong. But there is no NOPASSWD, so `sudo -n` fails
  and a playbook has no password to offer: the practical conclusion (keep the
  playbook free of `become`) is unchanged. Comments corrected in ansible.cfg
  and roles/docker_base.
- **"ansible-lint is clean" is a branch-scoped claim, not a project one.**
  The docker branch had never been linted with a modern lint (the clean note
  came from master/fedora-podman, and the workstation's lint is 4.2.0 which
  lacks the rules). First real lint on the target found a pre-existing
  var-naming violation in `docker_base` plus a line-length one in the new
  importer role. Re-lint on the target after adding any role.
- YAML **folded scalars (`>-`) do NOT fold a more-indented continuation
  line** — they keep its newline and its leading spaces. Inside a `{{ }}`
  expression that silently injects a newline. For a multi-filter Jinja
  expression in a var, keep it on ONE line (or use a literal block and mean
  it). Related: `| length` on a var that was produced by templating depends
  on whether Ansible handed back a real list or its string repr — resolve the
  var to the count instead and compare with `| int`.
- `select('match', <pattern>)` over a **dict** iterates its keys in Ansible
  Jinja, which makes "does the vault contain a key with this prefix?" a
  one-liner. Verified working on ansible-core 2.21.

- Grafana **Mimir/Thanos/Cortex/VictoriaMetrics are already importable** via
  the existing `type: prometheus` plugin — it builds `url.rstrip('/') +
  '/api/v1/query_range'` (prometheus.py:101), so Mimir just needs
  `url: http://mimir:8080/prometheus`. **But:** `HttpClient._apply_auth()` is
  an if/elif chain, so exactly ONE auth mechanism can be active. Multi-tenant
  Mimir needs `X-Scope-OrgID` *plus* an `Authorization` header — that combo is
  NOT expressible today; would need an additional `headers:` map on the source.
- Reading Mimir data **straight out of the S3 bucket is not viable here**: the
  blocks are Prometheus TSDB format (chunks/ + index + meta.json) with no
  maintained Python reader, the newest ~2h still sits in ingesters and never
  appears in the bucket, and pre-compaction blocks overlap/duplicate. The
  importer's watermark + half-open-window contract doesn't map onto blocks.
  Correct route: query Mimir's API, or point a store-gateway at the bucket and
  query that. One-off backfill: pull a block from S3, `promtool tsdb dump`.

- **Two Mimirs over one S3 bucket is viable iff exactly one runs a compactor.**
  The compactor writes compacted blocks, deletes source blocks after
  `-compactor.deletion-delay`, and owns the bucket index; two independent
  compactor rings both believe they own every tenant and corrupt each other.
  Read-only instance = `-target=query-frontend,querier,store-gateway` (NOT
  `all`, which includes the compactor), same tenant IDs and bucket prefix,
  ruler/alertmanager storage pointed elsewhere, and read-only S3 IAM
  (GetObject+ListBucket only) as the enforcement that survives a templating
  mistake. Store-gateway is designed for multiple replicas over one bucket.
  Two consequences for us: the read-only instance never has the last ~2h (still
  in ingesters), and it fails queries once the bucket index goes staler than
  `-blocks-storage.bucket-store.bucket-index.max-stale-period` — so it inherits
  the primary cluster's compactor uptime. Numeric defaults drift between
  releases; the single-compactor rule does not.

- **A Prometheus/Mimir *instant* query reports the query evaluation time, not
  the sample's own timestamp.** `result[].value[0]` is when the query ran; the
  value can be up to the staleness horizon older. Reading it as a sample time
  makes healthy data look skewed by seconds (nearly mis-diagnosed exactly that
  during the Mimir test). A *range* query is different — there the timestamps
  are the grid points, and the value is the last sample at or before each one,
  so `grid - sample_time` is a constant in `[0, step)` when everything is
  working. Use `timestamp(<selector>)` when the real sample time is what you
  want from an instant query.
- **PromQL aggregations drop every label not named in `by`.** So two streams of
  the same source legitimately have different label sets — `sum by (job) (...)`
  keeps only `job` (plus whatever the importer injects itself). Any assertion
  about labels has to be scoped per stream, not per source (bug-026).
- Mimir's Prometheus-compatible read API lives under **`/prometheus`**
  (`api.prometheus_http_prefix`), so the importer's `url` must include it:
  `http://host:8080/prometheus`. The push endpoint is `/api/v1/push` with no
  prefix.
- Mimir with `multitenancy_enabled: true` fails **two different ways**, and only
  one of them is loud: no `X-Scope-OrgID` header at all gives `401 no org id`,
  but a *wrong* tenant gives `200` with an empty result. "No data" from Mimir is
  therefore never by itself evidence that the data was not written.
- Verified 2026-09-16: `grafana/mimir:3.2.1` starts from the classic monolithic
  single-process config (filesystem blocks storage, memberlist rings,
  replication factor 1) unchanged — but set `activity_tracker.filepath` into the
  writable volume, since its default is relative to a CWD the image's
  unprivileged user cannot write.
- When running psql through `ssh ... bash -lc "..."`, SQL containing single
  quotes (`labels->>'source'`) is a quoting minefield — three levels of shell
  mangle it into `labels->>"source"` and Postgres then reports `column "source"
  does not exist`. Put the SQL in a file and pipe it, or call a script that
  holds the query; do not hand-escape it (cost two failed calls this session).

- **Prometheus staleness markers propagate through remote_write**, and that is
  why a scrape-based generator leaves no stale tail in an importer that reads
  via `query_range`. When the exporter died mid-run, the last grid point was
  the last real scrape and nothing after it — no 5-minute smear of a repeated
  value, which is what the naive expectation would be. Verified over a 4h run:
  480/480 points per series, value identity constant to 3 decimal places.
  (An instant `count()` against Mimir does drop to 0 once the series go stale,
  while `count_over_time(...[24h])` still reports all 480 — do not read the
  former as data loss.)
- The importer's watermark sits at the **end of the last window**, not at the
  last row it stored. After a source stops producing, watermark - max(time)
  keeps growing by one tick each cycle. Any check on the watermark must assert
  `watermark > newest row`, never equality.

- Importer sources now accept `headers:` (map, every HTTP source type), applied
  after `auth` in HttpClient; a header that `auth` already set (incl.
  Authorization from basic) is a startup error, not a silent override. This
  resolves the "X-Scope-OrgID + Authorization not expressible" limitation
  noted above. Values may use `${ENV}` refs like any other config value.
- The workstation's docker is a **snap**: it cannot see `/tmp` (compose
  reports `/var/lib/snapd/void/...`). Bind mounts / compose projects for
  local tests must live under `$HOME`.
- Docker Hub account `lojdr` is logged in on the workstation; images are
  published as `lojdr/cla-*` with one shared stack version tag + `latest`.
- In a compose deployment, Grafana dashboards must NOT be baked under
  `/var/lib/grafana` (a named volume keeps the first image's copy forever);
  they live in `/etc/grafana/dashboards` in the cla-grafana image.
- psql does not substitute `:'var'` inside `DO $$ ... $$`; use
  `SELECT format(...) \gexec` for parameterised CREATE/ALTER ROLE.

- 2026-10-22: compose deployment is LIVE on ubuntu-docker in `~/cla-metrics`
  (project `cla-metrics`, generated passwords in its `.env`, empty sources).
  The VM was clean beforehand (no Ansible stack) — the Ansible deployment and
  this one would collide on ports 3000/9201/5432/9202, never run both. First
  `docker compose up` there took 4m40s, almost all of it Docker Hub pulls.

## Do-Not-Repeat
- 2026-09-01: Don't leave `[privilege_escalation] become = True` as a global
  ansible.cfg default in a mixed root/rootless-user playbook — scope `become:
  true` to the specific root-requiring tasks only.
- 2026-09-01: Don't create `group_vars/<secretsfile>.yml` as a flat file for
  vault secrets unless `<secretsfile>` is a real inventory group name — use
  `group_vars/all/vault.yml` inside a `group_vars/all/` directory.
- 2026-09-01: Every file/directory bind-mounted (`Volume=...`) into a
  rootless Podman Quadlet container must be mode 0644/0755 (world-readable),
  never 0600/0750 — the containerized process runs as a different
  userns-mapped UID that is neither the host owner nor in its group. Hit
  this three separate times (TimescaleDB init SQL, TimescaleDB init
  directory, Grafana datasource file) by fixing one occurrence at a time
  instead of auditing every `mode:` on a templated/copied file destined for
  a container bind mount up front.
- 2026-09-01: If a task/role got run once with the wrong `become` scoping
  (creating root-owned files/dirs under the target user's home or
  `~/.config`), a later fix to `become` won't self-heal — must explicitly
  `chown -R` the affected paths back to the target user or the next run fails
  with "not writable".

- 2026-10-22: Don't trust an old default compose/env file to keep working on
  `restart` after editing an env_file — `docker compose restart` keeps the old
  env; `up -d <svc>` is what recreates the container with new env.

## Decision Log
- 2026-10-22: Compose deployment uses 4 custom images incl. TimescaleDB and
  Grafana (schema + provisioning baked in) rather than stock images + host
  bind mounts, so the server needs only the compose dir. Schema applied by a
  one-shot `db-init` service on every `up` (same idempotent approach as the
  Ansible role), not docker-entrypoint-initdb.d (bug-014). Passwords via
  PGPASSWORD, never inside DSN URLs. Ansible path kept working and unchanged
  apart from version bumps.
- 2026-09-01: `docker-entrypoint-initdb.d` proved too fragile to rely on as
  the *only* mechanism for TimescaleDB schema setup (hit 4 separate failure
  modes across bug-003/007/009/013 — permissions, extension ordering, a
  mid-init restart). Fixed structurally rather than patching each incident:
  made all init SQL idempotent (`IF NOT EXISTS` / `if_not_exists => true`
  everywhere) and added an explicit Ansible task that re-runs all 4 files via
  `podman exec ... psql -f` on *every* deploy, not just first volume
  creation. [[cla-grafana-timescaledb-schema]]
- 2026-09-01: Chose to write a small custom Python Prometheus remote_write
  receiver (manual protobuf wire-format decode, `cramjam` for raw snappy,
  `psycopg` for inserts) instead of Promscale (archived/unmaintained since
  2023) or an OpenTelemetry Collector exporter (no maintained Postgres/
  Timescale metrics exporter exists). Rationale: small, auditable, zero risk
  of depending on an abandoned project; documented limitation is no
  native-histogram support (classic float samples only, which covers
  standard Prometheus remote_write usage).
- 2026-09-01: Aggregation tiers implemented as TimescaleDB continuous
  aggregates: raw (7d retention, compressed after 1d) → `metrics_1h`
  (hierarchical from raw, 90d retention, compressed after 30d) →
  `metrics_4h` (hierarchical from `metrics_1h`, no retention/kept
  indefinitely). See `docs/AGGREGATION-DESIGN.md` for the "30 days vs 3
  months" ambiguity in the original request and the assumption made.
- 2026-09-01: TimescaleDB port 5432 is published to `127.0.0.1` only (host
  loopback for admin `psql`), never opened on firewalld/LAN — only Grafana
  (3000) and the receiver (9201) are LAN-reachable.
- 2026-09-09: Importer built as a **separate service/role**, not an extension
  of the receiver: different trigger model (scheduler vs. HTTP server),
  different failure modes (a dead source must not affect ingestion of pushed
  data), different DB grants. Shares only the target table.
- 2026-09-09: Watermark state in a new `importer_state` table rather than a
  JSON file on a Docker volume or `MAX(time)` from `metrics_raw`. User chose
  this from three options. Rationale: a file dies with the volume, and
  `MAX(time)` dies with the raw tier's 7-day retention (after a longer outage
  the importer would silently restart from `max_lookback`). The metric tables
  and tiers stay untouched, which was the user's stated constraint.
- 2026-09-09: Prometheus pulled via PromQL `query_range` (user's choice) and
  not `/federate`. `/federate` returns every series in one request but only
  the value at the instant of the request — an hourly import would store 1
  point/hour/series. `query_range` keeps the same fidelity as the push path,
  which is what lets both write into one table meaningfully.
- 2026-09-09: `max_lookback` default `7d` deliberately equals
  `retention_raw_days`. It is simultaneously the first-run bootstrap window
  and the cap on backfilling an outage; anything older would be dropped by
  the raw tier's retention policy anyway. This is the only place the importer
  knowingly leaves a gap, so it logs a WARNING naming the skipped range
  rather than narrowing the window silently.
- 2026-09-09: Source credentials passed as container **env vars**
  (`vault_importer_env` -> `${IMPORTER_CRED_*}` refs in the rendered config)
  instead of literal values in the rendered `sources.yml`. That file must be
  world-readable (bind-mounted, container runs as its own uid — the recurring
  constraint from bug-008/009/011), so literal tokens would sit in plaintext
  on the host. Env vars put them in the same exposure class as the existing
  POSTGRES_PASSWORD / receiver bearer token. Kept `vault_importer_env` flat
  (not nested per source) so the Ansible side is a one-line `combine`.
- 2026-09-09: Influx dependencies hand-rolled rather than adding
  `influxdb-client` — annotated CSV is ~60 lines of stdlib `csv` parsing, and
  the official client brings its own reactive machinery. Same reasoning as
  the receiver hand-decoding remote_write protobuf (2026-09-01 entry).
- 2026-09-09: Importer queries are *required* to reference the import window
  (`$timeFilter` / `$__start`), enforced at startup. A query with a fixed
  range (`range(start: -1h)`) would re-import the same data every tick and
  duplicate it into a table with no unique constraint — refusing to start is
  much cheaper than discovering fourfold data a week later.
- 2026-09-09: When asked to "create demo sources and test it", built a
  disposable real-endpoint stack (`tests/demo/`) rather than mocks or a
  deploy to the user's VM. Reasoning: the target host was empty (no repo, no
  containers) so deploying the whole production stack there would have been a
  large unrequested side effect, while mocks would have caught none of the
  three bugs actually found. Local throwaway containers gave real fidelity
  with zero blast radius. Namespaced `demo-*` on non-default ports so it
  cannot collide with anything real.
- 2026-09-09: The demo grants the `importer` DB role exactly the production
  grants (INSERT-only on metrics_raw, RW on importer_state) instead of
  running as superuser, and asserts that `SELECT` on metrics_raw is DENIED.
  A missing grant is a realistic failure mode and superuser would mask it.
- 2026-09-09: To verify the importer on a LIVE deployment without polluting
  production config, demo sources are injected with
  `-e @tests/demo/target-demo-extra-vars.yml` (extra-vars has the highest
  precedence) and the demo endpoints join the stack's own `metrics` network.
  Nothing in `group_vars` is edited, so there is nothing to revert — and
  because the importer reaches the demo endpoints by container name, the test
  also covers the container DNS/network path, not just the Python.

