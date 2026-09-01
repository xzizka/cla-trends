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

## Decision Log
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
