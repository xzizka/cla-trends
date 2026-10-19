# Anatomy — CLA-grafana

Ansible IaC project: deploys Grafana + TimescaleDB + two ingestion paths —
a custom Prometheus remote_write **receiver** (push) and an **importer**
(pull, polls Prometheus/InfluxDB on a schedule). On `master` these run as
rootless Podman Quadlet (systemd user) services on `fedora-podman`
(192.168.122.151, user `xzizka`); on the `docker` branch as Docker containers
on `ubuntu-docker` (192.168.122.115) — note the importer was built on the
`docker` branch and has no Quadlet equivalent on `master` yet.

**Dev workflow:** authored locally in this repo, then `rsync`'d to
`~/CLA-grafana` on fedora-podman and run FROM there (`ansible_connection:
local` in inventory) — local machine's Ansible (2.13/py3.8) is too old for
modern ansible-lint; fedora-podman has ansible-core 2.20 + ansible-lint 26.

| File | Description | ~tokens |
|---|---|---|
| `README.md` | deploy/access/operate instructions | 500 |
| `docs/AGGREGATION-DESIGN.md` | tiered retention/continuous-aggregate design rationale, disk risk notes | 900 |
| `docs/IMPORTER-DESIGN.md` | importer design: why watermark+half-open windows (metrics_raw has no unique constraint), step/align grid rationale, provenance labels, max_lookback as the one deliberate data-loss point, how to add a technology | 1100 |
| `ansible.cfg` | inventory path, vault_password_file; become is per-task NOT global (rootless podman tasks must run as xzizka, not root) | 100 |
| `inventory/hosts.yml` | single host `fedora-podman`, `ansible_connection: local` | 100 |
| `group_vars/all/vars.yml` | non-secret config: image tags, ports, retention days | 250 |
| `group_vars/all/vault.yml` | ansible-vault encrypted: grafana admin pw, timescaledb passwords, receiver bearer token | 100 |
| `.vault_pass` | plaintext vault password, gitignored, needed to decrypt vault.yml | 10 |
| `playbook.yml` | top-level play, sets `XDG_RUNTIME_DIR` via `id -u` lookup (not `ansible_user_uid` fact — unavailable during Gathering Facts) | 100 |
| `roles/podman_base/` | installs podman, enables linger, opens firewalld ports 3000+9201, deploys shared `metrics.network` + named volume Quadlet units | 400 |
| `roles/timescaledb/` | raw hypertable + `metrics_1h`/`metrics_4h` continuous aggregates + retention/compression policies (SQL in `files/init/`), roles/grants template, Quadlet unit | 700 |
| `roles/receiver/` | Python remote_write receiver (manual protobuf wire decode, no `protobuf` pkg) in `files/app/receiver.py`, Containerfile, built locally via `containers.podman.podman_image` | 900 |
| `roles/importer/` | **pull** ingestion: Python service polling Prometheus (`query_range`)/InfluxDB 1.x (InfluxQL)/InfluxDB 2.x (Flux) on a per-source schedule and writing into the same `metrics_raw`. Plugin registry in `files/app/sources/` (add a technology = one new file + one import). Watermarks in `importer_state`, half-open windows, rows+watermark in one transaction. Config rendered from `importer_sources` via `templates/sources.yml.j2`; secrets only as `${IMPORTER_CRED_*}` env refs | 1400 |
| `roles/grafana/` | Grafana Quadlet unit + provisioned read-only TimescaleDB datasource (fixed `uid: timescaledb`) + provisioned "Metrics trend" dashboard (tiered UNION query across raw/1h/4h, `$metric_name` template var) | 400 |

| `tests/importer_unit_test.py` | 63 offline unit tests for the importer (no DB/Prometheus/Influx needed — HTTP replayed from fixtures, store stubbed): window half-openness/contiguity, watermark behaviour, grid alignment, and response parsing for all three source types | 900 |
| `tests/load_test.py` | concurrency-ramp load test for the receiver; verifies zero data loss via exact DB row counts (not just HTTP codes), auto-cleans up its own test rows | 500 |
| `tests/results/load-test-history.md` | dated load-test result log, for comparing across future changes | 200 |

See `.wolf/buglog.json` for deployment gotchas hit while bringing this up
(handler name collisions across roles, global `become`, group_vars naming,
slow first image pull needing async pre-pull tasks).
