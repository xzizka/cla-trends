# CLA-grafana — Grafana + TimescaleDB metrics stack

Ansible playbook, který na `fedora-podman` (192.168.122.151) nasadí přes
rootless Podman + Quadlet (systemd user units):

- **TimescaleDB** — úložiště metrik (hypertable + continuous aggregates)
- **Grafana** — vizualizace, s předkonfigurovaným read-only datasourcem
- **receiver** — malá služba, která přijímá Prometheus `remote_write` a
  zapisuje vzorky do TimescaleDB

Návrh agregace/retenčních vrstev: [`docs/AGGREGATION-DESIGN.md`](docs/AGGREGATION-DESIGN.md).

## Nasazení

```bash
ansible-playbook playbook.yml
```

Vault heslo je v `.vault_pass` (v repu je jen `.gitignore`d, necommitne se).
Přesuň si ho do password manageru a soubor pak smaž, nebo si ho nech ležet
lokálně — bez něj `ansible-vault` neodemkne `group_vars/all/vault.yml`.

## Přístup

- Grafana: `http://192.168.122.151:3000` (LAN), uživatel `admin`, heslo viz
  `group_vars/all/vault.yml` (`ansible-vault view group_vars/all/vault.yml`).
- TimescaleDB: `psql` není nainstalovaný na hostiteli, jde se přes kontejner:
  `podman exec -it timescaledb psql -U postgres -d metrics`
- Receiver zdraví: `curl http://192.168.122.151:9201/healthz`

## Nastavení externího Prometheus

Do `prometheus.yml` na zdrojovém Prometheu přidej:

```yaml
remote_write:
  - url: http://192.168.122.151:9201/api/v1/write
    authorization:
      type: Bearer
      credentials: <RECEIVER_BEARER_TOKEN z vault.yml>
    queue_config:
      max_samples_per_send: 2000
```

Receiver podporuje jen klasický protobuf `remote_write` v1 (float samply),
ne native histograms — viz omezení v `docs/AGGREGATION-DESIGN.md`.

## Operativní příkazy (na fedora-podman jako xzizka)

```bash
# stav kontejnerů / unitů
systemctl --user status timescaledb receiver grafana
podman ps

# logy
journalctl --user -u receiver -f

# velikost dat
podman exec -i timescaledb psql -U postgres -d metrics \
  -c "SELECT hypertable_name, hypertable_size(format('%I', hypertable_name)::regclass) FROM timescaledb_information.hypertables;"
```

## Re-run po změnách

Playbook je idempotentní. Schéma/continuous aggregates/politiky/role+granty
(`roles/timescaledb/files/init/*.sql` + `04-roles-grants.sql.j2`) se
aplikují znovu při **každém** spuštění playbooku (úkol "Apply/reapply
schema..." v `roles/timescaledb/tasks/main.yml`), ne jen při prvním
vytvoření datového volume — všechny čtyři soubory jsou psané idempotentně
(`IF NOT EXISTS` / `if_not_exists => true`), takže opakované spuštění je
bezpečné. Uprav SQL, spusť `ansible-playbook playbook.yml`, hotovo.
