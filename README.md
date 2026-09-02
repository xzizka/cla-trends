# CLA-grafana — Grafana + TimescaleDB metrics stack (Docker branch)

Ansible playbook, který na `ubuntu-docker` (192.168.122.115, Ubuntu 24.04)
nasadí přes Docker (`community.docker` moduly, žádné Quadlet/systemd-user
jednotky):

- **TimescaleDB** — úložiště metrik (hypertable + continuous aggregates)
- **Grafana** — vizualizace, s předkonfigurovaným read-only datasourcem
- **receiver** — malá služba, která přijímá Prometheus `remote_write` a
  zapisuje vzorky do TimescaleDB

Toto je Docker varianta stejného stacku, který na branchi `master` běží na
`fedora-podman` (192.168.122.151) přes rootless Podman + Quadlet. Architektura
a SQL schéma jsou identické — liší se jen container runtime a to, co z toho
plyne (viz níže). Návrh agregace/retenčních vrstev:
[`docs/AGGREGATION-DESIGN.md`](docs/AGGREGATION-DESIGN.md).

## Rozdíly oproti `master` (proč tahle branch existuje)

- **Container engine**: Docker místo rootless Podman. Kontejnery se spravují
  přes `community.docker.docker_container`/`docker_image`/`docker_network`/
  `docker_volume`, ne přes Quadlet `.container`/`.network`/`.volume` jednotky
  a `systemctl --user`.
- **xzizka nemá na tomhle stroji sudo** (na rozdíl od fedora-podman). Playbook
  proto nikde nepoužívá `become` — viz `ansible.cfg` a
  `roles/docker_base/tasks/main.yml`. Praktické důsledky:
  - Instalace balíčků (dnf/apt), firewall (firewalld/ufw) a přidání uživatele
    do skupiny `docker` musí udělat předem někdo se sudo. Playbook to
    nezkouší a needěla to za tebe.
  - `roles/docker_base` má preflight krok (`docker info` jako xzizka) — pokud
    selže, playbook skončí srozumitelnou chybou ("přidej xzizka do skupiny
    docker"), místo aby to spadlo o pár rolí dál na nesrozumitelném
    "permission denied".
  - Otevření portů `{{ grafana_port }}`/`{{ receiver_port }}` na firewallu
    (pokud je na hostu vůbec nějaký aktivní) je manuální krok pro toho, kdo
    má na stroji root.
- **Spojení**: playbook se spouští z control node přes SSH
  (`ansible_host: 192.168.122.115` v `inventory/hosts.yml`), ne lokálně na
  cíli jako u fedora-podman (to bylo obcházení příliš staré Ansible verze na
  tehdejším control node + specifikum systemd `--user` session přes SSH —
  u Dockeru odpadá obojí).

## Nasazení

```bash
ansible-playbook playbook.yml
```

Vault heslo je v `.vault_pass` (v repu je jen `.gitignore`d, necommitne se).
Přesuň si ho do password manageru a soubor pak smaž, nebo si ho nech ležet
lokálně — bez něj `ansible-vault` neodemkne `group_vars/all/vault.yml`.

**Předpoklad, který jsem needěl (a needěl bych) sám ověřovat SSH přihlášením
na čerstvý stroj**: že `xzizka` je na `ubuntu-docker` členem skupiny `docker`
a že tam je Docker daemon nastartovaný. Pokud ne, preflight v
`roles/docker_base` to nahlásí — než pak popros někoho se sudo o
`usermod -aG docker xzizka` a re-login.

## Přístup

- Grafana: `http://192.168.122.115:3000` (LAN), uživatel `admin`, heslo viz
  `group_vars/all/vault.yml` (`ansible-vault view group_vars/all/vault.yml`).
- TimescaleDB: `psql` není nainstalovaný na hostiteli, jde se přes kontejner:
  `docker exec -it timescaledb psql -U postgres -d metrics`
- Receiver zdraví: `curl http://192.168.122.115:9201/healthz`

## Nastavení externího Prometheus

Do `prometheus.yml` na zdrojovém Prometheu přidej:

```yaml
remote_write:
  - url: http://192.168.122.115:9201/api/v1/write
    authorization:
      type: Bearer
      credentials: <RECEIVER_BEARER_TOKEN z vault.yml>
    queue_config:
      max_samples_per_send: 2000
```

Receiver podporuje jen klasický protobuf `remote_write` v1 (float samply),
ne native histograms — viz omezení v `docs/AGGREGATION-DESIGN.md`.

## Operativní příkazy (na ubuntu-docker jako xzizka)

```bash
# stav kontejnerů
docker ps

# logy
docker logs -f receiver

# velikost dat
docker exec -i timescaledb psql -U postgres -d metrics \
  -c "SELECT hypertable_name, hypertable_size(format('%I', hypertable_name)::regclass) FROM timescaledb_information.hypertables;"
```

## Load test

`tests/load_test.py` ověří, kolik vzorků/s receiver+TimescaleDB zvládne
bez ztráty dat nebo pádu (ztráta se ověřuje přesným počtem řádků v DB, ne
jen HTTP status kódy). Na této branchi má defaultní `--host`/`--ssh-target`
nastavené na `192.168.122.115` a mluví na cíli `docker exec`/`docker inspect`
(ne `podman`). Historie výsledků z fedora-podman:
[`tests/results/load-test-history.md`](tests/results/load-test-history.md)
(vznikla na master, časem doplníme čísla i z tohohle stroje).

```bash
TOKEN=$(ansible-vault view group_vars/all/vault.yml | grep vault_receiver_bearer_token | sed -E 's/.*: "(.*)"/\1/')
python3 tests/load_test.py --token "$TOKEN"
```

## Re-run po změnách

Playbook je idempotentní. Schéma/continuous aggregates/politiky/role+granty
(`roles/timescaledb/files/init/*.sql` + `04-roles-grants.sql.j2`) se
aplikují znovu při **každém** spuštění playbooku (úkol "Apply/reapply
schema..." v `roles/timescaledb/tasks/main.yml`), ne jen při prvním
vytvoření datového volume — všechny čtyři soubory jsou psané idempotentně
(`IF NOT EXISTS` / `if_not_exists => true`), takže opakované spuštění je
bezpečné. Uprav SQL, spusť `ansible-playbook playbook.yml`, hotovo.
