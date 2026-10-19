# CLA-grafana — Grafana + TimescaleDB metrics stack (Docker branch)

Ansible playbook, který na `ubuntu-docker` (192.168.122.115, Ubuntu 24.04)
nasadí přes Docker (`community.docker` moduly, žádné Quadlet/systemd-user
jednotky):

- **TimescaleDB** — úložiště metrik (hypertable + continuous aggregates)
- **Grafana** — vizualizace, s předkonfigurovaným read-only datasourcem
- **receiver** — malá služba, která **přijímá** Prometheus `remote_write` a
  zapisuje vzorky do TimescaleDB (pasivní, push)
- **importer** — služba, která si data **sama stahuje** z Prometheus/InfluxDB
  endpointů podle rozvrhu definovaného pro každý zdroj zvlášť (aktivní, pull);
  zapisuje do stejné `metrics_raw` jako receiver

Toto je Docker varianta stejného stacku, který na branchi `master` běží na
`fedora-podman` (192.168.122.151) přes rootless Podman + Quadlet. Architektura
a SQL schéma jsou identické — liší se jen container runtime a to, co z toho
plyne (viz níže). Návrh agregace/retenčních vrstev:
[`docs/AGGREGATION-DESIGN.md`](docs/AGGREGATION-DESIGN.md), návrh importeru:
[`docs/IMPORTER-DESIGN.md`](docs/IMPORTER-DESIGN.md).

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
- **Spojení**: playbook se stejně jako u fedora-podman spouští lokálně na
  cíli (`ansible_connection: local` v `inventory/hosts.yml`) — spouští se
  tedy přímo na `ubuntu-docker`, ne z control node přes SSH.

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
- Importer stav (jen z hostitele, port není na LAN):
  `curl -s http://127.0.0.1:9202/status | python3 -m json.tool`

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

## Importer — přidání zdroje

Importer si data sám stahuje (na rozdíl od receiveru, který na ně čeká).
Zdroje se definují v `importer_sources` v `group_vars/all/vars.yml`; v tom
souboru jsou zakomentované funkční příklady pro všechny tři podporované typy.
Detaily návrhu: [`docs/IMPORTER-DESIGN.md`](docs/IMPORTER-DESIGN.md).

Podporované `type`: `prometheus` (PromQL `query_range`), `influxdb1`
(InfluxQL), `influxdb2` (Flux), `postgres` (SQL přes libpq). Přidání další
technologie = jeden nový soubor v `roles/importer/files/app/sources/` —
postup v IMPORTER-DESIGN.md.

**1. Hesla/tokeny nepatří do `vars.yml`** (není šifrovaný). U zdroje se uvede
jen `credentials_id`, a vlastní tajemství se přidá do vaultu:

```bash
ansible-vault edit group_vars/all/vault.yml
```

```yaml
# heslo pro DB roli importeru (povinné, jednou za projekt)
vault_timescaledb_importer_password: "<vygeneruj: openssl rand -base64 24>"

# tajemství jednotlivých zdrojů: IMPORTER_CRED_<CREDENTIALS_ID>_<POLE>,
# credentials_id velkými písmeny, nealfanumerické znaky na "_".
# Podle auth.type se čte:  bearer/token -> _TOKEN
#                          basic        -> _PASSWORD (a volitelně _USERNAME)
#                          header       -> _VALUE
vault_importer_env:
  IMPORTER_CRED_PROM_MAIN_TOKEN: "<bearer token toho Promethea>"
  IMPORTER_CRED_INFLUX_LEGACY_PASSWORD: "<heslo>"
  IMPORTER_CRED_INFLUX_V2_TOKEN: "<influx API token>"
```

Do vygenerovaného `sources.yml` na hostiteli se tajemství **nezapisují** —
je tam jen odkaz `${IMPORTER_CRED_...}` a hodnoty se předávají kontejneru
jako env proměnné. (Ten soubor musí být world-readable, protože je
bind-mountnutý a proces v kontejneru běží pod vlastním uid.)

Zdroj s `credentials_id`, ke kterému ve vaultu nic není, playbook odmítne
hned na začátku role a řekne které to je — ne až za hodinu jako 401 od
endpointu.

**2. Rozvrh** se nastavuje u každého zdroje zvlášť, jednou z dvou variant:

```yaml
schedule: {every: "1h"}                              # 1x za hodinu, v :00
schedule: {every: "24h"}                             # 1x za den
schedule: {cron: "0 3 * * *", timezone: "Europe/Prague"}   # denně v 03:00 SEČ
```

`every` je ukotvené na mřížku v epoše (hodinový zdroj tiká v :00, ne v minutě,
kdy se náhodou nastartoval kontejner). `cron` je pro "v konkrétní čas".
Nastavit obojí je chyba, kterou playbook nahlásí.

**3. Nasazení a kontrola:**

```bash
ansible-playbook playbook.yml

# co importer dělá, kde má watermarky, co selhalo
curl -s http://127.0.0.1:9202/status | python3 -m json.tool
docker logs -f importer

# watermarky přímo v DB (last_ts = exkluzivní horní hranice importovaného)
docker exec -i timescaledb psql -U postgres -d metrics \
  -c "SELECT * FROM importer_state ORDER BY source_name, stream_key;"

# kolik čeho z kterého zdroje přiteklo
docker exec -i timescaledb psql -U postgres -d metrics -c \
  "SELECT labels->>'source' AS source, metric_name, count(*), max(time)
     FROM metrics_raw WHERE labels ? 'source'
    GROUP BY 1,2 ORDER BY 1,2;"
```

Prázdný `importer_sources` je v pořádku: kontejner běží, je healthy a jen
zaloguje varování — nasazení se tím nerozbije.

**Na co si dát pozor:**

- `queries[].name` je **klíč watermarku**. Přejmenování = stream se začne
  načítat znovu od `max_lookback`. Ber je jako stabilní id.
- U PromQL výrazů (`rate()`, `sum()`, …) chybí `__name__` — nastav
  `metric_name`, jinak se vzorky uloží pod jménem dotazu.
- U Influxu musí `align` odpovídat `GROUP BY time()` /
  `aggregateWindow(every:)`, jinak se na okrajích oken ukládají neúplné
  buckety.
- Influx dotaz musí použít `$timeFilter` (v1) resp. `range(start: $__start,
  stop: $__stop)` (v2). Pevný rozsah typu `range(start: -1h)` by při každém
  běhu naimportoval totéž znovu — playbook/importer to odmítne při startu.
- SQL dotaz (postgres) musí použít `$__start`/`$__stop` — obojí je
  povinné. Odesílají se jako parametry dotazu (ne v textu SQL), takže
  procento v SQL (např. `LIKE 'x%'`) se píše jako `%%`. Sloupec s časem
  má být `timestamptz`; obyčejný `timestamp` se bere jako UTC.
- **U Fluxu vždy `timeSrc: "_start"` v `aggregateWindow()`.** Flux defaultně
  označí bucket jeho koncem, InfluxQL a `time_bucket()` jeho začátkem — bez
  toho jsou data z InfluxDB 2.x o jeden interval posunutá proti všem ostatním
  v téže tabulce. Importer to nedokáže poznat.
- Výpadek delší než `max_lookback` (7 dní) se nedohání; importer to zaloguje
  jako `WARNING` s konkrétním přeskočeným rozsahem.

## Unit testy importeru

Běží offline — bez TimescaleDB, bez Promethea, bez Influxu, bez Postgresu
(HTTP odpovědi se přehrávají z fixtures, DB je stubnutá). Ověřují
půlotevřenost a spojitost oken, chování watermarku, zarovnání na mřížku a
parsování odpovědí všech čtyř typů zdroje:

```bash
python3 tests/importer_unit_test.py
```

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
