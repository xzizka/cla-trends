# CLA metrics stack — nasazení přes docker compose

Celý systém (TimescaleDB, Grafana, receiver, importer) z hotových image na
Docker Hubu (`lojdr/cla-*`) — bez Ansiblu, bez buildu na cílovém stroji.

| Služba | Image | Co dělá | Port |
|---|---|---|---|
| `timescaledb` | `lojdr/cla-timescaledb` | úložiště metrik (TimescaleDB 2.31.0, PostgreSQL 18) | `127.0.0.1:5432` |
| `db-init` | `lojdr/cla-timescaledb` | jednorázově při každém `up`: schéma, agregace, retence, role a granty (idempotentní), pak skončí | – |
| `grafana` | `lojdr/cla-grafana` | Grafana 13.2.4 s předkonfigurovaným datasourcem a dashboardy | `0.0.0.0:3000` |
| `receiver` | `lojdr/cla-receiver` | **push** — přijímá Prometheus `remote_write` | `0.0.0.0:9201` |
| `importer` | `lojdr/cla-importer` | **pull** — stahuje data ze zdrojů podle `config/sources.yml` | `127.0.0.1:9202` |

Všechny čtyři image mají společnou verzi (`STACK_VERSION`, teď `1.1.0`).

## 1. Instalace

Potřeba: Docker Engine s pluginem `docker compose` (v2.24+, kvůli
`env_file: required: false`).

Zkopíruj na server celý adresář `deploy/compose/` (stačí tyto soubory):

```
docker-compose.yml
.env.example
importer.env.example
config/sources.yml
```

```bash
cd deploy/compose            # nebo kamkoli jsi ho zkopíroval
cp .env.example .env
cp importer.env.example importer.env
chmod 600 .env importer.env

# vyplň hesla v .env, např. vygenerovaná takto:
for v in POSTGRES_PASSWORD RECEIVER_DB_PASSWORD IMPORTER_DB_PASSWORD \
         GRAFANA_RO_DB_PASSWORD GRAFANA_ADMIN_PASSWORD RECEIVER_BEARER_TOKEN; do
  sed -i "s|^$v=.*|$v=$(openssl rand -hex 24)|" .env
done

docker compose up -d
docker compose ps
```

Bez vyplněných hesel compose odmítne start a řekne, která proměnná chybí.
Hesla piš bez znaku `$` (compose ho v `.env` interpretuje) — hex z
`openssl rand -hex` je bezpečný.

> **Hesla DB rolí se mění jen přes `.env`.** `db-init` je při každém
> `docker compose up -d` nastaví znovu. Výjimka: `POSTGRES_PASSWORD` se
> použije jen při **prvním** vytvoření volume — pozdější změna v `.env` heslo
> superusera nezmění (chování oficiálního postgres image) a `db-init` se pak
> nepřihlásí. Pro změnu nejdřív
> `docker compose exec timescaledb psql -U postgres -c "ALTER USER postgres PASSWORD '<nové>'"`,
> potom totéž heslo do `.env`.

## 2. Přístup

- Grafana: `http://<server>:3000`, login `admin` / `GRAFANA_ADMIN_PASSWORD`.
  Dashboardy „Metrics trend“ a „Metrics records“ jsou připravené.
- Stav importeru (jen ze serveru):
  `curl -s http://127.0.0.1:9202/status | python3 -m json.tool`
- SQL: `docker compose exec timescaledb psql -U postgres -d metrics`
- Logy: `docker compose logs -f importer` (resp. `receiver`, `grafana`, `db-init`)

Prometheus, který má posílat data do receiveru (`prometheus.yml`):

```yaml
remote_write:
  - url: http://<server>:9201/api/v1/write
    authorization:
      type: Bearer
      credentials: <RECEIVER_BEARER_TOKEN z .env>
```

## 3. Úprava připojení ke zdrojům po nasazení

Zdroje, ze kterých si importer stahuje data, jsou v **`config/sources.yml`**
(adresář `config/` je do kontejneru namountovaný read-only). Tajemství
(tokeny, hesla) jsou v **`importer.env`**.

### Postup

1. Uprav `config/sources.yml` (přidej / změň / smaž zdroj).
2. Pokud zdroj potřebuje nové tajemství, přidej ho do `importer.env`.
3. Aplikuj:

   | Co se změnilo | Příkaz |
   |---|---|
   | jen `config/sources.yml` | `docker compose restart importer` |
   | `importer.env` (i v kombinaci) | `docker compose up -d importer` |

   `restart` nenačte změněné env proměnné — po změně `importer.env` je
   nutné `up -d`, které kontejner vytvoří znovu.
4. Zkontroluj:

   ```bash
   docker compose logs --tail 30 importer
   curl -s http://127.0.0.1:9202/status | python3 -m json.tool
   ```

Konfigurace se při startu přísně validuje. Chyba (překlep v klíči, neplatná
doba, špatné odsazení YAML, chybějící `${PROMĚNNÁ}`…) způsobí, že importer
nenaběhne a v logu bude `invalid configuration in /etc/importer/sources.yml:`
s přesným důvodem — jména zdroje, klíče nebo čísla řádku. Data se mezitím
neztrácejí: po opravě importer dožene výpadek od posledního watermarku
(nejvýš `max_lookback` zpět).

### Tajemství: `${PROMĚNNÁ}` + `importer.env`

Kdekoli v `sources.yml` lze napsat `${JMÉNO}` a hodnota se dosadí z prostředí
kontejneru. Do souboru `sources.yml` tak hesla nepíšeš:

```yaml
# config/sources.yml
auth: {type: bearer, token: "${IMPORTER_CRED_PROM_MAIN_TOKEN}"}
```

```bash
# importer.env
IMPORTER_CRED_PROM_MAIN_TOKEN=eyJhbGciOi...
```

Odkaz na nenastavenou proměnnou je chyba při startu (ne prázdný řetězec),
takže překlep v názvu se projeví hned, ne jako 401 od zdroje.

### Struktura zdroje

```yaml
sources:
  - name: prom-main            # unikátní; je součástí watermarku a labelu `source`
    type: prometheus           # prometheus | influxdb1 | influxdb2 | postgres
    url: http://prometheus.example:9090
    schedule: {every: 1h}      # nebo {cron: "0 3 * * *", timezone: Europe/Prague}
    enabled: true              # false = dočasně vypnout bez smazání
    auth: {type: bearer, token: "${IMPORTER_CRED_PROM_MAIN_TOKEN}"}
    headers:                   # volitelné, viz níže
      X-Scope-OrgID: tenant-1
    tls_verify: true           # false jen pro self-signed certifikát
    timeout_seconds: 60
    step: 60s                  # rozlišení query_range
    extra_labels: {env: prod}  # přidá se ke každému řádku
    queries:
      - name: up               # klíč watermarku — neměň ho
        query: up
      - name: cpu_rate
        query: rate(node_cpu_seconds_total[5m])
        metric_name: node_cpu_seconds_rate
        step: 30s              # přepíše `step` zdroje
```

Všechny klíče z `defaults:` (`timeout_seconds`, `max_lookback`, `end_lag`,
`tls_verify`, `retries`, `run_on_start`) lze přepsat u jednotlivého zdroje.

### Prometheus: autentizace a vlastní hlavičky

`auth` (jeden mechanismus) a `headers` (libovolné hlavičky) lze kombinovat:

| `auth.type` | Co se pošle |
|---|---|
| `none` (default) | nic |
| `bearer` | `Authorization: Bearer <token>` |
| `token` | `Authorization: Token <token>` (InfluxDB 2.x) |
| `basic` | `Authorization: Basic …` z `username` + `password` |
| `header` | jedna hlavička `name: value` |

`headers` je mapa `jméno: hodnota`, která se posílá s **každým** požadavkem
na daný zdroj. Typické použití — multi-tenant Mimir/Cortex/Thanos nebo API
gateway, které chtějí víc než jednu hlavičku:

```yaml
  - name: mimir-prod
    type: prometheus
    url: http://mimir.example:8080/prometheus   # Mimir: API je pod /prometheus
    schedule: {every: 15m}
    auth: {type: bearer, token: "${IMPORTER_CRED_MIMIR_TOKEN}"}
    headers:
      X-Scope-OrgID: tenant-1
      X-Api-Key: "${IMPORTER_CRED_MIMIR_API_KEY}"   # tajné hodnoty přes ${...}
    queries:
      - name: up
        query: up
```

Pravidla:

- Hlavičku, kterou už nastavuje `auth` (typicky `Authorization`), nelze
  v `headers` zopakovat — importer to odmítne jako konflikt. Buď `auth`, nebo
  `Authorization` v `headers`.
- Hodnota musí být řetězec na jednom řádku; jméno jen povolené znaky HTTP
  hlavičky.
- `headers` funguje u všech HTTP zdrojů (`prometheus`, `influxdb1`,
  `influxdb2`), ne u `postgres`.
- Pozor u Mimiru: **špatný** tenant v `X-Scope-OrgID` vrátí `200` s prázdným
  výsledkem (ne chybu). Prázdný import z Mimiru tedy nejdřív ověř tenantem.

### Na co si dát pozor při úpravách

- **`name` zdroje a `queries[].name` neměň.** Jsou klíčem watermarku;
  přejmenování = data toho streamu se začnou stahovat znovu od `max_lookback`.
  Změnit `url`, `auth`, `headers`, `schedule` apod. je bezpečné — importer
  pokračuje tam, kde skončil.
- Výpadek delší než `max_lookback` (default 7 dní = retence raw dat) se
  nedohání; v logu bude `WARNING` s přeskočeným rozsahem.
- Influx: `align` musí odpovídat `GROUP BY time()` / `aggregateWindow(every:)`
  a dotaz musí používat `$timeFilter` (v1) nebo
  `range(start: $__start, stop: $__stop)` (v2); u Fluxu vždy
  `timeSrc: "_start"`.
- Postgres: dotaz musí obsahovat `$__start` i `$__stop`; `%` v SQL se píše
  jako `%%`.
- Funkční příklady všech čtyř typů jsou zakomentované přímo v
  `config/sources.yml`. Podrobnosti návrhu: `docs/IMPORTER-DESIGN.md` v repu.

## 4. Provoz

```bash
# upgrade na novou verzi image: změň STACK_VERSION v .env, pak
docker compose pull && docker compose up -d

# zastavení (data zůstávají ve volumes)
docker compose down

# smazání VČETNĚ dat
docker compose down -v

# velikost dat
docker compose exec timescaledb psql -U postgres -d metrics -c \
  "SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass))
     FROM timescaledb_information.hypertables;"

# odkud přitekla jaká data
docker compose exec timescaledb psql -U postgres -d metrics -c \
  "SELECT coalesce(labels->>'source','<receiver>') AS source, count(*), max(time)
     FROM metrics_raw GROUP BY 1 ORDER BY 1;"
```

Volumes: `cla-metrics_timescaledb-data` (metriky) a `cla-metrics_grafana-data`
(uživatelé, ručně vytvořené dashboardy). Provisionované dashboardy jsou součástí
image a aktualizují se s ním.

## 5. Build a publikace image (pro vývojáře)

Z kořene repa:

```bash
docker login                       # účet s právem pushovat do lojdr/
docker/build-and-push.sh           # build + push :1.1.0 a :latest
PUSH=0 docker/build-and-push.sh    # jen build
STACK_VERSION=1.2.0 docker/build-and-push.sh
```

Verze upstream komponent jsou v `docker/build-and-push.sh`
(`TIMESCALEDB_TAG`, `GRAFANA_TAG`) a v `roles/*/files/app/Containerfile` +
`requirements.txt`.
