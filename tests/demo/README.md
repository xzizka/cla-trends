# Demo zdroje pro importer

End-to-end test importeru proti **skutečným** endpointům — ne mockům.
Postaví si vlastní jednorázový stack na privátní Docker síti, nasype do
Influxů data, pustí importer, a pak **tvrdí konkrétní věci o obsahu
`metrics_raw`**.

```bash
./tests/demo/run_demo.sh          # celý průběh (~2,5 min), stack nechá běžet
./tests/demo/run_demo.sh --verify-only   # jen znovu prověřit už běžící stack
./tests/demo/run_demo.sh --clean  # všechno smazat
```

Nepotřebuje vault, Ansible, ani nasazený produkční stack. Všechno je
pojmenované `demo-*` a jede na jiných portech (15432, 19090, 18086, 28086,
19202), takže se to nemůže potkat s ničím ostrým.

## Co se postaví

| Kontejner | Co ověřuje |
|---|---|
| `demo-timescaledb` | reálný projektový image + **reálný** `01-schema.sql`/`02`/`03` — stejná hypertable, `metrics_raw` i `importer_state` |
| `demo-prometheus` | scrapuje sám sebe → skutečné živé metriky, žádný exporter navíc |
| `demo-influxdb1` | InfluxDB 1.8 se **zapnutou HTTP auth** → testuje basic auth |
| `demo-influxdb2` | InfluxDB 2.7, token auth → testuje Flux + parsování annotated CSV |
| `demo-importer` | image postavený z `roles/importer/files/app` |

Granty pro roli `importer` (`grants.sql`) jsou **identické s produkčními** —
schválně, chybějící grant je reálný způsob, jak to rozbít, a běh pod
superuserem by ho zamaskoval.

## Jak je demo samo-ověřitelné

`seed.py` zapíše do obou Influxů body tak, že **hodnota každého bodu je jeho
vlastní epoch-minuta** (`value = floor(epoch/60)`), 6 bodů na minutu. Průměr
za 1minutový bucket je pak přesně to celé číslo. Po importu tedy musí platit:

```sql
value = floor(extract(epoch from time) / 60)
```

Jedna podmínka, která odchytí špatnou hodnotu, posunutý timestamp, neúplný
bucket (jeho průměr by spadl mezi celá čísla) i chybu v timezone. Body jsou
zpětně datované (2 h), takže importer má co dohánět hned na prvním tiku a
demo nemusí čekat, až data vzniknou.

## Klíčové kontroly (19)

- **žádné duplikáty** na `(time, metric_name, labels)` — jediné, co skutečně
  dokazuje, že watermark brání překrývajícím se re-importům. Demo importer
  před kontrolou 3× restartuje, a `run_on_start: true` znamená, že každý
  restart hned importuje znovu.
- `labels` je jsonb **objekt**, ne JSON string (viz bug-021 níže)
- hodnoty přesně, timestampy na 1minutové mřížce, počet řádků = počet
  nasypaných bucketů
- `rate()` dotaz uložený pod `metric_name` overridem, ne pod jménem dotazu
- `extra_labels` + `source` na každém řádku, žádné interní sloupce
  (`_field`, `table`, `__name__`, …) prosáklé do labelů
- data protečou do `metrics_1h` rollupu
- watermark řádek pro každý stream

## Co demo odchytilo

Obojí by unit testy se stubnutou DB najít nemohly:

- **bug-021**: `labels` se ukládaly jako dvojitě zakódovaný JSON *string*
  místo jsonb objektu (`set_types(["jsonb"])` + předem `json.dumps`ovaný
  `str` → psycopg to zakóduje ještě jednou). Nic nespadlo, řádky přitekly,
  hodnoty byly správné — jen `labels ? 'source'` nenašlo nic a všechny
  dotazy nad labely tiše vracely prázdno. Opraveno na `Json(dict)` wrapper,
  stejně jako to má receiver.
- **bug-022**: Flux `aggregateWindow()` označuje bucket jeho **koncem**,
  InfluxQL a `time_bucket()` jeho **začátkem** → data z InfluxDB 2.x byla
  konzistentně o 1 minutu vedle. Není to chyba importeru, ale semantika
  Fluxu, kterou importer nemá jak detekovat. Řešení: `timeSrc: "_start"`
  v dotazu, zdokumentováno na 4 místech.

## Ruční kontroly nad běžícím stackem

```bash
curl -s http://127.0.0.1:19202/status | python3 -m json.tool
docker logs -f demo-importer
docker exec -it demo-timescaledb psql -U postgres -d metrics

# least privilege: tohle MUSÍ selhat (importer nemá SELECT na metrics_raw)
docker exec -e PGPASSWORD=demo-importer-pw -i demo-timescaledb \
  psql -U importer -d metrics -c "SELECT count(*) FROM metrics_raw"

# odolnost: shoď zdroj, počkej na tik, watermark se nesmí posunout
docker stop demo-influxdb1 && sleep 80
docker exec -i demo-timescaledb psql -U postgres -d metrics \
  -c "SELECT * FROM importer_state WHERE source_name='demo-influx1'"
docker start demo-influxdb1   # po dalším tiku dohoní bez duplikátů
```

## Ověření ŽIVÉHO nasazení (na deployment hostu)

`run_demo.sh` staví celý vlastní stack lokálně. Když chceš ověřit importer,
který už opravdu běží na cílovém hostu, jde to bez zasahování do produkční
konfigurace — demo zdroje se připojí na síť `metrics` (aby na ně produkční
importer dosáhl jménem) a zdroje se do playbooku vstříknou přes `--extra-vars`,
které má nejvyšší precedenci:

```bash
# na cílovém hostu, v ~/CLA-grafana
./tests/demo/target-demo-sources.sh up        # nastartuje + naseeduje zdroje
ansible-playbook playbook.yml -e @tests/demo/target-demo-extra-vars.yml

# ověření (potřebuje psycopg: pip install --user --break-system-packages "psycopg[binary]")
SUPER=$(ansible-vault view group_vars/all/vault.yml \
        | sed -nE 's/^vault_timescaledb_superuser_password:[[:space:]]*"?([^"]*)"?$/\1/p')
python3 tests/demo/verify.py \
  --dsn "postgresql://postgres:$SUPER@127.0.0.1:5432/metrics" --influx-buckets 120

# úklid zpátky do produkčního stavu
ansible-playbook playbook.yml                 # importer zpět na prázdné zdroje
./tests/demo/target-demo-sources.sh purge     # smaže demo kontejnery i images
docker exec -i timescaledb psql -U postgres -d metrics \
  -c "DELETE FROM metrics_raw WHERE labels ? 'source'; DELETE FROM importer_state;"
```

`group_vars` se přitom nikdy neupravuje, takže není co vracet zpátky.

### Negativní test credential preflightu

Že preflight v `roles/importer/tasks/main.yml` opravdu chytá překlepy:

```bash
sed 's/credentials_id: demo_influx1/credentials_id: demo_inlfux1/' \
  tests/demo/target-demo-extra-vars.yml > /tmp/bad.yml
ansible-playbook playbook.yml -e @/tmp/bad.yml    # musí failnout na 1. importer tasku
```

Očekávaná hláška pojmenuje zdroj i chybějící klíč:
`Source 'demo-influx1' sets credentials_id='demo_inlfux1' but
vault_importer_env contains no IMPORTER_CRED_DEMO_INLFUX1_* entry.`
