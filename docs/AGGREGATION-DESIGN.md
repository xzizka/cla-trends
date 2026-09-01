# Návrh agregace dat (TimescaleDB continuous aggregates)

## Cíl

- 0–7 dní: stejná granularita jako na zdrojovém Prometheu (raw vzorky tak, jak
  přijdou přes remote_write).
- 7 dní – 3 měsíce: granularita 1 hodina.
- > 3 měsíce: granularita 4 hodiny.

**Poznámka k zadání:** v zadání je zmíněna hranice "do 30 dní 1h" i "více než
3 měsíce 4h", ale mezi 30. dnem a 3. měsícem není definována žádná zvláštní
granularita. Návrh proto předpokládá, že 1h granularita platí souvisle od
7. dne až do 3 měsíců (tj. "30 dní" beru jako orientační bod, ne jako další
hranici). Pokud má být mezi 30 a 90 dny jiná granularita (např. 2h), stačí
upravit `retention_1h_days` v `group_vars/all.yml` a přidat čtvrtou úroveň
hierarchického continuous aggregate — dej vědět a doplním to.

## Architektura

```
Externí Prometheus
   │  remote_write (protobuf + snappy, HTTPS/HTTP)
   ▼
receiver (Podman container, :9201)   — dekóduje WriteRequest, INSERT do metrics_raw
   │
   ▼
TimescaleDB (Podman container, metrics DB)
   ├── metrics_raw   (hypertable, chunk 1 den, retention 7 dní, komprese po 1 dni)
   ├── metrics_1h    (continuous aggregate z metrics_raw, retention 90 dní)
   └── metrics_4h    (hierarchický continuous aggregate z metrics_1h, bez retention)
   ▲
   │  SELECT (read-only role grafana_ro)
Grafana (Podman container, :3000) — PostgreSQL/TimescaleDB datasource
```

Všechny tři kontejnery běží na jednom sdíleném rootless Podman network
(`metrics.network`), takže si vidí navzájem podle jména kontejneru
(`timescaledb`, `receiver`, `grafana`). Ven na LAN jsou otevřené jen porty
Grafany (3000) a receiveru (9201); port TimescaleDB (5432) je publikovaný jen
na `127.0.0.1` hostitele (pro ruční `psql` administraci) a mimo hostitele
není vůbec dostupný.

## Proč tři samostatné hypertables/caggs místo jedné tabulky s "granularity" sloupcem

TimescaleDB continuous aggregates jsou materializované pohledy nad
hypertable, které se refreshují na pozadí přes `add_continuous_aggregate_policy`
a mají vlastní retention (`add_retention_policy`) a kompresní politiku
nezávisle na zdrojové tabulce. To přesně odpovídá požadavku "po X dnech
zahoď detail, drž jen agregát" — TimescaleDB to řeší nativně, není potřeba
psát vlastní cron/rollup joby.

`metrics_4h` je **hierarchický** continuous aggregate — počítá se z
`metrics_1h`, ne znovu z raw dat. To je levnější (agreguje už agregovaná
data) a je to důvod, proč `metrics_1h` musí mít `materialized_only = true`
před vytvořením potomka (TimescaleDB to vyžaduje u zřetězených CAGGs).

## Schéma (zjednodušeně)

```sql
metrics_raw(time timestamptz, metric_name text, labels jsonb, value double precision)
metrics_1h (bucket, metric_name, labels, avg_value, min_value, max_value, last_value, sample_count)
metrics_4h (bucket, metric_name, labels, avg_value, min_value, max_value, last_value, sample_count)
```

`labels` je jsonb (všechny Prometheus labely kromě `__name__`, který má
vlastní sloupec `metric_name` kvůli rychlému filtrování/indexům). Pro
menší nasazení (jeden malý VM, řádově desítky–stovky metrik) je to
dostatečně efektivní; při výrazném růstu cardinality by dávalo smysl
normalizovat na samostatnou `series` tabulku (přístup podobný Promscale /
VictoriaMetrics) — labels jednou, sample jen s `series_id`. Zmiňuji jako
budoucí optimalizaci, ne jako dnešní požadavek.

## Retence a komprese (viz `roles/timescaledb/files/init/03-policies.sql`)

| Vrstva       | Zdroj                | Refresh          | Retention (drop_chunks) | Komprese          |
|--------------|----------------------|-------------------|--------------------------|--------------------|
| `metrics_raw`| remote_write receiver| —                 | 7 dní                    | po 1 dni (segment podle `metric_name`) |
| `metrics_1h` | `metrics_raw`         | každou hodinu     | 90 dní                   | po 30 dnech |
| `metrics_4h` | `metrics_1h`          | každé 4 hodiny    | bez limitu (drženo trvale)| zatím bez komprese |

Retence a komprese jsou nezávislé — chunk může být zkomprimovaný a přesto
dál existovat, dokud ho nesmaže retention policy. Pořadí je důležité: raw
data se komprimují po 1 dni (výrazně sníží disk), ale drop_chunks je
smaže celé až po 7 dnech — tím vznikne plynulý handoff na `metrics_1h`,
který v tu chvíli už dávno obsahuje odpovídající hodinové agregáty
(refresh policy běží průběžně s `start_offset => 3 days`, takže nikdy
nečeká na poslední chvíli před smazáním raw dat).

## Prostor na disku

Cílový stroj má jen **13 GB volného místa** na `/` (kde leží i Podman
volumes). To je nejrizikovější bod celého nasazení:

- Komprese je zapnutá agresivně (raw po 1 dni, 1h agregát po 30 dnech) právě
  kvůli tomuto omezení.
- Doporučuji sledovat `df -h` a velikost hypertables
  (`SELECT hypertable_size('metrics_raw');` apod.) hlavně první týdny po
  nasazení, kdy ještě nejsou vidět efekty retention policy na `metrics_raw`
  (naplno se projeví až po 7 dnech provozu).
- Pokud objem metrik z externího Prometheus poroste, zvažte zvětšení disku
  VM nebo přesun `timescaledb-data` volume na samostatný větší disk.

## Reapplikace schématu

`docker-entrypoint-initdb.d` skripty (v `roles/timescaledb/files/init/` a
vygenerovaný `04-roles-grants.sql`) se spustí automaticky při prvním
vytvoření prázdného datového volume — ale to se v praxi ukázalo jako
křehké (restart kontejneru uprostřed initu, např. při reloadu kvůli
`shared_preload_libraries`, nechá datový adresář neprázdný, takže se při
dalším startu init přeskočí, i když doběhl jen částečně). Proto playbook
tyto SQL soubory aplikuje znovu explicitně při **každém** spuštění (task
"Apply/reapply schema..." v `roles/timescaledb/tasks/main.yml`) přes
`podman exec ... psql -f`. Všechny čtyři soubory jsou psané idempotentně
(`IF NOT EXISTS` / `if_not_exists => true`), takže je to bezpečné —
stačí upravit SQL (např. `retention_1h_days`) a spustit
`ansible-playbook playbook.yml`, žádný ruční zásah není potřeba.

## Co záměrně NENÍ součástí tohoto nasazení

- Downsampling/rollup přes 4h vrstvu (např. denní agregát) — nebylo
  požadováno, ale hierarchický cagg lze kdykoliv přidat nad `metrics_4h`.
- Alerting v Grafaně na základě těchto dat — datasource je připravený,
  alerty nejsou nastavené.
- TLS/reverse proxy před Grafanou a receiverem — běží zatím na HTTP/LAN.
- Nativní Prometheus histogramy (native histograms) v remote_write — receiver
  je záměrně jednoduchý a parsuje jen klasické float samply (labels+samples),
  což pokrývá běžné counters/gauges/histograms/summaries tak, jak je
  Prometheus remote_write posílá standardně.
