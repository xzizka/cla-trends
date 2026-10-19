# Návrh importeru (aktivní stahování dat)

## Cíl

Doplnit k existujícímu **receiveru** (pasivní příjem — čeká, až mu Prometheus
pošle `remote_write`) druhou, **aktivní** cestu: službu, která si sama v
zadaných časech sáhne na externí endpointy a stáhne, co se tam od posledního
běhu objevilo.

Zadání a jak je splněno:

| Požadavek | Řešení |
|---|---|
| aktivní stahování dat | `roles/importer` — samostatná služba s vlastním schedulerem |
| zdroj Influx, Prometheus nebo PostgreSQL | čtyři typy zdroje: `prometheus`, `influxdb1` (InfluxQL), `influxdb2` (Flux), `postgres` (SQL přes libpq) |
| více zdrojů současně | `importer_sources` je seznam; každý zdroj běží ve vlastním vlákně |
| do budoucna přidat další technologie | plugin registr v `sources/` — nová technologie = jeden nový soubor, viz níže |
| spouštění časem, per zdroj | `schedule: {every: 1h}` nebo `schedule: {cron: "0 3 * * *", timezone: ...}`, definované u každého zdroje zvlášť |
| struktura DB a ukládání zůstává stejná | importer píše do stejné `metrics_raw` jako receiver; hypertable, continuous aggregates, retence ani dashboardy se nemění |

## Architektura

```
Externí Prometheus ──remote_write──►  receiver  (:9201, push)   ─┐
                                                                 │
Externí Prometheus  ◄──query_range──┐                            │
Externí InfluxDB 1.x ◄──InfluxQL────┤                            │
Externí InfluxDB 2.x ◄──Flux────────┤ importer (:9202, pull)  ───┤
Externí PostgreSQL  ◄──SQL──────────┘   │                        │
                                        │ watermark              │
                                        ▼                        ▼
                              importer_state          TimescaleDB / metrics_raw
                                                              │
                                                     metrics_1h ──► metrics_4h
```

Obě cesty ústí do stejné tabulky, takže agregační vrstvy, retence, komprese i
Grafana dashboardy platí pro data z importeru úplně stejně jako pro data z
receiveru. Odlišit je jde podle labelu `source` (viz *Provenience* níže).

## Klíčové rozhodnutí: watermark a půlotevřená okna

`metrics_raw` **nemá** unique constraint — je to append-only hypertable
plněná receiverem ve vysokém tempu, kde by unique index byl jen režie. To ale
znamená, že databáze druhý zápis stejného vzorku klidně přijme a nikdo dál v
řetězci to už neuklidí. Duplicitám proto musí zabránit importer sám.

Mechanismus:

1. Pro každý *stream* (= jeden dotaz jednoho zdroje) se v tabulce
   `importer_state` drží `last_ts` — **exkluzivní** horní hranice: *"všechno s
   `time < last_ts` už je v databázi"*.
2. Okna jsou všude půlotevřená `[start, end)`. Vzorek přesně na `end` patří
   až do dalšího okna. Odtud plyne, že se hranice mezi okny nemůže ani
   zdvojit, ani vynechat.
3. Vzorky a nový watermark se commitují **v jedné transakci**
   (`store.commit_window`). Buď se okno zapsalo celé a watermark se posunul,
   nebo ani jedno. Díky tomu je importer bezpečné kdykoli zabít, restartovat
   nebo mu vypnout stroj v půlce backfillu.
4. Chyba watermark **neposune** → stejné okno se zkusí znovu při dalším tiku.
   Prázdné okno watermark **posune** → zdroj, který hodinu nic neměl,
   se na tu hodinu neptá donekonečna.

Volba tabulky (ne souboru na volume, ne `MAX(time)` z `metrics_raw`):
soubor by se ztratil se smazáním volume, a `MAX(time)` se ztratí s retencí
raw vrstvy (7 dní) — po delším výpadku by importer nevěděl, kde byl, a začal
by od `max_lookback`. Tabulka přežije obojí a je vidět přes `psql`.

## Časová mřížka (`step` / `align`)

Prometheus `query_range` klade vzorky na `start, start+step, start+2*step, …`
Kdyby se `start` řídil tím, kdy zrovna importer tikl, měla by stejná série
při každém běhu timestampy na jiné mřížce (10:00:03, 10:01:47, 10:02:11…).
Okna se proto zarovnávají na mřížku ukotvenou v epoše — uložené timestampy
jsou pak reprodukovatelné a buckety v rollupech čisté.

U Influxu je to totéž z jiného důvodu: `GROUP BY time(1m)` /
`aggregateWindow(every: 1m)` vrátí i bucket, který do okna zasahuje jen
částečně, agregovaný přes ty pár sekund překryvu. Bez zarovnání by tedy na
každém okraji okna vznikla "minutová průměrná hodnota" spočítaná z 23 sekund.
Proto `align: 1m` u dotazu — **musí** odpovídat `GROUP BY time()` /
`aggregateWindow(every:)` v dotazu.

### Flux: `timeSrc: "_start"` není volitelný

Flux `aggregateWindow()` defaultně označí bucket jeho **koncovým** časem
(`timeSrc: "_stop"`). InfluxQL `GROUP BY time()` i `time_bucket()` v
TimescaleDB označují **začátek**. Když se default nechá být, tatáž data
naimportovaná přes `influxdb2` skončí o celý interval později než přes
`influxdb1` — a protože oba píšou do jedné `metrics_raw`, není to chyba, ale
tiché posunutí o jeden bucket mezi zdroji.

Importer to nemůže detekovat (bucket označený koncem je nerozlišitelný od
legitimně pozdějšího), takže to musí být správně v dotazu:

```
|> aggregateWindow(every: 1m, fn: mean, createEmpty: false, timeSrc: "_start")
```

Odchyceno demem (`tests/demo/`): influx1 měl hodnoty přesně, influx2 byl
konzistentně o 1 minutu vedle.

## Provenience

Do `labels` každého řádku importer vždy vloží `source: <jméno zdroje>` (plus
volitelné `extra_labels`). Plugin tenhle label nemůže podvrhnout — runner ho
nastavuje po plugin, ne před ním.

Praktický důsledek: data z importeru tvoří vlastní série (label set se liší),
takže se nemíchají s daty z receiveru ani mezi zdroji navzájem, a jde je
adresně smazat:

```sql
DELETE FROM metrics_raw WHERE labels->>'source' = 'prom-main';
```

Cena: `labels` je součástí `GROUP BY` v continuous aggregates, takže každý
zdroj má vlastní řádky i v `metrics_1h`/`metrics_4h`. To je záměr, ne režijní
překvapení.

## `max_lookback` — jediné místo, kde se vědomě ztrácí data

První běh streamu začíná `max_lookback` (default 7 dní) v minulosti, ne od
epochy — jinak by první tik proti Prometheu s roční historií znamenal
neomezený backfill.

Stejná hodnota ale **zastropuje i existující watermark**: po výpadku delším
než 7 dní se ta mezera nedohání. Je to schválně sladěné s
`retention_raw_days: 7` — starší data by retenční politika raw vrstvy zahodila
tak jako tak. Importer to v takovém případě zaloguje jako `WARNING` s
konkrétním rozsahem, který přeskočil; tiché zúžení okna by byla přesně ta
věc, kvůli které si člověk za měsíc nevšimne děravého grafu.

## Přidání nové technologie

Kontrakt je v `roles/importer/files/app/sources/base.py`. Plugin odpovídá na
jednu otázku: *"jaké vzorky existují v `[start, end)`?"* — scheduling,
watermarky, dělení oken, deduplikace a zápis do DB řeší runner.

1. Nový soubor `sources/<technologie>.py`.
2. Podědit `Source`, implementovat `streams()` a `fetch()`, volitelně
   `validate()` pro kontrolu konfigurace při startu.
3. Ozdobit třídu `@register("<technologie>")`.
4. Přidat modul do importu na konci `sources/__init__.py` (tím se dekorátor
   spustí).
5. Přidat do seznamu souborů v `roles/importer/tasks/main.yml` a do `COPY`
   v `Containerfile`.

Mimo `sources/` se nemusí měnit nic.

## Omezení a záměrná zjednodušení

- **Zdroj musí umět odpovědět na časový rozsah.** To Prometheus, Influx i
  Postgres umí; technologie, která zvládá jen "aktuální hodnotu", by při
  hodinovém intervalu dala 1 bod za hodinu. Pro takový zdroj je vhodnější
  receiver (push) nebo krátký interval.
- **Žádné native histogramy** — stejné omezení jako u receiveru,
  `metrics_raw.value` je `DOUBLE PRECISION`.
- **`NaN` / `±Inf` se zahazují.** Jsou to legitimní hodnoty Promethea, ale v
  `avg()`/`sum()` v continuous aggregates otráví celý bucket.
- **Nenumerické hodnoty se zahazují** (stringová pole v Influxu, boolean) —
  v `DOUBLE PRECISION` je nelze reprezentovat. U Postgresu se to týká jen
  NULL/boolean; string ve explicitně nastaveném `value_column` je chyba
  konfigurace a importer hlásí chybu místo tichého přeskočení.
- **Streamy jednoho zdroje běží sériově**, různé zdroje paralelně. Zatížení
  jednoho endpointu je tak předvídatelné a mrtvý Influx nezdrží Prometheus.
- **`/status` neběží na LAN** (jen `127.0.0.1`). Otevírat port ve firewallu by
  na `ubuntu-docker` chtělo sudo, které tam nikdo nemá.

## Nastavitelné hodnoty

Vše v `group_vars/all/vars.yml` (`importer_defaults` = default pro všechny
zdroje, kterýkoli klíč jde přebít u konkrétního zdroje):

| Klíč | Default | Význam |
|---|---|---|
| `timeout_seconds` | 60 | HTTP timeout na dotaz |
| `max_lookback` | `7d` | bootstrap prvního běhu + strop na dohánění výpadku |
| `end_lag` | `0s` | o kolik se nestahuje až k `now` (pro zdroje, které ještě dopisují aktuální okamžik) |
| `tls_verify` | `true` | ověřování TLS certifikátu zdroje |
| `retries` | 3 | pokusů na HTTP dotaz (exponenciální backoff, jen na 429/5xx a síťové chyby) |
| `run_on_start` | `true` | naimportovat hned při startu, ne až na první tik |

Per zdroj/dotaz navíc: `step` (Prometheus, resoluce range dotazu),
`align` (Influx a Postgres, zarovnání oken), `max_span` (strop velikosti
jednoho okna), `max_points_per_request` (Prometheus, default 9000 — jeho
limit je 11000), `metric_name`, `metric_prefix`, `field_suffix`,
`extra_labels`, `enabled`. U postgres zdroje navíc `host`, `port`,
`database`, `sslmode` a v dotazu `time_column` / `value_column` /
`label_columns` (defaulty `time` / `value` / všechny ostatní sloupce).
