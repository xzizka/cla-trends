# Load test history

Výsledky `tests/load_test.py` v čase, pro porovnání dopadu budoucích změn
(např. zvýšení `psycopg_pool` `max_size` v receiveru, přechod na `COPY`
místo `executemany`, změna VM specifikací).

Sloupec **ztráta dat** je ověřený přesným počtem řádků v `metrics_raw`
(`labels->>'run_id'`), ne jen HTTP status kódy — request, který klientovi
vyprší (timeout), ale server ho stejně zpracuje, se nepočítá jako ztráta.

## 2026-09-01 — první běh, čerstvý deployment

- Stroj: fedora-podman (192.168.122.151), 4 CPU / 9.5 GB RAM / 15 GB disk
- Verze: TimescaleDB 2.29.2 (pg18), Grafana 13.2.0, receiver 1.0.0
- Příkaz: `python3 tests/load_test.py --token <token> --levels 1,2,4,8,16,32,64 --duration 10`
- Dávka: 2000 vzorků/request (4 série × 500), odpovídá Prometheus defaultu `max_samples_per_send`

| Konkurence | Vzorků/s | p50 latence | p99 latence | HTTP chyby | Ztráta dat | Restart |
|---|---|---|---|---|---|---|
| 1  | 7 156  | 277 ms  | 321 ms  | 0 | 0 | ne |
| 2  | 12 320 | 312 ms  | 516 ms  | 0 | 0 | ne |
| **4**  | **14 446** | 556 ms  | 737 ms  | 0 | 0 | ne |
| 8  | 8 273  | 1,95 s  | 2,46 s  | 0 | 0 | ne |
| 16 | 8 524  | 3,73 s  | 4,40 s  | 0 | 0 | ne |
| 32 | 8 429  | 6,85 s  | 8,40 s  | 1 (`Remote end closed connection`) | 0 | ne |

Ramp se zastavil na konkurenci 32 (1 HTTP chyba přerušila "clean" sérii),
ale **žádný vzorek se neztratil a žádný kontejner nerestartoval** v celém
testovaném rozsahu. Samostatný běh s vyšší konkurencí (64) dříve ukázal totéž:
32 z 95 requestů překročilo 15s klientský timeout, ale všech 95×2000 = 190 000
vzorků se ověřeně zapsalo do DB — jde o netrpělivost testovacího klienta, ne
o ztrátu dat na straně systému.

**Závěr:** bezpečná udržitelná propustnost ~14 000 vzorků/s (konkurence 4,
latence pod 1s). Nad tím propustnost dál neroste kvůli frontování na
`psycopg_pool` (`max_size=8` v `roles/receiver/files/app/receiver.py`) —
latence prudce stoupá, ale systém zůstává korektní (žádná ztráta, žádný pád)
minimálně do konkurence 64. Pro reálné nasazení (jeden Prometheus, řádově
stovky až nízké tisíce vzorků/s) je tato kapacita s velkou rezervou.

**Úzké hrdlo pro budoucí ladění:** `max_size=8` u connection poolu. Zvýšení
by mohlo posunout bod, kde latence začíná růst, výš — nebylo testováno.
