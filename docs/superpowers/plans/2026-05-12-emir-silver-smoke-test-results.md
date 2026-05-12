# EMIR Silver — E2 Smoke-Test Results

**Date:** 2026-05-12
**Branch:** `feat/emir-silver`
**Workspace:** `e2-demo-field-eng.cloud.databricks.com`
**Target schema:** `users.matthew_moorcroft`

## Pipeline run

| Field | Value |
|---|---|
| Pipeline name | `[dev matthew_moorcroft] EMIR Silver (domain-driven)` |
| Pipeline ID | `2c2cfba3-7e60-4f34-9a1c-0d546e0c7727` |
| Update ID (successful) | `b28d0d34-48f0-4d27-aa9b-06f0a9dc9f02` |
| State | COMPLETED |
| Wall time | ~3 min 5 s (185 s) for 32M bronze rows |
| Cluster | serverless + Photon |
| URL | https://e2-demo-field-eng.cloud.databricks.com/pipelines/2c2cfba3-7e60-4f34-9a1c-0d546e0c7727 |
| Update URL | https://e2-demo-field-eng.cloud.databricks.com/#joblist/pipelines/2c2cfba3-7e60-4f34-9a1c-0d546e0c7727/updates/b28d0d34-48f0-4d27-aa9b-06f0a9dc9f02 |

For comparison, the bronze pipeline took ~12 min for 131GB / 32M rows. Silver is ~4× faster (no XML parsing, no lxml UDFs — pure SQL transformations on Delta).

## Row counts

| Table | Rows |
|---|---|
| `users.matthew_moorcroft.trade` | 32,000,000 |
| `users.matthew_moorcroft.trade_schedule` | 211,136 |
| `users.matthew_moorcroft.trade_beneficiary` | 142,784 |
| `users.matthew_moorcroft.submission_file` | 64 |

Invariants verified:
- `trade` = bronze row count (1:1 per `<Stat>` element). ✓
- `submission_file` = number of source files (64 files in `landing/`). ✓
- `trade_schedule` populated across all 6 schedule_types (see breakdown below). ✓
- `trade_beneficiary` non-zero (synthetic CBI data has beneficiaries). ✓

## Schedule breakdown by type

| schedule_type | rows |
|---|---|
| NTNL_AMT_LEG_1 | 85,056 |
| PRICE | 49,280 |
| NTNL_AMT_LEG_2 | 46,400 |
| NTNL_QTY_LEG_1 | 20,288 |
| NTNL_QTY_LEG_2 | 7,808 |
| STRIKE | 2,304 |

All 6 discriminator values present, confirming the unified-explode pattern works end-to-end.

## Spot-check sample row

A representative row from `trade`:

```
trade_id                       : COMH0XB8…
reporter_lei                   : IKV2BUEK1YV9YB1I8ZC3  (20-char, LEI-shaped)
asset_class                    : BPGK  (synthetic — see caveat below)
contract_type                  : (synthetic random 4-char code)
is_cleared                     : false  (proper BOOLEAN)
notional_first_leg_amount      : 19438.05  (DECIMAL)
contract_value                 : 50283.02  (DECIMAL)
reporting_date                 : 2025-XX-XX
```

`reporter_lei` shape is correct (20 alphanumeric chars). `is_cleared` is a proper boolean, not the string "true". Decimal columns have sensible precision.

## Analyst feel-test query

```sql
SELECT reporter_lei, asset_class, COUNT(*) AS trades,
       SUM(ABS(notional_first_leg_amount)) AS gross_notional
FROM users.matthew_moorcroft.trade
WHERE reporting_date = (SELECT MAX(reporting_date) FROM users.matthew_moorcroft.trade)
GROUP BY reporter_lei, asset_class
ORDER BY gross_notional DESC NULLS LAST
LIMIT 10
```

Top 3 results (query returned in ~3 seconds):

| reporter_lei | asset_class | trades | gross_notional |
|---|---|---|---|
| YLJ2T4D2TE9CADBB5B0Q | BPGK | 27 | 26,999,892.00 |
| XTXZ6JNPB4AL97N9AGTG | WMYR | 27 | 26,998,650.00 |
| EKSYOZVFYG4QUQRFYTFU | KKXL | 27 | 26,997,408.00 |

This is the kind of query that was painful against bronze (5-level dot-notation paths). Silver makes it trivial.

## Spec-drift fixes applied during smoke test

While bringing up the pipeline, four schema-path mismatches surfaced between the design's assumed XSD shapes and the actual bronze struct shapes. All are captured in commit `8c55d73 fix(silver): schema-path corrections discovered at smoke-test time`:

1. **`trade_schedule` STRIKE rows** — `Optn.StrkPricSchdl[]` elements have the SAME row shape as `TxPric.SchdlPrd[]` (the SchedulePeriod1 XSD type is shared, not duplicated under `StrkPric`). Lambda updated to `r["Pric"]["MntryVal"]...`.

2. **`Cdt.PmtFrqcy` is STRING, not struct** — unlike `IntrstRate.PmtFrqcy`. Collapsed to a single `credit_payment_freq STRING` column.

3. **`Cdt.Trch` is a CHOICE** — `{Trnchd: {AttchmntPt, DtchmntPt}}` vs `{Utrnchd: <code>}`. Existing paths corrected to `Trch.Trnchd.*`; added `credit_tranche_untranched`.

4. **`OthrPmt[]` row shape** — `{PmtTp.Tp, PmtAmt.Amt._VALUE, PmtAmt.Amt._Ccy, PmtAmt.Sgn, PmtDt, PmtPyer.Lgl.LEI, PmtRcvr.Lgl.LEI}` — richer than spec assumed. Added `sign`, `payer_lei`, `receiver_lei` to the per-payment struct.

5. **`Cmmdty` taxonomy is two-level for 9 of 15 categories** — Agrcltrl/Nrgy/Envttl/Frtlzr/Frght/IndstrlPdct/Metl/Ppr/Plprpln further branch into sub-categories (e.g. `Agrcltrl.GrnOilSeed.BasePdct`). Indx/Infltn/MultiCmmdtyExtc/OffclEcnmcSttstcs/Othr/OthrC10 carry `BasePdct` directly. The three COALESCE blocks (`commodity_base_product`, `commodity_sub_product`, `commodity_additional_sub_product`) now enumerate the actual leaf paths (50/45/12 respectively).

These corrections don't change the silver-table column contract — every column still maps to a documented business meaning — but they replace assumed XSD paths with actual ones.

## Caveats / known anomalies

- **Synthetic data uses random codes for `asset_class` and `contract_type`** (e.g., `BPGK`, `WMYR`, `KKXL` for asset_class; random 4-char codes for contract_type). The XSD constrains these to enums (CR/EQ/IR/FX/CO and SWAP/FORW/OPTN/FUTR/CFDS/OTHR respectively), but the CBI synthetic generator doesn't enforce the constraint. Silver-layer code is correct; the data quality is an artifact of the synthetic source. Real EMIR data would have proper enum values.

- **Some long-tail columns may be NULL on this dataset** — synthetic CBI data doesn't exercise every XSD branch. Real production data may surface additional spec-drift cases on columns we haven't proven against data yet.

## Open follow-ups (separate branches)

- Real-data validation pass against production EMIR submissions
- Gold layer aggregations (`daily_exposure_by_counterparty`, etc.)
- MiFIR silver — reuses `submission_file` envelope
- SCD Type 2 migration when historical lifecycle queries are needed
- Star-schema pivot (`dim_legal_entity` + `dim_date`) for GLEIF integration
- Retire legacy `2_flatten_explode_table.py` once silver is production-confirmed
- Bronze filename-regex parameterization (separate small branch — see PR #1 follow-ups)

## Sign-off

Silver pipeline ships in this PR. Real-data validation is the next gate before retiring the legacy flatten path.
