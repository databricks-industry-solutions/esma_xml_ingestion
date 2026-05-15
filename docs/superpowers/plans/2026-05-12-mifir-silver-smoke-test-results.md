# MiFIR Silver — Azure Smoke-Test Results

**Date:** 2026-05-12
**Branch:** `feat/mifir-silver`
**Workspace:** `adb-984752964297111.11.azuredatabricks.net` (CLI profile `azure`)
**Target catalog/schema:** `esma_dev.default` (the richer MiFIR test data location)

## Pipeline runs

### Bronze (parameterized loader from PR #1)

| Field | Value |
|---|---|
| Pipeline name | `[dev matthew_moorcroft] MiFIR XML Loader (SDP)` |
| Pipeline ID | `7bb20912-99c3-4c46-a36c-88121cb8bf53` |
| Final successful update ID | `09448387-c254-46bf-8f80-28768767cd37` |
| State | COMPLETED |
| Output | `esma_dev.default.mifir_raw` = 2 rows (1 input XML file × 2 `<Tx>` elements) |
| URL | https://adb-984752964297111.11.azuredatabricks.net/#joblist/pipelines/7bb20912-99c3-4c46-a36c-88121cb8bf53 |

### Silver (this branch's pipeline)

| Field | Value |
|---|---|
| Pipeline name | `[dev matthew_moorcroft] MiFIR Silver (domain-driven)` |
| Pipeline ID | `edd6271d-14fc-47d6-97ae-ed6fe1069aac` |
| Final successful update ID | `23dd1404-04ea-4fe9-a9a7-080ee17dec42` (then `--full-refresh-all` after the `instrument_isin` struct fix) |
| State | COMPLETED |
| Cluster | serverless + Photon |
| URL | https://adb-984752964297111.11.azuredatabricks.net/#joblist/pipelines/edd6271d-14fc-47d6-97ae-ed6fe1069aac |

## Row counts

| Table | Rows |
|---|---|
| `esma_dev.default.transaction` | 2 |
| `esma_dev.default.transaction_party` | 8 |
| `esma_dev.default.submission_file` | 1 |

Invariants verified:
- `transaction` row count = bronze (1:1 per `<Tx>` element) ✓
- `submission_file` = 1 (single source file) ✓
- `transaction_party` = 8 = 4 party-rows × 2 transactions (BUYER × ACCT_OWNR, BUYER × DCSN_MAKR, SELLER × ACCT_OWNR, SELLER × DCSN_MAKR per Tx) ✓

## `action_type` distribution

| action_type | count |
|---|---|
| NEW | 2 |
| CXL | 0 |

The sample contains only NEW transactions. CXL row processing remains untested with real data — the silver code paths exist (per Tasks 8 + 9) but only NULL passthroughs on the shared 3-field shape were exercised.

## `transaction_party` distribution

| side | party_role | count |
|---|---|---|
| BUYER | ACCT_OWNR | 2 |
| BUYER | DCSN_MAKR | 2 |
| SELLER | ACCT_OWNR | 2 |
| SELLER | DCSN_MAKR | 2 |

All four legs of the unified explode produce data for this sample.

## Spot-check sample (`transaction`)

```
transaction_id                                            executing_party_lei      buyer_lei                trade_venue_mic  instrument_isin
TCXX4269MIFIRTRN20250408T110549Z0000001                  213800X8D2RDODXFIY15     213800D1EI4B9WTWWD28     XLON             GB00B0SWJX34
TCXX4269MIFIRTRN20250408T110549Z0000002                  213800X8D2RDODXFIY15     213800D1EI4B9WTWWD28     XLON             GB00B0SWJX34
```

- `executing_party_lei` and `buyer_lei` are 20-char LEI-shaped strings ✓
- `trade_venue_mic` = `XLON` (London Stock Exchange MIC) ✓
- `instrument_isin` = `GB00B0SWJX34` — plain STRING after the post-smoke `_VALUE` extraction fix ✓
- `action_type` = `NEW` for both rows ✓

## Spec-drift fixes committed during smoke test

Three commits made during smoke testing to bridge the design's assumed schema shape with the actual richer-schema bronze produced on the Azure workspace:

1. **`21d9de0`** `fix(loader): defensive corrupted_record column on schemas without it` — bronze-side: defensive `withColumn("corrupted_record", lit(None).cast("string"))` fallback because Auto Loader's `columnNameOfCorruptRecord` did not materialize the column on this Azure workspace's Spark/DBR build unless explicitly listed in the user-supplied schema. Downstream `mifir_quarantine`/`mifir_raw` flows filter on `corrupted_record IS [NOT] NULL` and were failing with `UNRESOLVED_COLUMN`.

2. **`7de19f0`** `fix(silver): schema-path corrections discovered at smoke-test time` — silver-side: ~20 schema-path corrections. Summary:
   - `transaction_party`: the `_explode_party` helper was split into `_explode_acct_ownr` + `_explode_dcsn_makr` because the two array element shapes differ structurally — DcsnMakr lacks the `Id` wrapper present in AcctOwnr. NULL-stubbed `party_other_*` (no `Id.Othr` in bronze) and `person_country` (no `Prsn.CtryOfBrnch` in bronze).
   - `transaction`: NULL-stubbed `buyer_other_*` / `seller_other_*` (no `Id.Othr` in bronze). Fixed `instrument_strike_price*` paths to `StrkPric.Pric.{MntryVal.Amt._VALUE/_Ccy, Pctg, Yld}`. NULL-stubbed `instrument_maturity_dt` (only `XpryDt` exists in this bronze schema), `instrument_commodity_derivative`, `commodity_derivative_indicator`, and `investment_decision_person_*` / `executing_person_*` `_lei` / `_first_name` / `_last_name` / `_birth_dt` (the richer schema carries only `Prsn.{CtryOfBrnch, Othr}` + `Algo` + `Clnt` on those person blocks; no LEI / first-name / last-name / DOB siblings in the actual ISO 20022 BAH at this workspace).

3. **`18fb919`** `fix(silver): extract _VALUE from struct-wrapped scalar fields` — silver-side: `instrument_isin` was landing as `struct<_VALUE: STRING, _sequence: STRING>` because the source XML carries an attribute on the `Id` element. Added `._VALUE` accessor so it returns a plain STRING. Verified via `DESCRIBE TABLE` — every other scalar column (`instrument_full_name`, `instrument_classification`, etc.) was correctly typed and didn't need the same treatment.

The silver-table column contract (column names, business meanings) is unchanged by these fixes — they're path corrections and NULL-stubs for fields that don't exist in this bronze. 100% coverage of bronze fields is preserved (you can't cover what bronze doesn't have).

## Caveats / known limitations

- **Single sample file (`sample_Tx.xml` with 2 Tx elements)**. Real-customer-data validation pass remains a follow-up — the sample exercises only a small subset of MiFIR XSD branches. In particular, the underlying-instrument 6-prefix groups (`underlying_swap_in_*`, `underlying_swap_out_*`, `underlying_other_*` × single/basket) are entirely NULL because the sample doesn't have a derivative with underlying-instrument detail.
- **CXL action_type untested with data.** Code paths exist (Tasks 8 + 9) but the sample has no cancellations.
- **`instrument_maturity_dt` is NULL-stubbed** because bronze schema has only `XpryDt`, not `MtrtyDt`. If a real customer file populates both, the spec should be updated to keep them as separate columns.
- **Local `dev-variables.yml`** (git-ignored) pins MiFIR overrides to `esma_dev.default.regulatory_data.mifir.*` for this smoke test. Production deployment would use a different target catalog/schema.

## Open follow-ups

- **Real-customer-data validation pass** — exercise more XSD branches once production MiFIR data lands
- **CXL action_type live verification** — capture a real cancellation event end-to-end
- **Gold layer** aggregations (counterparty exposure, daily venue volume, etc.)
- **UC column-mask policies for PII** — separate governance branch
- **Star-schema pivot** (`dim_legal_entity` shared across EMIR + MiFIR)
- **Production MiFIR filename regex** for customers with different naming conventions
- **Retire legacy MiFIR flatten notebook** once silver is production-confirmed
- **Spec §4 update**: align documented column list with what the richer Azure-workspace schema actually supports vs the field paths NULL-stubbed during smoke test

## Sign-off

Silver pipeline ships in PR #4. Real-data validation gates the legacy flatten notebook retirement.
