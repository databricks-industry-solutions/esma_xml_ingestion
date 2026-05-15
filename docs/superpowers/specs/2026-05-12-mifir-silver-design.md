# MiFIR Silver Layer — Design

**Status:** Approved (interactive review complete)
**Date:** 2026-05-12
**Author:** Matthew Moorcroft
**Branch:** `feat/mifir-silver`
**Reference bronze:** `esma_dev.default.mifir_raw` on Azure workspace `adb-984752964297111` (CLI profile `azure`)

---

## 1. Problem & Motivation

PR #3 shipped EMIR REFIT silver tables. PR #4 (this branch) does the same for MiFIR transaction reports (auth.016.001.01_ESMAUG_Reporting), reusing every pattern from the EMIR silver:

- Wide-flat fact tables with business-readable column names
- Choice fields collapsed to common branch + `*_other_id` fallback
- Repeating arrays exploded into child tables when they're real entities
- ARRAY/STRUCT columns kept only for deep choice taxonomies where >95% of leaves would be NULL
- `cluster_by_auto=True`, serverless + Photon, `pyspark.pipelines` modern API
- Append-only (event-based — MiFIR is execution-event, not snapshot)
- 100% bronze coverage — every leaf in `pyld_schema.json` and `hdr_pyld_metadata_schema.json` is represented in silver

The bronze loader from PR #1 is already parameterized for MiFIR (different `row_tag`, different schema paths). This branch is **silver only** — no bronze changes.

## 2. Goals & Non-Goals

### Goals

- 3 silver tables in `{mifir_catalog}.{mifir_raw_schema}`:
  - **`transaction`** — main fact, one row per `<Tx>` (NEW or CXL action type)
  - **`transaction_party`** — explode of `Buyr.AcctOwnr[]` + `Buyr.DcsnMakr[]` + same for `Sellr`, with `side` + `party_role` discriminators
  - **`submission_file`** — MiFIR-specific file-level envelope (UVHeader + full BizAppHeader + Rltd mirror)
- 100% coverage of all 449 bronze leaves (175 pyld + 274 hdr)
- Domain-driven, business-readable column names
- `action_type` discriminator on `transaction` to distinguish NEW from CXL
- Filename regex parameterized via a customer-replaceable `_add_filename_regex_columns()` function (same pattern as bronze PR #1)
- PII columns identified in code comments; UC column-mask policies applied externally by data stewards (governance not pipeline concern)
- New `mifir_silver_pipeline` SDP resource in `bundle.mifir_resources.yml`
- Validated on the Azure workspace where MiFIR test data lives

### Non-Goals

- Bronze changes (loader is already parameterized for MiFIR)
- SFTR silver (separate spec + branch)
- Gold layer
- UC column-mask DDL — applied externally via UC functions + `ALTER TABLE ... SET MASK`
- Real-customer-data validation pass (synthetic single-file sample is what we have)
- Cross-regulation conformed dimensions (`dim_legal_entity` shared across EMIR + MiFIR) — documented as future architectural option
- Retire legacy MiFIR flatten notebook — stays scheduled until silver is production-confirmed

## 3. Architecture

```
              src/pipelines/xml_loader.py       (PR #1 — bronze, parameterized for MiFIR)
                                │
                                ▼
            esma_dev.default.mifir_raw          (bronze)
                                │  spark.readStream.table(...)
                                ▼
            src/pipelines/silver_mifir.py       (NEW — this spec)
                                │
       ┌────────────────────────┼─────────────────────┐
       ▼                        ▼                     ▼
  transaction            transaction_party       submission_file
  (~135 + 15 arrays)     (~18 cols)              (~270 cols)
```

### 3.1 SCD strategy — append-only event semantics

MiFIR transactions are EVENTS (not daily snapshots like EMIR DAT TSR). Each `Tx` element is either a `New` (a freshly reported execution) or `Cxl` (cancellation of a prior reported transaction). Silver `transaction` is append-only:
- One row per `Tx` element
- `action_type ∈ {'NEW', 'CXL'}` discriminator column
- Active transactions queried via `WHERE action_type='NEW' AND NOT EXISTS (CXL with same transaction_id)`

No SCD2 migration is needed for v1 — the lifecycle pattern is already trivial via the `action_type` column. If analysts later want "lifecycle status" as a materialized column on the NEW row, a follow-up branch can add it.

### 3.2 Per-field decision rule (same as EMIR PR #3)

| Will an analyst filter, group, or aggregate on this leaf? | Outcome |
|---|---|
| Yes | Flat scalar column, business-named. Choice fields collapsed to common branch with `*_other_id` fallback. |
| No, but data is needed for fidelity | ARRAY or STRUCT column |
| No, and rarely populated | Still captured in silver (user explicitly required 100% bronze coverage) |

The MiFIR design has zero "deliberately dropped" data leaves (vs. EMIR's spec which dropped a few long-tail "Othr" name/scheme/domicile fields). Per user direction "want all in bronze".

## 4. Table Definitions

> **Note on column counts**: numbers approximate; final column names may shift slightly during implementation if the actual bronze struct shape differs from the JSON schema enumeration (same kind of spec-drift fixes we hit in EMIR PR #3 commit `8c55d73`). The contract is "every bronze leaf has a silver representation" — not "the exact column names listed below."

### 4.1 `transaction` — ~135 scalars + ~15 array columns

**Grain:** one row per `<Tx>` element.

**Clustering:** `cluster_by_auto=True` (Delta will likely pick `trade_dt`, `transaction_id`, `executing_party_lei`).

#### Identification (5)
```
transaction_id                            STRING       -- COALESCE(New.TxId, Cxl.TxId)
action_type                               STRING       -- 'NEW' | 'CXL'
executing_party_lei                       STRING       -- COALESCE(New.ExctgPty, Cxl.ExctgPty)
submitting_party_lei                      STRING       -- COALESCE(New.SubmitgPty, Cxl.SubmitgPty)
investment_party_indicator                BOOLEAN      -- New.InvstmtPtyInd
```

#### Buyer flat fields (~7 cols, first AcctOwnr only — multi-owner via `transaction_party`)
```
buyer_lei                                 STRING       -- New.Buyr.AcctOwnr[0].Id.LEI
buyer_other_id                            STRING       -- New.Buyr.AcctOwnr[0].Id.Othr.Id
buyer_other_id_scheme                     STRING       -- New.Buyr.AcctOwnr[0].Id.Othr.SchmeNm.Cd
buyer_other_id_scheme_proprietary         STRING       -- New.Buyr.AcctOwnr[0].Id.Othr.SchmeNm.Prtry
buyer_mic                                 STRING       -- New.Buyr.AcctOwnr[0].Id.MIC
buyer_intl_person_id                      STRING       -- New.Buyr.AcctOwnr[0].Id.Intl
buyer_country_of_branch                   STRING       -- New.Buyr.AcctOwnr[0].CtryOfBrnch
buyer_account_owner_count                 INT          -- size(New.Buyr.AcctOwnr)
buyer_decision_maker_count                INT          -- size(New.Buyr.DcsnMakr)
```

**Seller mirror (~9 cols):** same with `seller_*` prefix.

#### Order transmission (3)
```
order_transmission_indicator              BOOLEAN      -- New.OrdrTrnsmssn.TrnsmssnInd
order_transmitting_buyer_lei              STRING       -- New.OrdrTrnsmssn.TrnsmttgBuyr
order_transmitting_seller_lei             STRING       -- New.OrdrTrnsmssn.TrnsmttgSellr
```

#### Trade details (`New.Tx`) (~24 cols)
```
trade_dt                                  TIMESTAMP    -- TradDt
trading_capacity                          STRING       -- TradgCpcty
quantity_unit                             DECIMAL      -- Qty.Unit
quantity_nominal_value                    DECIMAL      -- Qty.NmnlVal._VALUE
quantity_nominal_currency                 STRING       -- Qty.NmnlVal._Ccy
quantity_monetary_value                   DECIMAL      -- Qty.MntryVal._VALUE
quantity_monetary_currency                STRING       -- Qty.MntryVal._Ccy
derivative_notional_change                STRING       -- DerivNtnlChng
price_amount                              DECIMAL      -- Pric.Pric.MntryVal.Amt._VALUE
price_currency                            STRING       -- Pric.Pric.MntryVal.Amt._Ccy
price_sign                                BOOLEAN      -- Pric.Pric.MntryVal.Sgn
price_percentage                          DECIMAL      -- Pric.Pric.Pctg
price_yield                               DECIMAL      -- Pric.Pric.Yld
price_basis_points                        DECIMAL      -- Pric.Pric.BsisPts
price_pending_reason                      STRING       -- Pric.NoPric.Pdg
price_pending_currency                    STRING       -- Pric.NoPric.Ccy
up_front_payment_amount                   DECIMAL      -- UpFrntPmt.Amt._VALUE
up_front_payment_currency                 STRING       -- UpFrntPmt.Amt._Ccy
up_front_payment_sign                     BOOLEAN      -- UpFrntPmt.Sgn
net_amount                                DECIMAL      -- NetAmt
trade_venue_mic                           STRING       -- TradVn (MIC code)
trade_country_of_branch                   STRING       -- CtryOfBrnch
trade_place_matching_id                   STRING       -- TradPlcMtchgId
complex_trade_component_id                STRING       -- CmplxTradCmpntId
```

#### Instrument — general + derivative attributes (~18 cols)
```
instrument_isin                           STRING       -- COALESCE(FinInstrm.Id, FinInstrm.Othr.FinInstrmGnlAttrbts.Id)
instrument_full_name                      STRING       -- Othr.FinInstrmGnlAttrbts.FullNm
instrument_classification                 STRING       -- Othr.FinInstrmGnlAttrbts.ClssfctnTp
instrument_notional_currency              STRING       -- Othr.FinInstrmGnlAttrbts.NtnlCcy
instrument_commodity_derivative           BOOLEAN      -- Othr.FinInstrmGnlAttrbts.CmmdtyDerivInd
interest_other_notional_currency          STRING       -- Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.Intrst.OthrNtnlCcy
fx_other_notional_currency                STRING       -- Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.FX.OthrNtnlCcy
instrument_price_multiplier               DECIMAL      -- Othr.DerivInstrmAttrbts.PricMltplr
instrument_delivery_type                  STRING       -- Othr.DerivInstrmAttrbts.DlvryTp
instrument_maturity_dt                    DATE         -- Othr.DerivInstrmAttrbts.MtrtyDt
instrument_expiry_dt                      DATE         -- Othr.DerivInstrmAttrbts.XpryDt
instrument_strike_price                   DECIMAL      -- StrkPric.MntryVal._VALUE
instrument_strike_price_ccy               STRING       -- StrkPric.MntryVal._Ccy
instrument_strike_price_percent           DECIMAL      -- StrkPric.Pctg
instrument_strike_price_yield             DECIMAL      -- StrkPric.Yld
instrument_option_type                    STRING       -- OptnTp
instrument_option_exercise_style          STRING       -- OptnExrcStyle
underlying_type                           STRING       -- 'SWAP' | 'INDEX' | 'BASKET' | 'OTHER' | NULL
```

#### Underlying instrument — 6 sub-prefix groups (~30 scalars + ~18 array columns)

For each of {`underlying_swap_in`, `underlying_swap_out`, `underlying_other`}, both single AND basket sub-prefixes:

```
-- swap_in single (when underlying_type='SWAP' and the IN-leg is Sngl)
underlying_swap_in_single_isin            STRING
underlying_swap_in_single_index_isin      STRING
underlying_swap_in_single_index_ref_rate_code STRING
underlying_swap_in_single_index_ref_rate_name STRING
underlying_swap_in_single_index_term_unit STRING
underlying_swap_in_single_index_term_value DECIMAL(3,0)

-- swap_in basket (when underlying_type='SWAP' and the IN-leg is Bskt)
underlying_swap_in_basket_isins           ARRAY<STRING>
underlying_swap_in_basket_index_isins     ARRAY<STRING>
underlying_swap_in_basket_index_ref_rate_codes ARRAY<STRING>
underlying_swap_in_basket_index_ref_rate_names ARRAY<STRING>
underlying_swap_in_basket_index_term_units ARRAY<STRING>
underlying_swap_in_basket_index_term_values ARRAY<DECIMAL(3,0)>
```

Same shape × 3 more groups: `underlying_swap_out_single_*`, `underlying_swap_out_basket_*`, `underlying_other_single_*`, `underlying_other_basket_*`. Total: 24 single-leg scalars + 24 basket-leg arrays = ~48 columns for the underlying section.

#### Investment decision person (~9 cols)
```
investment_decision_person_lei            STRING       -- New.InvstmtDcsnPrsn.LEI
investment_decision_person_first_name     STRING       -- Prsn.FrstNm                                          (PII)
investment_decision_person_last_name      STRING       -- Prsn.Nm                                              (PII)
investment_decision_person_birth_dt       DATE         -- Prsn.BirthDt                                         (PII)
investment_decision_person_country        STRING       -- Prsn.CtryOfBrnch
investment_decision_person_other_id       STRING       -- Prsn.Othr.Id                                         (PII)
investment_decision_person_other_scheme   STRING       -- Prsn.Othr.SchmeNm.Cd
investment_decision_person_other_scheme_proprietary STRING -- Prsn.Othr.SchmeNm.Prtry
investment_decision_algo_id               STRING       -- New.InvstmtDcsnPrsn.Algo
```

#### Executing person (~10 cols)
Mirror of investment decision person with `executing_person_*` prefix, PLUS:
```
executing_person_client_indicator         STRING       -- New.ExctgPrsn.Clnt
executing_algo_id                         STRING       -- New.ExctgPrsn.Algo
```

#### Additional attributes (`New.AddtlAttrbts`) (~6)
```
short_selling_indicator                   STRING       -- ShrtSellgInd (single value)
waiver_indicators                         ARRAY<STRING> -- WvrInd[]._VALUE (multi-valued)
otc_post_trade_indicators                 ARRAY<STRING> -- OTCPstTradInd[]._VALUE (multi-valued)
commodity_derivative_indicator            BOOLEAN      -- CmmdtyDerivInd (AddtlAttrbts-level — distinct from instrument_commodity_derivative)
risk_reducing_transaction                 BOOLEAN      -- RskRdcgTx
securities_financing_tx_indicator         BOOLEAN      -- SctiesFincgTxInd
trading_relevant_to_market                BOOLEAN      -- (other AddtlAttrbts flag if present)
```

#### Audit / lineage (~4)
```
file_path                                 STRING
file_name                                 STRING
ingested_at                               TIMESTAMP
silver_processed_at                       TIMESTAMP
```

**Total: ~135 scalars + ~15 array columns** covering 175 - 2 = 173 pyld leaves (corrupted_record + _sequence are bronze-side plumbing).

### 4.2 `transaction_party` — unified party explode (~18 cols)

**Grain:** one row per AcctOwnr OR DcsnMakr per side per transaction.

```
transaction_id                            STRING        -- FK to transaction.transaction_id
side                                      STRING        -- 'BUYER' | 'SELLER'
party_role                                STRING        -- 'ACCT_OWNR' | 'DCSN_MAKR'
sequence_no                               INT           -- posexplode position within its array
party_lei                                 STRING
party_other_id                            STRING
party_other_id_scheme                     STRING
party_other_id_scheme_proprietary         STRING
party_mic                                 STRING                  -- AcctOwnr only
party_intl_person_id                      STRING                  -- AcctOwnr only
party_country_of_branch                   STRING                  -- AcctOwnr only
person_first_name                         STRING        (PII)
person_last_name                          STRING        (PII)
person_birth_dt                           DATE          (PII)
person_country                            STRING
person_other_id                           STRING        (PII)
person_other_scheme                       STRING
person_other_scheme_proprietary           STRING
ingested_at                               TIMESTAMP
silver_processed_at                       TIMESTAMP
```

For the ~99% common case (single AcctOwnr per side, no DcsnMakr or LEI-only DcsnMakr), this table provides redundant fidelity over `transaction.buyer_*`/`seller_*`. The minority case (joint accounts, multiple decision makers) is faithfully captured here.

Built via `posexplode_outer` of each of 4 arrays (Buyr.AcctOwnr, Buyr.DcsnMakr, Sellr.AcctOwnr, Sellr.DcsnMakr), unioned with `side` + `party_role` discriminators, filtered to drop NULL rows.

### 4.3 `submission_file` — MiFIR-specific envelope (~270 cols)

**Grain:** one row per ingested MiFIR XML file (via `dropDuplicates(["file_path"])`).

Built via:
```python
spark.readStream.table(TBL_BRONZE)
  .dropDuplicates(["file_path"])
  .select(...)  # ~270 columns
```

#### File metadata (~10 cols)
```
file_path                                 STRING        -- PK
file_name                                 STRING
ingested_at                               TIMESTAMP
silver_processed_at                       TIMESTAMP
client_id_from_filename                   STRING        -- e.g., "9795" from "9795_20250729154019_3_sample_data.xml"
filename_timestamp                        STRING        -- "20250729154019"
filename_timestamp_parsed                 TIMESTAMP     -- parsed to TIMESTAMP for analytics
filename_sequence                         INT           -- "3"
reporting_date                            DATE          -- from filename_timestamp_parsed
regulation                                STRING        -- constant 'MIFIR'
```

#### UVHeader (UnaVista vendor wrapper) (4)
```
unavista_internal_client_id               STRING       -- UVHeader.UVHeader.InternalClientId
unavista_data_category                    STRING       -- UVHeader.UVHeader.DataCategory
unavista_submitting_entity_id             STRING       -- UVHeader.UVHeader.SubmittingEntityID
unavista_file_id                          STRING       -- UVHeader.UVHeader.FileID
```

#### AppHdr top-level (~10 cols)
```
header_char_set                           STRING       -- BizAppHeader.AppHdr.CharSet
biz_msg_id                                STRING       -- BizMsgIdr
message_def_id                            STRING       -- MsgDefIdr
business_service                          STRING       -- BizSvc
header_creation_ts                        TIMESTAMP    -- CreDt
copy_duplicate_indicator                  STRING       -- CpyDplct
possible_duplicate                        BOOLEAN      -- PssblDplct
priority                                  STRING       -- Prty
signature_xml                             STRING       -- Sgntr.xs_any (raw signature XML)
number_of_records                         BIGINT       -- COUNT(*) of Tx per file (derived)
```

#### Sender block — `Fr.OrgId` (~35 cols)
All `sender_*` prefix:
```
sender_bic                                STRING       -- Fr.OrgId.Id.OrgId.AnyBIC
sender_org_name                           STRING       -- Fr.OrgId.Nm
sender_org_address_type                   STRING       -- Fr.OrgId.PstlAdr.AdrTp
sender_org_department                     STRING
sender_org_sub_department                 STRING
sender_org_street_name                    STRING
sender_org_building_number                STRING
sender_org_post_code                      STRING
sender_org_town_name                      STRING
sender_org_country_sub_division           STRING
sender_org_country                        STRING
sender_org_address_lines                  ARRAY<STRING>
sender_org_other_ids                      ARRAY<STRING>          -- Fr.OrgId.Id.OrgId.Othr[].Id
sender_org_other_scheme_codes             ARRAY<STRING>
sender_org_other_scheme_proprietaries     ARRAY<STRING>
sender_org_other_issuers                  ARRAY<STRING>
sender_person_birth_dt                    DATE         (PII)     -- Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.BirthDt
sender_person_province_of_birth           STRING       (PII)
sender_person_city_of_birth               STRING       (PII)
sender_person_country_of_birth            STRING       (PII)
sender_person_other_ids                   ARRAY<STRING> (PII)
sender_person_other_scheme_codes          ARRAY<STRING>
sender_person_other_scheme_proprietaries  ARRAY<STRING>
sender_person_other_issuers               ARRAY<STRING>
sender_country_of_residence               STRING
sender_contact_name_prefix                STRING
sender_contact_name                       STRING
sender_contact_phone                      STRING
sender_contact_mobile                     STRING
sender_contact_fax                        STRING
sender_contact_email                      STRING
sender_contact_other                      STRING
```

#### Sender FI block — `Fr.FIId` (~29 cols)
All `sender_fi_*` prefix:
```
sender_fi_bic                             STRING
sender_fi_clearing_system_code            STRING
sender_fi_clearing_system_proprietary     STRING
sender_fi_clearing_member_id              STRING
sender_fi_name                            STRING
sender_fi_address_type                    STRING
sender_fi_department                      STRING
sender_fi_sub_department                  STRING
sender_fi_street_name                     STRING
sender_fi_building_number                 STRING
sender_fi_post_code                       STRING
sender_fi_town_name                       STRING
sender_fi_country_sub_division            STRING
sender_fi_country                         STRING
sender_fi_address_lines                   ARRAY<STRING>
sender_fi_other_id                        STRING
sender_fi_other_scheme_code               STRING
sender_fi_other_scheme_proprietary        STRING
sender_fi_other_issuer                    STRING
sender_fi_branch_id                       STRING
sender_fi_branch_name                     STRING
sender_fi_branch_address_type             STRING
sender_fi_branch_street_name              STRING
sender_fi_branch_building_number          STRING
sender_fi_branch_post_code                STRING
sender_fi_branch_town_name                STRING
sender_fi_branch_country_sub_division     STRING
sender_fi_branch_country                  STRING
sender_fi_branch_address_lines            ARRAY<STRING>
```

#### Recipient block — `To.OrgId` (~35 cols)
Mirror of Sender block with `recipient_*` prefix.

#### Recipient FI block — `To.FIId` (~29 cols)
Mirror of Sender FI block with `recipient_fi_*` prefix.

#### Related-message block — `AppHdr.Rltd.*` (~135 cols)
Full mirror of the entire AppHdr structure (Fr.OrgId + Fr.FIId + To.OrgId + To.FIId + BizMsgIdr + MsgDefIdr + BizSvc + CreDt + CpyDplct + PssblDplct + Prty + Sgntr + CharSet) with `related_*` prefix. Mostly NULL in production data (Rltd is used for corrections / amendments referencing a prior message) but captured for fidelity.

**Total submission_file: ~270 columns** covering all 274 hdr leaves.

## 5. Implementation Details

### 5.1 SDP source file structure

`src/pipelines/silver_mifir.py`:
- Module docstring + design-doc reference
- Imports (`from pyspark import pipelines as dp`, `from pyspark.sql import functions as F`, `from pyspark.sql import DataFrame`)
- Module-level config (`spark.conf.get(...)`): `catalog`, `raw_schema`, `silver_schema` (default = raw_schema), `bronze_table`, `regulation` (default 'MIFIR'), `enable_filename_regex` (default "true")
- Table-name constants: `TBL_BRONZE`, `TBL_TRANSACTION`, `TBL_TRANSACTION_PARTY`, `TBL_SUBMISSION_FILE`
- Helpers:
  - `_reporting_date(df)` — parses `filename_timestamp` (`YYYYMMDDhhmmss`) into a DATE
  - `_add_filename_regex_columns(df)` — MiFIR-specific filename parser; same customer-override pattern as bronze
- Three `@dp.table(cluster_by_auto=True)` functions: `submission_file`, `transaction_party`, `transaction`

### 5.2 Bundle resource (`resources/bundle.mifir_resources.yml`)

Add to the existing `# === Spark Declarative Pipelines ===` section:

```yaml
    mifir_silver_pipeline:
      name: "MiFIR Silver (domain-driven)"
      catalog: ${var.mifir_catalog}
      schema: ${var.mifir_raw_schema}
      serverless: true
      channel: PREVIEW
      development: false
      photon: true
      continuous: false

      libraries:
        - file:
            path: ../src/pipelines/silver_mifir.py

      configuration:
        catalog: ${var.mifir_catalog}
        raw_schema: ${var.mifir_raw_schema}
        silver_schema: ${var.mifir_raw_schema}
        bronze_table: ${var.mifir_table_prefix}_raw
        regulation: "MIFIR"
        enable_filename_regex: "true"
```

### 5.3 Target overrides (`databricks.yml`)

Add `mifir_silver_pipeline: { development: true|false }` under `targets.{dev,prod}.resources.pipelines`.

### 5.4 No new bundle variables

Reuses existing `${var.mifir_*}` from PR #1.

## 6. Validation Plan

### 6.1 Target environment
- Workspace `adb-984752964297111.11.azuredatabricks.net` (CLI profile `azure`)
- Catalog `esma_dev`, schema `default`
- Bronze `esma_dev.default.mifir_raw` (assumed populated; trigger MiFIR bronze pipeline first if needed)
- Sample data: `/Volumes/esma_dev/default/regulatory_data/mifir/landing/9795_20250729154019_3_sample_data.xml`

### 6.2 Test sequence
1. `databricks bundle validate -t dev --profile azure` — passes
2. Confirm bronze: `SELECT COUNT(*) FROM esma_dev.default.mifir_raw` — non-zero
3. `databricks bundle deploy -t dev --profile azure`
4. `databricks bundle run mifir_silver_pipeline -t dev --profile azure`
5. Row count invariants:
   - `transaction` row count = `mifir_raw` row count
   - `submission_file` = number of source files
   - `transaction_party` ≥ 2× transaction (1 AcctOwnr per side per NEW row, plus any DcsnMakr or multi-owner cases)
6. `action_type` distribution: `SELECT action_type, COUNT(*) FROM transaction GROUP BY action_type`
7. Spot-checks:
   - `buyer_lei` 20-char LEI
   - `trade_venue_mic` valid MIC (e.g., `XLON`)
   - `instrument_isin` 12-char
   - NEW rows have populated `New.*` paths; CXL rows have NULLs except 3 shared fields
8. Lifecycle smoke-test (if any CXL rows exist): verify NEW counterparts exist via the `WHERE NOT EXISTS` pattern from §3.1

### 6.3 Performance baseline
Wall time + cluster sizing for the smoke-test run, captured in `docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md`.

## 7. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Spec-drift on assumed XSD paths vs actual bronze struct shapes (same as EMIR's 8c55d73) | Audit script run during implementation; smoke test fails fast on path mismatches; spec lists ~135 + ~270 columns but exact names may shift |
| 270-col `submission_file` heavy | Delta column-prunes; row count is small (one per file); acceptable |
| Rltd block is 135 mostly-NULL columns | Delta storage cost minimal; analysts who don't need it just don't SELECT it |
| PII columns unmasked in silver | UC column-mask policies applied by data stewards via UC functions + ALTER TABLE — governance not pipeline concern |
| Sample data is single LSE file — limited XSD branch coverage | Real-data validation is a follow-up |
| MiFIR filename regex `<client_id>_<YYYYMMDDhhmmss>_<seq>_*.xml` may not match all customers | `_add_filename_regex_columns()` is a replaceable function (same customer-override pattern as bronze PR #1's TODO) |

## 8. Open Follow-Ups

- **SFTR silver** — separate brainstorm + spec + branch (`feat/sftr-silver`)
- **Gold layer** aggregations — defer to a follow-up after analyst queries are known
- **UC column-mask policies for PII** — separate governance branch (creates UC functions + applies via ALTER TABLE)
- **Real-customer-data validation pass** — synthetic single-file sample is insufficient for production confidence
- **Star-schema pivot** (`dim_legal_entity` shared across EMIR + MiFIR) — same as EMIR PR #3's documented future option
- **Cross-regulation `regulation_submissions` rolled-up VIEW** if analysts ever want "all files ingested today across both regs"
- **SCD Type 2 if lifecycle status as a materialized column on NEW rows is wanted** — currently captured implicitly via action_type + lifecycle query
- **Production MiFIR filename regex** for customers with different naming conventions

## 9. Approval

All six sections (scope, architecture, table definitions, implementation, validation, risks/follow-ups) reviewed and approved interactively. Decisions captured:
- 3-table cut: `transaction`, `transaction_party`, `submission_file`
- Wide-flat with business-readable names, choice fields collapsed to LEI + fallback
- `DcsnMakr` is an ARRAY — explodes into `transaction_party` with `party_role='DCSN_MAKR'`
- 100% bronze leaf coverage (all 449 leaves): includes all `Othr.SchmeNm.Prtry` companions, the full BAH party + FI blocks, and the 135-leaf `Rltd` related-message mirror
- ~135 + ~15-array cols on `transaction`, ~18 on `transaction_party`, ~270 on `submission_file`
- PII handling delegated to UC column-mask policies (external governance)
- Customer-replaceable `_add_filename_regex_columns()` for non-default MiFIR filename conventions
- SCD: append-only with `action_type` discriminator
- Schema layout: silver tables in `{catalog}.{mifir_raw_schema}` (same schema as bronze, no prefix on silver names)
- Branch `feat/mifir-silver`; PR #4 when opened
