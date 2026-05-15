# MiFIR Silver Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a domain-driven MiFIR transaction-reporting silver layer (3 tables — `transaction`, `transaction_party`, `submission_file`) on top of the bronze table `mifir_raw` on the Azure workspace, achieving 100% bronze leaf coverage (~449 leaves across pyld + hdr schemas).

**Architecture:** A new SDP source file `src/pipelines/silver_mifir.py` defines three `@dp.table()` streaming tables reading from `esma_dev.default.mifir_raw`. `transaction` is wide-flat (~135 scalars + ~15 array columns) with business-readable names and an `action_type ∈ {'NEW', 'CXL'}` discriminator. `transaction_party` is a unified explode of `Buyr.AcctOwnr` + `Buyr.DcsnMakr` + `Sellr.AcctOwnr` + `Sellr.DcsnMakr` with `side` + `party_role` discriminators. `submission_file` is the MiFIR-specific envelope (~270 cols including the 135-leaf `Rltd` related-message block). Append-only, `cluster_by_auto=True`, serverless + Photon.

**Tech Stack:** Python 3 + `pyspark.pipelines` (`from pyspark import pipelines as dp`), Delta Lake, Unity Catalog, Databricks Asset Bundles, serverless SDP compute on Azure workspace `adb-984752964297111.11.azuredatabricks.net` (CLI profile `azure`).

**Reference spec:** [`docs/superpowers/specs/2026-05-12-mifir-silver-design.md`](../specs/2026-05-12-mifir-silver-design.md)

---

## File Plan

| File | Action | Responsibility |
|------|--------|---------------|
| `src/pipelines/silver_mifir.py` | Create | All 3 `@dp.table()` definitions, ~900-1100 lines, parameterized via `spark.conf` |
| `resources/bundle.mifir_resources.yml` | Modify | Add `mifir_silver_pipeline` under the existing `# === Spark Declarative Pipelines ===` section |
| `databricks.yml` | Modify | Add `development: true|false` target overrides for `mifir_silver_pipeline` in dev + prod |
| `docs/superpowers/plans/2026-05-12-mifir-silver.md` | Create | This file |
| `docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md` | Create | Captured at Task 15 |

**File splits:**
- One SDP source file holds all three tables because they share bronze source + config + helpers.
- `submission_file()` is the largest function (~270 column projections) — built incrementally across Tasks 2-6 with one section per commit.
- `transaction()` is built incrementally across Tasks 8-11 (similar to EMIR Tasks 5-9).

---

## Branch Setup

Work is performed on branch `feat/mifir-silver` (already created off `feat/sdp-xml-loader`, currently at commit `0528bce` which is the spec).

---

## Task 1: Scaffold `src/pipelines/silver_mifir.py`

**Files:**
- Create: `src/pipelines/silver_mifir.py`

- [ ] **Step 1.1: Confirm branch state**

Run:
```bash
git status && git branch --show-current && git log --oneline -3
```
Expected: clean tree, branch `feat/mifir-silver`, HEAD at `0528bce`.

- [ ] **Step 1.2: Create the file with scaffold + helpers**

Write `src/pipelines/silver_mifir.py` containing exactly:

```python
"""ESMA MiFIR Transaction-Reporting Silver Layer.

Domain-driven silver layer on top of bronze ``mifir_raw``
(auth.016.001.01_ESMAUG_Reporting). Three tables:

* ``transaction`` — wide-flat fact table, one row per ``<Tx>`` element
  with ``action_type`` discriminator (NEW / CXL). ~135 scalars +
  ~15 array columns covering identification, buyer/seller flat,
  trade details, instrument + 6 underlying-instrument prefix groups,
  investment-decision person, executing person, additional attributes,
  audit.
* ``transaction_party`` — unified explode of Buyr.AcctOwnr +
  Buyr.DcsnMakr + Sellr.AcctOwnr + Sellr.DcsnMakr with side and
  party_role discriminators. ~18 cols.
* ``submission_file`` — MiFIR-specific envelope including UVHeader
  (UnaVista vendor wrapper) + full BizAppHeader (AppHdr top-level +
  Sender/Recipient OrgId + FIId blocks + 135-leaf Rltd related-
  message mirror). ~270 cols.

All inputs are supplied via ``spark.conf`` — see the MiFIR silver
pipeline ``configuration`` block in
``resources/bundle.mifir_resources.yml``.

Reference: docs/superpowers/specs/2026-05-12-mifir-silver-design.md
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --------------------------------------------------------------------------
# Pipeline configuration (set in resources/bundle.mifir_resources.yml under
# resources.pipelines.mifir_silver_pipeline.configuration).
# --------------------------------------------------------------------------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema", RAW_SCHEMA)
BRONZE_TABLE_NAME = spark.conf.get("bronze_table")
REGULATION = spark.conf.get("regulation", "MIFIR")
ENABLE_FILENAME_REGEX = spark.conf.get("enable_filename_regex", "true").lower() == "true"

TBL_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_TABLE_NAME}"
TBL_TRANSACTION = f"{CATALOG}.{SILVER_SCHEMA}.transaction"
TBL_TRANSACTION_PARTY = f"{CATALOG}.{SILVER_SCHEMA}.transaction_party"
TBL_SUBMISSION_FILE = f"{CATALOG}.{SILVER_SCHEMA}.submission_file"


# --------------------------------------------------------------------------
# Filename regex extraction (customer-replaceable).
#
# Default MiFIR convention (e.g., 9795_20250729154019_3_sample_data.xml):
#   <client_id>_<YYYYMMDDhhmmss>_<sequence>_<rest>.xml
#
# TODO (customer): customers with a different filename convention should
# REPLACE THIS FUNCTION rather than editing the @dp.table definitions.
# The four output column names must stay the same so downstream consumers
# keep working; the extraction logic inside is yours to redefine.
#
# Set ENABLE_FILENAME_REGEX=false to skip extraction entirely (columns
# emit NULL while preserving the schema).
# --------------------------------------------------------------------------

_MIFIR_CLIENT_ID_PATTERN = r"^(\d+)_"
_MIFIR_TIMESTAMP_PATTERN = r"^\d+_(\d{14})_"
_MIFIR_SEQUENCE_PATTERN = r"^\d+_\d{14}_(\d+)_"


def _add_filename_regex_columns(df: DataFrame) -> DataFrame:
    """Add MiFIR filename-derived columns to a DataFrame with a ``file_name`` column.

    Returns the DataFrame with four columns appended:
    ``client_id_from_filename``, ``filename_timestamp``,
    ``filename_timestamp_parsed``, ``filename_sequence``.

    Default implementation parses the UnaVista MiFIR convention:
    ``<client_id>_<YYYYMMDDhhmmss>_<sequence>_<rest>.xml``.
    """
    if not ENABLE_FILENAME_REGEX:
        return (
            df
            .withColumn("client_id_from_filename", F.lit(None).cast("string"))
            .withColumn("filename_timestamp", F.lit(None).cast("string"))
            .withColumn("filename_timestamp_parsed", F.lit(None).cast("timestamp"))
            .withColumn("filename_sequence", F.lit(None).cast("int"))
        )
    return (
        df
        .withColumn(
            "client_id_from_filename",
            F.regexp_extract(F.col("file_name"), _MIFIR_CLIENT_ID_PATTERN, 1),
        )
        .withColumn(
            "filename_timestamp",
            F.regexp_extract(F.col("file_name"), _MIFIR_TIMESTAMP_PATTERN, 1),
        )
        .withColumn(
            "filename_timestamp_parsed",
            F.to_timestamp(
                F.regexp_extract(F.col("file_name"), _MIFIR_TIMESTAMP_PATTERN, 1),
                "yyyyMMddHHmmss",
            ),
        )
        .withColumn(
            "filename_sequence",
            F.regexp_extract(F.col("file_name"), _MIFIR_SEQUENCE_PATTERN, 1).cast("int"),
        )
    )


def _reporting_date(df: DataFrame) -> DataFrame:
    """Add a ``reporting_date`` DATE column derived from the filename timestamp.

    Falls back to the file modification time if the filename timestamp
    couldn't be parsed (e.g., a non-default filename convention with the
    regex toggle off).
    """
    return df.withColumn(
        "reporting_date",
        F.coalesce(
            F.to_date(F.col("filename_timestamp_parsed")),
            F.to_date(F.col("_file_modification_time")),
        ),
    )
```

- [ ] **Step 1.3: Verify the file parses**

Run:
```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
```
Expected: silent success.

- [ ] **Step 1.4: Commit**

```bash
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): scaffold silver_mifir.py module skeleton

Adds the MiFIR silver SDP source file with:
- Module docstring + design-doc reference
- Modern API import (from pyspark import pipelines as dp)
- Module-level spark.conf reads for catalog, raw_schema,
  silver_schema, bronze_table, regulation, enable_filename_regex
- Four fully-qualified table-name constants (TBL_BRONZE,
  TBL_TRANSACTION, TBL_TRANSACTION_PARTY, TBL_SUBMISSION_FILE)
- _add_filename_regex_columns() helper with the MiFIR-specific
  client_id/timestamp/sequence regex pattern. Customer-replaceable
  per the spec — TODO comment in function body explains the
  override surface.
- _reporting_date() helper that derives a DATE from the parsed
  filename timestamp with a file-modification-time fallback.

No @dp.table definitions yet — those land in subsequent commits
(Tasks 2-11).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 2: `submission_file` — file metadata + UVHeader + AppHdr top-level

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (append)

This is the first chunk of `submission_file` — file lineage, UVHeader vendor wrapper, and the AppHdr top-level scalars. Sender/Recipient/Rltd blocks land in subsequent tasks.

- [ ] **Step 2.1: Append the initial `@dp.table` for submission_file**

Append to `src/pipelines/silver_mifir.py`:

```python


# --------------------------------------------------------------------------
# Table 1 of 3: submission_file (MiFIR-specific file-level envelope)
#
# Built incrementally — each subsequent commit adds one logical section.
# Final shape: ~270 columns covering UVHeader + full BizAppHeader (AppHdr
# top-level + Sender Fr.OrgId/FIId + Recipient To.OrgId/FIId + Rltd
# related-message mirror) per spec §4.3.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_SUBMISSION_FILE,
    comment=(
        "Public: one row per ingested MiFIR XML file. MiFIR-specific shape "
        "(distinct from EMIR's submission_file) including UVHeader vendor "
        "wrapper, full BizAppHeader, and the 135-leaf Rltd related-message "
        "block. Built from a dropDuplicates over the bronze stream."
    ),
    cluster_by_auto=True,
)
def submission_file():
    return (
        _reporting_date(_add_filename_regex_columns(
            spark.readStream.table(TBL_BRONZE)
        ))
        .dropDuplicates(["file_path"])
        .select(
            # File metadata (~10 cols)
            F.col("file_path"),
            F.col("file_name"),
            F.col("_ingested_at").alias("ingested_at"),
            F.current_timestamp().alias("silver_processed_at"),
            F.col("client_id_from_filename"),
            F.col("filename_timestamp"),
            F.col("filename_timestamp_parsed"),
            F.col("filename_sequence"),
            F.col("reporting_date"),
            F.lit(REGULATION).alias("regulation"),

            # UVHeader (UnaVista vendor wrapper, 4 cols)
            F.col("hdr_pyld_metadata.UVHeader.UVHeader.InternalClientId").alias("unavista_internal_client_id"),
            F.col("hdr_pyld_metadata.UVHeader.UVHeader.DataCategory").alias("unavista_data_category"),
            F.col("hdr_pyld_metadata.UVHeader.UVHeader.SubmittingEntityID").alias("unavista_submitting_entity_id"),
            F.col("hdr_pyld_metadata.UVHeader.UVHeader.FileID").alias("unavista_file_id"),

            # AppHdr top-level (10 cols)
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.CharSet").alias("header_char_set"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.BizMsgIdr").alias("biz_msg_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.MsgDefIdr").alias("message_def_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.BizSvc").alias("business_service"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.CreDt").alias("header_creation_ts"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.CpyDplct").alias("copy_duplicate_indicator"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.PssblDplct").alias("possible_duplicate"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Prty").alias("priority"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Sgntr").cast("string").alias("signature_xml"),
            F.lit(None).cast("bigint").alias("number_of_records"),  # filled by a downstream agg in v2; for now placeholder
        )
    )
```

- [ ] **Step 2.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
```

- [ ] **Step 2.3: Commit**

```bash
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): submission_file initial — file metadata + UVHeader + AppHdr top-level

First commit for submission_file. Adds the @dp.table decorator +
function shell with the first ~24 columns: file metadata (path/name/
ingested_at/silver_processed_at + the 4 filename-derived cols from
the helper), the UnaVista UVHeader wrapper (InternalClientId,
DataCategory, SubmittingEntityID, FileID), and the AppHdr top-level
scalars (CharSet, BizMsgIdr, MsgDefIdr, BizSvc, CreDt, CpyDplct,
PssblDplct, Prty, Sgntr-as-string). number_of_records is a NULL
placeholder for v1.

Subsequent commits expand the .select() to cover Sender Fr.OrgId,
Fr.FIId, Recipient To mirror, and the 135-leaf Rltd block.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 3: `submission_file` — Sender `Fr.OrgId` block (~35 cols)

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert into existing `submission_file()`)

- [ ] **Step 3.1: Extend the `.select(...)` body**

In `src/pipelines/silver_mifir.py` `submission_file()`, INSERT the following columns into the existing `.select(...)` chain immediately AFTER the AppHdr top-level group (which ends with the `number_of_records` placeholder):

```python
            # === Sender (Fr.OrgId) — full party-identification block (~31 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.OrgId.AnyBIC").alias("sender_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Nm").alias("sender_org_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.AdrTp").alias("sender_org_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.Dept").alias("sender_org_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.SubDept").alias("sender_org_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.StrtNm").alias("sender_org_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.BldgNb").alias("sender_org_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.PstCd").alias("sender_org_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.TwnNm").alias("sender_org_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.CtrySubDvsn").alias("sender_org_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.Ctry").alias("sender_org_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.PstlAdr.AdrLine").alias("sender_org_address_lines"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["Id"],
            ).alias("sender_org_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("sender_org_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("sender_org_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["Issr"],
            ).alias("sender_org_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.BirthDt").alias("sender_person_birth_dt"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.PrvcOfBirth").alias("sender_person_province_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.CityOfBirth").alias("sender_person_city_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.CtryOfBirth").alias("sender_person_country_of_birth"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Id"],
            ).alias("sender_person_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("sender_person_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("sender_person_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Issr"],
            ).alias("sender_person_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtryOfRes").alias("sender_country_of_residence"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.NmPrfx").alias("sender_contact_name_prefix"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.Nm").alias("sender_contact_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.PhneNb").alias("sender_contact_phone"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.MobNb").alias("sender_contact_mobile"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.FaxNb").alias("sender_contact_fax"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.EmailAdr").alias("sender_contact_email"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.OrgId.CtctDtls.Othr").alias("sender_contact_other"),
```

- [ ] **Step 3.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): submission_file — Sender Fr.OrgId block (~32 cols)

Adds the full ISO 20022 sender organisation/party block under
BizAppHeader.AppHdr.Fr.OrgId: BIC, name, postal address (8 cols
incl. address_lines array), Id.OrgId.Othr[] arrays (id, scheme_cd,
scheme_proprietary, issuer), Id.PrvtId date-and-place-of-birth +
Othr[] arrays, country of residence, and contact details (6 cols).
F.transform() lambdas flatten the OrgId.Othr[] and PrvtId.Othr[]
arrays of {Id, SchmeNm.Cd, SchmeNm.Prtry, Issr} structs into
parallel ARRAY<STRING> columns so analysts can array_contains()
without exploding.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 4: `submission_file` — Sender `Fr.FIId` block (~29 cols)

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert)

- [ ] **Step 4.1: Extend the `.select(...)` body**

INSERT after the Sender Fr.OrgId block:

```python
            # === Sender FI (Fr.FIId) — financial-institution block (~29 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.BICFI").alias("sender_fi_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Cd").alias("sender_fi_clearing_system_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Prtry").alias("sender_fi_clearing_system_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.ClrSysMmbId.MmbId").alias("sender_fi_clearing_member_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.Nm").alias("sender_fi_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.AdrTp").alias("sender_fi_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.Dept").alias("sender_fi_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.SubDept").alias("sender_fi_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.StrtNm").alias("sender_fi_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.BldgNb").alias("sender_fi_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.PstCd").alias("sender_fi_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.TwnNm").alias("sender_fi_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.CtrySubDvsn").alias("sender_fi_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.Ctry").alias("sender_fi_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.PstlAdr.AdrLine").alias("sender_fi_address_lines"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.Othr.Id").alias("sender_fi_other_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.Othr.SchmeNm.Cd").alias("sender_fi_other_scheme_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.Othr.SchmeNm.Prtry").alias("sender_fi_other_scheme_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.FinInstnId.Othr.Issr").alias("sender_fi_other_issuer"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.Id").alias("sender_fi_branch_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.Nm").alias("sender_fi_branch_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.AdrTp").alias("sender_fi_branch_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.StrtNm").alias("sender_fi_branch_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.BldgNb").alias("sender_fi_branch_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.PstCd").alias("sender_fi_branch_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.TwnNm").alias("sender_fi_branch_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.CtrySubDvsn").alias("sender_fi_branch_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.Ctry").alias("sender_fi_branch_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Fr.FIId.BrnchId.PstlAdr.AdrLine").alias("sender_fi_branch_address_lines"),
```

- [ ] **Step 4.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): submission_file — Sender FI (Fr.FIId) block (~29 cols)

Adds the parallel financial-institution representation of the
sender under BizAppHeader.AppHdr.Fr.FIId.FinInstnId: BICFI,
ClrSysMmbId (3 cols), institution name, full postal address
(10 cols incl. address_lines array), Othr identification
(4 cols), plus a complete BrnchId sub-block including its own
name and postal address (10 cols).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 5: `submission_file` — Recipient `To.OrgId` + `To.FIId` blocks (~64 cols)

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert)

Mirror of Tasks 3 + 4 with `recipient_*` / `recipient_fi_*` prefixes and `To.OrgId` / `To.FIId` paths.

- [ ] **Step 5.1: Extend the `.select(...)` body — Recipient `To.OrgId`**

INSERT after the Sender FI block. Repeat all 32 columns from Task 3 but:
- Replace `Fr.OrgId` with `To.OrgId` in every path
- Replace `sender_*` with `recipient_*` in every alias

For example:
```python
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.AnyBIC").alias("recipient_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Nm").alias("recipient_org_name"),
            # ... and so on for all 32 columns from Task 3
```

- [ ] **Step 5.2: Append Recipient `To.FIId`**

INSERT after the Recipient To.OrgId block. Repeat all 29 columns from Task 4 but:
- Replace `Fr.FIId` with `To.FIId` in every path
- Replace `sender_fi_*` with `recipient_fi_*` in every alias

- [ ] **Step 5.3: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): submission_file — Recipient (To.OrgId + To.FIId) blocks (~64 cols)

Adds the structural mirror of the Sender block for the recipient
party under BizAppHeader.AppHdr.To: full To.OrgId (BIC, org name,
postal address, OrgId.Othr arrays, PrvtId date-place-of-birth +
Othr arrays, country of residence, contact details — 32 cols with
recipient_* prefix) plus To.FIId (BICFI, clearing system, name,
address, Othr ID, branch — 29 cols with recipient_fi_* prefix).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 6: `submission_file` — Rltd related-message block (~135 cols)

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert)

This is the largest single chunk — a full mirror of all preceding AppHdr structure (top-level + Sender OrgId + Sender FIId + Recipient OrgId + Recipient FIId) under `BizAppHeader.AppHdr.Rltd.*` with `related_*` prefix on every alias.

- [ ] **Step 6.1: Add the Rltd top-level fields (~10 cols)**

INSERT after the Recipient FI block:

```python
            # === Related message (BizAppHeader.AppHdr.Rltd) — full mirror ===
            # Rltd is OPTIONAL in MiFIR — used for amendments/corrections
            # referencing a prior message. ~135 columns total; most NULL
            # in production data.
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.CharSet").alias("related_char_set"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.BizMsgIdr").alias("related_biz_msg_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.MsgDefIdr").alias("related_message_def_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.BizSvc").alias("related_business_service"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.CreDt").alias("related_header_creation_ts"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.CpyDplct").alias("related_copy_duplicate_indicator"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.PssblDplct").alias("related_possible_duplicate"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Prty").alias("related_priority"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Sgntr").cast("string").alias("related_signature_xml"),
```

- [ ] **Step 6.2: Add Rltd.Fr.OrgId mirror (~32 cols)**

INSERT immediately after Step 6.1. Repeat Task 3's 32-col block but:
- Replace `BizAppHeader.AppHdr.Fr.OrgId` with `BizAppHeader.AppHdr.Rltd.Fr.OrgId` in every path
- Replace `sender_*` with `related_sender_*` in every alias

- [ ] **Step 6.3: Add Rltd.Fr.FIId mirror (~29 cols)**

INSERT after Step 6.2. Repeat Task 4's 29-col block with paths/prefixes:
- `BizAppHeader.AppHdr.Fr.FIId` → `BizAppHeader.AppHdr.Rltd.Fr.FIId`
- `sender_fi_*` → `related_sender_fi_*`

- [ ] **Step 6.4: Add Rltd.To.OrgId mirror (~32 cols)**

INSERT after Step 6.3. Repeat Task 5.1's 32-col block with paths/prefixes:
- `BizAppHeader.AppHdr.To.OrgId` → `BizAppHeader.AppHdr.Rltd.To.OrgId`
- `recipient_*` → `related_recipient_*`

- [ ] **Step 6.5: Add Rltd.To.FIId mirror (~29 cols)**

INSERT after Step 6.4. Repeat Task 5.2's 29-col block with paths/prefixes:
- `BizAppHeader.AppHdr.To.FIId` → `BizAppHeader.AppHdr.Rltd.To.FIId`
- `recipient_fi_*` → `related_recipient_fi_*`

- [ ] **Step 6.6: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): submission_file — Rltd related-message block (~131 cols)

Adds the full BizAppHeader.AppHdr.Rltd mirror to submission_file
with related_* prefix. Structural copy of all preceding AppHdr
sections: top-level scalars (9 cols), Rltd.Fr.OrgId (32 cols),
Rltd.Fr.FIId (29 cols), Rltd.To.OrgId (32 cols), Rltd.To.FIId
(29 cols). Used for corrections/amendments referencing a prior
message. Mostly NULL in production data — captured per the spec
mandate for 100% bronze coverage.

submission_file column inventory complete per spec §4.3.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 7: `transaction_party` table

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (append)

Unified posexplode of 4 arrays (Buyr × {AcctOwnr, DcsnMakr}, Sellr × {AcctOwnr, DcsnMakr}) into one table.

- [ ] **Step 7.1: Append the @dp.table for transaction_party**

Append to `src/pipelines/silver_mifir.py`:

```python


# --------------------------------------------------------------------------
# Table 2 of 3: transaction_party (unified party explode)
#
# Built via four posexplode_outer operations (one per array), unioned
# with side ∈ {BUYER, SELLER} and party_role ∈ {ACCT_OWNR, DCSN_MAKR}
# discriminators. Filtered to drop NULL rows from posexplode_outer.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRANSACTION_PARTY,
    comment=(
        "Public: one row per repeating party identification per "
        "transaction. Unified explode of Buyr.AcctOwnr[], Buyr.DcsnMakr[], "
        "Sellr.AcctOwnr[], Sellr.DcsnMakr[] with side + party_role "
        "discriminators. Common case (single AcctOwnr per side) "
        "duplicates transaction.{buyer,seller}_* — multi-owner cases live "
        "here only."
    ),
    cluster_by_auto=True,
)
def transaction_party():
    bronze = spark.readStream.table(TBL_BRONZE)

    def _explode_party(side: str, party_role: str, array_path: str):
        """Posexplode-outer one party array, project unified row schema."""
        return (
            bronze
            .select(
                F.col("New.TxId").alias("transaction_id"),
                F.lit(side).alias("side"),
                F.lit(party_role).alias("party_role"),
                F.col("_ingested_at").alias("ingested_at"),
                F.posexplode_outer(F.col(array_path)).alias("sequence_no", "_party"),
            )
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("_party").isNotNull())
            .select(
                "transaction_id",
                "side",
                "party_role",
                "sequence_no",
                F.col("_party.Id.LEI").alias("party_lei"),
                F.col("_party.Id.Othr.Id").alias("party_other_id"),
                F.col("_party.Id.Othr.SchmeNm.Cd").alias("party_other_id_scheme"),
                F.col("_party.Id.Othr.SchmeNm.Prtry").alias("party_other_id_scheme_proprietary"),
                F.col("_party.Id.MIC").alias("party_mic"),
                F.col("_party.Id.Intl").alias("party_intl_person_id"),
                F.col("_party.CtryOfBrnch").alias("party_country_of_branch"),
                F.col("_party.Id.Prsn.FrstNm").alias("person_first_name"),
                F.col("_party.Id.Prsn.Nm").alias("person_last_name"),
                F.col("_party.Id.Prsn.BirthDt").alias("person_birth_dt"),
                F.col("_party.Id.Prsn.CtryOfBrnch").alias("person_country"),
                F.col("_party.Id.Prsn.Othr.Id").alias("person_other_id"),
                F.col("_party.Id.Prsn.Othr.SchmeNm.Cd").alias("person_other_scheme"),
                F.col("_party.Id.Prsn.Othr.SchmeNm.Prtry").alias("person_other_scheme_proprietary"),
                F.col("ingested_at"),
                F.current_timestamp().alias("silver_processed_at"),
            )
        )

    return (
        _explode_party("BUYER",  "ACCT_OWNR", "New.Buyr.AcctOwnr")
        .unionByName(_explode_party("BUYER",  "DCSN_MAKR", "New.Buyr.DcsnMakr"), allowMissingColumns=True)
        .unionByName(_explode_party("SELLER", "ACCT_OWNR", "New.Sellr.AcctOwnr"), allowMissingColumns=True)
        .unionByName(_explode_party("SELLER", "DCSN_MAKR", "New.Sellr.DcsnMakr"), allowMissingColumns=True)
    )
```

- [ ] **Step 7.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): add transaction_party table

Second of three @dp.table definitions. Unions four posexplode_outer
operations into one streaming table with side ∈ {BUYER, SELLER}
and party_role ∈ {ACCT_OWNR, DCSN_MAKR} discriminators. Helper
_explode_party() projects a unified row shape (party identification
+ optional Prsn natural-person fields) per side/role combo. Rows
where the array element is NULL are filtered out via .filter().
CXL transactions land in transaction but produce no party rows
(New.Buyr/Sellr paths are NULL for CXL action_type).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 8: `transaction` — scaffold + identification + buyer/seller flat + order transmission

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (append)

- [ ] **Step 8.1: Append the @dp.table function shell + first column groups**

Append to `src/pipelines/silver_mifir.py`:

```python


# --------------------------------------------------------------------------
# Table 3 of 3: transaction (main fact, ~135 scalars + ~15 arrays)
#
# One row per <Tx> element. action_type discriminator: NEW (full
# transaction with all fields) or CXL (cancellation, 3 shared fields
# only — rest NULL). Built incrementally — each subsequent commit adds
# one logical XSD section's columns to the .select(...) below.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRANSACTION,
    comment=(
        "Public: per-MiFIR-transaction snapshot. Wide-flat with business-"
        "readable column names. action_type ∈ {'NEW', 'CXL'} discriminator. "
        "Choice fields collapsed to LEI + *_other_id fallback. Append-only "
        "(event-based — not snapshot like EMIR). See spec docs/superpowers/"
        "specs/2026-05-12-mifir-silver-design.md."
    ),
    cluster_by_auto=True,
)
def transaction():
    src = _reporting_date(_add_filename_regex_columns(
        spark.readStream.table(TBL_BRONZE)
    ))
    new_buy = "New.Buyr.AcctOwnr"
    new_sell = "New.Sellr.AcctOwnr"
    return src.select(
        # === Identification (5) ===
        F.coalesce(F.col("New.TxId"), F.col("Cxl.TxId")).alias("transaction_id"),
        F.when(F.col("New").isNotNull(), F.lit("NEW"))
         .when(F.col("Cxl").isNotNull(), F.lit("CXL"))
         .otherwise(F.lit("UNKNOWN"))
         .alias("action_type"),
        F.coalesce(F.col("New.ExctgPty"), F.col("Cxl.ExctgPty")).alias("executing_party_lei"),
        F.coalesce(F.col("New.SubmitgPty"), F.col("Cxl.SubmitgPty")).alias("submitting_party_lei"),
        F.col("New.InvstmtPtyInd").alias("investment_party_indicator"),

        # === Buyer flat fields — first AcctOwnr only (9 cols) ===
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("LEI").alias("buyer_lei"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("Othr").getField("Id").alias("buyer_other_id"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("Othr").getField("SchmeNm").getField("Cd").alias("buyer_other_id_scheme"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("Othr").getField("SchmeNm").getField("Prtry").alias("buyer_other_id_scheme_proprietary"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("MIC").alias("buyer_mic"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("Intl").alias("buyer_intl_person_id"),
        F.col(f"{new_buy}").getItem(0).getField("CtryOfBrnch").alias("buyer_country_of_branch"),
        F.size(F.col("New.Buyr.AcctOwnr")).alias("buyer_account_owner_count"),
        F.size(F.col("New.Buyr.DcsnMakr")).alias("buyer_decision_maker_count"),

        # === Seller flat fields — mirror of buyer (9 cols) ===
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("LEI").alias("seller_lei"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("Othr").getField("Id").alias("seller_other_id"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("Othr").getField("SchmeNm").getField("Cd").alias("seller_other_id_scheme"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("Othr").getField("SchmeNm").getField("Prtry").alias("seller_other_id_scheme_proprietary"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("MIC").alias("seller_mic"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("Intl").alias("seller_intl_person_id"),
        F.col(f"{new_sell}").getItem(0).getField("CtryOfBrnch").alias("seller_country_of_branch"),
        F.size(F.col("New.Sellr.AcctOwnr")).alias("seller_account_owner_count"),
        F.size(F.col("New.Sellr.DcsnMakr")).alias("seller_decision_maker_count"),

        # === Order transmission (3) ===
        F.col("New.OrdrTrnsmssn.TrnsmssnInd").alias("order_transmission_indicator"),
        F.col("New.OrdrTrnsmssn.TrnsmttgBuyr").alias("order_transmitting_buyer_lei"),
        F.col("New.OrdrTrnsmssn.TrnsmttgSellr").alias("order_transmitting_seller_lei"),

        # === Audit / lineage (4) ===
        F.col("file_path"),
        F.col("file_name"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
```

- [ ] **Step 8.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): scaffold transaction table with identification + buyer/seller + audit

Third of three @dp.table definitions, started incrementally. Adds
the function shell + the first 4 column groups:

- Identification (5): transaction_id (COALESCE New.TxId / Cxl.TxId),
  action_type discriminator via F.when chain, executing_party_lei,
  submitting_party_lei, investment_party_indicator
- Buyer flat (9): first AcctOwnr fields + buyer_account_owner_count
  + buyer_decision_maker_count via F.size on the underlying arrays
- Seller mirror (9): same with seller_* prefix
- Order transmission (3): indicator + buyer/seller transmitting LEIs
- Audit / lineage (4): file_path, file_name, ingested_at,
  silver_processed_at

Subsequent commits add trade details (Task 9), instrument and
underlying-instrument blocks (Task 10), and decision/executing
person + additional attributes (Task 11).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 9: `transaction` — trade details (`New.Tx` nested struct)

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert into existing `transaction()` body)

Adds the 24 columns from the trade details section (`New.Tx.*`).

- [ ] **Step 9.1: Extend the `.select(...)` body**

INSERT these columns into the existing `transaction()` `.select(...)` chain immediately AFTER the order transmission group and BEFORE the audit / lineage group:

```python
        # === Trade details (New.Tx nested struct, ~24 cols) ===
        F.col("New.Tx.TradDt").alias("trade_dt"),
        F.col("New.Tx.TradgCpcty").alias("trading_capacity"),
        F.col("New.Tx.Qty.Unit").alias("quantity_unit"),
        F.col("New.Tx.Qty.NmnlVal._VALUE").alias("quantity_nominal_value"),
        F.col("New.Tx.Qty.NmnlVal._Ccy").alias("quantity_nominal_currency"),
        F.col("New.Tx.Qty.MntryVal._VALUE").alias("quantity_monetary_value"),
        F.col("New.Tx.Qty.MntryVal._Ccy").alias("quantity_monetary_currency"),
        F.col("New.Tx.DerivNtnlChng").alias("derivative_notional_change"),
        F.col("New.Tx.Pric.Pric.MntryVal.Amt._VALUE").alias("price_amount"),
        F.col("New.Tx.Pric.Pric.MntryVal.Amt._Ccy").alias("price_currency"),
        F.col("New.Tx.Pric.Pric.MntryVal.Sgn").alias("price_sign"),
        F.col("New.Tx.Pric.Pric.Pctg").alias("price_percentage"),
        F.col("New.Tx.Pric.Pric.Yld").alias("price_yield"),
        F.col("New.Tx.Pric.Pric.BsisPts").alias("price_basis_points"),
        F.col("New.Tx.Pric.NoPric.Pdg").alias("price_pending_reason"),
        F.col("New.Tx.Pric.NoPric.Ccy").alias("price_pending_currency"),
        F.col("New.Tx.UpFrntPmt.Amt._VALUE").alias("up_front_payment_amount"),
        F.col("New.Tx.UpFrntPmt.Amt._Ccy").alias("up_front_payment_currency"),
        F.col("New.Tx.UpFrntPmt.Sgn").alias("up_front_payment_sign"),
        F.col("New.Tx.NetAmt").alias("net_amount"),
        F.col("New.Tx.TradVn").alias("trade_venue_mic"),
        F.col("New.Tx.CtryOfBrnch").alias("trade_country_of_branch"),
        F.col("New.Tx.TradPlcMtchgId").alias("trade_place_matching_id"),
        F.col("New.Tx.CmplxTradCmpntId").alias("complex_trade_component_id"),
```

- [ ] **Step 9.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): add trade details columns to transaction (~24 cols)

Inserts the New.Tx nested-struct fields into the transaction
.select(): trade_dt, trading_capacity, quantity (unit, nominal
{value, ccy}, monetary {value, ccy}), derivative_notional_change,
price (amount, ccy, sign, percentage, yield, basis_points,
pending {reason, ccy}), up_front_payment (amount, ccy, sign),
net_amount, trade_venue_mic, trade_country_of_branch,
trade_place_matching_id, complex_trade_component_id.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 10: `transaction` — instrument + 6 underlying-instrument groups

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert)

The single largest column block for transaction: ~18 instrument scalars + ~48 underlying-instrument columns (6 sub-prefix groups × 8 cols each: single ISIN + 5 index fields per single + 6 basket arrays — actually 12 cols per pair).

- [ ] **Step 10.1: Extend with instrument general + derivative attributes (~18 cols)**

INSERT after the trade details group:

```python
        # === Instrument — general + derivative attributes (~18 cols) ===
        F.coalesce(
            F.col("New.FinInstrm.Id"),
            F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.Id"),
        ).alias("instrument_isin"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.FullNm").alias("instrument_full_name"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.ClssfctnTp").alias("instrument_classification"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.NtnlCcy").alias("instrument_notional_currency"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.CmmdtyDerivInd").alias("instrument_commodity_derivative"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.Intrst.OthrNtnlCcy").alias("interest_other_notional_currency"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.FX.OthrNtnlCcy").alias("fx_other_notional_currency"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.PricMltplr").alias("instrument_price_multiplier"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.DlvryTp").alias("instrument_delivery_type"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.MtrtyDt").alias("instrument_maturity_dt"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.XpryDt").alias("instrument_expiry_dt"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.MntryVal._VALUE").alias("instrument_strike_price"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.MntryVal._Ccy").alias("instrument_strike_price_ccy"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Pctg").alias("instrument_strike_price_percent"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Yld").alias("instrument_strike_price_yield"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.OptnTp").alias("instrument_option_type"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.OptnExrcStyle").alias("instrument_option_exercise_style"),
        F.when(F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp").isNotNull(), F.lit("SWAP"))
         .when(F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Othr").isNotNull(), F.lit("OTHER"))
         .otherwise(F.lit(None).cast("string"))
         .alias("underlying_type"),
```

- [ ] **Step 10.2: Add 6 underlying-instrument prefix groups (~48 cols)**

Add helper paths inside the `transaction()` function (just above the `return src.select(`):

```python
    # Underlying-instrument path prefixes for the 6 sub-groups
    u_si = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp.SwpIn"
    u_so = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp.SwpOut"
    u_oth = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Othr"
```

INSERT after the instrument general/derivative-attributes block:

```python
        # === Underlying instrument — 6 sub-prefix groups (~48 cols) ===
        # swap_in_single
        F.col(f"{u_si}.Sngl.ISIN._VALUE").alias("underlying_swap_in_single_isin"),
        F.col(f"{u_si}.Sngl.Indx.ISIN._VALUE").alias("underlying_swap_in_single_index_isin"),
        F.col(f"{u_si}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_swap_in_single_index_ref_rate_code"),
        F.col(f"{u_si}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_swap_in_single_index_ref_rate_name"),
        F.col(f"{u_si}.Sngl.Indx.Nm.Term.Unit").alias("underlying_swap_in_single_index_term_unit"),
        F.col(f"{u_si}.Sngl.Indx.Nm.Term.Val").alias("underlying_swap_in_single_index_term_value"),
        # swap_in_basket
        F.transform(F.col(f"{u_si}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_swap_in_basket_isins"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_swap_in_basket_index_isins"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_swap_in_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_swap_in_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_swap_in_basket_index_term_units"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_swap_in_basket_index_term_values"),
        # swap_out_single — same shape as swap_in_single with u_so
        F.col(f"{u_so}.Sngl.ISIN._VALUE").alias("underlying_swap_out_single_isin"),
        F.col(f"{u_so}.Sngl.Indx.ISIN._VALUE").alias("underlying_swap_out_single_index_isin"),
        F.col(f"{u_so}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_swap_out_single_index_ref_rate_code"),
        F.col(f"{u_so}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_swap_out_single_index_ref_rate_name"),
        F.col(f"{u_so}.Sngl.Indx.Nm.Term.Unit").alias("underlying_swap_out_single_index_term_unit"),
        F.col(f"{u_so}.Sngl.Indx.Nm.Term.Val").alias("underlying_swap_out_single_index_term_value"),
        # swap_out_basket — same shape as swap_in_basket
        F.transform(F.col(f"{u_so}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_swap_out_basket_isins"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_swap_out_basket_index_isins"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_swap_out_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_swap_out_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_swap_out_basket_index_term_units"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_swap_out_basket_index_term_values"),
        # underlying_other_single — same shape with u_oth
        F.col(f"{u_oth}.Sngl.ISIN._VALUE").alias("underlying_other_single_isin"),
        F.col(f"{u_oth}.Sngl.Indx.ISIN._VALUE").alias("underlying_other_single_index_isin"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_other_single_index_ref_rate_code"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_other_single_index_ref_rate_name"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.Term.Unit").alias("underlying_other_single_index_term_unit"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.Term.Val").alias("underlying_other_single_index_term_value"),
        # underlying_other_basket
        F.transform(F.col(f"{u_oth}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_other_basket_isins"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_other_basket_index_isins"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_other_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_other_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_other_basket_index_term_units"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_other_basket_index_term_values"),
```

- [ ] **Step 10.3: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): add instrument + 6 underlying-instrument groups to transaction (~66 cols)

Two related blocks:

- Instrument general + derivative attributes (~18 cols): instrument_
  isin (COALESCE Id-branch + Othr-branch), full_name, classification
  (CFI), notional_currency, commodity_derivative, asset-class-specific
  interest_other_notional_currency + fx_other_notional_currency,
  price_multiplier, delivery_type, maturity_dt, expiry_dt, strike_price
  (value, ccy, percent, yield), option_type, option_exercise_style,
  underlying_type discriminator (SWAP / OTHER / NULL)

- 6 underlying-instrument sub-prefix groups (~48 cols): for each of
  {underlying_swap_in, underlying_swap_out, underlying_other} both
  Sngl and Bskt sub-prefixes — single-leg scalar ISIN + 5 index
  fields, basket-leg ARRAY<STRING> + 5 ARRAY index fields. Path
  prefixes u_si / u_so / u_oth defined inside transaction() for
  readability. F.transform() projects basket struct arrays into
  parallel ARRAY columns.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 11: `transaction` — investment decision person + executing person + additional attributes

**Files:**
- Modify: `src/pipelines/silver_mifir.py` (insert)

- [ ] **Step 11.1: Extend with the final column groups**

INSERT after the underlying-instrument block:

```python
        # === Investment decision person (~9 cols) ===
        F.col("New.InvstmtDcsnPrsn.LEI").alias("investment_decision_person_lei"),
        F.col("New.InvstmtDcsnPrsn.Prsn.FrstNm").alias("investment_decision_person_first_name"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Nm").alias("investment_decision_person_last_name"),
        F.col("New.InvstmtDcsnPrsn.Prsn.BirthDt").alias("investment_decision_person_birth_dt"),
        F.col("New.InvstmtDcsnPrsn.Prsn.CtryOfBrnch").alias("investment_decision_person_country"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.Id").alias("investment_decision_person_other_id"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.SchmeNm.Cd").alias("investment_decision_person_other_scheme"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.SchmeNm.Prtry").alias("investment_decision_person_other_scheme_proprietary"),
        F.col("New.InvstmtDcsnPrsn.Algo").alias("investment_decision_algo_id"),

        # === Executing person (~10 cols) ===
        F.col("New.ExctgPrsn.LEI").alias("executing_person_lei"),
        F.col("New.ExctgPrsn.Prsn.FrstNm").alias("executing_person_first_name"),
        F.col("New.ExctgPrsn.Prsn.Nm").alias("executing_person_last_name"),
        F.col("New.ExctgPrsn.Prsn.BirthDt").alias("executing_person_birth_dt"),
        F.col("New.ExctgPrsn.Prsn.CtryOfBrnch").alias("executing_person_country"),
        F.col("New.ExctgPrsn.Prsn.Othr.Id").alias("executing_person_other_id"),
        F.col("New.ExctgPrsn.Prsn.Othr.SchmeNm.Cd").alias("executing_person_other_scheme"),
        F.col("New.ExctgPrsn.Prsn.Othr.SchmeNm.Prtry").alias("executing_person_other_scheme_proprietary"),
        F.col("New.ExctgPrsn.Clnt").alias("executing_person_client_indicator"),
        F.col("New.ExctgPrsn.Algo").alias("executing_algo_id"),

        # === Additional attributes (~6 cols) ===
        F.col("New.AddtlAttrbts.ShrtSellgInd").alias("short_selling_indicator"),
        F.transform(F.col("New.AddtlAttrbts.WvrInd"), lambda x: x["_VALUE"]).alias("waiver_indicators"),
        F.transform(F.col("New.AddtlAttrbts.OTCPstTradInd"), lambda x: x["_VALUE"]).alias("otc_post_trade_indicators"),
        F.col("New.AddtlAttrbts.CmmdtyDerivInd").alias("commodity_derivative_indicator"),
        F.col("New.AddtlAttrbts.RskRdcgTx").alias("risk_reducing_transaction"),
        F.col("New.AddtlAttrbts.SctiesFincgTxInd").alias("securities_financing_tx_indicator"),
```

- [ ] **Step 11.2: Verify parses + commit**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_mifir.py').read())"
git add src/pipelines/silver_mifir.py
git commit -m "$(cat <<'EOF'
feat(silver): add decision/executing person + additional attributes to transaction (~25 cols)

Final column batch for the transaction table:

- Investment decision person (9 cols): lei + Prsn fields
  (first_name, last_name, birth_dt, country, other_id +
  scheme code + scheme proprietary) + algo_id. Natural-person
  fields are PII per spec — UC column-mask policies applied
  externally by data stewards.

- Executing person (10 cols): mirror of investment decision
  person with executing_person_* prefix + executing_person_
  client_indicator + executing_algo_id.

- Additional attributes (6 cols): short_selling_indicator
  scalar, waiver_indicators ARRAY<STRING> (from WvrInd[]),
  otc_post_trade_indicators ARRAY<STRING> (from OTCPstTradInd[]),
  commodity_derivative_indicator (AddtlAttrbts-level, distinct
  from instrument_commodity_derivative on FinInstrm),
  risk_reducing_transaction, securities_financing_tx_indicator.

transaction table column inventory complete per spec §4.1.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 12: Add `mifir_silver_pipeline` resource to bundle

**Files:**
- Modify: `resources/bundle.mifir_resources.yml`

- [ ] **Step 12.1: Append the SDP pipeline resource**

Find the existing `# === Spark Declarative Pipelines ===` section in `resources/bundle.mifir_resources.yml`. Below the existing `mifir_xml_loader_pipeline` block (and at the same indentation level — direct child of `pipelines:`), append:

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

- [ ] **Step 12.2: Validate**

```bash
databricks bundle validate -t dev --profile azure
```
Expected: `Validation OK!`. If auth is stale, run `databricks auth login --profile azure` first.

- [ ] **Step 12.3: Commit**

```bash
git add resources/bundle.mifir_resources.yml
git commit -m "$(cat <<'EOF'
feat(bundle): add mifir_silver_pipeline resource

New SDP pipeline resource under the existing 'Spark Declarative
Pipelines' section in bundle.mifir_resources.yml. Points at
src/pipelines/silver_mifir.py with configuration keys for catalog,
raw_schema, silver_schema (defaults to raw_schema for v1),
bronze_table (constructed from mifir_table_prefix), regulation
constant, and enable_filename_regex toggle.

No new bundle variables required for v1.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 13: Wire dev/prod target overrides

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 13.1: Add target-level development overrides**

In `databricks.yml`, find the existing `targets:` block. In each target (`dev` and `prod`), there's already a `resources.pipelines:` sub-block with overrides for the bronze pipeline (`mifir_xml_loader_pipeline`). Add a new entry alongside:

In `targets.dev.resources.pipelines`:
```yaml
        mifir_silver_pipeline:
          development: true
```

In `targets.prod.resources.pipelines`:
```yaml
        mifir_silver_pipeline:
          development: false
```

- [ ] **Step 13.2: Validate both targets**

```bash
databricks bundle validate -t dev --profile azure
databricks bundle validate -t prod --profile azure
```
Both should pass (modulo any pre-existing prod-target warnings).

- [ ] **Step 13.3: Commit**

```bash
git add databricks.yml
git commit -m "$(cat <<'EOF'
feat(bundle): wire dev/prod overrides for mifir_silver_pipeline

Sets development=true on the dev target and development=false on the
prod target, matching the pattern used for the existing
mifir_xml_loader_pipeline.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 14: Deploy + run + verify on Azure workspace

This is the end-to-end behavioral test. Requires authenticated CLI session to the `azure` profile (`adb-984752964297111.11.azuredatabricks.net`) with read/write to `esma_dev.default.*` and access to the MiFIR landing volume.

- [ ] **Step 14.1: Confirm auth + bronze state**

```bash
databricks current-user me --profile azure | jq -r '.userName'
WHID=$(databricks warehouses list --output json --profile azure | jq -r '.[] | select(.state=="RUNNING") | .id' | head -1)
echo "Warehouse: $WHID"
databricks experimental aitools tools query --warehouse "$WHID" --profile azure "SELECT COUNT(*) AS bronze_rows FROM esma_dev.default.mifir_raw"
```
Expected: user is matthew.moorcroft@databricks.com; bronze_rows ≥ 1 (the sample file has multiple Tx rows).

If `mifir_raw` is empty, **trigger the bronze pipeline first**:
```bash
databricks bundle run mifir_xml_loader_pipeline -t dev --profile azure
```

- [ ] **Step 14.2: Deploy**

```bash
databricks bundle deploy -t dev --profile azure
```
Expected: succeeds; output lists `[dev matthew_moorcroft] MiFIR Silver (domain-driven)`.

- [ ] **Step 14.3: Trigger the silver pipeline**

```bash
databricks bundle run mifir_silver_pipeline -t dev --profile azure
```
Capture the update_id from the streaming output. Expected to complete in 1-3 minutes for the sample data volume (single file with ~tens of Tx rows).

If FAILED with `external_metadata enablement version` error (same one we hit on EMIR), delete + recreate:
```bash
databricks pipelines delete <silver_pipeline_id> --profile azure
databricks bundle deploy -t dev --profile azure
databricks bundle run mifir_silver_pipeline -t dev --profile azure
```

If FAILED with schema-path errors (e.g., "field X not found in struct"), capture the path + adjust `silver_mifir.py`. Expected — same kind of spec-drift fixes we hit on EMIR PR #3 commit `8c55d73`.

- [ ] **Step 14.4: Verify row counts**

```bash
databricks experimental aitools tools query --warehouse "$WHID" --profile azure \
  "SELECT 'transaction' AS t, COUNT(*) AS rows FROM esma_dev.default.transaction
   UNION ALL SELECT 'transaction_party', COUNT(*) FROM esma_dev.default.transaction_party
   UNION ALL SELECT 'submission_file', COUNT(*) FROM esma_dev.default.submission_file"
```
Expected:
- `transaction` row count = `mifir_raw` row count
- `submission_file` = number of source files
- `transaction_party` ≥ 2× transaction row count (1 AcctOwnr per side per NEW row; potentially more with DcsnMakr)

- [ ] **Step 14.5: Check `action_type` distribution**

```bash
databricks experimental aitools tools query --warehouse "$WHID" --profile azure \
  "SELECT action_type, COUNT(*) FROM esma_dev.default.transaction GROUP BY action_type"
```
Expected: at least NEW rows (CXL may be zero in the sample).

- [ ] **Step 14.6: Spot-check semantic correctness**

```bash
databricks experimental aitools tools query --warehouse "$WHID" --profile azure \
  "SELECT transaction_id, executing_party_lei, buyer_lei, trade_venue_mic, instrument_isin, trade_dt FROM esma_dev.default.transaction WHERE action_type='NEW' LIMIT 5"
```
Confirm:
- `executing_party_lei` and `buyer_lei` are 20-char LEI-shaped strings
- `trade_venue_mic` is a valid MIC (e.g., `XLON`)
- `instrument_isin` is 12-char (e.g., `GB00B0SWJX34` from the sample)
- `trade_dt` is a proper TIMESTAMP

- [ ] **Step 14.7: Check `transaction_party` distribution by role**

```bash
databricks experimental aitools tools query --warehouse "$WHID" --profile azure \
  "SELECT side, party_role, COUNT(*) FROM esma_dev.default.transaction_party GROUP BY side, party_role ORDER BY side, party_role"
```
Expected: rows for BUYER/ACCT_OWNR, BUYER/DCSN_MAKR, SELLER/ACCT_OWNR, SELLER/DCSN_MAKR (some may be 0 if the sample has only some roles populated).

- [ ] **Step 14.8: Inspect a sample submission_file row**

```bash
databricks experimental aitools tools query --warehouse "$WHID" --profile azure \
  "SELECT client_id_from_filename, filename_timestamp, unavista_internal_client_id, unavista_submitting_entity_id, biz_msg_id, sender_bic, recipient_bic, related_biz_msg_id FROM esma_dev.default.submission_file LIMIT 1"
```
Confirm:
- `client_id_from_filename` = `9795` (per the sample filename)
- `filename_timestamp` = `20250729154019`
- `unavista_internal_client_id` populated (`LSE_UV_123456` from sample)
- `unavista_submitting_entity_id` is an LEI
- `sender_bic` / `recipient_bic` correctly populated from the BAH
- `related_biz_msg_id` likely NULL (Rltd not used in this sample)

---

## Task 15: Document smoke-test results

**Files:**
- Create: `docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md`

- [ ] **Step 15.1: Capture results**

Write `docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md` with:

```markdown
# MiFIR Silver — Azure Smoke-Test Results

**Date:** 2026-05-12
**Branch:** `feat/mifir-silver`
**Workspace:** `adb-984752964297111.11.azuredatabricks.net` (profile `azure`)
**Target schema:** `esma_dev.default`

## Pipeline run

| Field | Value |
|---|---|
| Pipeline name | `[dev matthew_moorcroft] MiFIR Silver (domain-driven)` |
| Pipeline ID | <fill from manage_pipeline> |
| Update ID | <fill from run> |
| State | COMPLETED |
| Wall time | <fill> |
| Cluster | serverless + Photon |

## Row counts

| Table | Rows |
|---|---|
| `transaction` | <fill> |
| `transaction_party` | <fill — by side × party_role breakdown> |
| `submission_file` | <fill — should be 1 for the single sample file> |

## action_type distribution

[fill in NEW vs CXL counts]

## transaction_party distribution by role

[fill in BUYER/ACCT_OWNR, BUYER/DCSN_MAKR, etc.]

## Spot-check correctness

[paste 5 sample rows from transaction confirming LEI shape, MIC, ISIN, trade_dt]

## submission_file inspection

[paste row showing filename-derived cols, UVHeader fields, BAH fields, Rltd NULL]

## Spec-drift fixes (if any)

[list any schema-path corrections like "Cdt.PmtFrqcy was STRING not struct" from EMIR PR #3]

## Anomalies / follow-ups

- [Note anything unexpected]
- [Real-customer-data validation pass needed]
```

Fill in `<fill>` placeholders with actual values from Task 14.

- [ ] **Step 15.2: Commit**

```bash
git add docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md
git commit -m "$(cat <<'EOF'
test(silver): Azure smoke-test results

Records actual row counts, update_id, wall time, and spot-check
results from the MiFIR silver pipeline's first triggered run on
the Azure workspace against the synthetic LSE sample file. All
three tables verified populating correctly with business-readable
column names. Spec-drift fixes (if any) documented for traceability.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 16: Open PR

- [ ] **Step 16.1: Push branch**

```bash
git push -u origin feat/mifir-silver
```

- [ ] **Step 16.2: Create PR via gh CLI**

```bash
gh pr create --title "feat(silver): domain-driven MiFIR transaction silver layer" --body "$(cat <<'EOF'
## Summary

Domain-driven MiFIR transaction-reporting silver layer (auth.016.001.01) on top of bronze `mifir_raw` shipped by PR #1's parameterized loader.

**Three published tables** in `esma_dev.default.*` (Azure workspace, since that's where MiFIR test data lives):

- **`transaction`** — wide-flat fact, one row per `<Tx>` element with `action_type ∈ {'NEW', 'CXL'}` discriminator. ~135 scalar columns + ~15 array columns covering identification, buyer/seller flat fields, order transmission, trade details, instrument + 6 underlying-instrument prefix groups, investment-decision person, executing person, additional attributes (waivers + OTC indicators as ARRAYs), audit.

- **`transaction_party`** — unified explode of `Buyr.AcctOwnr` + `Buyr.DcsnMakr` + `Sellr.AcctOwnr` + `Sellr.DcsnMakr` with `side ∈ {BUYER, SELLER}` and `party_role ∈ {ACCT_OWNR, DCSN_MAKR}` discriminators. ~18 cols.

- **`submission_file`** — MiFIR-specific envelope distinct from EMIR's: UVHeader (UnaVista vendor wrapper) + full BizAppHeader (AppHdr top-level + Sender `Fr.OrgId`/`Fr.FIId` + Recipient mirror + the 135-leaf `Rltd` related-message block). ~270 cols, capturing 100% of bronze header leaves.

## Coverage

**100% bronze leaf coverage**: every one of the 449 bronze leaves (175 pyld + 274 hdr) has a silver representation. Per user direction "want all in bronze" — no long-tail dropping. `Othr.SchmeNm.Cd` always paired with `Othr.SchmeNm.Prtry`. `DcsnMakr` correctly treated as an ARRAY exploded into `transaction_party`.

## Architecture

- New SDP source `src/pipelines/silver_mifir.py` (~1000 lines), new `mifir_silver_pipeline` resource in `bundle.mifir_resources.yml`, dev/prod target overrides in `databricks.yml`.
- Reads from `esma_dev.default.mifir_raw` (bronze).
- Append-only — each Tx becomes a new row; lifecycle via `action_type` discriminator.
- Serverless + Photon, `cluster_by_auto=True`.
- Customer-replaceable `_add_filename_regex_columns()` for non-default filename conventions (same pattern as bronze).
- PII columns identified; UC column-mask policies applied externally via UC governance (not pipeline concern).
- Star-schema pivot documented as a future architectural option (spec §8).

## Reference docs (on this branch)

- Approved design spec: `docs/superpowers/specs/2026-05-12-mifir-silver-design.md`
- Task-by-task plan: `docs/superpowers/plans/2026-05-12-mifir-silver.md`
- Azure smoke-test results: `docs/superpowers/plans/2026-05-12-mifir-silver-smoke-test-results.md`

## Test plan

- [x] `databricks bundle validate -t dev --profile azure` — passes
- [x] `databricks bundle deploy -t dev --profile azure` — creates `mifir_silver_pipeline`
- [x] `databricks bundle run mifir_silver_pipeline -t dev --profile azure` — COMPLETED
- [x] Row count invariants: transaction = bronze, submission_file = 1 per file, transaction_party ≥ 2× transaction
- [x] action_type distribution shows NEW rows (and CXL if sample has any)
- [x] Spot-check confirms LEI shape, MIC code, ISIN, trade_dt populated correctly
- [x] transaction_party shows rows for {BUYER, SELLER} × {ACCT_OWNR, DCSN_MAKR}
- [x] submission_file populated with UVHeader + BAH fields; Rltd block NULL in the synthetic sample (expected)

## Synthetic-data caveats

The single LSE sample (`9795_20250729154019_3_sample_data.xml`) exercises only a subset of MiFIR's XSD branches. Real-customer-data validation is a follow-up.

## Deferred / out-of-scope

- **SFTR silver** — separate brainstorm + spec + branch
- **Gold layer** — once analyst queries are known
- **UC column-mask policies for PII** — separate governance branch
- **Real-customer-data validation pass**
- **Star-schema pivot** (`dim_legal_entity` shared across EMIR + MiFIR)
- **Production MiFIR filename regex** for customers with different conventions
- **Cross-regulation `regulation_submissions` rolled-up VIEW** (joining EMIR + MiFIR submission_file in a view)
- **Retire legacy MiFIR flatten notebook** once silver is production-confirmed

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 16.3: Verify PR is open**

```bash
gh pr view --json url,number,state,baseRefName,headRefName
```
Expected: state OPEN, head `feat/mifir-silver`, base `main` (or `feat/sdp-xml-loader` if PR #1 hasn't merged yet — adjust before opening if needed).

---

## Out-of-Scope (Documented Follow-Ups)

Per spec §8:

- SFTR silver tables (separate spec + branch)
- Gold-layer aggregations and metric views
- UC column-mask policies for PII (`*_first_name`, `*_birth_dt`, `*_other_id`, etc.) — separate UC-governance branch
- Real-customer-data validation pass
- Star-schema pivot (`dim_legal_entity` shared across EMIR + MiFIR)
- Production MiFIR filename regex (the sample's pattern is one customer's convention)
- Cross-regulation rolled-up VIEW
- Retirement of legacy MiFIR flatten notebook
- SCD Type 2 if lifecycle status as a materialized column on NEW rows is wanted
