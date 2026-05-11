# SDP XML Loader — Design

**Status:** Approved
**Date:** 2026-05-11
**Author:** Matthew Moorcroft
**Branch:** `feat/sdp-xml-loader`

## 1. Problem & Motivation

The `esma_xml_ingestion` accelerator currently ingests ESMA regulatory XML files (EMIR, MiFIR) via a classic Databricks notebook (`src/1_xml_file_loader_body.py`) executed inside a job task. The notebook uses Spark Structured Streaming with Auto Loader (`cloudFiles` format `xml`), an `lxml.iterparse`-based Python UDF to extract per-file header metadata, and writes a single combined Delta raw table after a streaming self-join between payload rows and per-file headers.

That design has several rough edges:

- It uses `.distinct()` on a streaming source, which has no formal semantics and silently produces best-effort micro-batch-local distinct.
- The stream-stream join state is unbounded (no watermarks).
- Malformed XML rows are tolerated by Auto Loader's `rowValidationXSDPath` + `PERMISSIVE` mode but never surfaced with a human-readable validation error — they sit silently in `corrupted_record` alongside good rows.
- It is a notebook (with `# COMMAND ----------` cells), which is at odds with modern Databricks recommendations for ingestion (plain `.py` files inside Lakeflow Spark Declarative Pipelines).

This branch converts that loader to a Spark Declarative Pipeline (SDP / LDP) using the modern `from pyspark import pipelines as dp` API, while fixing the streaming pattern, adding an explicit quarantine path, and preserving the public output table so downstream consumers do not break.

The two other notebooks — `0_1_xml_schema_xsd.py` (XSD → JSON Spark schema, Scala-based, ad-hoc) and `2_flatten_explode_table.py` (recursive flatten → bronze) — are out of scope for this branch and continue to run as classic notebook jobs.

## 2. Goals & Non-Goals

### Goals

- Replace the loader notebook with a parameterized SDP pipeline source file.
- One pipeline source file, two pipeline resource definitions (EMIR + MiFIR), reusing the existing per-regulation bundle variables.
- Preserve the public output table `{catalog}.{raw_schema}.{prefix}_raw` so the existing flatten notebook keeps working as a drop-in consumer.
- Introduce an explicit, queryable quarantine table for malformed rows, enriched with a verbose `lxml.etree.XMLSchema.assertValid` error message.
- Use bounded-state streaming primitives (watermark + `dropDuplicatesWithinWatermark`, watermarked stream-stream join) so the pipeline is fully incremental and state does not grow unbounded.
- Restructure the repo into `src/notebooks/` (legacy classic notebooks) and `src/pipelines/` (SDP source files).
- Serverless compute by default.

### Non-Goals

- Converting `0_1_xml_schema_xsd.py` to SDP. (It is a one-off XSD prep step that depends on the Scala `XSDToSchema` utility and is not a streaming workload.)
- Converting `2_flatten_explode_table.py` to SDP. That is a follow-up branch; once it lands, the SDP loader and the SDP flatten will be re-linked under a single job.
- Changing the filename regex, header XSD content, or any downstream consumer logic.
- Adding unit tests for the existing UDFs (preserved verbatim).
- CI/docs-site updates to handle plain `.py` pipeline files (flagged as a separate follow-up; not a blocker for this branch).

## 3. Architecture

Four streaming tables, all in `{catalog}.{raw_schema}`, all `cluster_by_auto=True` (Liquid Clustering auto mode), parameterized via `spark.conf.get(...)`.

```
Auto Loader (cloudFiles xml + rowValidationXSDPath + PERMISSIVE
             + columnNameOfCorruptRecord="corrupted_record"
             + rescuedDataColumn="rescued_data"
             + .schema(payload_json_schema))
        │
        ▼
┌──────────────────────────────────────────────┐
│ {prefix}_raw_xml_payload         private=True│
│ streaming table                              │
│ ALL rows (good + corrupted)                  │
│ + file_path, file_name, _file_modification   │
│   _time, _ingested_at, corrupted_record,     │
│   rescued_data                               │
└─────┬────────────────────────────────────┬───┘
      │                                    │
      │ filter corrupted_record IS NULL    │ filter corrupted_record IS NOT NULL
      ▼                                    ▼
┌────────────────────────┐  ┌────────────────────────────────┐
│ {prefix}_file_hdr_     │  │ {prefix}_quarantine    PUBLIC  │
│   metadata             │  │ streaming table                │
│ private=True           │  │ + xsd_validation_result        │
│ watermark +            │  │   (singleton-cached XSD UDF)   │
│ dropDuplicatesWithin   │  └────────────────────────────────┘
│   Watermark(file_path) │
│ + lxml header UDF      │
│ + from_xml(header)     │
│ + filename regex       │
│   (FileBatchIndex,     │
│    FileBatchSize,      │
│    FileVersion,        │
│    ESMADate)           │
└─────────┬──────────────┘
          │  watermarked stream-stream join on file_path
          ▼
┌──────────────────────────────────────────────┐
│ {prefix}_raw                          PUBLIC │
│ streaming table                              │
│ GOOD payload columns + header struct +       │
│   filename-regex columns                     │
│ Drop-in replacement for today's output       │
│ (consumed by src/notebooks/                  │
│  2_flatten_explode_table.py)                 │
└──────────────────────────────────────────────┘
```

### 3.1 Table-by-table

#### `{prefix}_raw_xml_payload` (private)

- Streaming table fed by Auto Loader (`format("cloudFiles")`, `option("cloudFiles.format","xml")`, `rowTag`, `rowValidationXSDPath`, `PERMISSIVE`, `columnNameOfCorruptRecord`, `rescuedDataColumn`).
- `.schema(...)` is set from the payload JSON schema (read once at pipeline-start via the ported `readSchema` helper).
- Adds: `file_path`, `file_name`, `_file_modification_time` (from `_metadata`), `_ingested_at` (`current_timestamp()`).
- Append-only.

#### `{prefix}_file_hdr_metadata` (private)

- Reads `STREAM` from `{prefix}_raw_xml_payload`, filtered to `corrupted_record IS NULL`.
- Adds watermark on `_file_modification_time` with `watermark_interval` (default 15 minutes).
- Applies `dropDuplicatesWithinWatermark("file_path")` so the per-file UDFs run once per file per trigger and state evicts after the watermark advances.
- Calls the existing `extract_hdr_pyld_metadata` lxml UDF on `file_path` to produce the header XML string.
- Parses that string with `from_xml(_, header_json_schema)` into `hdr_pyld_metadata` struct.
- Applies the existing filename regexes to produce `FileBatchIndex`, `FileBatchSize`, `FileVersion`, `ESMADate`.
- Header-UDF failures return `None` (lenient behavior preserved from today). Adding a `dp.expect` is a deliberate follow-up, not part of this branch.

#### `{prefix}_quarantine` (public)

- Reads `STREAM` from `{prefix}_raw_xml_payload`, filtered to `corrupted_record IS NOT NULL`.
- Adds `xsd_validation_result` column from a singleton-cached XSD UDF (compiles `etree.XMLSchema` once per Python worker, calls `assertValid` per row). Returns `"XML is valid"` or `"Invalid XML: <message>"`.
- Columns: `file_path`, `file_name`, `_file_modification_time`, `_ingested_at`, `corrupted_record`, `rescued_data`, `xsd_validation_result`.
- Public so Ops / data stewards can query it without touching pipeline internals.

#### `{prefix}_raw` (public, drop-in)

- Streaming table joining `{prefix}_raw_xml_payload` (good rows) with `{prefix}_file_hdr_metadata` on `file_path`. Both sides watermarked on `_file_modification_time` so the join state is bounded by in-flight file count, not row count.
- Output schema = payload columns + `hdr_pyld_metadata` struct + filename-regex columns + the file metadata columns. Matches the existing output of `src/1_xml_file_loader_body.py` for the columns that the flatten notebook actually consumes.
- `cluster_by_auto=True`.

### 3.2 Why this is better than today

- `.distinct()` (no semantics) → `dropDuplicatesWithinWatermark` (bounded state, deterministic).
- Unbounded stream-stream join → watermarked join, state = O(in-flight files).
- Silent `corrupted_record` rows in the same table as good rows → explicit `_quarantine` table with detailed error messages.
- Notebook with `%pip` and `# COMMAND ----------` → plain `.py` SDP source file with `serverless` + declarative `environment` dependencies.
- One job task wrapping streaming → SDP managed run, event log, lineage, and orchestration.

### 3.3 Parameterization

Every input is supplied via `spark.conf.get("...")` and wired through the pipeline's `configuration` block in the bundle:

| Config key | Source variable (EMIR) | Used for |
|---|---|---|
| `catalog` | `${var.emir_catalog}` | Pipeline `catalog` field |
| `raw_schema` | `${var.emir_raw_schema}` | Pipeline `schema` field |
| `table_prefix` | `${var.emir_table_prefix}` | Table names |
| `landing_path` | `${var.emir_landing_path}` | Auto Loader source |
| `row_tag` | `${var.emir_row_tag}` (`Stat`) | Auto Loader `rowTag`; passed to header UDF |
| `xml_schema_pyld_path` | `${var.emir_xml_schema_pyld_path}` | Payload JSON schema |
| `xml_schema_hdr_pyld_metadata_path` | `${var.emir_xml_schema_hdr_pyld_metadata_path}` | Header JSON schema for `from_xml` |
| `xml_xsd_schema_pyld_path` | `${var.emir_xml_xsd_schema_pyld_path}` | Auto Loader `rowValidationXSDPath` AND the quarantine XSD UDF |
| `watermark_interval` | new constant `"15 minutes"` | `withWatermark` on both intermediate streams |

MiFIR mirrors with `${var.mifir_*}` and `row_tag=Tx`.

## 4. Repository Layout

```
esma_xml_ingestion/
├── databricks.yml                          # +include local config overrides
├── resources/
│   ├── bundle.variables.yml                # unchanged
│   ├── bundle.emir_resources.yml           # split into Jobs + Pipelines sections
│   ├── bundle.mifir_resources.yml          # split into Jobs + Pipelines sections
│   ├── bundle.new-type_resources.yml.template
│   └── config/
│       └── local/                          # git-ignored
│           └── dev-variables.yml           # E2 smoke-test overrides
├── src/
│   ├── __init__.py
│   ├── notebooks/                          # NEW — legacy classic notebooks
│   │   ├── 0_1_xml_schema_xsd.py           # moved from src/
│   │   ├── 1_xml_file_loader_body.py       # moved from src/, unscheduled, reference only
│   │   └── 2_flatten_explode_table.py      # moved from src/; jobs updated to new path
│   ├── pipelines/                          # NEW — SDP source files
│   │   └── xml_loader.py                   # NEW — the parameterized SDP
│   └── util/
│       ├── __init__.py
│       └── xsd_processor.py                # unchanged
├── fixtures/                               # unchanged
└── scratch/                                # unchanged
```

Key constraints:

- `src/pipelines/xml_loader.py` is a plain `.py` file: no `# Databricks notebook source` header, no `# COMMAND ----------` cells. This is required for SDP to import it as a pipeline library.
- `src/notebooks/1_xml_file_loader_body.py` is preserved but unreferenced by any job — kept for reference / parity testing.
- README's project-structure section gets a small accuracy fix (it currently describes `src/notebooks/` even though the path didn't exist; the move makes the README true).

## 5. Bundle Resources

### 5.1 Pipeline definitions

A single new pipeline per regulation, both in their existing per-regulation `bundle.*_resources.yml` files under a clearly commented `# === Spark Declarative Pipelines ===` section.

EMIR (sketch):

```yaml
# === Spark Declarative Pipelines ===
  pipelines:
    emir_xml_loader_pipeline:
      name: "EMIR XML Loader (SDP)"
      catalog: ${var.emir_catalog}
      schema: ${var.emir_raw_schema}
      serverless: true
      channel: PREVIEW
      development: false
      photon: true
      continuous: false
      libraries:
        - file:
            path: ../src/pipelines/xml_loader.py
      environment:
        dependencies:
          - lxml==5.3.0
      configuration:
        catalog:                              ${var.emir_catalog}
        raw_schema:                           ${var.emir_raw_schema}
        table_prefix:                         ${var.emir_table_prefix}
        landing_path:                         ${var.emir_landing_path}
        row_tag:                              ${var.emir_row_tag}
        xml_schema_pyld_path:                 ${var.emir_xml_schema_pyld_path}
        xml_schema_hdr_pyld_metadata_path:    ${var.emir_xml_schema_hdr_pyld_metadata_path}
        xml_xsd_schema_pyld_path:             ${var.emir_xml_xsd_schema_pyld_path}
        watermark_interval:                   "15 minutes"
```

MiFIR identical shape with `${var.mifir_*}` substitutions and `row_tag=Tx`.

### 5.2 Existing jobs

- `EMIR_Schema_Creation` / `MiFIR_Schema_Creation`: unchanged (still run `0_1_xml_schema_xsd.py`, path updated to `../src/notebooks/0_1_xml_schema_xsd.py`).
- `EMIR_XML_Processing` / `MiFIR_XML_Processing`: the `*_xml_load` task is removed (replaced by the SDP pipeline). The remaining `*_xml_flatten` task points at `../src/notebooks/2_flatten_explode_table.py`.

The SDP pipeline and the flatten job stay decoupled on this branch. When `2_flatten_explode_table.py` is later converted to SDP, both will be re-linked under a single orchestration unit.

### 5.3 Target overrides

`databricks.yml` dev/prod targets get an additional `resources.pipelines.*.development` override (`true` in dev, `false` in prod). No new top-level variables.

### 5.4 Local dev overrides

`databricks.yml` gains `include: resources/config/local/*.yml`. `resources/config/local/` is git-ignored. A new `dev-variables.yml` template documents the E2 smoke-test values (see §7).

## 6. Implementation Details

### 6.1 Dependencies

`lxml==5.3.0` declared in the pipeline's `environment.dependencies` block. Works on serverless SDP without notebook magics or cluster-init scripts. The original notebook's `%pip install lxml` is removed.

### 6.2 Watermark column

`_metadata.file_modification_time` is exposed by Auto Loader cloudFiles and is monotonic per source file. We use it as the event-time column on both intermediate streaming tables. If a future Auto Loader behavior change removes it, the fallback is `current_timestamp()` with a larger watermark interval — but this is not expected.

### 6.3 Watermark interval

Default `"15 minutes"`, configurable via the pipeline `configuration` block. Trades state size vs. tolerance for late files arriving in subsequent triggers. 15 minutes comfortably covers single-trigger arrival skew.

### 6.4 UDFs

#### `extract_hdr_pyld_metadata` (lxml header extractor)

Ported verbatim from the existing notebook. Returns a String containing the header-only XML (everything up to but not including the first row-tag element). Returns `None` on any exception — lenient.

#### `xsd_error` (quarantine validator)

New, runs **only** on quarantine rows (`corrupted_record IS NOT NULL`), which are the minority of input. Uses a module-level singleton cache so the `etree.XMLSchema` object is compiled once per Python worker per XSD path:

```python
_xsd_cache = {}
def _get_xsd_schema(xsd_path):
    if xsd_path not in _xsd_cache:
        from lxml import etree
        with open(xsd_path, "rb") as f:
            _xsd_cache[xsd_path] = etree.XMLSchema(etree.XML(f.read()))
    return _xsd_cache[xsd_path]

@udf(StringType())
def xsd_error(xml_str, xsd_path):
    from lxml import etree
    try:
        schema = _get_xsd_schema(xsd_path)
        schema.assertValid(etree.fromstring(xml_str.encode("utf-8")))
        return "XML is valid"
    except Exception as e:
        return f"Invalid XML: {str(e)}"
```

Return type stays `StringType` for parity with the snippet provided. A structured `{is_valid: bool, error_message: string}` return is a deliberate follow-up.

### 6.5 Schema file loading

`xml_loader.py` reads two JSON schema files (payload + header) at pipeline-start via the ported `readSchema(file) -> StructType` helper. The XSD path is passed through unchanged to Auto Loader's `rowValidationXSDPath` AND to the quarantine UDF via `lit(...)`. All three paths are Unity Catalog Volume paths, readable from serverless SDP with no additional configuration.

### 6.6 Public-table column contract

`{prefix}_raw` must contain at minimum the columns the downstream flatten notebook reads. The flatten notebook reads `df.schema` recursively and treats nested structs / arrays generically, so the practical contract is: same payload struct + same `hdr_pyld_metadata` struct + same filename-regex columns + `file_name` (used as a passthrough key in the flatten step). The implementation must preserve these.

## 7. Validation Plan

### 7.1 Target environment

| Setting | Value |
|---|---|
| Workspace | `e2-demo-field-eng.cloud.databricks.com` |
| Bundle target | `dev` (with local overrides from §5.4) |
| Catalog | `users` |
| Schema | `matthew_moorcroft` |
| Landing path | `/Volumes/users/matthew_moorcroft/central_bank_ireland/landing/` |
| Schemas dir | `/Volumes/users/matthew_moorcroft/central_bank_ireland/schemas/` |
| Payload JSON schema | `…/schemas/pyld_schema.json` |
| Header JSON schema | `…/schemas/hdr_pyld_metadata_schema.json` |
| Row-tag XSD | `…/schemas/row_tag_schema.xsd` |
| Regulation under test | EMIR (`row_tag=Stat`) |

`resources/config/local/dev-variables.yml` (git-ignored) overrides the relevant `emir_*` variables to point at this volume.

### 7.2 Test sequence

1. `databricks bundle validate -t dev` — confirms both new pipelines and the rewired jobs parse and resolve all variables.
2. `databricks bundle deploy -t dev` — creates EMIR + MiFIR pipelines in the `[dev <user>]` namespace alongside the existing jobs.
3. Confirm a known-good EMIR XML file already exists in the E2 landing path (CBI data is present). If quarantine needs exercising and no malformed file exists, stage one alongside.
4. `databricks bundle run emir_xml_loader_pipeline`.
5. Pipeline event log green; verify via `manage_pipeline(action="get", pipeline_id=...)`.
6. `get_table_stats_and_schema` on:
   - `users.matthew_moorcroft.emir_raw` — non-zero rows; `hdr_pyld_metadata` struct populated; `FileBatchIndex` / `FileBatchSize` / `FileVersion` / `ESMADate` populated.
   - `users.matthew_moorcroft.emir_quarantine` — exists; populated if a malformed file was staged; `xsd_validation_result` non-null.
7. Parity check: run `src/notebooks/1_xml_file_loader_body.py` against the same input into a parallel `users.matthew_moorcroft.emir__raw_legacy` table; spot-check row counts and a handful of payload-struct columns vs. `users.matthew_moorcroft.emir_raw`. Acceptable deltas: `inserted_at`, ordering, absence of malformed rows in the SDP `_raw` (they live in `_quarantine`).

### 7.3 Negative tests

- Stop the pipeline mid-update and restart — confirm checkpoint resume, no duplicate rows in `_raw`.
- Drop a new XML file into landing while pipeline is idle, retrigger — only the new file is processed.
- Drop a file with a non-`<Stat>` root element — it appears in `_quarantine` with a clear `xsd_validation_result`, not in `_raw`.

### 7.4 Downstream regression

Run the (modified, flatten-only) `EMIR_XML_Processing` job pointing at `users.matthew_moorcroft.emir_raw`. Confirm `2_flatten_explode_table.py` still produces bronze tables. This is the practical proof that the public surface did not change.

### 7.5 MiFIR

`databricks bundle deploy -t dev` must successfully create the MiFIR pipeline. A live run-test is only performed if MiFIR data is available on E2; otherwise validation-only.

### 7.6 Cleanup

Drop `[dev <user>] emir_xml_loader_pipeline` and the `users.matthew_moorcroft.emir__raw_legacy` parity-check table after the branch lands.

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| `_metadata.file_modification_time` not exposed in an edge case | Watermark column missing | Fallback to `current_timestamp()` event time with larger watermark; flagged as an implementation check |
| `lxml` version mismatch on serverless SDP | UDF import failure | Pin `lxml==5.3.0` in pipeline `environment.dependencies` |
| Header XSD too strict, valid files quarantined | Data loss in `_raw` | Lenient `try/except None` retained on header UDF; quarantine path provides a verbose error to triage |
| Public column contract drift breaks flatten notebook | Downstream regression | §7.4 explicit downstream regression test; column contract documented in §6.6 |
| Bundle local override file accidentally committed | Workspace-specific paths leak into prod | `resources/config/local/` git-ignored; only a template is committed |
| Parity check shows row-count delta | Cannot prove drop-in compatibility | Investigate before merge — most likely cause is malformed-row routing to quarantine; if so, the count delta should equal the quarantine row count |
| Decorator argument names drift (e.g., `private=True`, `cluster_by_auto=True`) between this design and the live `pyspark.pipelines` API | Pipeline fails to import | Implementation plan will explicitly verify each decorator argument against the current SDP API reference (preferring the AI dev-kit SDP skill's syntax-basics doc) before writing the source file; substitute equivalent supported argument if name differs |

## 9. Open Follow-Ups (Future Branches)

- Convert `2_flatten_explode_table.py` to SDP; re-link with this loader under a single job.
- Add `dp.expect` / `dp.expect_or_drop` on header parse success once the lenient behavior is no longer needed.
- Move the `xsd_error` UDF return type from `StringType` to `StructType({is_valid: bool, error_message: string})`.
- Unit tests for `extract_hdr_pyld_metadata` and `xsd_error` (none today).
- Update `.github/scripts/export_databricks_notebooks.py` and the docs site CI to include plain `.py` pipeline files.
- Convert `0_1_xml_schema_xsd.py` Scala XSD step to a Python-native equivalent so the whole accelerator runs on serverless without Scala.

## 10. Approval

All six design sections (Scope/Branch, Architecture, Repo Layout, Bundle Resources, Implementation Details, Validation) reviewed and approved interactively before this document was written.
