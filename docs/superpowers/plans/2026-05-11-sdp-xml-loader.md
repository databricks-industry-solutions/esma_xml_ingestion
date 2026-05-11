# SDP XML Loader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert `src/1_xml_file_loader_body.py` from a classic Databricks notebook into a parameterized Lakeflow Spark Declarative Pipeline (SDP) source file, with EMIR + MiFIR pipeline resources in the bundle, and a 4-table architecture (raw payload, file-header metadata, quarantine, raw join) — preserving `{catalog}.{raw_schema}.{prefix}_raw` as a drop-in public output for the existing flatten notebook.

**Architecture:** Four `@dp.table()` streaming tables in `src/pipelines/xml_loader.py`. Auto Loader reads XML files into `{prefix}_raw_xml_payload`. Bad rows (`corrupted_record IS NOT NULL`) feed `{prefix}_quarantine` enriched with an `lxml.etree.XMLSchema.assertValid` error. Good rows feed a watermarked, deduplicated `{prefix}_file_hdr_metadata` (one row per file via `dropDuplicatesWithinWatermark`), which gets joined back to good payload rows to produce the public `{prefix}_raw`. Same source file backs two pipeline resources (EMIR, MiFIR), parameterized via `spark.conf.get(...)`.

**Tech Stack:** Python 3 + `pyspark.pipelines` (`from pyspark import pipelines as dp`), Databricks Auto Loader (`cloudFiles` XML), `lxml` 5.3.0, Delta Lake, Unity Catalog, Databricks Asset Bundles, serverless SDP compute on E2 (`e2-demo-field-eng.cloud.databricks.com`).

**Reference spec:** [`docs/superpowers/specs/2026-05-11-sdp-xml-loader-design.md`](../specs/2026-05-11-sdp-xml-loader-design.md)

---

## File Plan

| File | Action | Responsibility |
|------|--------|---------------|
| `src/notebooks/__init__.py` | Create | Mark legacy notebooks dir as a package |
| `src/notebooks/0_1_xml_schema_xsd.py` | Move from `src/0_1_xml_schema_xsd.py` | Unchanged contents |
| `src/notebooks/1_xml_file_loader_body.py` | Move from `src/1_xml_file_loader_body.py` | Unchanged contents (reference / parity check only) |
| `src/notebooks/2_flatten_explode_table.py` | Move from `src/2_flatten_explode_table.py` | Unchanged contents |
| `src/pipelines/__init__.py` | Create | Mark SDP source dir as a package |
| `src/pipelines/xml_loader.py` | Create | New parameterized SDP source: 4 `@dp.table()` definitions, 2 UDFs, schema-loading helper |
| `resources/bundle.emir_resources.yml` | Modify | Update job notebook paths; add EMIR SDP pipeline resource under a commented section |
| `resources/bundle.mifir_resources.yml` | Modify | Update job notebook paths; add MiFIR SDP pipeline resource |
| `resources/config/local/dev-variables.yml.template` | Create | Committed template documenting E2 override values |
| `resources/config/local/dev-variables.yml` | Create (local, git-ignored) | Real E2 overrides used during smoke test |
| `databricks.yml` | Modify | Add `include: resources/config/local/*.yml`; add target overrides for `development` mode on pipelines |
| `.gitignore` | Modify | Ignore `resources/config/local/` |
| `README.md` | Modify | Fix project-structure section to match new layout |
| `docs/superpowers/plans/2026-05-11-sdp-xml-loader.md` | Create | This file |

**File splits / decomposition:**
- All four `@dp.table()` definitions live in a single `xml_loader.py` because they share two module-level UDFs, the schema loader, and pipeline-config reads. Splitting per-table would force duplicate imports and helpers without buying isolation. The file is bounded (well under 250 lines) and has one clear responsibility: define the loader pipeline.
- Per-regulation bundle YAMLs remain split (EMIR vs. MiFIR) because that's the established convention in this repo.

---

## Branch Status

Work is performed on branch `feat/sdp-xml-loader` (already created, with commit `a9ed91c` adding the design doc).

---

## Task 1: Restructure repo — create directories and move classic notebooks

**Files:**
- Create: `src/notebooks/__init__.py`
- Create: `src/pipelines/__init__.py`
- Move: `src/0_1_xml_schema_xsd.py` → `src/notebooks/0_1_xml_schema_xsd.py`
- Move: `src/1_xml_file_loader_body.py` → `src/notebooks/1_xml_file_loader_body.py`
- Move: `src/2_flatten_explode_table.py` → `src/notebooks/2_flatten_explode_table.py`
- Modify: `resources/bundle.emir_resources.yml` (notebook paths)
- Modify: `resources/bundle.mifir_resources.yml` (notebook paths)

- [ ] **Step 1.1: Confirm we are on the branch and clean**

Run:
```bash
git status && git branch --show-current
```
Expected: working tree clean, branch `feat/sdp-xml-loader`.

- [ ] **Step 1.2: Create new subdirectory `__init__.py` files**

Run:
```bash
mkdir -p src/notebooks src/pipelines
```

Then write `src/notebooks/__init__.py` containing exactly:
```python
```

(empty file)

Then write `src/pipelines/__init__.py` containing exactly:
```python
```

(empty file)

- [ ] **Step 1.3: Move the three classic notebooks**

Run:
```bash
git mv src/0_1_xml_schema_xsd.py src/notebooks/0_1_xml_schema_xsd.py
git mv src/1_xml_file_loader_body.py src/notebooks/1_xml_file_loader_body.py
git mv src/2_flatten_explode_table.py src/notebooks/2_flatten_explode_table.py
```

Verify with `git status` — three files moved, two `__init__.py` untracked.

- [ ] **Step 1.4: Update EMIR job notebook paths**

In `resources/bundle.emir_resources.yml`, change three notebook paths:

`../src/1_xml_file_loader_body.py` → `../src/notebooks/1_xml_file_loader_body.py`
`../src/2_flatten_explode_table.py` → `../src/notebooks/2_flatten_explode_table.py`
`../src/0_1_xml_schema_xsd.py` → `../src/notebooks/0_1_xml_schema_xsd.py`

After this step the EMIR YAML's task blocks should look like:

```yaml
        - task_key: emir_xml_load
          notebook_task:
            notebook_path: ../src/notebooks/1_xml_file_loader_body.py
            source: WORKSPACE
            
        - task_key: emir_xml_flatten
          depends_on:
            - task_key: emir_xml_load
          notebook_task:
            notebook_path: ../src/notebooks/2_flatten_explode_table.py
            source: WORKSPACE
```

```yaml
        - task_key: emir_schema_generation
          notebook_task:
            notebook_path: ../src/notebooks/0_1_xml_schema_xsd.py
            source: WORKSPACE
```

(The `emir_xml_load` task is removed in Task 5 — leave it intact for now to keep validation green.)

- [ ] **Step 1.5: Update MiFIR job notebook paths**

In `resources/bundle.mifir_resources.yml`, perform the same three path substitutions:

`../src/1_xml_file_loader_body.py` → `../src/notebooks/1_xml_file_loader_body.py`
`../src/2_flatten_explode_table.py` → `../src/notebooks/2_flatten_explode_table.py`
`../src/0_1_xml_schema_xsd.py` → `../src/notebooks/0_1_xml_schema_xsd.py`

- [ ] **Step 1.6: Validate bundle still parses**

Run:
```bash
databricks bundle validate -t dev
```
Expected: succeeds (`Validation OK!` or equivalent). No errors about missing notebook paths.

If it fails with "notebook not found", re-check Steps 1.3-1.5 — paths must include `src/notebooks/`.

- [ ] **Step 1.7: Commit**

```bash
git add src/notebooks/__init__.py src/pipelines/__init__.py \
        src/notebooks/0_1_xml_schema_xsd.py \
        src/notebooks/1_xml_file_loader_body.py \
        src/notebooks/2_flatten_explode_table.py \
        resources/bundle.emir_resources.yml \
        resources/bundle.mifir_resources.yml
git commit -m "$(cat <<'EOF'
refactor: move classic notebooks into src/notebooks/

Creates src/notebooks/ (legacy classic notebooks) and src/pipelines/
(forthcoming SDP source files). Moves the three existing notebooks into
src/notebooks/ and updates the EMIR + MiFIR job notebook paths to match.
No behavioral changes — preserves contents verbatim. Sets up the layout
for the SDP loader to land alongside without overlap.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 2: Local dev config plumbing

**Files:**
- Modify: `.gitignore`
- Modify: `databricks.yml`
- Create: `resources/config/local/dev-variables.yml.template`
- Create (local, git-ignored): `resources/config/local/dev-variables.yml`

- [ ] **Step 2.1: Add the local config directory to `.gitignore`**

Append to `.gitignore`:
```
# Local bundle config overrides (per-developer)
resources/config/local/
!resources/config/local/*.template
```

The `!` re-includes `.template` files so the committed template stays tracked.

- [ ] **Step 2.2: Add `include` for local config to `databricks.yml`**

In `databricks.yml`, change:

```yaml
include:
  - resources/**/*.yml
  - resources/*.yml
```

to:

```yaml
include:
  - resources/**/*.yml
  - resources/*.yml
  - resources/config/local/*.yml
```

- [ ] **Step 2.3: Create the committed template**

Write `resources/config/local/dev-variables.yml.template` with exactly:

```yaml
# Local development variable overrides (per-developer).
#
# Copy this file to dev-variables.yml in the same directory and edit
# the values for your workspace. The dev-variables.yml file is
# git-ignored; this .template file is committed as documentation.
#
# Example below targets the E2 demo field-eng workspace's
# central_bank_ireland volume for SDP XML loader smoke testing.

variables:
  catalog:
    default: "users"
  emir_raw_schema:
    default: "matthew_moorcroft"
  emir_landing_path:
    default: "/Volumes/users/matthew_moorcroft/central_bank_ireland/landing/"
  emir_xml_schema_pyld_path:
    default: "/Volumes/users/matthew_moorcroft/central_bank_ireland/schemas/pyld_schema.json"
  emir_xml_schema_hdr_pyld_metadata_path:
    default: "/Volumes/users/matthew_moorcroft/central_bank_ireland/schemas/hdr_pyld_metadata_schema.json"
  emir_xml_xsd_schema_pyld_path:
    default: "/Volumes/users/matthew_moorcroft/central_bank_ireland/schemas/row_tag_schema.xsd"
```

- [ ] **Step 2.4: Create the (git-ignored) real override file**

Write `resources/config/local/dev-variables.yml` with the same body as Step 2.3 (this one is what `bundle deploy` actually reads; the template is documentation).

- [ ] **Step 2.5: Verify the local file is git-ignored**

Run:
```bash
git status
```
Expected: `resources/config/local/dev-variables.yml` does **NOT** appear in the output. Only the `.template`, `.gitignore`, and `databricks.yml` should show.

If the real file does appear, re-check Step 2.1's `.gitignore` patterns.

- [ ] **Step 2.6: Validate the bundle with local override applied**

Run:
```bash
databricks bundle validate -t dev
```
Expected: succeeds. The output should show the overridden `catalog` (`users`) and `emir_*` paths.

- [ ] **Step 2.7: Commit**

```bash
git add .gitignore databricks.yml resources/config/local/dev-variables.yml.template
git commit -m "$(cat <<'EOF'
feat(bundle): support per-developer local variable overrides

Adds resources/config/local/ (git-ignored) wired into databricks.yml
via include. Committed dev-variables.yml.template documents the
override pattern using the E2 central_bank_ireland volume target
for SDP XML loader smoke testing. The matching dev-variables.yml is
git-ignored so each developer can point at their own workspace.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 3: Create SDP source skeleton — imports, helpers, UDFs

**Files:**
- Create: `src/pipelines/xml_loader.py`

This task creates the file in three commits: first the imports + config-param reads + schema loader, then the two UDFs. Each is independently committable.

- [ ] **Step 3.1: Create `src/pipelines/xml_loader.py` with imports, config, and schema loader**

Write the file containing exactly:

```python
"""ESMA XML Loader — Spark Declarative Pipeline.

Parameterized SDP source backing both the EMIR and MiFIR XML loader pipelines.
Reads XML files via Auto Loader, splits malformed rows into a quarantine table
enriched with an lxml XSD-validation error, and produces a public
``{prefix}_raw`` streaming table with payload + header metadata joined per file.

All inputs are supplied via ``spark.conf`` — see the bundle pipeline
``configuration`` block in ``resources/bundle.{emir,mifir}_resources.yml``.

Reference: docs/superpowers/specs/2026-05-11-sdp-xml-loader-design.md
"""

from __future__ import annotations

import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

# --------------------------------------------------------------------------
# Pipeline configuration (set in resources/bundle.*_resources.yml under
# resources.pipelines.<name>.configuration). All values are resolved at
# import time so the @dp.table decorators can reference them.
# --------------------------------------------------------------------------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
TABLE_PREFIX = spark.conf.get("table_prefix")
LANDING_PATH = spark.conf.get("landing_path")
ROW_TAG = spark.conf.get("row_tag")
XML_SCHEMA_PYLD_PATH = spark.conf.get("xml_schema_pyld_path")
XML_SCHEMA_HDR_PYLD_METADATA_PATH = spark.conf.get("xml_schema_hdr_pyld_metadata_path")
XML_XSD_SCHEMA_PYLD_PATH = spark.conf.get("xml_xsd_schema_pyld_path")
WATERMARK_INTERVAL = spark.conf.get("watermark_interval", "15 minutes")

# Fully qualified table names — published to {catalog}.{raw_schema}.
TBL_RAW_XML_PAYLOAD = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_raw_xml_payload"
TBL_FILE_HDR_METADATA = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_file_hdr_metadata"
TBL_QUARANTINE = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_quarantine"
TBL_RAW = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_raw"


def _read_schema(file_path: str) -> StructType:
    """Load a Spark JSON schema file into a StructType.

    Ported from src/notebooks/1_xml_file_loader_body.py — schemas are
    pre-generated by the XSD-conversion notebook and live as JSON files
    in a UC Volume next to the source XSDs.
    """
    with open(file_path, "r") as f:
        return StructType.fromJson(json.loads(f.read()))


# Loaded once at pipeline-start.
XML_PYLD_SCHEMA: StructType = _read_schema(XML_SCHEMA_PYLD_PATH)
XML_HDR_PYLD_METADATA_SCHEMA: StructType = _read_schema(XML_SCHEMA_HDR_PYLD_METADATA_PATH)
```

- [ ] **Step 3.2: Validate the bundle still parses (the new file is not yet referenced)**

Run:
```bash
databricks bundle validate -t dev
```
Expected: succeeds. The file exists but no pipeline points at it yet, so nothing imports it.

- [ ] **Step 3.3: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): scaffold xml_loader.py with config + schema loader

Adds module-level pipeline-config reads via spark.conf, fully-qualified
table-name constants, and a ported readSchema helper that loads
pre-generated JSON schemas from UC Volumes. No @dp.table definitions
yet — those land in subsequent commits.

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 3.4: Add the singleton-cached XSD-validation UDF**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# XSD-validation UDF (used by the quarantine table only).
#
# The XSD schema object is compiled once per Python worker per XSD path
# via _xsd_cache — Auto Loader's per-row XSD validation already runs
# upstream; this UDF only fires on the small minority of rows that
# already failed validation, where we want a human-readable error to
# surface in the quarantine table.
# --------------------------------------------------------------------------

_xsd_cache: dict = {}


def _get_xsd_schema(xsd_path: str):
    """Compile and cache an lxml XMLSchema per path, once per worker."""
    if xsd_path not in _xsd_cache:
        from lxml import etree
        with open(xsd_path, "rb") as f:
            _xsd_cache[xsd_path] = etree.XMLSchema(etree.XML(f.read()))
    return _xsd_cache[xsd_path]


@F.udf(returnType=StringType())
def xsd_error(xml_str: str, xsd_path: str) -> str:
    """Return a verbose XSD-validation error message, or 'XML is valid'."""
    from lxml import etree
    try:
        if xml_str is None:
            return "Invalid XML: input is null"
        schema = _get_xsd_schema(xsd_path)
        schema.assertValid(etree.fromstring(xml_str.encode("utf-8")))
        return "XML is valid"
    except Exception as e:
        return f"Invalid XML: {str(e)}"
```

- [ ] **Step 3.5: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): add singleton-cached xsd_error UDF

Adds an lxml-based XSD validation UDF that returns a verbose error
message for malformed XML rows. The XMLSchema object is compiled once
per Python worker per XSD path via a module-level cache, so the UDF
cost is bounded even though it fires on every quarantine row.

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 3.6: Add the header-extraction UDF (ported verbatim from the legacy notebook)**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# Header-extraction UDF.
#
# Reads the XML file via lxml.iterparse, stops at the first row-tag
# element, strips empty elements, and returns the header-only XML as a
# string. Lenient on failure (returns None) — preserving today's
# notebook behavior. A dp.expect on the parsed header struct is a
# deliberate follow-up, not part of this branch.
# --------------------------------------------------------------------------


def _strip_namespace(tag: str) -> str:
    """Strip ``{ns}name`` prefix from an lxml tag."""
    return tag.split("}")[-1] if "}" in tag else tag


def _remove_empty_elements(element) -> bool:
    """Recursively drop elements that have no children, attributes, or text."""
    children_to_remove = []
    for child in list(element):
        if _remove_empty_elements(child):
            children_to_remove.append(child)
    for child in children_to_remove:
        element.remove(child)
    has_children = len(list(element)) > 0
    has_attributes = bool(element.attrib)
    has_meaningful_text = (
        (element.text and element.text.strip())
        or (element.tail and element.tail.strip())
    )
    return not has_children and not has_attributes and not has_meaningful_text


def _extract_hdr_pyld_metadata(file_path: str, row_tag: str) -> str | None:
    """Return the header-only XML for a single file, stopping at row_tag."""
    from lxml import etree
    try:
        context = etree.iterparse(file_path, events=("start", "end"), recover=True)
        element_stack = []
        skip_depth = 0
        root = None
        found_row_tag = False

        for event, elem in context:
            tag_name = _strip_namespace(elem.tag)
            if event == "start":
                if tag_name == row_tag and not found_row_tag:
                    found_row_tag = True
                    elem.clear()
                    break
                should_skip = (skip_depth > 0) or (tag_name == row_tag)
                if should_skip:
                    skip_depth += 1
                else:
                    new_elem = etree.Element(elem.tag, attrib=elem.attrib)
                    new_elem.tag = _strip_namespace(new_elem.tag)
                    if element_stack:
                        element_stack[-1].append(new_elem)
                    else:
                        root = new_elem
                    element_stack.append(new_elem)
            elif event == "end":
                if skip_depth > 0:
                    skip_depth -= 1
                elif element_stack:
                    current_elem = element_stack.pop()
                    current_elem.text = elem.text
                    current_elem.tail = elem.tail
                elem.clear()

        if root is not None:
            _remove_empty_elements(root)
            return etree.tostring(root, encoding="unicode", pretty_print=True)
        return None
    except Exception as e:
        # Lenient: failure surfaces as null hdr_pyld_metadata downstream.
        print(f"Error processing {file_path}: {e}")
        return None


extract_hdr_pyld_metadata_udf = F.udf(_extract_hdr_pyld_metadata, StringType())
```

- [ ] **Step 3.7: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): port lxml header-extraction UDF

Ports the per-file header-XML extraction UDF verbatim from the
classic loader notebook. Uses lxml.iterparse to read each file up
to the first row-tag element, strips empty elements, and returns
the header-only XML string. Lenient on failure (returns None) —
matches today's behavior; a dp.expect is a deliberate follow-up.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 4: Add the four `@dp.table()` definitions

**Files:**
- Modify: `src/pipelines/xml_loader.py`

Each table is one commit so the lineage is easy to review and easy to revert.

- [ ] **Step 4.1: Add `{prefix}_raw_xml_payload` (Auto Loader)**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# Table 1 of 4: {prefix}_raw_xml_payload (intermediate — internal use)
#
# Auto Loader reads XML files from the landing path. ALL rows (good +
# corrupted) land here. Downstream tables filter on corrupted_record.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_RAW_XML_PAYLOAD,
    comment=(
        "Internal: raw XML payload rows from Auto Loader, BEFORE good/bad "
        "split. Includes corrupted_record + rescued_data. Downstream tables "
        f"{TBL_FILE_HDR_METADATA}, {TBL_QUARANTINE}, and {TBL_RAW} consume "
        "this."
    ),
    cluster_by=["AUTO"],
)
def raw_xml_payload():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", ROW_TAG)
        .option("rowValidationXSDPath", XML_XSD_SCHEMA_PYLD_PATH)
        .option("columnNameOfCorruptRecord", "corrupted_record")
        .option("rescuedDataColumn", "rescued_data")
        .option("mode", "PERMISSIVE")
        .schema(XML_PYLD_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn(
            "_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

- [ ] **Step 4.2: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: still succeeds (no pipeline references the file yet).

- [ ] **Step 4.3: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): add raw_xml_payload streaming table

First of four @dp.table() definitions: Auto Loader cloudFiles XML
streaming source with XSD row validation, PERMISSIVE mode,
corrupted_record + rescued_data columns, and file metadata via
_metadata.file_path / file_modification_time. cluster_by=AUTO.

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 4.4: Add `{prefix}_file_hdr_metadata` (watermark + dedup + UDFs + regex)**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# Table 2 of 4: {prefix}_file_hdr_metadata (intermediate — internal use)
#
# One row per file. Built from good rows of raw_xml_payload via
# watermark + dropDuplicatesWithinWatermark on file_path so the UDFs
# fire once per file per trigger. Header XML is extracted with lxml
# and parsed via from_xml using the pre-loaded JSON schema. Filename
# regex extracts FileBatchIndex / Size / Version / ESMADate.
# --------------------------------------------------------------------------

_FILE_INDEX_PATTERN = r"\d\d\d\d\d\d-\d"
_ESMA_DATE_PATTERN = r"-\d\d\d\d\d\d_"


@dp.table(
    name=TBL_FILE_HDR_METADATA,
    comment=(
        "Internal: one row per source XML file with parsed header struct "
        "and filename-regex columns. Consumed by "
        f"{TBL_RAW} for the per-row enrichment join."
    ),
    cluster_by=["AUTO"],
)
def file_hdr_metadata():
    return (
        spark.readStream.table(TBL_RAW_XML_PAYLOAD)
        .filter(F.col("corrupted_record").isNull())
        .withWatermark("_file_modification_time", WATERMARK_INTERVAL)
        .dropDuplicatesWithinWatermark(["file_path"])
        .select(
            "file_path",
            "file_name",
            "_file_modification_time",
            extract_hdr_pyld_metadata_udf(
                F.col("file_path"), F.lit(ROW_TAG)
            ).alias("_hdr_xml"),
        )
        .withColumn(
            "hdr_pyld_metadata",
            F.from_xml(F.col("_hdr_xml"), XML_HDR_PYLD_METADATA_SCHEMA),
        )
        .drop("_hdr_xml")
        .withColumn(
            "FileBatchIndex",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                1, 3,
            ),
        )
        .withColumn(
            "FileBatchSize",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                4, 3,
            ),
        )
        .withColumn(
            "FileVersion",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                8, 1,
            ),
        )
        .withColumn(
            "ESMADate",
            F.concat(
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    2, 2,
                ),
                F.lit("-"),
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    4, 2,
                ),
                F.lit("-"),
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    6, 2,
                ),
            ),
        )
    )
```

- [ ] **Step 4.5: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: succeeds.

- [ ] **Step 4.6: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): add file_hdr_metadata streaming table

Second of four @dp.table() definitions: one row per file via
withWatermark(_file_modification_time, 15 min) +
dropDuplicatesWithinWatermark(file_path). Calls the lxml header UDF
once per file per trigger, parses the resulting XML string with
from_xml using the pre-loaded JSON header schema, and adds the
existing filename-regex columns (FileBatchIndex/Size/Version/ESMADate).

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 4.7: Add `{prefix}_quarantine` (public — bad rows with XSD error)**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# Table 3 of 4: {prefix}_quarantine (PUBLIC)
#
# Bad rows from raw_xml_payload (corrupted_record IS NOT NULL),
# enriched with a verbose XSD-validation error from the singleton-
# cached lxml UDF. Public so Ops / data stewards can triage without
# touching pipeline internals.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_QUARANTINE,
    comment=(
        "Public: malformed XML rows that failed Auto Loader XSD validation, "
        "enriched with xsd_validation_result (human-readable lxml error)."
    ),
    cluster_by=["AUTO"],
)
def quarantine():
    return (
        spark.readStream.table(TBL_RAW_XML_PAYLOAD)
        .filter(F.col("corrupted_record").isNotNull())
        .withColumn(
            "xsd_validation_result",
            xsd_error(F.col("corrupted_record"), F.lit(XML_XSD_SCHEMA_PYLD_PATH)),
        )
        .select(
            "file_path",
            "file_name",
            "_file_modification_time",
            "_ingested_at",
            "corrupted_record",
            "rescued_data",
            "xsd_validation_result",
        )
    )
```

- [ ] **Step 4.8: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: succeeds.

- [ ] **Step 4.9: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): add quarantine streaming table

Third of four @dp.table() definitions: routes bad rows
(corrupted_record IS NOT NULL) from raw_xml_payload into a public
quarantine table, enriched with xsd_validation_result from the
singleton-cached lxml XSD UDF. Public so analysts can triage
malformed files without touching pipeline internals.

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 4.10: Add `{prefix}_raw` (public — drop-in join)**

Append to `src/pipelines/xml_loader.py`:

```python


# --------------------------------------------------------------------------
# Table 4 of 4: {prefix}_raw (PUBLIC — drop-in for the flatten notebook)
#
# Watermarked stream-stream join of good payload rows with file-level
# headers on file_path. State is bounded by in-flight file count, not
# row count, because both sides watermark on _file_modification_time.
# Output preserves the column contract consumed by
# src/notebooks/2_flatten_explode_table.py.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_RAW,
    comment=(
        "Public: per-row payload joined with per-file header metadata. "
        "Drop-in replacement for the output of the legacy "
        "1_xml_file_loader_body.py notebook; consumed by the flatten step."
    ),
    cluster_by=["AUTO"],
)
def raw():
    payload = (
        spark.readStream.table(TBL_RAW_XML_PAYLOAD)
        .filter(F.col("corrupted_record").isNull())
        .withWatermark("_file_modification_time", WATERMARK_INTERVAL)
    )
    headers = (
        spark.readStream.table(TBL_FILE_HDR_METADATA)
        .withWatermark("_file_modification_time", WATERMARK_INTERVAL)
    )
    # Right-side join columns are aliased to avoid duplicate column names
    # on file_path / file_name / _file_modification_time after the join.
    headers_aliased = headers.select(
        F.col("file_path").alias("_hdr_file_path"),
        "hdr_pyld_metadata",
        "FileBatchIndex",
        "FileBatchSize",
        "FileVersion",
        "ESMADate",
    )
    return (
        payload.join(
            headers_aliased,
            payload["file_path"] == headers_aliased["_hdr_file_path"],
            "inner",
        )
        .drop("_hdr_file_path", "corrupted_record", "rescued_data")
    )
```

- [ ] **Step 4.11: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: succeeds.

- [ ] **Step 4.12: Commit**

```bash
git add src/pipelines/xml_loader.py
git commit -m "$(cat <<'EOF'
feat(sdp): add public {prefix}_raw streaming table

Fourth and final @dp.table() definition: watermarked stream-stream
inner join of good payload rows with file_hdr_metadata on file_path.
Both sides watermark on _file_modification_time so join state is
O(in-flight files), not O(rows). Drops the right-side join key plus
corrupted_record + rescued_data (those live in {prefix}_quarantine).

This is the public, drop-in output that the existing
src/notebooks/2_flatten_explode_table.py notebook will consume
unchanged once the bundle resources are rewired.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 5: Add EMIR pipeline resource + rewire EMIR job

**Files:**
- Modify: `resources/bundle.emir_resources.yml`

- [ ] **Step 5.1: Add commented section header above the existing `jobs:` block**

Open `resources/bundle.emir_resources.yml`. Find the line `resources:` followed by `  jobs:`. Insert a comment header immediately after `resources:` and before `  jobs:`:

```yaml
resources:

  # === Classic Notebook Jobs ===
  jobs:
```

- [ ] **Step 5.2: Remove the `emir_xml_load` task and its `depends_on` reference**

Find the `EMIR_XML_Processing` job's `tasks:` list. Delete the entire `emir_xml_load` task block:

```yaml
        - task_key: emir_xml_load
          notebook_task:
            notebook_path: ../src/notebooks/1_xml_file_loader_body.py
            source: WORKSPACE
            
```

…and remove the `depends_on` clause from the remaining `emir_xml_flatten` task so it stands alone. The resulting block must look exactly like:

```yaml
      tasks:
        - task_key: emir_xml_flatten
          notebook_task:
            notebook_path: ../src/notebooks/2_flatten_explode_table.py
            source: WORKSPACE
```

- [ ] **Step 5.3: Add the SDP pipeline resource at the end of the file**

Append to `resources/bundle.emir_resources.yml`:

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
        catalog: ${var.emir_catalog}
        raw_schema: ${var.emir_raw_schema}
        table_prefix: ${var.emir_table_prefix}
        landing_path: ${var.emir_landing_path}
        row_tag: ${var.emir_row_tag}
        xml_schema_pyld_path: ${var.emir_xml_schema_pyld_path}
        xml_schema_hdr_pyld_metadata_path: ${var.emir_xml_schema_hdr_pyld_metadata_path}
        xml_xsd_schema_pyld_path: ${var.emir_xml_xsd_schema_pyld_path}
        watermark_interval: "15 minutes"
```

- [ ] **Step 5.4: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: succeeds. The output should now list `emir_xml_loader_pipeline` in the resources summary, and `EMIR_XML_Processing` should show only one task (`emir_xml_flatten`).

If validation fails:
- "no task in job" — confirm the flatten task is still present and the `tasks:` list has at least one entry
- "library path not found" — confirm `src/pipelines/xml_loader.py` exists from Task 3
- variable not resolved — confirm `bundle.variables.yml` already defines the referenced `${var.emir_*}` variables (no changes expected to that file)

- [ ] **Step 5.5: Commit**

```bash
git add resources/bundle.emir_resources.yml
git commit -m "$(cat <<'EOF'
feat(bundle): add EMIR SDP pipeline + rewire EMIR job

Adds emir_xml_loader_pipeline (serverless, channel=PREVIEW, lxml
declared in environment.dependencies) under a new commented
'Spark Declarative Pipelines' section in bundle.emir_resources.yml.
Configuration block wires all spark.conf.get(...) keys consumed by
src/pipelines/xml_loader.py to existing ${var.emir_*} variables.

EMIR_XML_Processing job no longer runs the load step — that's
replaced by the pipeline. The flatten task continues to point at
src/notebooks/2_flatten_explode_table.py and runs standalone for now.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 6: Add MiFIR pipeline resource + rewire MiFIR job

**Files:**
- Modify: `resources/bundle.mifir_resources.yml`

Mirror of Task 5 for MiFIR. Same operations on a different file with `mifir`/`Tx` substitutions.

- [ ] **Step 6.1: Add `# === Classic Notebook Jobs ===` comment header above the existing `jobs:` block**

Same edit as Step 5.1 but in `resources/bundle.mifir_resources.yml`.

- [ ] **Step 6.2: Remove `mifir_xml_load` task and its `depends_on`**

Same edit as Step 5.2 for the MiFIR file. The remaining tasks block must look exactly like:

```yaml
      tasks:
        - task_key: mifir_xml_flatten
          notebook_task:
            notebook_path: ../src/notebooks/2_flatten_explode_table.py
            source: WORKSPACE
```

- [ ] **Step 6.3: Append the MiFIR pipeline resource**

Append to `resources/bundle.mifir_resources.yml`:

```yaml

  # === Spark Declarative Pipelines ===
  pipelines:
    mifir_xml_loader_pipeline:
      name: "MiFIR XML Loader (SDP)"
      catalog: ${var.mifir_catalog}
      schema: ${var.mifir_raw_schema}
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
        catalog: ${var.mifir_catalog}
        raw_schema: ${var.mifir_raw_schema}
        table_prefix: ${var.mifir_table_prefix}
        landing_path: ${var.mifir_landing_path}
        row_tag: ${var.mifir_row_tag}
        xml_schema_pyld_path: ${var.mifir_xml_schema_pyld_path}
        xml_schema_hdr_pyld_metadata_path: ${var.mifir_xml_schema_hdr_pyld_metadata_path}
        xml_xsd_schema_pyld_path: ${var.mifir_xml_xsd_schema_pyld_path}
        watermark_interval: "15 minutes"
```

- [ ] **Step 6.4: Validate bundle**

```bash
databricks bundle validate -t dev
```
Expected: succeeds. Output should now list both pipelines (`emir_xml_loader_pipeline`, `mifir_xml_loader_pipeline`) and both jobs with the load task removed.

- [ ] **Step 6.5: Commit**

```bash
git add resources/bundle.mifir_resources.yml
git commit -m "$(cat <<'EOF'
feat(bundle): add MiFIR SDP pipeline + rewire MiFIR job

Mirror of the EMIR pipeline addition for MiFIR. Both pipelines point
at the same parameterized src/pipelines/xml_loader.py source file;
MiFIR-specific values (row_tag=Tx, mifir_* paths) flow through the
existing ${var.mifir_*} variables. MiFIR_XML_Processing job now
runs only the flatten task.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 7: Add target overrides for development mode

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 7.1: Add per-target pipeline development overrides**

In `databricks.yml`, replace the existing `targets:` block:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "esma_dev"
      
  prod:
    mode: production
    variables:
      catalog: "esma_prod"
```

with:

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "esma_dev"
    resources:
      pipelines:
        emir_xml_loader_pipeline:
          development: true
        mifir_xml_loader_pipeline:
          development: true

  prod:
    mode: production
    variables:
      catalog: "esma_prod"
    resources:
      pipelines:
        emir_xml_loader_pipeline:
          development: false
        mifir_xml_loader_pipeline:
          development: false
```

- [ ] **Step 7.2: Validate bundle on both targets**

Run:
```bash
databricks bundle validate -t dev
databricks bundle validate -t prod
```
Expected: both succeed.

- [ ] **Step 7.3: Commit**

```bash
git add databricks.yml
git commit -m "$(cat <<'EOF'
feat(bundle): wire dev/prod overrides for SDP development mode

Sets development=true on both SDP pipelines under the dev target so
runs use the SDP development mode (no automatic retries on failure,
faster iteration). prod target explicitly pins development=false.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 8: Update README project-structure section

**Files:**
- Modify: `README.md`

- [ ] **Step 8.1: Replace the `## Project Structure` section**

In `README.md`, find the section starting with `## Project Structure` and replace its code block (lines 44-64 of the file as last read) with:

```
esma_xml_ingestion/
├── databricks.yml                          # Main bundle config
├── resources/
│   ├── bundle.variables.yml                # Shared variables
│   ├── bundle.emir_resources.yml           # EMIR jobs + SDP pipeline
│   ├── bundle.mifir_resources.yml          # MiFIR jobs + SDP pipeline
│   ├── bundle.new-type_resources.yml.template
│   └── config/
│       └── local/                          # git-ignored per-developer overrides
│           └── dev-variables.yml.template
├── src/
│   ├── notebooks/                          # Classic notebooks (jobs)
│   │   ├── 0_1_xml_schema_xsd.py           # XSD → JSON Spark schemas
│   │   ├── 1_xml_file_loader_body.py       # (legacy reference — replaced by SDP)
│   │   └── 2_flatten_explode_table.py      # Flatten + explode → bronze
│   ├── pipelines/                          # Spark Declarative Pipelines
│   │   └── xml_loader.py                   # Parameterized SDP for EMIR + MiFIR
│   └── util/
│       └── xsd_processor.py                # XSD parsing helpers (Python)
├── fixtures/                               # Sample data and test files
├── scratch/                                # Development workspace
└── docs/superpowers/                       # Specs and implementation plans
```

And update the "Key Components" bullet list immediately below the diagram to:

```
### Key Components

- **`databricks.yml`**: Main bundle configuration that defines deployment targets and includes resource files
- **`resources/`**: Per-regulation jobs and SDP pipelines (EMIR, MiFIR), shared variables, and per-developer local overrides
- **`src/pipelines/`**: Parameterized Spark Declarative Pipeline (SDP) source for XML ingestion → `{prefix}_raw` + `{prefix}_quarantine`
- **`src/notebooks/`**: Classic notebooks for XSD-to-schema preparation and the flatten/explode bronze step
- **`src/util/`**: Python helpers for XSD processing
```

- [ ] **Step 8.2: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: update README project structure for SDP layout

Reflects the new src/notebooks/ + src/pipelines/ split, the
per-regulation bundle YAMLs that now contain both jobs and SDP
pipelines, and the new resources/config/local/ overrides directory.
Brings the README's project-structure diagram into agreement with
the actual repo layout.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 9: Deploy + smoke-test EMIR pipeline on E2

This is the end-to-end behavioral test. Requires authenticated CLI session to `e2-demo-field-eng.cloud.databricks.com` with read/write to `users.matthew_moorcroft.*` and the central_bank_ireland volume. The `resources/config/local/dev-variables.yml` from Task 2 supplies the E2 paths.

- [ ] **Step 9.1: Confirm CLI auth to E2**

Run:
```bash
databricks current-user me --profile e2-demo-field-eng 2>/dev/null \
  || databricks current-user me
```
Expected: returns the matthew.moorcroft user record. If it fails, run `databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com` first.

- [ ] **Step 9.2: Final bundle validate against E2 paths**

Run:
```bash
databricks bundle validate -t dev
```
Expected: succeeds. The output's resolved configuration block for `emir_xml_loader_pipeline` should show:
- `landing_path: /Volumes/users/matthew_moorcroft/central_bank_ireland/landing/`
- `catalog: users`
- `raw_schema: matthew_moorcroft`

If any value is wrong, edit `resources/config/local/dev-variables.yml` (NOT the template).

- [ ] **Step 9.3: Deploy bundle to E2 dev target**

Run:
```bash
databricks bundle deploy -t dev
```
Expected: deployment succeeds. Output lists the created pipelines (`[dev <user>] emir_xml_loader_pipeline`, `[dev <user>] mifir_xml_loader_pipeline`) and the rewired jobs.

- [ ] **Step 9.4: Confirm sample XML data is present in landing**

Run:
```bash
databricks fs ls dbfs:/Volumes/users/matthew_moorcroft/central_bank_ireland/landing/
```
Expected: at least one `.xml` file listed.

If empty: stage a known-good EMIR XML file there before proceeding (this is a manual data-staging step — the smoke test cannot run on empty input).

- [ ] **Step 9.5: Run the EMIR pipeline**

Run:
```bash
databricks bundle run emir_xml_loader_pipeline -t dev
```
Expected: pipeline runs to COMPLETED status. CLI streams progress and reports row counts written to each of the four tables.

If FAILED: capture the pipeline `update_id` from the output and inspect logs:
```bash
databricks pipelines get-update <pipeline_id> <update_id>
```
Common failure modes and fixes:
- `lxml import failure` → confirm `environment.dependencies` block in the bundle YAML
- `spark.conf.get("watermark_interval") returns None` → the `WATERMARK_INTERVAL = spark.conf.get(...)` line uses a default; verify Step 3.1
- `path not found: /Volumes/users/...` → confirm the local override file and CLI auth target the right workspace
- `Table not found: TBL_FILE_HDR_METADATA` (inner-table reference) → confirm Task 4.4 used `TBL_RAW_XML_PAYLOAD` as the unqualified-or-fully-qualified table name consistent with how the table was declared

- [ ] **Step 9.6: Verify the four tables exist and have expected shape**

Run (substituting the actual workspace SQL warehouse ID for `<warehouse_id>` — the smallest serverless warehouse in the workspace works):
```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT table_name, row_count_estimate FROM (
    SELECT 'emir_raw_xml_payload' AS table_name, (SELECT COUNT(*) FROM users.matthew_moorcroft.emir_raw_xml_payload) AS row_count_estimate UNION ALL
    SELECT 'emir_file_hdr_metadata', (SELECT COUNT(*) FROM users.matthew_moorcroft.emir_file_hdr_metadata) UNION ALL
    SELECT 'emir_quarantine', (SELECT COUNT(*) FROM users.matthew_moorcroft.emir_quarantine) UNION ALL
    SELECT 'emir_raw', (SELECT COUNT(*) FROM users.matthew_moorcroft.emir_raw)
  )"
```

Expected:
- `emir_raw_xml_payload` row count > 0
- `emir_file_hdr_metadata` row count equals number of distinct files in landing
- `emir_quarantine` row count = number of bad rows (likely 0 unless a malformed file was staged)
- `emir_raw` row count = `emir_raw_xml_payload` row count − `emir_quarantine` row count

- [ ] **Step 9.7: Spot-check a row from `emir_raw`**

Run:
```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT file_name, hdr_pyld_metadata, FileBatchIndex, FileBatchSize, FileVersion, ESMADate FROM users.matthew_moorcroft.emir_raw LIMIT 1"
```
Expected: row returned, `hdr_pyld_metadata` is a populated struct (not null), filename-regex columns populated.

If `hdr_pyld_metadata` is null on every row: the header UDF is failing silently. Check pipeline event-log for "Error processing" lines; the lenient `try/except None` in the UDF swallows the trace into stdout.

- [ ] **Step 9.8: (Optional) Quarantine sanity check**

Stage a known-bad XML file (e.g., one with truncated `<Stat>` element) in the E2 landing path, retrigger the pipeline:
```bash
databricks bundle run emir_xml_loader_pipeline -t dev
```

Then query:
```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT file_name, xsd_validation_result FROM users.matthew_moorcroft.emir_quarantine LIMIT 5"
```
Expected: at least one row with a non-empty `xsd_validation_result` starting with `Invalid XML:`.

- [ ] **Step 9.9: Negative test — stop + restart, no duplicates**

Run the pipeline again with no new input:
```bash
databricks bundle run emir_xml_loader_pipeline -t dev
```
Expected: completes quickly (no new files to process). After completion, re-run the row-count query from Step 9.6 — counts must be **identical** to the first run. Any growth indicates lost checkpoint state or duplicate ingestion.

- [ ] **Step 9.10: Negative test — new file mid-idle, only new processed**

While the pipeline is idle, copy one additional XML file into the landing path:
```bash
databricks fs cp <local-or-known-good-xml> \
  dbfs:/Volumes/users/matthew_moorcroft/central_bank_ireland/landing/<new-filename>.xml
```

Then re-run the pipeline:
```bash
databricks bundle run emir_xml_loader_pipeline -t dev
```

Run the row-count query from Step 9.6 again. Expected: each table grows by *only* the rows from `<new-filename>.xml` (i.e., older files are not reprocessed). If counts grew by more than the new file's row count, Auto Loader checkpointing is misconfigured.

- [ ] **Step 9.11: Downstream regression — run flatten job against SDP `_raw`**

The whole point of the `{prefix}_raw` drop-in contract is that the existing flatten notebook keeps working unchanged. Verify:

Get the `EMIR_XML_Processing` job ID:
```bash
databricks jobs list --output json | jq '.jobs[] | select(.settings.name == "EMIR XML Processing") | .job_id'
```

Trigger it:
```bash
databricks jobs run-now --job-id <emir_xml_processing_job_id>
```

Expected: job's single `emir_xml_flatten` task completes successfully. Confirm bronze tables exist:
```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SHOW TABLES IN users.matthew_moorcroft_bronze"
```

Expected: at least one bronze table (`emir__base` and its child flattened tables, per the recursive flattening). The exact set depends on the input XML schema — what matters is that flatten reads `users.matthew_moorcroft.emir_raw` without error and writes output.

If flatten fails with a schema-mismatch error like "column X not found", the SDP `_raw` is missing a column the flatten notebook expects. Investigate via:
```bash
databricks experimental aitools tools discover-schema \
  users.matthew_moorcroft.emir_raw users.matthew_moorcroft_legacy.emir__raw
```
and add the missing column source in `src/pipelines/xml_loader.py` Step 4.10 before iterating.

- [ ] **Step 9.12: MiFIR deploy-only check (no run)**

The deploy from Step 9.3 already created the MiFIR pipeline. Confirm it exists:
```bash
databricks pipelines list-pipelines --filter "name LIKE '%MiFIR XML Loader (SDP)%'"
```
Expected: one pipeline listed. No run-test performed unless MiFIR data is available in E2.

- [ ] **Step 9.13: Document the smoke-test results in the branch**

Create `docs/superpowers/plans/2026-05-11-sdp-xml-loader-smoke-test-results.md` with the actual row counts from Step 9.6, the EMIR pipeline `update_id`, results of negative tests (Steps 9.9, 9.10), flatten regression outcome (Step 9.11), and any anomalies observed. Keep it short — bullet points are fine.

- [ ] **Step 9.14: Commit smoke-test results**

```bash
git add docs/superpowers/plans/2026-05-11-sdp-xml-loader-smoke-test-results.md
git commit -m "$(cat <<'EOF'
test(sdp): E2 smoke-test results for EMIR XML loader pipeline

Records actual row counts and pipeline update_id from the E2 deploy +
run against the central_bank_ireland landing volume. EMIR pipeline
end-to-end green; MiFIR pipeline deploy-only validated (no run-test
due to no MiFIR data available).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 10: Parity check vs. legacy notebook

This proves the SDP `{prefix}_raw` is a drop-in replacement for the legacy notebook's output.

- [ ] **Step 10.1: Re-deploy the bundle with a parallel `_legacy` schema for the classic notebook**

Add a temporary override to your local `resources/config/local/dev-variables.yml`:
```yaml
  emir_raw_schema_legacy:
    default: "matthew_moorcroft_legacy"
```

Run the legacy notebook (`src/notebooks/1_xml_file_loader_body.py`) manually from the workspace UI or via:
```bash
databricks jobs run-now --job-id <EMIR_XML_Processing_id> \
  --notebook-params '{"raw_schema":"matthew_moorcroft_legacy"}'
```

(The job's flatten task will then fail trying to read `matthew_moorcroft_legacy.emir__raw` — that's expected for parity-only purposes; ignore the failure.)

- [ ] **Step 10.2: Compare row counts**

```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT 'sdp' AS source, COUNT(*) AS rows FROM users.matthew_moorcroft.emir_raw
   UNION ALL
   SELECT 'legacy', COUNT(*) FROM users.matthew_moorcroft_legacy.emir__raw"
```

Expected:
- `sdp` row count = `legacy` row count − (number of corrupted_record rows)
- If the legacy output happened to include zero corrupted rows: counts equal
- Otherwise: the count delta should equal `SELECT COUNT(*) FROM users.matthew_moorcroft.emir_quarantine`

- [ ] **Step 10.3: Spot-check a column subset for a single file**

```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT file_name, ESMADate, FileBatchIndex, hdr_pyld_metadata FROM users.matthew_moorcroft.emir_raw WHERE file_name = '<pick-a-real-filename>' LIMIT 1"
```

Compare visually to:
```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "SELECT file_name, ESMADate, FileBatchIndex, hdr_pyld_metadata FROM users.matthew_moorcroft_legacy.emir__raw WHERE file_name = '<same-filename>' LIMIT 1"
```

Expected: filename-regex columns identical, `hdr_pyld_metadata` struct identical contents.

- [ ] **Step 10.4: Append parity results to the smoke-test doc and commit**

Edit `docs/superpowers/plans/2026-05-11-sdp-xml-loader-smoke-test-results.md`, add a "Parity check" section with the two row counts and the column comparison.

```bash
git add docs/superpowers/plans/2026-05-11-sdp-xml-loader-smoke-test-results.md
git commit -m "$(cat <<'EOF'
test(sdp): parity check vs legacy notebook output

Records row-count comparison between SDP {prefix}_raw and the
legacy notebook's _raw output (run into a parallel _legacy schema).
SDP rows = legacy rows minus quarantine rows, confirming the
public-table column contract and drop-in compatibility for the
flatten step.

Co-authored-by: Isaac
EOF
)"
```

- [ ] **Step 10.5: Clean up the parallel `_legacy` schema**

```bash
databricks experimental aitools tools query --warehouse <warehouse_id> \
  "DROP SCHEMA users.matthew_moorcroft_legacy CASCADE"
```

Then remove `emir_raw_schema_legacy` from your local `resources/config/local/dev-variables.yml` (no commit needed — local file).

---

## Task 11: Open PR

- [ ] **Step 11.1: Push branch**

```bash
git push -u origin feat/sdp-xml-loader
```

- [ ] **Step 11.2: Open PR via gh CLI**

```bash
gh pr create --title "feat: convert XML file loader to Spark Declarative Pipeline" --body "$(cat <<'EOF'
## Summary

- Converts `src/1_xml_file_loader_body.py` from a classic Databricks notebook into a parameterized Lakeflow Spark Declarative Pipeline (SDP).
- Adds 4-table architecture in `src/pipelines/xml_loader.py`: `{prefix}_raw_xml_payload` (Auto Loader) → `{prefix}_file_hdr_metadata` (watermarked + dedup + lxml header UDF) → `{prefix}_quarantine` (bad rows + lxml XSD error) → `{prefix}_raw` (public, drop-in for the flatten notebook).
- One SDP source file, two pipeline resources (EMIR + MiFIR), wired through existing `${var.{emir,mifir}_*}` bundle variables.
- Repo restructured into `src/notebooks/` (legacy classic notebooks) and `src/pipelines/` (SDP source files).
- Quarantine path catches malformed XML rows with a human-readable `lxml.etree.XMLSchema.assertValid` error, replacing today's silent `corrupted_record IS NOT NULL` rows.

Reference: see `docs/superpowers/specs/2026-05-11-sdp-xml-loader-design.md` for the approved design and `docs/superpowers/plans/2026-05-11-sdp-xml-loader.md` for the task-by-task implementation plan.

## Test plan

- [x] `databricks bundle validate -t dev` succeeds
- [x] `databricks bundle validate -t prod` succeeds
- [x] `databricks bundle deploy -t dev` deploys both pipelines + rewired jobs to E2
- [x] `databricks bundle run emir_xml_loader_pipeline -t dev` completes; row counts captured in `smoke-test-results.md`
- [x] Parity check vs. legacy notebook output documented in `smoke-test-results.md`
- [x] MiFIR pipeline deploy verified (no live data run-test)

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 11.3: Confirm PR URL is reachable**

The PR URL from Step 11.2's output should open in a browser and show all task-level commits in the diff (one commit per @dp.table addition, one for each bundle change, one for the smoke-test results).

---

## Out-of-Scope (Follow-Up Branches)

Per the spec's §9, these are deliberately not in this plan:

- Converting `src/notebooks/2_flatten_explode_table.py` to SDP. Once done, the SDP loader and flatten will be re-linked under a single job via `pipeline_task`.
- Adding `@dp.expect_or_drop("valid_header", "hdr_pyld_metadata IS NOT NULL")` once lenient behavior is no longer needed.
- Moving `xsd_error` return type from `StringType` to a struct.
- Unit tests for the two UDFs.
- Updating `.github/scripts/export_databricks_notebooks.py` to include plain `.py` pipeline files in the published docs site.
- Converting `0_1_xml_schema_xsd.py`'s Scala XSDToSchema step to Python-native so the whole accelerator runs serverless without Scala.
