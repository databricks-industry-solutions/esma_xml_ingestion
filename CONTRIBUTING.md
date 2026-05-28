# Contributing

Thanks for your interest in improving the ESMA XML Ingestion Solution Accelerator. This file covers the contributor agreement, local dev setup, branch conventions, the asset-bundle workflow, and how to add a new ESMA regulation.

---

## Contributor License Agreement (CLA)

By submitting a contribution to this repository, you certify that:

1. **You have the right to submit the contribution.**
   You created the code/content yourself, or you have the right to submit it under the project's license.

2. **You grant us a license to use your contribution.**
   You agree that your contribution will be licensed under the same terms as the rest of this project, and you grant the project maintainers the right to use, modify, and distribute your contribution as part of the project.

3. **You are not submitting confidential or proprietary information.**
   Your contribution does not include anything you don't have permission to share publicly.

If you are contributing on behalf of an organization, you confirm that you have the authority to do so. You agree to confirm these terms in your pull request. Any request that does not explicitly accept the terms will be assumed to have accepted.

---

## Local development setup

```bash
# 1. Clone
git clone git@github.com:databricks-industry-solutions/esma_xml_ingestion.git
cd esma_xml_ingestion

# 2. Install the Databricks CLI (v0.218.0+)
brew tap databricks/tap && brew install databricks
# or follow https://docs.databricks.com/dev-tools/cli/install.html

# 3. Authenticate to your dev workspace
databricks configure --profile dev
# (or use OAuth M2M — DATABRICKS_HOST + DATABRICKS_CLIENT_ID/_SECRET)

# 4. Copy the dev-variables template and edit for your workspace
cp resources/config/local/dev-variables.yml.template \
   resources/config/local/dev-variables.yml
# Replace <your_catalog>, <your_username>, <your_volume> with real values

# 5. Validate the bundle
databricks bundle validate --target dev
```

---

## Branch conventions

| Branch | Purpose |
|---|---|
| `main` | Current SDP-based architecture. Default branch. All PRs target `main`. |
| `legacy/notebook-approach` | Historical snapshot of the pre-SDP, notebook-based architecture. Preserved as a reference. Do not merge into `main`. |
| `feat/<short-name>` | Feature branches. Open a PR into `main` when ready. |
| `fix/<short-name>` | Bug-fix branches. Same workflow as `feat/*`. |

Feature branches should be deleted once merged. `main` and `legacy/notebook-approach` are the only long-lived branches.

---

## Asset Bundle workflow

```bash
# Validate (catches YAML / variable resolution errors before deploying)
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run a specific resource (job or pipeline)
databricks bundle run EMIR_Schema_Creation --target dev
databricks bundle run emir_pipeline --target dev   # bronze + silver in one update
databricks bundle run mifir_pipeline --target dev

# Tear it down
./scripts/cleanup.sh
```

For prod deployments, switch the target: `--target prod`. The `databricks.yml` file maps targets to catalogs (`esma_dev` vs `esma_prod`) and toggles `development: true|false` on the SDP pipelines.

---

## Adding a new ESMA regulation

1. **Copy the template:**

   ```bash
   cp resources/bundle.new-type_resources.yml.template \
      resources/bundle.<regulation>_resources.yml
   ```

   Replace `new-type` / `New-Type` / `NEW_TYPE` placeholders throughout the file with your regulation's prefix (e.g. `sftr`, `Sftr`, `SFTR`).

2. **Add variables** to `resources/bundle.variables.yml` — mirror the existing `emir_*` / `mifir_*` blocks. Required variables include:

   - `<prefix>_catalog`, `<prefix>_raw_schema`, `<prefix>_table_prefix`
   - `<prefix>_landing_path`, `<prefix>_processed_path`, `<prefix>_checkpoint_path`
   - `<prefix>_row_tag` (the ISO 20022 row element, e.g. `Tx`, `Stat`, `Rpt`)
   - `<prefix>_xml_schema_pyld_path`, `<prefix>_xml_schema_hdr_pyld_metadata_path`, `<prefix>_xml_xsd_schema_pyld_path`
   - `<prefix>_enable_xsd_validation` (default `"true"`)
   - `<prefix>_enable_filename_regex` (default `"true"`)
   - `<prefix>_clean_source_mode` (default `"MOVE"`)
   - `<prefix>_clean_source_retention` (default `"7 days"`)

3. **Run Schema Prep** against the regulator's XSDs:

   ```bash
   databricks bundle run <PREFIX>_Schema_Creation --target dev
   ```

   This produces the JSON schemas + row-tag XSD that the bronze loader consumes.

4. **Bronze ingests for free.** The bronze SDP (`src/pipelines/xml_loader.py`) is regulation-agnostic — once Schema Prep has run and the DAB resources point at the new schemas, the bronze pipeline ingests the new regime without code changes.

5. **Author a domain silver module** (optional but recommended). Mirror `src/pipelines/silver_emir.py` or `silver_mifir.py`:
   - One wide-flat fact table per record type (e.g. `transaction`, `position_report`).
   - Per-regime envelope table (`submission_file`).
   - Explode + discriminator pattern for party/counterparty arrays.

6. **Deploy and smoke-test in dev** before opening a PR.

---

## Pull request expectations

- **Pre-commit secret scanning runs automatically.** The Databricks pre-commit hook scans for credentials/tokens before allowing a commit. Don't disable it.
- **Pre-push secret scanning runs on push.** Same hook, second check.
- **PRs target `main`.** Squash-merge or rebase-merge — both are fine; project doesn't enforce.
- **Co-authored-by** trailers are encouraged when AI assistance was used.
- **No automated tests yet.** Smoke-test changes by deploying to your dev workspace and triggering the relevant pipeline. A `tests/` directory with pytest is on the roadmap.

---

## What lives where

- `src/pipelines/` — Spark Declarative Pipelines (bronze loader + per-regime silver). The active architecture on `main`.
- `src/notebooks/` — `0_1_xml_schema_xsd.py`, the Schema Prep notebook (one-time XSD → JSON conversion consumed by the SDP loader). The original notebook-based ingest / flatten approach is preserved on the [`legacy/notebook-approach`](https://github.com/databricks-industry-solutions/esma_xml_ingestion/tree/legacy/notebook-approach) branch.
- `src/util/` — Python helpers (XSD processing).
- `resources/` — DAB resource definitions (per-regime Schema Prep jobs + SDP pipelines), shared variables, and a template for new regulations.
- `.github/workflows/` — CI for bundle validation/deploy, and GitHub Pages publishing.
- `scripts/cleanup.sh` — wipe the deployed bundle from a workspace.

Questions? Open an issue or reach out via your Databricks account team.
