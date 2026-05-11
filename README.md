# ESMA XML Ingestion Solution Accelerator

<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

This Databricks Asset Bundle provides a production-ready, cloud-agnostic solution for ingesting and processing complex XML files that comply with ESMA (European Securities and Markets Authority) requirements. Built through partnerships with Central Bank of Ireland (CBI) and London Stock Exchange Group (LSEG), this accelerator addresses common regulatory challenges in financial data processing.

## Executive Summary

Financial institutions operating under ESMA regulation are required to submit or consume XML-based transaction reports. These XML files present several challenges:

- **Deeply nested schemas** (e.g., ISO 20022) make parsing and flattening difficult
- **Non-uniform schemas** require schema evolution support
- **Large file sizes** (up to 2GB+) put pressure on memory and force workarounds
- **Lack of off-the-shelf tools** has led to siloed, inefficient solutions across institutions

This accelerator provides a standardized, scalable, and cloud-native ingestion solution that positions Databricks as the go-to platform for regulatory data processing across central banks, trading venues, and regulated entities in the EU and UK.

## What are Databricks Asset Bundles (DABs)?

Databricks Asset Bundles are an Infrastructure-as-Code (IaC) tool that facilitates software engineering best practices for data and AI projects, including:

- **Source control and version management**
- **Code review and collaboration**
- **Testing and validation**
- **Continuous integration and delivery (CI/CD)**

A bundle includes:
- Source files (notebooks, Python files) with business logic
- Definitions for Databricks resources (jobs, pipelines, models)
- Cloud infrastructure and workspace configurations
- Unit and integration tests

Benefits of using DABs:
- **Reproducible deployments** across environments
- **Version control** for all project components
- **Automated testing** and validation
- **Easy collaboration** in team environments
- **Production-ready** CI/CD workflows

## Project Structure

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

### Key Components

- **`databricks.yml`**: Main bundle configuration that defines deployment targets and includes resource files
- **`resources/`**: Per-regulation jobs and SDP pipelines (EMIR, MiFIR), shared variables, and per-developer local overrides
- **`src/pipelines/`**: Parameterized Spark Declarative Pipeline (SDP) source for XML ingestion → `{prefix}_raw` + `{prefix}_quarantine`
- **`src/notebooks/`**: Classic notebooks for XSD-to-schema preparation and the flatten/explode bronze step
- **`src/util/`**: Python helpers for XSD processing

## Prerequisites

Before deploying this solution, ensure the following prerequisites are met:

### 1. Unity Catalog Setup

Unity Catalog must be enabled in your Databricks workspace:

- **For new workspaces**: Unity Catalog is enabled by default (November 2023+)
- **For existing workspaces**: An account admin must enable Unity Catalog
- **Verification**: Run `SELECT CURRENT_METASTORE()` in a notebook to confirm

### 2. Unity Catalog Volume Configuration

Configure a Unity Catalog volume for data storage in the `volume_path`:

**Managed Volume** (Recommended for development):
```sql
CREATE VOLUME <catalog>.<schema>.<volume_name>
```

**External Volume** (For production with existing storage):
```sql
CREATE EXTERNAL VOLUME <catalog>.<schema>.<volume_name>
LOCATION 's3://<bucket>/<path>/' -- or Azure/GCP equivalent
```

Volume requirements:
- **Path format**: `/Volumes/<catalog>/<schema>/<volume>/<path>/`
- **Compute requirements**: Databricks Runtime 13.3 LTS or above
- **Permissions**: Appropriate `READ VOLUME` and `WRITE VOLUME` privileges

### 3. Managed File Events Configuration

Enable file events for efficient XML file processing using Auto Loader:

**For External Locations** (Recommended):
1. Create storage credential and external location in Unity Catalog
2. Enable file events for the external location via workspace admin
3. Benefits include:
   - Databricks-managed file notification queue
   - Automatic subscription and credential management
   - Better performance than directory listing mode
   - Reduced cloud provider API costs

**File Events Features**:
- **Real-time processing**: Files processed as they arrive
- **Scalability**: Handle millions of files per hour
- **Cost optimization**: Reduced LIST operations and API calls
- **Automatic backfill**: Ensures no files are missed

### 4. Additional Requirements

- **Databricks CLI**: Version v0.218.0 or above
- **Workspace files**: Enabled (default for Databricks Runtime 11.3 LTS+)
- **Compute access mode**: Standard or Dedicated access mode for Unity Catalog
- **Schema privileges**: `USE CATALOG`, `CREATE TABLE`, `USE SCHEMA` on target schemas

## Quick Start

### 1. Setup Development Environment

```bash
# Clone and navigate to project
git clone <repository-url>
cd esma_xml_ingestion

# Copy and customize development variables
cp resources/config/local/dev-variables.yml.template resources/config/local/dev-variables.yml
# Edit dev-variables.yml with your workspace-specific settings
```

### 2. Configure Variables

Update `resources/config/local/dev-variables.yml`:

```yaml
variables:
  workspace_url:
    default: "https://your-workspace.cloud.databricks.com"
  catalog:
    default: "your_catalog"
  volume_path:
    default: "/Volumes/your_catalog/your_schema/regulatory_data"
```

### 3. Deploy and Run

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy to development environment
databricks bundle deploy -t dev

# Run EMIR processing job
databricks jobs run-now --job-id <emir-job-id>

# Run MiFIR processing job  
databricks jobs run-now --job-id <mifir-job-id>
```

## Production Deployment

For production deployment:

```bash
# Deploy with production overrides
databricks bundle deploy -t prod \
  --var workspace_url="https://prod-workspace.cloud.databricks.com" \
  --var catalog="prod_catalog" \
  --var volume_path="/Volumes/prod_catalog/regulatory/data"
```

## Solution Benefits

### Technical Benefits
- **Reduce pipeline build time** from weeks to hours
- **Lower memory costs** via native Spark-based XML parsing
- **Ensure regulatory compliance** via schema validation and lineage
- **Improve pipeline observability** and maintainability
- **Cloud-agnostic deployment** using serverless compute

### Business Benefits
- **Faster time-to-market** for regulatory reporting solutions
- **Reduced operational costs** through efficient processing
- **Enhanced data governance** with Unity Catalog integration
- **Improved compliance posture** with audit trails and lineage
- **Scalable architecture** supporting multiple regulatory frameworks

## Supported Regulations

- **EMIR** (European Market Infrastructure Regulation)
- **MiFIR** (Markets in Financial Instruments Regulation)
- **Extensible framework** for additional regulations

## Next Steps

1. **Customize for your data**: Update schema definitions and processing logic
2. **Configure file events**: Enable managed file events for optimal performance
3. **Set up CI/CD**: Implement automated testing and deployment pipelines
4. **Monitor and optimize**: Use Databricks monitoring tools for performance tuning
5. **Extend for new regulations**: Use the template structure for additional regulatory requirements

## Support

For questions about this accelerator, please contact your Databricks representative or open an issue in this repository.