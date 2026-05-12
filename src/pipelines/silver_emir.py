"""ESMA EMIR REFIT DAT TSR Silver Layer.

Domain-driven silver layer on top of bronze ``emir_raw``. Four tables:

* ``trade`` — wide-flat fact table, one row per ``<Stat>`` per submission
  snapshot. ~232 scalar columns + array/struct columns for long-tail.
* ``trade_schedule`` — unified schedule periods (price + notional amount/qty
  for first/second legs + strike-price schedule for options) with
  ``schedule_type`` discriminator.
* ``trade_beneficiary`` — exploded beneficiary array.
* ``submission_file`` — one row per ingested XML file (regulation-agnostic
  envelope).

All inputs are supplied via ``spark.conf`` — see the EMIR silver pipeline
``configuration`` block in ``resources/bundle.emir_resources.yml``.

Reference: docs/superpowers/specs/2026-05-12-emir-silver-design.md
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --------------------------------------------------------------------------
# Pipeline configuration (set in resources/bundle.emir_resources.yml under
# resources.pipelines.emir_silver_pipeline.configuration).
# --------------------------------------------------------------------------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema", RAW_SCHEMA)
BRONZE_TABLE_NAME = spark.conf.get("bronze_table")
REGULATION = spark.conf.get("regulation", "EMIR")

TBL_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_TABLE_NAME}"
TBL_TRADE = f"{CATALOG}.{SILVER_SCHEMA}.trade"
TBL_TRADE_SCHEDULE = f"{CATALOG}.{SILVER_SCHEMA}.trade_schedule"
TBL_TRADE_BENEFICIARY = f"{CATALOG}.{SILVER_SCHEMA}.trade_beneficiary"
TBL_SUBMISSION_FILE = f"{CATALOG}.{SILVER_SCHEMA}.submission_file"


def _reporting_date(df: DataFrame) -> DataFrame:
    """Add a reporting_date DATE column parsed from ESMADate or filename.

    ESMADate from the bronze regex is in 'YY-MM-DD' format (e.g.,
    '24-12-15'). Convert to a proper DATE assuming 20YY century.
    """
    return df.withColumn(
        "reporting_date",
        F.when(
            F.col("ESMADate").rlike(r"^\d\d-\d\d-\d\d$"),
            F.to_date(F.concat(F.lit("20"), F.col("ESMADate")), "yyyy-MM-dd"),
        ).otherwise(F.to_date(F.col("_file_modification_time")))
    )
