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


# --------------------------------------------------------------------------
# Table 1 of 4: submission_file (file-level envelope)
#
# Regulation-agnostic. MiFIR (and any future regulation) writes to the
# same table with regulation='MIFIR' under its own silver pipeline.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_SUBMISSION_FILE,
    comment=(
        "Public: one row per ingested ESMA XML file. Regulation-agnostic "
        "envelope shared across EMIR/MiFIR. Built from a dropDuplicates "
        "over the bronze stream."
    ),
    cluster_by_auto=True,
)
def submission_file():
    return (
        _reporting_date(spark.readStream.table(TBL_BRONZE))
        .dropDuplicates(["file_path"])
        .select(
            F.col("file_path"),
            F.col("file_name"),
            F.col("reporting_date"),
            F.col("ESMADate").alias("esma_date_str"),
            F.col("FileBatchIndex").cast("int").alias("batch_index"),
            F.col("FileBatchSize").cast("int").alias("batch_size"),
            F.col("FileVersion").cast("int").alias("file_version"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizMsgIdr").alias("biz_msg_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.Id").alias("sender_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.Id").alias("recipient_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.MsgDefIdr").alias("message_def_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizSvc").alias("business_service"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.CreDt").alias("header_creation_ts"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.RptHdr.NbRcrds").cast("bigint").alias("number_of_records"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.TradData.DataSetActn").alias("data_set_action"),
            F.col("_ingested_at").alias("ingested_at"),
            F.current_timestamp().alias("silver_processed_at"),
            F.lit(REGULATION).alias("regulation"),
        )
    )


# --------------------------------------------------------------------------
# Table 2 of 4: trade_beneficiary (exploded array)
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE_BENEFICIARY,
    comment=(
        "Public: one row per beneficiary per trade. Exploded from "
        "CtrPtySpcfcData.CtrPty.Bnfcry[]. beneficiary_type column "
        "discriminates Lgl (legal entity, LEI) vs Ntrl (natural person)."
    ),
    cluster_by_auto=True,
)
def trade_beneficiary():
    bronze = _reporting_date(spark.readStream.table(TBL_BRONZE))
    exploded = (
        bronze
        .select(
            F.col("CmonTradData.TxData.TxId.UnqTxIdr").alias("trade_id"),
            F.col("reporting_date"),
            F.col("_ingested_at"),
            F.posexplode_outer(F.col("CtrPtySpcfcData.CtrPty.Bnfcry")).alias("sequence_no", "bnfcry"),
        )
        .filter(F.col("bnfcry").isNotNull())
    )
    return exploded.select(
        "trade_id",
        "reporting_date",
        "sequence_no",
        F.col("bnfcry.Lgl.Id.LEI").alias("beneficiary_lei"),
        F.col("bnfcry.Lgl.Id.Othr.Id.Id").alias("beneficiary_other_id"),
        F.col("bnfcry.Ntrl.Id.Id.Id").alias("beneficiary_natural_person_id"),
        F.when(F.col("bnfcry.Lgl.Id.LEI").isNotNull(), F.lit("LEGAL"))
         .when(F.col("bnfcry.Ntrl.Id.Id.Id").isNotNull(), F.lit("NATURAL"))
         .otherwise(F.lit("OTHER"))
         .alias("beneficiary_type"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
