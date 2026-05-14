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
