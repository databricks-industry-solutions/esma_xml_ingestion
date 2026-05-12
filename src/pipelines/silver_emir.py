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


# --------------------------------------------------------------------------
# Table 3 of 4: trade_schedule (six schedule arrays unified)
#
# Source paths:
#   TxPric.SchdlPrd[]                     -> PRICE
#   NtnlAmt.FrstLeg.SchdlPrd[]            -> NTNL_AMT_LEG_1
#   NtnlAmt.ScndLeg.SchdlPrd[]            -> NTNL_AMT_LEG_2
#   NtnlQty.FrstLeg.Dtls.SchdlPrd[]       -> NTNL_QTY_LEG_1
#   NtnlQty.ScndLeg.Dtls.SchdlPrd[]       -> NTNL_QTY_LEG_2
#   Optn.StrkPricSchdl[]                  -> STRIKE
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE_SCHEDULE,
    comment=(
        "Public: unified schedule periods across price, notional amount/qty "
        "first/second legs, and option strike-price schedule. schedule_type "
        "discriminator column says which source path each row came from."
    ),
    cluster_by_auto=True,
)
def trade_schedule():
    bronze = _reporting_date(spark.readStream.table(TBL_BRONZE))
    base = bronze.select(
        F.col("CmonTradData.TxData.TxId.UnqTxIdr").alias("trade_id"),
        F.col("reporting_date"),
        F.col("_ingested_at"),
        F.col("CmonTradData.TxData.TxPric.SchdlPrd").alias("_price_schdl"),
        F.col("CmonTradData.TxData.NtnlAmt.FrstLeg.SchdlPrd").alias("_ntnl_amt_leg1"),
        F.col("CmonTradData.TxData.NtnlAmt.ScndLeg.SchdlPrd").alias("_ntnl_amt_leg2"),
        F.col("CmonTradData.TxData.NtnlQty.FrstLeg.Dtls.SchdlPrd").alias("_ntnl_qty_leg1"),
        F.col("CmonTradData.TxData.NtnlQty.ScndLeg.Dtls.SchdlPrd").alias("_ntnl_qty_leg2"),
        F.col("CmonTradData.TxData.Optn.StrkPricSchdl").alias("_strike_schdl"),
    )

    def _schedule(arr_col: str, schedule_type: str, mapper):
        """Posexplode-outer one schedule array, apply a row-shape mapper."""
        return (
            base.select(
                "trade_id", "reporting_date", "_ingested_at",
                F.posexplode_outer(F.col(arr_col)).alias("sequence_no", "_row"),
            )
            .filter(F.col("_row").isNotNull())
            .select(
                "trade_id", "reporting_date", "_ingested_at", "sequence_no",
                F.lit(schedule_type).alias("schedule_type"),
                *mapper(F.col("_row")),
            )
        )

    def _unified_cols(eff, end, amt, amt_ccy, amt_sgn, pct, qty):
        return [
            (eff or F.lit(None).cast("date")).alias("unadj_effective_dt"),
            (end or F.lit(None).cast("date")).alias("unadj_end_dt"),
            (amt or F.lit(None).cast("decimal(25,19)")).alias("amount"),
            (amt_ccy or F.lit(None).cast("string")).alias("amount_ccy"),
            (amt_sgn or F.lit(None).cast("boolean")).alias("amount_sign"),
            (pct or F.lit(None).cast("decimal(11,10)")).alias("percentage"),
            (qty or F.lit(None).cast("decimal(25,5)")).alias("quantity"),
        ]

    price_df = _schedule(
        "_price_schdl", "PRICE",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Pric"]["MntryVal"]["Amt"]["_VALUE"],
            r["Pric"]["MntryVal"]["Amt"]["_Ccy"],
            r["Pric"]["MntryVal"]["Sgn"],
            r["Pric"]["Pctg"], None,
        ),
    )
    ntnl_amt1_df = _schedule(
        "_ntnl_amt_leg1", "NTNL_AMT_LEG_1",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Amt"]["Amt"]["_VALUE"], r["Amt"]["Amt"]["_Ccy"], None,
            None, None,
        ),
    )
    ntnl_amt2_df = _schedule(
        "_ntnl_amt_leg2", "NTNL_AMT_LEG_2",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Amt"]["Amt"]["_VALUE"], r["Amt"]["Amt"]["_Ccy"], None,
            None, None,
        ),
    )
    ntnl_qty1_df = _schedule(
        "_ntnl_qty_leg1", "NTNL_QTY_LEG_1",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"], None, None, None,
            None, r["Qty"],
        ),
    )
    ntnl_qty2_df = _schedule(
        "_ntnl_qty_leg2", "NTNL_QTY_LEG_2",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"], None, None, None,
            None, r["Qty"],
        ),
    )
    # Note: StrkPricSchdl row shape is assumed to match
    # {UadjstdFctvDt, UadjstdEndDt, StrkPric: {MntryVal: {Amt: {_VALUE, _Ccy}, Sgn}}}.
    # If the actual bronze struct shape differs (it's product-specific),
    # the pipeline run will fail with a clear "field not found in struct"
    # error and the path here needs adjustment.
    strike_df = _schedule(
        "_strike_schdl", "STRIKE",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["StrkPric"]["MntryVal"]["Amt"]["_VALUE"],
            r["StrkPric"]["MntryVal"]["Amt"]["_Ccy"],
            r["StrkPric"]["MntryVal"]["Sgn"],
            None, None,
        ),
    )

    unioned = (
        price_df
        .unionByName(ntnl_amt1_df, allowMissingColumns=True)
        .unionByName(ntnl_amt2_df, allowMissingColumns=True)
        .unionByName(ntnl_qty1_df, allowMissingColumns=True)
        .unionByName(ntnl_qty2_df, allowMissingColumns=True)
        .unionByName(strike_df, allowMissingColumns=True)
    )
    return unioned.withColumn("silver_processed_at", F.current_timestamp())
