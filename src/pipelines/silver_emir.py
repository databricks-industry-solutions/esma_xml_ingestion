# Databricks notebook source
# MAGIC %md
# MAGIC # EMIR REFIT — Silver SDP
# MAGIC Domain tables over bronze `emir_raw`:
# MAGIC `trade` (wide-flat, ~232 cols) · `trade_schedule` (unified schedule periods)
# MAGIC · `trade_beneficiary` (exploded) · `submission_file` (per-file envelope).
# MAGIC Config via `spark.conf` (see `resources/bundle.emir_resources.yml`).

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
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# MAGIC %md
# MAGIC ## Pipeline configuration
# MAGIC From `resources/bundle.emir_resources.yml` (silver pipeline `configuration`).

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema", RAW_SCHEMA)
BRONZE_TABLE_NAME = spark.conf.get("bronze_table")
FILE_HEADERS_TABLE_NAME = spark.conf.get("file_headers_table")
REGULATION = spark.conf.get("regulation", "EMIR")

TBL_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_TABLE_NAME}"
TBL_FILE_HEADERS = f"{CATALOG}.{RAW_SCHEMA}.{FILE_HEADERS_TABLE_NAME}"
TBL_TRADE = f"{CATALOG}.{SILVER_SCHEMA}.trade"
TBL_TRADE_SCHEDULE = f"{CATALOG}.{SILVER_SCHEMA}.trade_schedule"
TBL_TRADE_BENEFICIARY = f"{CATALOG}.{SILVER_SCHEMA}.trade_beneficiary"
TBL_SUBMISSION_FILE = f"{CATALOG}.{SILVER_SCHEMA}.submission_file"


def _reporting_date_from_headers(df: DataFrame) -> DataFrame:
    """Add `reporting_date` DATE from `ESMADate` ('YY-MM-DD', 20YY century) or file mtime.
    Used by `submission_file` (which reads from `file_headers` and has ESMADate)."""
    return df.withColumn(
        "reporting_date",
        F.when(
            F.col("ESMADate").rlike(r"^\d\d-\d\d-\d\d$"),
            F.to_date(F.concat(F.lit("20"), F.col("ESMADate")), "yyyy-MM-dd"),
        ).otherwise(F.to_date(F.col("_file_modification_time")))
    )


def _reporting_date_from_raw(df: DataFrame) -> DataFrame:
    """Add `reporting_date` DATE from file mtime. Used by per-row silver tables
    (`trade`, `trade_schedule`, `trade_beneficiary`) which read from `emir_raw`
    where the ESMADate filename-regex column is not present."""
    return df.withColumn("reporting_date", F.to_date(F.col("_file_modification_time")))
# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 of 4: `submission_file`
# MAGIC Regulation-agnostic envelope. MiFIR writes to the same table with
# MAGIC `regulation='MIFIR'` from its own silver pipeline.

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
        "envelope shared across EMIR/MiFIR. Reads directly from "
        "{prefix}_file_headers (already one row per file) — no need to "
        "scan the payload bronze."
    ),
    cluster_by_auto=True,
)
def submission_file():
    return (
        _reporting_date_from_headers(spark.readStream.table(TBL_FILE_HEADERS))
        .select(
            F.col("file_path"),
            F.col("file_name"),
            F.col("reporting_date"),
            F.col("ESMADate").alias("esma_date_str"),
            F.col("FileBatchIndex").cast("int").alias("batch_index"),
            F.col("FileBatchSize").cast("int").alias("batch_size"),
            F.col("FileVersion").cast("int").alias("file_version"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizMsgIdr").alias("biz_msg_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.CharSet").alias("header_char_set"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.Id").alias("sender_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.SchmeNm.Cd").alias("sender_scheme_cd"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.SchmeNm.Prtry").alias("sender_scheme_proprietary"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.Id").alias("recipient_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.SchmeNm.Cd").alias("recipient_scheme_cd"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.SchmeNm.Prtry").alias("recipient_scheme_proprietary"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.MsgDefIdr").alias("message_def_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizSvc").alias("business_service"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.CreDt").alias("header_creation_ts"),
            # Related BAH — present on resubmission/correction messages that
            # reference an earlier submission. Whole Rltd sub-tree captured
            # for full traceability.
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.CharSet").alias("related_char_set"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr.Id").alias("related_sender_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr.SchmeNm.Cd").alias("related_sender_scheme_cd"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr.SchmeNm.Prtry").alias("related_sender_scheme_proprietary"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr.Id").alias("related_recipient_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr.SchmeNm.Cd").alias("related_recipient_scheme_cd"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr.SchmeNm.Prtry").alias("related_recipient_scheme_proprietary"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.BizMsgIdr").alias("related_biz_msg_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.MsgDefIdr").alias("related_message_def_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.BizSvc").alias("related_business_service"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Rltd.CreDt").alias("related_creation_ts"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.RptHdr.NbRcrds").cast("bigint").alias("number_of_records"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.TradData.DataSetActn").alias("data_set_action"),
            F.col("_ingested_at").alias("ingested_at"),
            F.current_timestamp().alias("silver_processed_at"),
            F.lit(REGULATION).alias("regulation"),
        )
    )

# MAGIC %md
# MAGIC ## Table 2 of 4: `trade_beneficiary`
# MAGIC Exploded `CtrPtySpcfcData.CtrPty.Bnfcry[]`; `beneficiary_type` ∈ LEGAL / NATURAL / OTHER.

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
    bronze = _reporting_date_from_raw(spark.readStream.table(TBL_BRONZE))
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

# MAGIC %md
# MAGIC ## Table 3 of 4: `trade_schedule`
# MAGIC Six schedule arrays unified via `schedule_type` discriminator:
# MAGIC PRICE / NTNL_AMT_LEG_1 / NTNL_AMT_LEG_2 / NTNL_QTY_LEG_1 / NTNL_QTY_LEG_2 / STRIKE.

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
    bronze = _reporting_date_from_raw(spark.readStream.table(TBL_BRONZE))
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
            (eff if eff is not None else F.lit(None).cast("date")).alias("unadj_effective_dt"),
            (end if end is not None else F.lit(None).cast("date")).alias("unadj_end_dt"),
            (amt if amt is not None else F.lit(None).cast("decimal(25,19)")).alias("amount"),
            (amt_ccy if amt_ccy is not None else F.lit(None).cast("string")).alias("amount_ccy"),
            (amt_sgn if amt_sgn is not None else F.lit(None).cast("boolean")).alias("amount_sign"),
            (pct if pct is not None else F.lit(None).cast("decimal(11,10)")).alias("percentage"),
            (qty if qty is not None else F.lit(None).cast("decimal(25,5)")).alias("quantity"),
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
    # StrkPricSchdl row shape (per bronze inference): same as TxPric.SchdlPrd —
    # {UadjstdFctvDt, UadjstdEndDt, Pric: {MntryVal: {Amt: {_VALUE, _Ccy}, Sgn}, Pctg}}.
    # The XSD schedule is genuinely shared between price and strike-price schedules.
    strike_df = _schedule(
        "_strike_schdl", "STRIKE",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Pric"]["MntryVal"]["Amt"]["_VALUE"],
            r["Pric"]["MntryVal"]["Amt"]["_Ccy"],
            r["Pric"]["MntryVal"]["Sgn"],
            r["Pric"]["Pctg"], None,
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

# MAGIC %md
# MAGIC ## Table 4 of 4: `trade`
# MAGIC Wide-flat fact, ~232 scalar cols + 5 arrays + 1 struct. One row per
# MAGIC `<Stat>` per snapshot. Choice fields collapsed to LEI + `*_other_id`
# MAGIC fallback.

# --------------------------------------------------------------------------
# Table 4 of 4: trade (main fact, ~232 scalar cols + 5 arrays + 1 struct)
#
# One row per <Stat> per submission snapshot. Wide-flat by design;
# business-readable column names; choice fields collapsed to LEI common
# branch + *_other_id fallback. See spec §4.0 for the decision rule.
#
# Built incrementally — each commit adds one logical XSD section's
# columns to the .select(...) below.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE,
    comment=(
        "Public: per-trade snapshot, wide-flat with business-readable "
        "column names. Choice fields collapsed to LEI primary + "
        "*_other_id fallback. Partition/cluster by reporting_date. "
        "Append-only — each daily snapshot lands as new rows."
    ),
    cluster_by_auto=True,
)
def trade():
    src = _reporting_date_from_raw(spark.readStream.table(TBL_BRONZE))
    cp = "CtrPtySpcfcData.CtrPty"
    txd = "CmonTradData.TxData"
    cd = "CmonTradData.CtrctData"
    return src.select(
        # === Identification ===
        F.col(f"{txd}.TxId.UnqTxIdr").alias("trade_id"),
        F.col(f"{txd}.TxId.Prtry.Id").alias("trade_id_proprietary"),
        F.col(f"{txd}.PrrTxId.UnqTxIdr").alias("prior_trade_id"),
        F.col(f"{txd}.PrrTxId.Prtry.Id").alias("prior_trade_id_proprietary"),
        F.col(f"{txd}.PrrTxId.NotAvlbl").alias("prior_trade_id_not_available"),
        F.col(f"{txd}.SbsqntTxId.UnqTxIdr").alias("subsequent_trade_id"),
        F.col(f"{txd}.SbsqntTxId.Prtry.Id").alias("subsequent_trade_id_proprietary"),
        F.col(f"{txd}.SbsqntTxId.NotAvlbl").alias("subsequent_trade_id_not_available"),
        F.col(f"{txd}.RptTrckgNb").alias("report_tracking_number"),
        F.col(f"{txd}.PltfmIdr").alias("platform_id"),

        # === Reporting counterparty (CtrPty.RptgCtrPty) ===
        F.col(f"{cp}.RptgCtrPty.Id.Lgl.Id.LEI").alias("reporter_lei"),
        F.col(f"{cp}.RptgCtrPty.Id.Lgl.Id.Othr.Id.Id").alias("reporter_other_id"),
        F.when(F.col(f"{cp}.RptgCtrPty.Ntr.FI").isNotNull(), F.lit("FI"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.NFI").isNotNull(), F.lit("NFI"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.CntrlCntrPty").isNotNull(), F.lit("CCP"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.Othr").isNotNull(), F.lit("OTHR"))
         .alias("reporter_nature"),
        F.coalesce(
            F.transform(F.col(f"{cp}.RptgCtrPty.Ntr.FI.Sctr"), lambda x: x["Cd"]),
            F.transform(F.col(f"{cp}.RptgCtrPty.Ntr.NFI.Sctr"), lambda x: x["Id"]),
        ).alias("reporter_sectors"),
        F.coalesce(
            F.col(f"{cp}.RptgCtrPty.Ntr.FI.ClrThrshld"),
            F.col(f"{cp}.RptgCtrPty.Ntr.NFI.ClrThrshld"),
        ).alias("reporter_clr_threshold"),
        F.col(f"{cp}.RptgCtrPty.Ntr.NFI.DrctlyLkdActvty").alias("reporter_nfi_directly_linked_activity"),
        F.col(f"{cp}.RptgCtrPty.Ntr.CntrlCntrPty").isNotNull().alias("reporter_is_central_counterparty"),
        F.col(f"{cp}.RptgCtrPty.TradgCpcty").alias("reporter_trading_capacity"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.Drctn.DrctnOfTheFrstLeg").alias("reporter_direction_first_leg"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.Drctn.DrctnOfTheScndLeg").alias("reporter_direction_second_leg"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.CtrPtySd").alias("reporter_side"),

        # === Other counterparty (CtrPty.OthrCtrPty) ===
        F.col(f"{cp}.OthrCtrPty.IdTp.Lgl.Id.LEI").alias("other_cp_lei"),
        F.coalesce(
            F.col(f"{cp}.OthrCtrPty.IdTp.Lgl.Ctry"),
            F.col(f"{cp}.OthrCtrPty.IdTp.Ntrl.Ctry"),
        ).alias("other_cp_country"),
        F.col(f"{cp}.OthrCtrPty.IdTp.Ntrl.Id.Id.Id").alias("other_cp_natural_person_id"),
        F.when(F.col(f"{cp}.OthrCtrPty.Ntr.FI").isNotNull(), F.lit("FI"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.NFI").isNotNull(), F.lit("NFI"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.CntrlCntrPty").isNotNull(), F.lit("CCP"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.Othr").isNotNull(), F.lit("OTHR"))
         .alias("other_cp_nature"),
        F.coalesce(
            F.transform(F.col(f"{cp}.OthrCtrPty.Ntr.FI.Sctr"), lambda x: x["Cd"]),
            F.transform(F.col(f"{cp}.OthrCtrPty.Ntr.NFI.Sctr"), lambda x: x["Id"]),
        ).alias("other_cp_sectors"),
        F.coalesce(
            F.col(f"{cp}.OthrCtrPty.Ntr.FI.ClrThrshld"),
            F.col(f"{cp}.OthrCtrPty.Ntr.NFI.ClrThrshld"),
        ).alias("other_cp_clr_threshold"),
        F.col(f"{cp}.OthrCtrPty.Ntr.CntrlCntrPty").isNotNull().alias("other_cp_is_central_counterparty"),
        F.col(f"{cp}.OthrCtrPty.RptgOblgtn").alias("other_cp_has_reporting_obligation"),

        # === Other counterparty roles ===
        F.col(f"{cp}.Brkr.LEI").alias("broker_lei"),
        F.col(f"{cp}.Brkr.Othr.Id.Id").alias("broker_other_id"),
        F.col(f"{cp}.Brkr.Othr.Id.SchmeNm").alias("broker_other_id_scheme"),
        F.col(f"{cp}.Brkr.Othr.Id.Issr").alias("broker_other_id_issuer"),
        F.col(f"{cp}.Brkr.Othr.Nm").alias("broker_name"),
        F.col(f"{cp}.Brkr.Othr.Dmcl").alias("broker_domicile"),
        F.col(f"{cp}.SubmitgAgt.LEI").alias("submitting_agent_lei"),
        F.col(f"{cp}.SubmitgAgt.Othr.Id.Id").alias("submitting_agent_other_id"),
        F.col(f"{cp}.SubmitgAgt.Othr.Id.SchmeNm").alias("submitting_agent_other_id_scheme"),
        F.col(f"{cp}.SubmitgAgt.Othr.Id.Issr").alias("submitting_agent_other_id_issuer"),
        F.col(f"{cp}.SubmitgAgt.Othr.Nm").alias("submitting_agent_name"),
        F.col(f"{cp}.SubmitgAgt.Othr.Dmcl").alias("submitting_agent_domicile"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.LEI").alias("clearing_member_lei"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Id.Id").alias("clearing_member_other_id"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Id.SchmeNm").alias("clearing_member_other_id_scheme"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Id.Issr").alias("clearing_member_other_id_issuer"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Nm").alias("clearing_member_name"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Dmcl").alias("clearing_member_domicile"),
        F.col(f"{cp}.NttyRspnsblForRpt.LEI").alias("entity_responsible_for_report_lei"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Id.Id").alias("entity_responsible_for_report_other_id"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Id.SchmeNm").alias("entity_responsible_for_report_other_id_scheme"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Id.Issr").alias("entity_responsible_for_report_other_id_issuer"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Nm").alias("entity_responsible_for_report_name"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Dmcl").alias("entity_responsible_for_report_domicile"),

        # === Contract data (CmonTradData.CtrctData) ===
        F.col(f"{cd}.CtrctTp").alias("contract_type"),
        F.col(f"{cd}.AsstClss").alias("asset_class"),
        F.col(f"{cd}.PdctClssfctn").alias("product_classification"),
        F.col(f"{cd}.PdctId.ISIN").alias("product_isin"),
        F.col(f"{cd}.PdctId.UnqPdctIdr.Id").alias("product_unq_pdct_idr"),
        F.col(f"{cd}.PdctId.AltrntvInstrmId").alias("product_alternative_id"),
        F.col(f"{cd}.UndrlygInstrm.ISIN").alias("underlying_isin"),
        F.col(f"{cd}.UndrlygInstrm.AltrntvInstrmId").alias("underlying_alternative_id"),
        F.col(f"{cd}.UndrlygInstrm.UnqPdctIdr.Id").alias("underlying_unq_pdct_idr"),
        F.col(f"{cd}.UndrlygInstrm.Indx.ISIN").alias("underlying_index_isin"),
        F.col(f"{cd}.UndrlygInstrm.Indx.Nm").alias("underlying_index_name"),
        F.col(f"{cd}.UndrlygInstrm.Indx.Indx").alias("underlying_index_value"),
        F.col(f"{cd}.UndrlygInstrm.Bskt.Strr").alias("underlying_basket_structure"),
        F.col(f"{cd}.UndrlygInstrm.Bskt.Id").alias("underlying_basket_id"),
        F.transform(
            F.col(f"{cd}.UndrlygInstrm.Bskt.Cnsttnts"),
            lambda c: F.struct(c["InstrmId"]["ISIN"].alias("isin"),
                               c["InstrmId"]["AltrntvInstrmId"].alias("alternative_id")),
        ).alias("basket_constituents"),
        F.col(f"{cd}.UndrlygInstrm.IdNotAvlbl").alias("underlying_id_not_available"),
        F.col(f"{cd}.SttlmCcy.Ccy").alias("settlement_ccy"),
        F.col(f"{cd}.SttlmCcyScndLeg.Ccy").alias("settlement_ccy_second_leg"),
        F.col(f"{cd}.DerivBasedOnCrptAsst").alias("deriv_based_on_crypto"),

        # === Transaction core (TxData) ===
        F.col(f"{txd}.ExctnTmStmp").alias("execution_ts"),
        F.col(f"{txd}.FctvDt").alias("effective_dt"),
        F.col(f"{txd}.XprtnDt").alias("expiration_dt"),
        F.col(f"{txd}.EarlyTermntnDt").alias("early_termination_dt"),
        F.col(f"{txd}.SttlmDt").alias("settlement_dates"),
        F.col(f"{txd}.DlvryTp").alias("delivery_type"),
        F.col(f"{txd}.CollPrtflCd.Prtfl.Cd").alias("collateral_portfolio_cd"),
        F.col(f"{txd}.CollPrtflCd.Prtfl.NoPrtfl").alias("has_no_collateral_portfolio"),
        F.col(f"{txd}.MstrAgrmt.Tp.Tp").alias("master_agreement_type"),
        F.col(f"{txd}.MstrAgrmt.Tp.Prtry").alias("master_agreement_type_proprietary"),
        F.col(f"{txd}.MstrAgrmt.Vrsn").alias("master_agreement_version"),
        F.col(f"{txd}.MstrAgrmt.OthrMstrAgrmtDtls").alias("master_agreement_other_details"),

        # === Pricing (TxData.TxPric) ===
        F.col(f"{txd}.TxPric.Pric.MntryVal.Amt._VALUE").alias("price_monetary_value"),
        F.col(f"{txd}.TxPric.Pric.MntryVal.Amt._Ccy").alias("price_monetary_ccy"),
        F.col(f"{txd}.TxPric.Pric.MntryVal.Sgn").alias("price_monetary_sign"),
        F.col(f"{txd}.TxPric.Pric.Unit").alias("price_unit"),
        F.col(f"{txd}.TxPric.Pric.Pctg").alias("price_percentage"),
        F.col(f"{txd}.TxPric.Pric.Yld").alias("price_yield"),
        F.col(f"{txd}.TxPric.Pric.PdgPric").alias("price_pending"),
        F.col(f"{txd}.TxPric.Pric.Othr.Val").alias("price_other_value"),
        F.col(f"{txd}.TxPric.Pric.Othr.Tp").alias("price_other_type"),
        F.col(f"{txd}.TxPric.PricMltplr").alias("price_multiplier"),

        # === Notional amounts (TxData.NtnlAmt) ===
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Amt._VALUE").alias("notional_first_leg_amount"),
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Amt._Ccy").alias("notional_first_leg_ccy"),
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Sgn").alias("notional_first_leg_sign"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Amt._VALUE").alias("notional_second_leg_amount"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Amt._Ccy").alias("notional_second_leg_ccy"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Sgn").alias("notional_second_leg_sign"),
        # Leg-level Ccy attribute (distinct from amount-level _Ccy above);
        # set on the leg element itself in the XSD for cross-currency swaps.
        F.col(f"{txd}.NtnlAmt.ScndLeg.Ccy").alias("notional_second_leg_currency"),

        # === Notional quantities (TxData.NtnlQty) ===
        F.col(f"{txd}.NtnlQty.FrstLeg.TtlQty").alias("notional_first_leg_total_qty"),
        F.col(f"{txd}.NtnlQty.ScndLeg.TtlQty").alias("notional_second_leg_total_qty"),

        # === Quantity (TxData.Qty) ===
        F.col(f"{txd}.Qty.Unit").alias("qty_unit"),
        F.col(f"{txd}.Qty.NmnlVal._VALUE").alias("qty_nominal_value"),
        F.col(f"{txd}.Qty.NmnlVal._Ccy").alias("qty_nominal_ccy"),
        F.col(f"{txd}.Qty.MntryVal._VALUE").alias("qty_monetary_value"),
        F.col(f"{txd}.Qty.MntryVal._Ccy").alias("qty_monetary_ccy"),

        # === Clearing (TxData.TradClr) ===
        F.col(f"{txd}.TradClr.ClrOblgtn").alias("clearing_obligation"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd").isNotNull().alias("is_cleared"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.LEI").alias("ccp_lei"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Id.Id").alias("ccp_other_id"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Id.SchmeNm").alias("ccp_other_id_scheme"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Id.Issr").alias("ccp_other_id_issuer"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Nm").alias("ccp_name"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Dmcl").alias("ccp_domicile"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.ClrDtTm").alias("cleared_ts"),
        F.col(f"{txd}.TradClr.ClrSts.NonClrd.Rsn").alias("clearing_non_cleared_reason"),
        F.col(f"{txd}.TradClr.IntraGrp").alias("is_intragroup"),

        # === Interest rate first leg (TxData.IntrstRate.FrstLeg) ===
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.Rate.Rate").alias("ir_first_leg_fixed_rate"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.DayCnt.Cd").alias("ir_first_leg_fixed_day_count"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.DayCnt.Nrrtv").alias("ir_first_leg_fixed_day_count_narr"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Term.Unit").alias("ir_first_leg_fixed_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Term.Val").alias("ir_first_leg_fixed_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Prtry").alias("ir_first_leg_fixed_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Id").alias("ir_first_leg_floating_index_id"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Nm").alias("ir_first_leg_floating_index_name"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Rate.Cd").alias("ir_first_leg_floating_rate_cd"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Rate.Prtry").alias("ir_first_leg_floating_rate_prop"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RefPrd.Unit").alias("ir_first_leg_floating_ref_period_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RefPrd.Val").alias("ir_first_leg_floating_ref_period_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Amt._VALUE").alias("ir_first_leg_floating_spread_value"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Amt._Ccy").alias("ir_first_leg_floating_spread_ccy"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Sgn").alias("ir_first_leg_floating_spread_sign"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.Pctg").alias("ir_first_leg_floating_spread_pct"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.BsisPtSprd").alias("ir_first_leg_floating_spread_bps"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.DayCnt.Cd").alias("ir_first_leg_floating_day_count"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.PmtFrqcy.Term.Unit").alias("ir_first_leg_floating_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.PmtFrqcy.Term.Val").alias("ir_first_leg_floating_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.PmtFrqcy.Prtry").alias("ir_first_leg_floating_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RstFrqcy.Term.Unit").alias("ir_first_leg_floating_rst_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RstFrqcy.Term.Val").alias("ir_first_leg_floating_rst_freq_val"),

        # === Interest rate second leg (TxData.IntrstRate.ScndLeg) ===
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.Rate.Rate").alias("ir_second_leg_fixed_rate"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.DayCnt.Cd").alias("ir_second_leg_fixed_day_count"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.DayCnt.Nrrtv").alias("ir_second_leg_fixed_day_count_narr"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Term.Unit").alias("ir_second_leg_fixed_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Term.Val").alias("ir_second_leg_fixed_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Prtry").alias("ir_second_leg_fixed_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Id").alias("ir_second_leg_floating_index_id"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Nm").alias("ir_second_leg_floating_index_name"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Rate.Cd").alias("ir_second_leg_floating_rate_cd"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Rate.Prtry").alias("ir_second_leg_floating_rate_prop"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RefPrd.Unit").alias("ir_second_leg_floating_ref_period_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RefPrd.Val").alias("ir_second_leg_floating_ref_period_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Amt._VALUE").alias("ir_second_leg_floating_spread_value"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Amt._Ccy").alias("ir_second_leg_floating_spread_ccy"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Sgn").alias("ir_second_leg_floating_spread_sign"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.Pctg").alias("ir_second_leg_floating_spread_pct"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.BsisPtSprd").alias("ir_second_leg_floating_spread_bps"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.DayCnt.Cd").alias("ir_second_leg_floating_day_count"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.PmtFrqcy.Term.Unit").alias("ir_second_leg_floating_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.PmtFrqcy.Term.Val").alias("ir_second_leg_floating_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.PmtFrqcy.Prtry").alias("ir_second_leg_floating_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RstFrqcy.Term.Unit").alias("ir_second_leg_floating_rst_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RstFrqcy.Term.Val").alias("ir_second_leg_floating_rst_freq_val"),

        # === FX (TxData.Ccy) ===
        F.col(f"{txd}.Ccy.DlvrblCrossCcy").alias("delivery_ccy_cross"),
        F.col(f"{txd}.Ccy.XchgRate").alias("xchg_rate"),
        F.col(f"{txd}.Ccy.FwdXchgRate").alias("forward_xchg_rate"),
        F.col(f"{txd}.Ccy.XchgRateBsis.CcyPair.BaseCcy").alias("xchg_base_ccy"),
        F.col(f"{txd}.Ccy.XchgRateBsis.CcyPair.QtdCcy").alias("xchg_quoted_ccy"),
        F.col(f"{txd}.Ccy.XchgRateBsis.Prtry").alias("xchg_rate_basis_proprietary"),

        # === Lifecycle / risk-reduction / confirmation ===
        F.col("CmonTradData.CtrctMod.ActnTp").alias("contract_modification_action_type"),
        F.col("CmonTradData.CtrctMod.Lvl").alias("contract_modification_level"),
        F.col(f"{txd}.Cmprssn").alias("is_compression"),
        F.col(f"{txd}.PstTradRskRdctnFlg").alias("is_post_trade_risk_reduction"),
        F.col(f"{txd}.PstTradRskRdctnEvt.Tchnq").alias("ptrr_technique"),
        F.col(f"{txd}.PstTradRskRdctnEvt.SvcPrvdr.LEI").alias("ptrr_service_provider_lei"),
        F.col(f"{txd}.DerivEvt.Tp").alias("deriv_event_type"),
        F.col(f"{txd}.DerivEvt.Id.PstTradRskRdctnIdr.Strr").alias("deriv_event_ptrr_strr"),
        F.col(f"{txd}.DerivEvt.Id.PstTradRskRdctnIdr.Id").alias("deriv_event_ptrr_id"),
        F.col(f"{txd}.DerivEvt.TmStmp.Dt").alias("deriv_event_dt"),
        F.coalesce(F.col(f"{txd}.TradConf.Confd.Tp"), F.col(f"{txd}.TradConf.NonConfd.Tp")).alias("trade_confirmation_type"),
        F.col(f"{txd}.TradConf.Confd.TmStmp").alias("trade_confirmation_ts"),

        # === Valuation (CtrPtySpcfcData.Valtn) ===
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Amt._VALUE").alias("contract_value"),
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Amt._Ccy").alias("contract_value_ccy"),
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Sgn").alias("contract_value_sign"),
        F.col("CtrPtySpcfcData.Valtn.Dlta").alias("delta"),
        F.col("CtrPtySpcfcData.Valtn.TmStmp").alias("valuation_ts"),
        F.col("CtrPtySpcfcData.Valtn.Tp").alias("valuation_type"),

        # === Option attributes (TxData.Optn) ===
        F.col(f"{txd}.Optn.Tp").alias("option_type"),
        F.col(f"{txd}.Optn.ExrcStyle").alias("option_exercise_style"),
        F.col(f"{txd}.Optn.StrkPric.MntryVal.Amt._VALUE").alias("option_strike_price"),
        F.col(f"{txd}.Optn.StrkPric.MntryVal.Amt._Ccy").alias("option_strike_price_ccy"),
        F.col(f"{txd}.Optn.StrkPric.MntryVal.Sgn").alias("option_strike_price_sign"),
        F.col(f"{txd}.Optn.StrkPric.Unit").alias("option_strike_price_unit"),
        F.col(f"{txd}.Optn.StrkPric.Pctg").alias("option_strike_price_pct"),
        F.col(f"{txd}.Optn.StrkPric.Yld").alias("option_strike_price_yield"),
        F.col(f"{txd}.Optn.PrmAmt._VALUE").alias("option_premium_amount"),
        F.col(f"{txd}.Optn.PrmAmt._Ccy").alias("option_premium_ccy"),
        F.col(f"{txd}.Optn.PrmPmtDt").alias("option_premium_payment_dt"),
        F.col(f"{txd}.Optn.MtrtyDtOfUndrlyg").alias("option_underlying_maturity_dt"),

        # === Credit derivative attributes (TxData.Cdt) ===
        F.col(f"{txd}.Cdt.Snrty").alias("credit_seniority"),
        F.col(f"{txd}.Cdt.RefPty.LEI").alias("credit_reference_party_lei"),
        F.col(f"{txd}.Cdt.RefPty.Ctry").alias("credit_reference_party_country"),
        F.col(f"{txd}.Cdt.RefPty.CtrySubDvsn").alias("credit_reference_party_country_subdivision"),
        # Note: Cdt.PmtFrqcy is a STRING code in bronze (not a struct like
        # IntrstRate.PmtFrqcy), so we capture it directly as a single column.
        F.col(f"{txd}.Cdt.PmtFrqcy").alias("credit_payment_freq"),
        F.col(f"{txd}.Cdt.ClctnBsis").alias("credit_calculation_basis"),
        F.col(f"{txd}.Cdt.Srs").alias("credit_series"),
        F.col(f"{txd}.Cdt.Vrsn").alias("credit_version"),
        F.col(f"{txd}.Cdt.IndxFctr").alias("credit_index_factor"),
        F.col(f"{txd}.Cdt.Trch.Trnchd.AttchmntPt").alias("credit_tranche_attachment"),
        F.col(f"{txd}.Cdt.Trch.Trnchd.DtchmntPt").alias("credit_tranche_detachment"),
        F.col(f"{txd}.Cdt.Trch.Utrnchd").alias("credit_tranche_untranched"),

        # === Package transactions (TxData.Packg) ===
        F.col(f"{txd}.Packg.CmplxTradId").alias("package_complex_trade_id"),
        F.col(f"{txd}.Packg.Pric").alias("package_price"),
        F.col(f"{txd}.Packg.Sprd").alias("package_spread"),

        # === Other payments (TxData.OthrPmt[]) — ARRAY<STRUCT> ===
        # Bronze row shape: {PmtTp.Tp, PmtAmt.Amt._VALUE, PmtAmt.Amt._Ccy,
        # PmtDt, PmtPyer, PmtRcvr}. The synthetic data follows the XSD.
        # Payer/Receiver each have a Lgl (LEI) and Ntrl (natural-person ID)
        # choice — both branches are projected so neither is dropped.
        F.transform(
            F.col(f"{txd}.OthrPmt"),
            lambda p: F.struct(
                p["PmtTp"]["Tp"].alias("payment_type"),
                p["PmtAmt"]["Amt"]["_VALUE"].alias("amount"),
                p["PmtAmt"]["Amt"]["_Ccy"].alias("ccy"),
                p["PmtAmt"]["Sgn"].alias("sign"),
                p["PmtDt"].alias("payment_dt"),
                p["PmtPyer"]["Lgl"]["LEI"].alias("payer_lei"),
                p["PmtPyer"]["Ntrl"]["Id"]["Id"].alias("payer_natural_person_id"),
                p["PmtRcvr"]["Lgl"]["LEI"].alias("receiver_lei"),
                p["PmtRcvr"]["Ntrl"]["Id"]["Id"].alias("receiver_natural_person_id"),
            ),
        ).alias("other_payments"),

        # === Commodity taxonomy (TxData.Cmmdty) — COALESCE'd promoted cols ===
        # Per bronze schema, Agrcltrl/Nrgy/Envttl/Frtlzr/Frght/IndstrlPdct/Metl/
        # Ppr/Plprpln have sub-categories (e.g. Agrcltrl.GrnOilSeed.BasePdct),
        # while Indx/Infltn/MultiCmmdtyExtc/OffclEcnmcSttstcs/Othr/OthrC10
        # carry BasePdct directly. We coalesce across all valid leaf paths.
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.GrnOilSeed.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Soft.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Ptt.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.OlvOil.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Dairy.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Frstry.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Sfd.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.LiveStock.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Grn.BasePdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Elctrcty.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.NtrlGas.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Oil.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Coal.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.IntrNrgy.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.RnwblNrgy.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.LghtEnd.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Dstllts.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Emssns.BasePdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Wthr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Envttl.CrbnRltd.BasePdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Ammn.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.DmmnmPhspht.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Ptsh.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Slphr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Urea.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.UreaAndAmmnmNtrt.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frght.Dry.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frght.Wet.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frght.CntnrShip.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frght.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.Cnstrctn.BasePdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.Manfctg.BasePdct"),
            F.col(f"{txd}.Cmmdty.Metl.NonPrcs.BasePdct"),
            F.col(f"{txd}.Cmmdty.Metl.Prcs.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.CntnrBrd.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Nwsprnt.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Pulp.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.RcvrdPpr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.Plstc.BasePdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Indx.BasePdct"),
            F.col(f"{txd}.Cmmdty.Infltn.BasePdct"),
            F.col(f"{txd}.Cmmdty.MultiCmmdtyExtc.BasePdct"),
            F.col(f"{txd}.Cmmdty.OffclEcnmcSttstcs.BasePdct"),
            F.col(f"{txd}.Cmmdty.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.OthrC10.BasePdct"),
        ).alias("commodity_base_product"),
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.GrnOilSeed.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Soft.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Ptt.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.OlvOil.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Dairy.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Frstry.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Sfd.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.LiveStock.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Grn.SubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Elctrcty.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.NtrlGas.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Oil.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Coal.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.IntrNrgy.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.RnwblNrgy.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.LghtEnd.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Dstllts.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Emssns.SubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Wthr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.CrbnRltd.SubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Ammn.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.DmmnmPhspht.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Ptsh.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Slphr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Urea.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.UreaAndAmmnmNtrt.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.Dry.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.Wet.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.CntnrShip.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.Cnstrctn.SubPdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.Manfctg.SubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.NonPrcs.SubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.Prcs.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.CntnrBrd.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Nwsprnt.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Pulp.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.RcvrdPpr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.Plstc.SubPdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.Othr.SubPdct"),
        ).alias("commodity_sub_product"),
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.GrnOilSeed.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Soft.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.OlvOil.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Agrcltrl.Grn.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Elctrcty.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.NtrlGas.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.Oil.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.Emssns.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.Dry.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.Wet.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.NonPrcs.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.Prcs.AddtlSubPdct"),
        ).alias("commodity_additional_sub_product"),

        # === Energy-specific (TxData.NrgySpcfcAttrbts) ===
        F.col(f"{txd}.NrgySpcfcAttrbts.IntrCnnctnPt").alias("energy_interconnection_point"),
        F.col(f"{txd}.NrgySpcfcAttrbts.LdTp").alias("energy_load_type"),
        F.col(f"{txd}.NrgySpcfcAttrbts.DlvryPtOrZone").alias("energy_delivery_zones"),
        F.col(f"{txd}.NrgySpcfcAttrbts.DlvryAttr").alias("energy_delivery_attributes"),

        # === TechAttrbts ===
        F.col("TechAttrbts.RcncltnFlg").alias("reconciliation_flag"),

        # === Reporting metadata ===
        F.col("CtrPtySpcfcData.RptgTmStmp").alias("reporting_ts"),
        # NOTE: file-level header context (batch_*, biz_msg_id, sender/recipient_lei,
        # data_set_action) intentionally NOT denormalized here. Join to `submission_file`
        # on `file_path` when those columns are needed by a downstream consumer.

        # === Audit / lineage ===
        F.col("reporting_date"),
        F.col("file_path"),
        F.col("file_name"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
