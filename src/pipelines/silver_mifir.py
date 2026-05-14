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

            # === Recipient (To.OrgId) — full party-identification block (~31 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.AnyBIC").alias("recipient_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Nm").alias("recipient_org_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.AdrTp").alias("recipient_org_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.Dept").alias("recipient_org_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.SubDept").alias("recipient_org_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.StrtNm").alias("recipient_org_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.BldgNb").alias("recipient_org_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.PstCd").alias("recipient_org_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.TwnNm").alias("recipient_org_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.CtrySubDvsn").alias("recipient_org_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.Ctry").alias("recipient_org_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.PstlAdr.AdrLine").alias("recipient_org_address_lines"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["Id"],
            ).alias("recipient_org_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("recipient_org_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("recipient_org_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["Issr"],
            ).alias("recipient_org_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.BirthDt").alias("recipient_person_birth_dt"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.PrvcOfBirth").alias("recipient_person_province_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.CityOfBirth").alias("recipient_person_city_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.CtryOfBirth").alias("recipient_person_country_of_birth"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Id"],
            ).alias("recipient_person_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("recipient_person_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("recipient_person_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Issr"],
            ).alias("recipient_person_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtryOfRes").alias("recipient_country_of_residence"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.NmPrfx").alias("recipient_contact_name_prefix"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.Nm").alias("recipient_contact_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.PhneNb").alias("recipient_contact_phone"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.MobNb").alias("recipient_contact_mobile"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.FaxNb").alias("recipient_contact_fax"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.EmailAdr").alias("recipient_contact_email"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.OrgId.CtctDtls.Othr").alias("recipient_contact_other"),

            # === Recipient FI (To.FIId) — financial-institution block (~29 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.BICFI").alias("recipient_fi_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Cd").alias("recipient_fi_clearing_system_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Prtry").alias("recipient_fi_clearing_system_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.ClrSysMmbId.MmbId").alias("recipient_fi_clearing_member_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.Nm").alias("recipient_fi_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.AdrTp").alias("recipient_fi_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.Dept").alias("recipient_fi_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.SubDept").alias("recipient_fi_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.StrtNm").alias("recipient_fi_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.BldgNb").alias("recipient_fi_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.PstCd").alias("recipient_fi_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.TwnNm").alias("recipient_fi_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.CtrySubDvsn").alias("recipient_fi_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.Ctry").alias("recipient_fi_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.PstlAdr.AdrLine").alias("recipient_fi_address_lines"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.Othr.Id").alias("recipient_fi_other_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.Othr.SchmeNm.Cd").alias("recipient_fi_other_scheme_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.Othr.SchmeNm.Prtry").alias("recipient_fi_other_scheme_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.FinInstnId.Othr.Issr").alias("recipient_fi_other_issuer"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.Id").alias("recipient_fi_branch_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.Nm").alias("recipient_fi_branch_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.AdrTp").alias("recipient_fi_branch_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.StrtNm").alias("recipient_fi_branch_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.BldgNb").alias("recipient_fi_branch_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.PstCd").alias("recipient_fi_branch_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.TwnNm").alias("recipient_fi_branch_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.CtrySubDvsn").alias("recipient_fi_branch_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.Ctry").alias("recipient_fi_branch_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.To.FIId.BrnchId.PstlAdr.AdrLine").alias("recipient_fi_branch_address_lines"),

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

            # === Rltd.Fr.OrgId mirror (~32 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.OrgId.AnyBIC").alias("related_sender_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Nm").alias("related_sender_org_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.AdrTp").alias("related_sender_org_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.Dept").alias("related_sender_org_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.SubDept").alias("related_sender_org_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.StrtNm").alias("related_sender_org_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.BldgNb").alias("related_sender_org_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.PstCd").alias("related_sender_org_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.TwnNm").alias("related_sender_org_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.CtrySubDvsn").alias("related_sender_org_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.Ctry").alias("related_sender_org_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.PstlAdr.AdrLine").alias("related_sender_org_address_lines"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["Id"],
            ).alias("related_sender_org_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("related_sender_org_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("related_sender_org_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.OrgId.Othr"),
                lambda o: o["Issr"],
            ).alias("related_sender_org_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.BirthDt").alias("related_sender_person_birth_dt"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.PrvcOfBirth").alias("related_sender_person_province_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.CityOfBirth").alias("related_sender_person_city_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.DtAndPlcOfBirth.CtryOfBirth").alias("related_sender_person_country_of_birth"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Id"],
            ).alias("related_sender_person_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("related_sender_person_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("related_sender_person_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Issr"],
            ).alias("related_sender_person_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtryOfRes").alias("related_sender_country_of_residence"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.NmPrfx").alias("related_sender_contact_name_prefix"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.Nm").alias("related_sender_contact_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.PhneNb").alias("related_sender_contact_phone"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.MobNb").alias("related_sender_contact_mobile"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.FaxNb").alias("related_sender_contact_fax"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.EmailAdr").alias("related_sender_contact_email"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.OrgId.CtctDtls.Othr").alias("related_sender_contact_other"),

            # === Rltd.Fr.FIId mirror (~29 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.BICFI").alias("related_sender_fi_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Cd").alias("related_sender_fi_clearing_system_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Prtry").alias("related_sender_fi_clearing_system_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.ClrSysMmbId.MmbId").alias("related_sender_fi_clearing_member_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.Nm").alias("related_sender_fi_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.AdrTp").alias("related_sender_fi_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.Dept").alias("related_sender_fi_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.SubDept").alias("related_sender_fi_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.StrtNm").alias("related_sender_fi_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.BldgNb").alias("related_sender_fi_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.PstCd").alias("related_sender_fi_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.TwnNm").alias("related_sender_fi_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.CtrySubDvsn").alias("related_sender_fi_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.Ctry").alias("related_sender_fi_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.PstlAdr.AdrLine").alias("related_sender_fi_address_lines"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.Othr.Id").alias("related_sender_fi_other_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.Othr.SchmeNm.Cd").alias("related_sender_fi_other_scheme_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.Othr.SchmeNm.Prtry").alias("related_sender_fi_other_scheme_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.FinInstnId.Othr.Issr").alias("related_sender_fi_other_issuer"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.Id").alias("related_sender_fi_branch_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.Nm").alias("related_sender_fi_branch_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.AdrTp").alias("related_sender_fi_branch_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.StrtNm").alias("related_sender_fi_branch_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.BldgNb").alias("related_sender_fi_branch_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.PstCd").alias("related_sender_fi_branch_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.TwnNm").alias("related_sender_fi_branch_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.CtrySubDvsn").alias("related_sender_fi_branch_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.Ctry").alias("related_sender_fi_branch_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.Fr.FIId.BrnchId.PstlAdr.AdrLine").alias("related_sender_fi_branch_address_lines"),

            # === Rltd.To.OrgId mirror (~32 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.OrgId.AnyBIC").alias("related_recipient_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Nm").alias("related_recipient_org_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.AdrTp").alias("related_recipient_org_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.Dept").alias("related_recipient_org_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.SubDept").alias("related_recipient_org_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.StrtNm").alias("related_recipient_org_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.BldgNb").alias("related_recipient_org_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.PstCd").alias("related_recipient_org_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.TwnNm").alias("related_recipient_org_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.CtrySubDvsn").alias("related_recipient_org_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.Ctry").alias("related_recipient_org_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.PstlAdr.AdrLine").alias("related_recipient_org_address_lines"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["Id"],
            ).alias("related_recipient_org_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("related_recipient_org_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("related_recipient_org_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.OrgId.Othr"),
                lambda o: o["Issr"],
            ).alias("related_recipient_org_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.BirthDt").alias("related_recipient_person_birth_dt"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.PrvcOfBirth").alias("related_recipient_person_province_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.CityOfBirth").alias("related_recipient_person_city_of_birth"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.DtAndPlcOfBirth.CtryOfBirth").alias("related_recipient_person_country_of_birth"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Id"],
            ).alias("related_recipient_person_other_ids"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Cd"],
            ).alias("related_recipient_person_other_scheme_codes"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["SchmeNm"]["Prtry"],
            ).alias("related_recipient_person_other_scheme_proprietaries"),
            F.transform(
                F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.Id.PrvtId.Othr"),
                lambda o: o["Issr"],
            ).alias("related_recipient_person_other_issuers"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtryOfRes").alias("related_recipient_country_of_residence"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.NmPrfx").alias("related_recipient_contact_name_prefix"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.Nm").alias("related_recipient_contact_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.PhneNb").alias("related_recipient_contact_phone"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.MobNb").alias("related_recipient_contact_mobile"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.FaxNb").alias("related_recipient_contact_fax"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.EmailAdr").alias("related_recipient_contact_email"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.OrgId.CtctDtls.Othr").alias("related_recipient_contact_other"),

            # === Rltd.To.FIId mirror (~29 cols) ===
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.BICFI").alias("related_recipient_fi_bic"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Cd").alias("related_recipient_fi_clearing_system_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.ClrSysMmbId.ClrSysId.Prtry").alias("related_recipient_fi_clearing_system_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.ClrSysMmbId.MmbId").alias("related_recipient_fi_clearing_member_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.Nm").alias("related_recipient_fi_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.AdrTp").alias("related_recipient_fi_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.Dept").alias("related_recipient_fi_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.SubDept").alias("related_recipient_fi_sub_department"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.StrtNm").alias("related_recipient_fi_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.BldgNb").alias("related_recipient_fi_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.PstCd").alias("related_recipient_fi_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.TwnNm").alias("related_recipient_fi_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.CtrySubDvsn").alias("related_recipient_fi_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.Ctry").alias("related_recipient_fi_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.PstlAdr.AdrLine").alias("related_recipient_fi_address_lines"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.Othr.Id").alias("related_recipient_fi_other_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.Othr.SchmeNm.Cd").alias("related_recipient_fi_other_scheme_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.Othr.SchmeNm.Prtry").alias("related_recipient_fi_other_scheme_proprietary"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.FinInstnId.Othr.Issr").alias("related_recipient_fi_other_issuer"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.Id").alias("related_recipient_fi_branch_id"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.Nm").alias("related_recipient_fi_branch_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.AdrTp").alias("related_recipient_fi_branch_address_type"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.StrtNm").alias("related_recipient_fi_branch_street_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.BldgNb").alias("related_recipient_fi_branch_building_number"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.PstCd").alias("related_recipient_fi_branch_post_code"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.TwnNm").alias("related_recipient_fi_branch_town_name"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.CtrySubDvsn").alias("related_recipient_fi_branch_country_sub_division"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.Ctry").alias("related_recipient_fi_branch_country"),
            F.col("hdr_pyld_metadata.BizAppHeader.AppHdr.Rltd.To.FIId.BrnchId.PstlAdr.AdrLine").alias("related_recipient_fi_branch_address_lines"),
        )
    )
