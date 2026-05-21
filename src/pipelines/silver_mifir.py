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


# --------------------------------------------------------------------------
# Table 2 of 3: transaction_party (unified party explode)
#
# Built via four posexplode_outer operations (one per array), unioned
# with side ∈ {BUYER, SELLER} and party_role ∈ {ACCT_OWNR, DCSN_MAKR}
# discriminators. Filtered to drop NULL rows from posexplode_outer.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRANSACTION_PARTY,
    comment=(
        "Public: one row per repeating party identification per "
        "transaction. Unified explode of Buyr.AcctOwnr[], Buyr.DcsnMakr[], "
        "Sellr.AcctOwnr[], Sellr.DcsnMakr[] with side + party_role "
        "discriminators. Common case (single AcctOwnr per side) "
        "duplicates transaction.{buyer,seller}_* — multi-owner cases live "
        "here only."
    ),
    cluster_by_auto=True,
)
def transaction_party():
    bronze = _reporting_date(_add_filename_regex_columns(
        spark.readStream.table(TBL_BRONZE)
    ))

    # NOTE: AcctOwnr and DcsnMakr have DIFFERENT shapes in the richer ESMA payload schema:
    #   AcctOwnr[]: {Id: {LEI, MIC, Prsn: {FrstNm, Nm, BirthDt, Othr: {Id, SchmeNm}}, Intl}, CtryOfBrnch}
    #     -- NO Othr directly under Id; NO Prsn.CtryOfBrnch.
    #   DcsnMakr[]: {LEI, Prsn: {FrstNm, Nm, BirthDt, Othr: {Id, SchmeNm}}}
    #     -- flat LEI (no Id wrapper); no MIC/Intl/CtryOfBrnch.
    # We use two helpers and unionByName(allowMissingColumns=True) — but Spark still
    # needs every referenced column to exist in its source, so we project explicitly.

    def _explode_acct_ownr(side: str, array_path: str):
        return (
            bronze
            .select(
                F.col("New.TxId").alias("transaction_id"),
                F.lit(side).alias("side"),
                F.lit("ACCT_OWNR").alias("party_role"),
                F.col("reporting_date"),
                F.col("_ingested_at").alias("ingested_at"),
                F.posexplode_outer(F.col(array_path)).alias("sequence_no", "_party"),
            )
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("_party").isNotNull())
            .select(
                "transaction_id",
                "side",
                "party_role",
                "sequence_no",
                F.col("reporting_date"),
                F.col("_party.Id.LEI").alias("party_lei"),
                # Id.Othr.* doesn't exist on AcctOwnr.Id (Id is choice of LEI/MIC/Prsn/Intl)
                F.lit(None).cast("string").alias("party_other_id"),
                F.lit(None).cast("string").alias("party_other_id_scheme"),
                F.lit(None).cast("string").alias("party_other_id_scheme_proprietary"),
                F.col("_party.Id.MIC").alias("party_mic"),
                F.col("_party.Id.Intl").alias("party_intl_person_id"),
                F.col("_party.CtryOfBrnch").alias("party_country_of_branch"),
                F.col("_party.Id.Prsn.FrstNm").alias("person_first_name"),
                F.col("_party.Id.Prsn.Nm").alias("person_last_name"),
                F.col("_party.Id.Prsn.BirthDt").alias("person_birth_dt"),
                # Prsn.CtryOfBrnch doesn't exist in the richer Prsn struct
                F.lit(None).cast("string").alias("person_country"),
                F.col("_party.Id.Prsn.Othr.Id").alias("person_other_id"),
                F.col("_party.Id.Prsn.Othr.SchmeNm.Cd").alias("person_other_scheme"),
                F.col("_party.Id.Prsn.Othr.SchmeNm.Prtry").alias("person_other_scheme_proprietary"),
                F.col("ingested_at"),
                F.current_timestamp().alias("silver_processed_at"),
            )
        )

    def _explode_dcsn_makr(side: str, array_path: str):
        return (
            bronze
            .select(
                F.col("New.TxId").alias("transaction_id"),
                F.lit(side).alias("side"),
                F.lit("DCSN_MAKR").alias("party_role"),
                F.col("reporting_date"),
                F.col("_ingested_at").alias("ingested_at"),
                F.posexplode_outer(F.col(array_path)).alias("sequence_no", "_party"),
            )
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("_party").isNotNull())
            .select(
                "transaction_id",
                "side",
                "party_role",
                "sequence_no",
                F.col("reporting_date"),
                # DcsnMakr has flat LEI (no .Id wrapper)
                F.col("_party.LEI").alias("party_lei"),
                F.lit(None).cast("string").alias("party_other_id"),
                F.lit(None).cast("string").alias("party_other_id_scheme"),
                F.lit(None).cast("string").alias("party_other_id_scheme_proprietary"),
                # DcsnMakr has no MIC / Intl / CtryOfBrnch
                F.lit(None).cast("string").alias("party_mic"),
                F.lit(None).cast("string").alias("party_intl_person_id"),
                F.lit(None).cast("string").alias("party_country_of_branch"),
                F.col("_party.Prsn.FrstNm").alias("person_first_name"),
                F.col("_party.Prsn.Nm").alias("person_last_name"),
                F.col("_party.Prsn.BirthDt").alias("person_birth_dt"),
                F.lit(None).cast("string").alias("person_country"),
                F.col("_party.Prsn.Othr.Id").alias("person_other_id"),
                F.col("_party.Prsn.Othr.SchmeNm.Cd").alias("person_other_scheme"),
                F.col("_party.Prsn.Othr.SchmeNm.Prtry").alias("person_other_scheme_proprietary"),
                F.col("ingested_at"),
                F.current_timestamp().alias("silver_processed_at"),
            )
        )

    return (
        _explode_acct_ownr("BUYER", "New.Buyr.AcctOwnr")
        .unionByName(_explode_dcsn_makr("BUYER", "New.Buyr.DcsnMakr"), allowMissingColumns=True)
        .unionByName(_explode_acct_ownr("SELLER", "New.Sellr.AcctOwnr"), allowMissingColumns=True)
        .unionByName(_explode_dcsn_makr("SELLER", "New.Sellr.DcsnMakr"), allowMissingColumns=True)
    )


# --------------------------------------------------------------------------
# Table 3 of 3: transaction (main fact, ~135 scalars + ~15 arrays)
#
# One row per <Tx> element. action_type discriminator: NEW (full
# transaction with all fields) or CXL (cancellation, 3 shared fields
# only — rest NULL). Built incrementally — each subsequent commit adds
# one logical XSD section's columns to the .select(...) below.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRANSACTION,
    comment=(
        "Public: per-MiFIR-transaction snapshot. Wide-flat with business-"
        "readable column names. action_type ∈ {'NEW', 'CXL'} discriminator. "
        "Choice fields collapsed to LEI + *_other_id fallback. Append-only "
        "(event-based — not snapshot like EMIR)."
    ),
    cluster_by_auto=True,
)
def transaction():
    src = _reporting_date(_add_filename_regex_columns(
        spark.readStream.table(TBL_BRONZE)
    ))
    new_buy = "New.Buyr.AcctOwnr"
    new_sell = "New.Sellr.AcctOwnr"
    # Underlying-instrument path prefixes for the 6 sub-groups
    u_si = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp.SwpIn"
    u_so = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp.SwpOut"
    u_oth = "New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Othr"
    return src.select(
        # === Identification (5) ===
        F.coalesce(F.col("New.TxId"), F.col("Cxl.TxId")).alias("transaction_id"),
        F.when(F.col("New").isNotNull(), F.lit("NEW"))
         .when(F.col("Cxl").isNotNull(), F.lit("CXL"))
         .otherwise(F.lit("UNKNOWN"))
         .alias("action_type"),
        F.coalesce(F.col("New.ExctgPty"), F.col("Cxl.ExctgPty")).alias("executing_party_lei"),
        F.coalesce(F.col("New.SubmitgPty"), F.col("Cxl.SubmitgPty")).alias("submitting_party_lei"),
        F.col("New.InvstmtPtyInd").alias("investment_party_indicator"),

        # === Buyer flat fields — first AcctOwnr only (9 cols) ===
        # NOTE: Id.Othr.* path does not exist in the richer ESMA payload schema
        # (Id is a choice of {LEI, MIC, Prsn, Intl} — no Othr sibling). NULL-stubbed.
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("LEI").alias("buyer_lei"),
        F.lit(None).cast("string").alias("buyer_other_id"),
        F.lit(None).cast("string").alias("buyer_other_id_scheme"),
        F.lit(None).cast("string").alias("buyer_other_id_scheme_proprietary"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("MIC").alias("buyer_mic"),
        F.col(f"{new_buy}").getItem(0).getField("Id").getField("Intl").alias("buyer_intl_person_id"),
        F.col(f"{new_buy}").getItem(0).getField("CtryOfBrnch").alias("buyer_country_of_branch"),
        F.size(F.col("New.Buyr.AcctOwnr")).alias("buyer_account_owner_count"),
        F.size(F.col("New.Buyr.DcsnMakr")).alias("buyer_decision_maker_count"),

        # === Seller flat fields — mirror of buyer (9 cols) ===
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("LEI").alias("seller_lei"),
        F.lit(None).cast("string").alias("seller_other_id"),
        F.lit(None).cast("string").alias("seller_other_id_scheme"),
        F.lit(None).cast("string").alias("seller_other_id_scheme_proprietary"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("MIC").alias("seller_mic"),
        F.col(f"{new_sell}").getItem(0).getField("Id").getField("Intl").alias("seller_intl_person_id"),
        F.col(f"{new_sell}").getItem(0).getField("CtryOfBrnch").alias("seller_country_of_branch"),
        F.size(F.col("New.Sellr.AcctOwnr")).alias("seller_account_owner_count"),
        F.size(F.col("New.Sellr.DcsnMakr")).alias("seller_decision_maker_count"),

        # === Order transmission (3) ===
        F.col("New.OrdrTrnsmssn.TrnsmssnInd").alias("order_transmission_indicator"),
        F.col("New.OrdrTrnsmssn.TrnsmttgBuyr").alias("order_transmitting_buyer_lei"),
        F.col("New.OrdrTrnsmssn.TrnsmttgSellr").alias("order_transmitting_seller_lei"),

        # === Trade details (New.Tx nested struct, ~24 cols) ===
        F.col("New.Tx.TradDt").alias("trade_dt"),
        F.col("New.Tx.TradgCpcty").alias("trading_capacity"),
        F.col("New.Tx.Qty.Unit").alias("quantity_unit"),
        F.col("New.Tx.Qty.NmnlVal._VALUE").alias("quantity_nominal_value"),
        F.col("New.Tx.Qty.NmnlVal._Ccy").alias("quantity_nominal_currency"),
        F.col("New.Tx.Qty.MntryVal._VALUE").alias("quantity_monetary_value"),
        F.col("New.Tx.Qty.MntryVal._Ccy").alias("quantity_monetary_currency"),
        F.col("New.Tx.DerivNtnlChng").alias("derivative_notional_change"),
        F.col("New.Tx.Pric.Pric.MntryVal.Amt._VALUE").alias("price_amount"),
        F.col("New.Tx.Pric.Pric.MntryVal.Amt._Ccy").alias("price_currency"),
        F.col("New.Tx.Pric.Pric.MntryVal.Sgn").alias("price_sign"),
        F.col("New.Tx.Pric.Pric.Pctg").alias("price_percentage"),
        F.col("New.Tx.Pric.Pric.Yld").alias("price_yield"),
        F.col("New.Tx.Pric.Pric.BsisPts").alias("price_basis_points"),
        F.col("New.Tx.Pric.NoPric.Pdg").alias("price_pending_reason"),
        F.col("New.Tx.Pric.NoPric.Ccy").alias("price_pending_currency"),
        F.col("New.Tx.UpFrntPmt.Amt._VALUE").alias("up_front_payment_amount"),
        F.col("New.Tx.UpFrntPmt.Amt._Ccy").alias("up_front_payment_currency"),
        F.col("New.Tx.UpFrntPmt.Sgn").alias("up_front_payment_sign"),
        F.col("New.Tx.NetAmt").alias("net_amount"),
        F.col("New.Tx.TradVn").alias("trade_venue_mic"),
        F.col("New.Tx.CtryOfBrnch").alias("trade_country_of_branch"),
        F.col("New.Tx.TradPlcMtchgId").alias("trade_place_matching_id"),
        F.col("New.Tx.CmplxTradCmpntId").alias("complex_trade_component_id"),

        # === Instrument — general + derivative attributes (~18 cols) ===
        F.coalesce(
            F.col("New.FinInstrm.Id._VALUE"),
            F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.Id._VALUE"),
        ).alias("instrument_isin"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.FullNm").alias("instrument_full_name"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.ClssfctnTp").alias("instrument_classification"),
        F.col("New.FinInstrm.Othr.FinInstrmGnlAttrbts.NtnlCcy").alias("instrument_notional_currency"),
        # NOTE: CmmdtyDerivInd not present on FinInstrmGnlAttrbts in richer schema. NULL-stubbed.
        F.lit(None).cast("string").alias("instrument_commodity_derivative"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.Intrst.OthrNtnlCcy").alias("interest_other_notional_currency"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.AsstClssSpcfcAttrbts.FX.OthrNtnlCcy").alias("fx_other_notional_currency"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.PricMltplr").alias("instrument_price_multiplier"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.DlvryTp").alias("instrument_delivery_type"),
        # NOTE: DerivInstrmAttrbts has only XpryDt in richer schema (no MtrtyDt). NULL-stubbed.
        F.lit(None).cast("date").alias("instrument_maturity_dt"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.XpryDt").alias("instrument_expiry_dt"),
        # NOTE: StrkPric is wrapped one level deeper as StrkPric.Pric.{MntryVal/Pctg/Yld}
        # in the richer schema (mirrors Tx.Pric.Pric.* pattern), and monetary value
        # lives under .MntryVal.Amt._VALUE/_Ccy, not directly under .MntryVal.
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Pric.MntryVal.Amt._VALUE").alias("instrument_strike_price"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Pric.MntryVal.Amt._Ccy").alias("instrument_strike_price_ccy"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Pric.Pctg").alias("instrument_strike_price_percent"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.StrkPric.Pric.Yld").alias("instrument_strike_price_yield"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.OptnTp").alias("instrument_option_type"),
        F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.OptnExrcStyle").alias("instrument_option_exercise_style"),
        F.when(F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Swp").isNotNull(), F.lit("SWAP"))
         .when(F.col("New.FinInstrm.Othr.DerivInstrmAttrbts.UndrlygInstrm.Othr").isNotNull(), F.lit("OTHER"))
         .otherwise(F.lit(None).cast("string"))
         .alias("underlying_type"),

        # === Underlying instrument — 6 sub-prefix groups (~48 cols) ===
        # swap_in_single
        F.col(f"{u_si}.Sngl.ISIN._VALUE").alias("underlying_swap_in_single_isin"),
        F.col(f"{u_si}.Sngl.Indx.ISIN._VALUE").alias("underlying_swap_in_single_index_isin"),
        F.col(f"{u_si}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_swap_in_single_index_ref_rate_code"),
        F.col(f"{u_si}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_swap_in_single_index_ref_rate_name"),
        F.col(f"{u_si}.Sngl.Indx.Nm.Term.Unit").alias("underlying_swap_in_single_index_term_unit"),
        F.col(f"{u_si}.Sngl.Indx.Nm.Term.Val").alias("underlying_swap_in_single_index_term_value"),
        # swap_in_basket
        F.transform(F.col(f"{u_si}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_swap_in_basket_isins"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_swap_in_basket_index_isins"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_swap_in_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_swap_in_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_swap_in_basket_index_term_units"),
        F.transform(F.col(f"{u_si}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_swap_in_basket_index_term_values"),
        # swap_out_single — same shape as swap_in_single with u_so
        F.col(f"{u_so}.Sngl.ISIN._VALUE").alias("underlying_swap_out_single_isin"),
        F.col(f"{u_so}.Sngl.Indx.ISIN._VALUE").alias("underlying_swap_out_single_index_isin"),
        F.col(f"{u_so}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_swap_out_single_index_ref_rate_code"),
        F.col(f"{u_so}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_swap_out_single_index_ref_rate_name"),
        F.col(f"{u_so}.Sngl.Indx.Nm.Term.Unit").alias("underlying_swap_out_single_index_term_unit"),
        F.col(f"{u_so}.Sngl.Indx.Nm.Term.Val").alias("underlying_swap_out_single_index_term_value"),
        # swap_out_basket — same shape as swap_in_basket
        F.transform(F.col(f"{u_so}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_swap_out_basket_isins"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_swap_out_basket_index_isins"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_swap_out_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_swap_out_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_swap_out_basket_index_term_units"),
        F.transform(F.col(f"{u_so}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_swap_out_basket_index_term_values"),
        # underlying_other_single — same shape with u_oth
        F.col(f"{u_oth}.Sngl.ISIN._VALUE").alias("underlying_other_single_isin"),
        F.col(f"{u_oth}.Sngl.Indx.ISIN._VALUE").alias("underlying_other_single_index_isin"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.RefRate.Indx._VALUE").alias("underlying_other_single_index_ref_rate_code"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.RefRate.Nm").alias("underlying_other_single_index_ref_rate_name"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.Term.Unit").alias("underlying_other_single_index_term_unit"),
        F.col(f"{u_oth}.Sngl.Indx.Nm.Term.Val").alias("underlying_other_single_index_term_value"),
        # underlying_other_basket
        F.transform(F.col(f"{u_oth}.Bskt.ISIN"), lambda x: x["_VALUE"]).alias("underlying_other_basket_isins"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["ISIN"]["_VALUE"]).alias("underlying_other_basket_index_isins"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Indx"]["_VALUE"]).alias("underlying_other_basket_index_ref_rate_codes"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["RefRate"]["Nm"]).alias("underlying_other_basket_index_ref_rate_names"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Unit"]).alias("underlying_other_basket_index_term_units"),
        F.transform(F.col(f"{u_oth}.Bskt.Indx"), lambda x: x["Nm"]["Term"]["Val"]).alias("underlying_other_basket_index_term_values"),

        # === Investment decision person (~9 cols) ===
        # NOTE: InvstmtDcsnPrsn in the richer schema has only {Prsn.{CtryOfBrnch, Othr}, Algo}
        # — no LEI, no Prsn.FrstNm/Nm/BirthDt. NULL-stubbed.
        F.lit(None).cast("string").alias("investment_decision_person_lei"),
        F.lit(None).cast("string").alias("investment_decision_person_first_name"),
        F.lit(None).cast("string").alias("investment_decision_person_last_name"),
        F.lit(None).cast("date").alias("investment_decision_person_birth_dt"),
        F.col("New.InvstmtDcsnPrsn.Prsn.CtryOfBrnch").alias("investment_decision_person_country"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.Id").alias("investment_decision_person_other_id"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.SchmeNm.Cd").alias("investment_decision_person_other_scheme"),
        F.col("New.InvstmtDcsnPrsn.Prsn.Othr.SchmeNm.Prtry").alias("investment_decision_person_other_scheme_proprietary"),
        F.col("New.InvstmtDcsnPrsn.Algo").alias("investment_decision_algo_id"),

        # === Executing person (~10 cols) ===
        # NOTE: ExctgPrsn in the richer schema has only {Prsn.{CtryOfBrnch, Othr}, Algo, Clnt}
        # — no LEI, no Prsn.FrstNm/Nm/BirthDt. NULL-stubbed.
        F.lit(None).cast("string").alias("executing_person_lei"),
        F.lit(None).cast("string").alias("executing_person_first_name"),
        F.lit(None).cast("string").alias("executing_person_last_name"),
        F.lit(None).cast("date").alias("executing_person_birth_dt"),
        F.col("New.ExctgPrsn.Prsn.CtryOfBrnch").alias("executing_person_country"),
        F.col("New.ExctgPrsn.Prsn.Othr.Id").alias("executing_person_other_id"),
        F.col("New.ExctgPrsn.Prsn.Othr.SchmeNm.Cd").alias("executing_person_other_scheme"),
        F.col("New.ExctgPrsn.Prsn.Othr.SchmeNm.Prtry").alias("executing_person_other_scheme_proprietary"),
        F.col("New.ExctgPrsn.Clnt").alias("executing_person_client_indicator"),
        F.col("New.ExctgPrsn.Algo").alias("executing_algo_id"),

        # === Additional attributes (~6 cols) ===
        F.col("New.AddtlAttrbts.ShrtSellgInd").alias("short_selling_indicator"),
        F.transform(F.col("New.AddtlAttrbts.WvrInd"), lambda x: x["_VALUE"]).alias("waiver_indicators"),
        F.transform(F.col("New.AddtlAttrbts.OTCPstTradInd"), lambda x: x["_VALUE"]).alias("otc_post_trade_indicators"),
        # NOTE: AddtlAttrbts has no CmmdtyDerivInd in richer schema. NULL-stubbed.
        F.lit(None).cast("string").alias("commodity_derivative_indicator"),
        F.col("New.AddtlAttrbts.RskRdcgTx").alias("risk_reducing_transaction"),
        F.col("New.AddtlAttrbts.SctiesFincgTxInd").alias("securities_financing_tx_indicator"),

        # === Audit / lineage (5) ===
        F.col("file_path"),
        F.col("file_name"),
        F.col("reporting_date"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
