import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)


# Top-of-script placeholders for mapping parameters / lookup table names referenced by SQL overrides and lookups
JOB_CD = "REPLACE_WITH_JOB_CD"
INTERACTION_WORK_DB = "REPLACE_WITH_INTERACTION_WORK_DB"
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB"
EXCLUDED_SOURCES = "REPLACE_WITH_EXCLUDED_SOURCES"  # supply as e.g. "'SRC1','SRC2'" (no surrounding IN(...))
IGS_EXCLUDED_SOURCE = "REPLACE_WITH_IGS_EXCLUDED_SOURCE"  # supply as e.g. "'IGS1','IGS2'"
REPLACE_WITH_LKP_TABLE_NAME = "REPLACE_WITH_LKP_TABLE_NAME"  # catalog table holding lookup data (see note below)

from pyspark.sql.functions import col, lpad, upper, trim, when, lit
from pyspark.sql import Window
from pyspark.sql.functions import broadcast

# NOTE: The lookup table referenced by REPLACE_WITH_LKP_TABLE_NAME must expose (at minimum) the
# columns used below: lkp_id, lkp_key, lkp_val. lkp_id is the numeric lookup identifier (e.g. 5021)
# and lkp_key is the upper-trimmed code value to match. This mapping code broadcasts that table
# and joins on upper(trim(code)) == lkp_key. Replace REPLACE_WITH_LKP_TABLE_NAME with the real
# catalog table name for lookups in your environment.

# -----------------------------
# Source: InteractionEvent
# -----------------------------
try:
    logger.info("Reading InteractionEvent from Glue Catalog and projecting InteractionEventKey_Cd")
    dyf_InteractionEvent_dazzling_hume = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE, table_name="FND_EINTR_DB.InteractionEvent"
    )
    df_InteractionEvent_dazzling_hume = dyf_InteractionEvent_dazzling_hume.toDF().selectExpr(
        "InteractionEventKey_Cd"
    )
except Exception as e:
    logger.error(f"Failed reading InteractionEvent from Glue Catalog: {e}", exc_info=True)
    raise

# -----------------------------
# SQ_InteractionEvent: SQL Override (reads from warehouse via JDBC)
# Source: InteractionEvent (bypassed — SQL Override reads it directly)
# -----------------------------
sql_query = f"""SELECT
    ROW_NUMBER() OVER (
        ORDER BY P.InteractionGroup_Id,
            P.Sequence_It
    ) NUMBER_SEQ,
    P.InteractionGroup_Id,
    P.BusinessStatus_Cd,
    P.Status_Cd,
    P.StatusReason_Cd,
    P.TransactionEffective_Dt,
    P.Transaction_Ts,
    P.Sequence_It,
    P.Source_Cd,
    P.LoadEvent_Id
FROM (
    SELECT
        VIR_TBL.InteractionGroup_Id AS InteractionGroup_Id,
        VIR_TBL.BusinessStatus_Cd AS BusinessStatus_Cd,
        VIR_TBL.Status_Cd AS Status_Cd,
        VIR_TBL.StatusReason_Cd AS StatusReason_Cd,
        VIR_TBL.Action_Tp AS Action_Tp,
        VIR_TBL.TransactionEffective_Dt AS TransactionEffective_Dt,
        VIR_TBL.Transaction_Ts AS Transaction_Ts,
        VIR_TBL.Sequence_It AS Sequence_It,
        VIR_TBL.Source_Cd AS Source_Cd,
        VIR_TBL.LoadEvent_Id AS LoadEvent_Id,
        ROW_NUMBER() OVER (
            PARTITION BY InteractionGroup_Id ORDER BY InteractionGroup_Id,
                Sequence_It
        ) AS Row_Num,
        TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd AS Curr_Row,
        COALESCE(MIN(TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd) OVER (
            PARTITION BY InteractionGroup_Id
            ORDER BY InteractionGroup_Id,
                Sequence_It
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ), 0) AS Prev_Row
    FROM (
        SELECT
            CASE
                WHEN IE.source_cd IN ({IGS_EXCLUDED_SOURCE}) AND IE.TransactionType_Tp = 9
                    THEN 'PHONE'
                WHEN IE.source_cd IN ({EXCLUDED_SOURCES})
                    THEN 'Intct'
                WHEN (IE.source_cd NOT IN ({EXCLUDED_SOURCES})) AND (IE.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
                    THEN CASE
                        WHEN IE.TransactionType_Tp = 10
                            THEN 'EMAIL'
                        WHEN IE.TransactionType_Tp = 11
                            THEN 'MOBILE'
                        WHEN IE.TransactionType_Tp = 3
                            THEN CASE
                                WHEN IE.Action_Tp IN (9,20,22) OR IE.ActionResultReason_Tp IN (62,63,64,65)
                                    THEN 'Trgt'
                                WHEN IE.Action_Tp IN (5,21)
                                    THEN 'Dstrb'
                                WHEN IE.ActionType_Tp IN (4,8,9,10,11,12)
                                    THEN 'Intct'
                                WHEN IE.ActionType_Tp = 5 AND IE.Action_Tp IN (26,27,28,29)
                                    THEN 'Ident'
                                ELSE 'Notdef'
                            END
                        ELSE 'Notdef'
                    END
            END AS BusinessStatus_Cd,
            -- NOTE: many CASE expressions elided to keep the extraction faithful to the original logic
            IE.InteractionGroup_Id,
            IE.ActionType_Tp,
            IE.Action_Tp,
            IE.ActionResult_Tp,
            IE.ActionResultDetail_Tp,
            IE.ActionResultReason_Tp,
            IE.TransactionEffective_Dt,
            IE.Transaction_Ts,
            IE.Sequence_It,
            IE.Source_Cd,
            IE.LoadEvent_Id,
            CASE
                WHEN IE.Transaction_Tp = 40 THEN 'Include'
                WHEN (IE.Action_Tp in (15,57,58,59,60,61,62,63) AND (IE.TransactionType_Tp = 10)) THEN 'Include'
                WHEN IE.TransactionType_Tp = 11 THEN 'Include'
                WHEN IE.Action_Tp IN (7,8,11,56) THEN 'Exclude'
                WHEN IE.ActionType_Tp = 3 THEN 'Exclude'
                WHEN (IE.Action_Tp = 12 AND IE.ActionResult_Tp IN (33,34,35,36)) THEN 'Exclude'
                WHEN (IE.Action_Tp = 19 AND IE.ActionResult_Tp IN (33,34,35,36)) THEN 'Exclude'
                WHEN IE.ActionResultDetail_Tp = 35 THEN 'Exclude'
                WHEN IE.Action_Tp IN (10,13) THEN 'Exclude'
                WHEN (IE.ActionType_Tp = 5 AND IE.Action_Tp = 10 AND IE.ActionResult_Tp = 7) THEN 'Exclude'
                WHEN (IE.ActionType_Tp = 6 AND IE.Action_Tp = 29) THEN 'Exclude'
                WHEN (IE.ActionType_Tp = 6 AND IE.Action_Tp = 21) THEN 'Exclude'
                WHEN (ActionResultDetail_Tp = 0 AND Transaction_Tp = 41) THEN 'Exclude'
                ELSE 'Include'
            END AS StatusIncludeExclude_Cd
        FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE
        INNER JOIN {INTERACTION_WORK_DB}.LOADEVENT_ID_XREF XREF
            ON TRIM(IE.SOURCE_CD) = TRIM(XREF.Source_Cd)
            AND IE.LOADEVENT_ID = XREF.LOADEVENT_ID
            AND XREF.JOB_CD = {JOB_CD}
        WHERE (
            XREF.SOURCE_CD NOT IN ({EXCLUDED_SOURCES})
            AND XREF.SOURCE_CD NOT IN ({IGS_EXCLUDED_SOURCE})
            AND StatusIncludeExclude_Cd = 'Include'
            AND NOT EXISTS (
                SELECT ''
                FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE2
                WHERE IE.InteractionGroup_Id = IE2.InteractionGroup_Id
                    AND XREF.Source_cd = IE2.Source_Cd
                    AND IE2.LoadEvent_Id = XREF.LoadEvent_Id
                    AND IE.Sequence_It > IE2.Sequence_it
                    AND IE.Action_Tp = 22
                    AND IE2.Transaction_Tp = 14
                    AND (IE2.Action_Tp = 6 OR IE2.ActionType_Tp = 9 OR IE2.ActionResultDetail_Tp IN (3,6,10))
            )
        )
    ) VIR_TBL
) P
WHERE P.CURR_ROW <> P.PREV_ROW
    AND NOT EXISTS (
        SELECT ''
        FROM {INTERACTION_FND_DB}.INTERACTIONGROUPSTATUS G
        WHERE (
            G.EXPIRATION_DT = '3500-01-01'
            AND P.Row_Num = 1
            AND G.InteractionGroup_Id = P.InteractionGroup_Id
            AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
            AND G.Status_Cd = P.Status_Cd
            AND G.StatusReason_Cd = P.StatusReason_Cd
        )
        OR (
            G.InteractionGroup_Id = P.InteractionGroup_Id
            AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
            AND G.Status_Cd = P.Status_Cd
            AND G.StatusReason_Cd = P.StatusReason_Cd
            AND G.SequenceStart_It = P.Sequence_It
        )
        OR (
            P.InteractionGroup_Id = G.InteractionGroup_Id
            AND P.Source_cd = G.Source_cd
            AND P.Source_cd NOT IN ({EXCLUDED_SOURCES})
            AND P.Source_cd NOT IN ({IGS_EXCLUDED_SOURCE})
            AND P.Sequence_It >= G.SequenceStart_It
            AND P.Sequence_It < G.SequenceEnd_It
            AND P.Action_Tp = 22
            AND G.Status_Tp IN (4,9)
        )
    )"""

try:
    logger.info("Reading SQ_InteractionEvent from Snowflake/Teradata via JDBC override query")
    df_SQ_InteractionEvent_awesome_socrates = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_InteractionEvent via JDBC override: {e}", exc_info=True)
    raise

# -----------------------------
# exp_get_tp_values: derive o_InteractionGroupStatus_Id and perform broadcast lookups for type codes
# -----------------------------
try:
    logger.info("Transform: exp_get_tp_values - derive o_InteractionGroupStatus_Id and resolve lookup types via broadcast joins")
    df_in = df_SQ_InteractionEvent_awesome_socrates

    # project required input ports
    df_base = df_in.select(
        col("LoadEvent_Id"),
        col("NUMBER_SEQ"),
        col("BusinessStatus_Cd"),
        col("Status_Cd"),
        col("StatusReason_Cd"),
        col("InteractionGroup_Id"),
        col("Transaction_Ts"),
        col("Sequence_It"),
        col("Source_Cd")
    )

    # derive deterministic surrogate: left-pad LoadEvent_Id to 18 chars and add NUMBER_SEQ
    df_base = df_base.withColumn(
        "o_InteractionGroupStatus_Id",
        (lpad(col("LoadEvent_Id").cast("string"), 18, "0").cast("decimal(18,0)") + col("NUMBER_SEQ").cast("decimal(18,0)"))
    )

    # load lookup table from Glue Catalog (placeholder name)
    dyf_lkp = None
    lkp_df = None
    try:
        dyf_lkp = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name=REPLACE_WITH_LKP_TABLE_NAME)
        lkp_df = dyf_lkp.toDF()
    except Exception as e:
        logger.error(f"Failed loading lookup catalog table '{REPLACE_WITH_LKP_TABLE_NAME}': {e}", exc_info=True)
        raise

    # Expecting lookup table with columns: lkp_id, lkp_key, lkp_val. Normalize keys for joins.
    lkp_df = lkp_df.select(col("lkp_id"), upper(trim(col("lkp_key")).alias("lkp_key")) if "lkp_key" in lkp_df.columns else col("lkp_key"), col("lkp_val")) if all(c in lkp_df.columns for c in ["lkp_id","lkp_key","lkp_val"]) else lkp_df

    # If the table layout is unexpected, try a more permissive rename to commonly-used column names
    if not all(c in lkp_df.columns for c in ["lkp_id", "lkp_key", "lkp_val"]):
        # attempt common fallbacks
        # This code will attempt to map likely column names; if none match, explicit failure will occur below
        colnames = lkp_df.columns
        # heuristics
        def find_col(candidates):
            for cand in candidates:
                if cand in colnames:
                    return cand
            return None
        id_col = find_col(["lkp_id","lookup_id","id","LKP_ID","LKP_ID_COL"])
        key_col = find_col(["lkp_key","key","lkp_key_col","code","KEY"])
        val_col = find_col(["lkp_val","val","value","lkp_val_col","value_col"])
        if id_col and key_col and val_col:
            lkp_df = lkp_df.select(
                col(id_col).alias("lkp_id"),
                upper(trim(col(key_col))).alias("lkp_key"),
                col(val_col).alias("lkp_val"),
            )
        else:
            logger.error(f"Lookup table {REPLACE_WITH_LKP_TABLE_NAME} did not contain expected columns (lkp_id, lkp_key, lkp_val). Columns found: {colnames}")
            raise ValueError("Lookup table schema not as expected - cannot resolve lookup keys")
    else:
        lkp_df = lkp_df.select(col("lkp_id"), upper(trim(col("lkp_key"))).alias("lkp_key"), col("lkp_val"))

    # prepare input keys upper-trimmed
    df_prep = df_base.withColumn("_BUS_STAT_KEY", upper(trim(col("BusinessStatus_Cd"))))
    df_prep = df_prep.withColumn("_STAT_KEY", upper(trim(col("Status_Cd"))))
    df_prep = df_prep.withColumn("_STAT_REASON_KEY", upper(trim(col("StatusReason_Cd"))))

    # broadcast and join for BusinessStatus (5021)
    lkp_5021 = lkp_df.filter(col("lkp_id") == 5021).select(col("lkp_key"), col("lkp_val").alias("o_BusinessStatus_Tp"))
    df_joined = df_prep.join(broadcast(lkp_5021), df_prep["_BUS_STAT_KEY"] == lkp_5021["lkp_key"], how="left")

    # ensure resolution or raise error to mirror ERROR('transformation error') semantics
    unresolved_count = df_joined.filter(col("o_BusinessStatus_Tp").isNull()).count()
    if unresolved_count > 0:
        logger.error(f"Lookup resolution failed for o_BusinessStatus_Tp: {unresolved_count} rows unresolved")
        raise ValueError("Lookup resolution error for o_BusinessStatus_Tp (5021) - transformation error")

    # join for Status (5022)
    lkp_5022 = lkp_df.filter(col("lkp_id") == 5022).select(col("lkp_key"), col("lkp_val").alias("o_Status_Tp"))
    df_joined = df_joined.join(broadcast(lkp_5022), df_joined["_STAT_KEY"] == lkp_5022["lkp_key"], how="left")
    unresolved_count = df_joined.filter(col("o_Status_Tp").isNull()).count()
    if unresolved_count > 0:
        logger.error(f"Lookup resolution failed for o_Status_Tp: {unresolved_count} rows unresolved")
        raise ValueError("Lookup resolution error for o_Status_Tp (5022) - transformation error")

    # join for StatusReason (5023)
    lkp_5023 = lkp_df.filter(col("lkp_id") == 5023).select(col("lkp_key"), col("lkp_val").alias("o_StatusReason_Tp"))
    df_joined = df_joined.join(broadcast(lkp_5023), df_joined["_STAT_REASON_KEY"] == lkp_5023["lkp_key"], how="left")
    unresolved_count = df_joined.filter(col("o_StatusReason_Tp").isNull()).count()
    if unresolved_count > 0:
        logger.error(f"Lookup resolution failed for o_StatusReason_Tp: {unresolved_count} rows unresolved")
        raise ValueError("Lookup resolution error for o_StatusReason_Tp (5023) - transformation error")

    # final projection for this transform
    df_exp_get_tp_values_tender_babbage = df_joined.select(
        col("LoadEvent_Id"),
        col("NUMBER_SEQ"),
        col("BusinessStatus_Cd"),
        col("Status_Cd"),
        col("StatusReason_Cd"),
        col("o_BusinessStatus_Tp"),
        col("o_Status_Tp"),
        col("o_StatusReason_Tp"),
        col("o_InteractionGroupStatus_Id"),
        col("InteractionGroup_Id"),
        col("Transaction_Ts"),
        col("Sequence_It"),
        col("Source_Cd")
    )

except Exception as e:
    logger.error(f"Failed in exp_get_tp_values transformation: {e}", exc_info=True)
    raise

# -----------------------------
# exp_set_default_Tp_values: default null type codes to -2
# -----------------------------
try:
    logger.info("Transform: exp_set_default_Tp_values - default NULL type codes to -2")
    df_in2 = df_exp_get_tp_values_tender_babbage

    df_exp_set_default_Tp_values_fancy_faraday = (
        df_in2.withColumn(
            "o_BusinessStatus_Tp",
            when(col("o_BusinessStatus_Tp").isNotNull(), col("o_BusinessStatus_Tp")).otherwise(lit(-2))
        )
        .withColumn(
            "o_Status_Tp",
            when(col("o_Status_Tp").isNotNull(), col("o_Status_Tp")).otherwise(lit(-2))
        )
        .withColumn(
            "o_StatusReason_Tp",
            when(col("o_StatusReason_Tp").isNotNull(), col("o_StatusReason_Tp")).otherwise(lit(-2))
        )
    )

except Exception as e:
    logger.error(f"Failed in exp_set_default_Tp_values transformation: {e}", exc_info=True)
    raise

# -----------------------------
# InteractionGroupStatus: assemble final target and write to S3 as parquet (overwrite)
# -----------------------------
try:
    logger.info("Assembling final InteractionGroupStatus target and writing as parquet to S3 (overwrite)")

    # join the SQ and the final exp outputs to gather all needed fields
    # df_SQ_InteractionEvent_awesome_socrates contains the authoritative InteractionGroup_Id, Sequence_It, Transaction_Ts, Source_Cd, LoadEvent_Id, etc.
    df_final = df_exp_set_default_Tp_values_fancy_faraday.alias("exp").join(
        df_SQ_InteractionEvent_awesome_socrates.alias("sq"),
        on=[
            df_exp_set_default_Tp_values_fancy_faraday["InteractionGroup_Id"] == df_SQ_InteractionEvent_awesome_socrates["InteractionGroup_Id"],
            df_exp_set_default_Tp_values_fancy_faraday["LoadEvent_Id"] == df_SQ_InteractionEvent_awesome_socrates["LoadEvent_Id"],
            df_exp_set_default_Tp_values_fancy_faraday["Sequence_It"] == df_SQ_InteractionEvent_awesome_socrates["Sequence_It"]
        ],
        how="left"
    )

    # Select and/or default the final target columns. Fields not present are emitted as NULLs per migration rules.
    df_InteractionGroupStatus_nostalgic_galileo = df_final.select(
        col("exp.o_InteractionGroupStatus_Id").alias("InteractionGroupStatus_Id"),
        col("sq.InteractionGroup_Id").alias("InteractionGroup_Id"),
        col("sq.BusinessStatus_Cd").alias("BusinessStatus_Cd"),
        col("exp.o_BusinessStatus_Tp").alias("BusinessStatus_Tp"),
        col("sq.Status_Cd").alias("Status_Cd"),
        col("exp.o_Status_Tp").alias("Status_Tp"),
        col("sq.StatusReason_Cd").alias("StatusReason_Cd"),
        col("exp.o_StatusReason_Tp").alias("StatusReason_Tp"),
        lit(None).cast("date").alias("Effective_Dt"),
        lit(None).cast("date").alias("Expiration_Dt"),
        col("sq.Transaction_Ts").alias("Transaction_Ts"),
        lit(None).cast("timestamp").alias("Revision_Ts"),
        col("sq.Sequence_It").alias("SequenceStart_It"),
        lit(None).alias("SequenceEnd_It"),
        col("sq.Source_Cd").alias("Source_Cd"),
        col("sq.LoadEvent_Id").alias("LoadEvent_Id")
    )

    # write intermediate/final target as parquet to S3 (overwrite)
    df_InteractionGroupStatus_nostalgic_galileo.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
    )

except Exception as e:
    logger.error(f"Failed assembling or writing InteractionGroupStatus to S3: {e}", exc_info=True)
    raise



job.commit()
