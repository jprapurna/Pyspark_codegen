import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SNOWFLAKE_SECRET_NAME = "REPLACE_WITH_SNOWFLAKE_SECRET_NAME"
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)

from pyspark.sql.functions import col, expr, lit, round as spark_round, first
from pyspark.sql import functions as F

# Top-of-script placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_CATALOG_DB = "REPLACE_WITH_GLUE_DATABASE"

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_ intermediate) - S3-first, fallback to Glue Catalog
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 parquet (staged intermediate)")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    # project exactly the fields defined on the source node
    df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan = df_tmp.selectExpr(
        "NISS_APRM_DETL_SK",
        "CLNDR_YR",
        "CALL_YR",
        "NAIC_CMPNY_CD",
        "NISS_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "NISS_ST_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "BI_LMT",
        "GA_ADDED_AT_FAULT_IND",
        "FA2_PLCY_IND",
        "UM_UMI_STACKING",
        "PIP_WVR_WL_IND",
        "PIP_MED_SEC_IND",
        "PIP_LOSS_INCOME_IND",
        "MI_PPO_IND",
        "PRD_GRP_CD",
        "NJ_HLTH_INSR_PRIM",
        "NJ_EXTR_PIP_PKG",
        "NJ_RESDNC_RLTNSHP_PIP_IND",
        "NY_SSL_IND",
        "NY_FULL_CVG_GLASS_COMP_IND",
        "GRGNG_ZIP_5",
        "NISS_TERR_CD",
        "RATNG_CMPY_CD",
        "MLT_CAR_IND",
        "RT_CLS",
        "AGE",
        "GENDR",
        "MRTL_STAT",
        "AUTO_USE_CD",
        "MILES_TO_WRK",
        "GOOD_STDNT_IND",
        "DRVR_TRNG_IND",
        "SOI_TYP",
        "PHY_DMG_IND",
        "NJ_RATD_PNTS",
        "VEH_MDL_YR",
        "NJ_EXCPTION_CD",
        "NJ_FGVN_PNTS",
        "PASSV_RESTRA_DISC",
        "SNR_DRVR_IND",
        "DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC",
        "DAY_TM_RUN_LIGHTS",
        "LMT_TORT",
        "ANNL_STMNT_LOB_CD",
        "CVG_TYP_IND",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD",
        "NISS_CMMCL_IND_CD",
        "NISS_EXCPN_CD",
        "NISS_FGVNS_CD",
        "NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD",
        "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
        "NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD",
        "NISS_LIAB_OR_NO_FAULT_CD",
        "NISS_ANNL_STMNT_LOB_CD",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE",
        "NISS_MNFCTRS_MDL_YR",
        "CR_BY_MAPNG_ID",
        "DW_CR_TMSP",
        "UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP",
        "WRK_FLOW_RUN_ID",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "EXPS_VAL_ROLLED",
        "CVG_CNT_IND",
        "CVG_CNT",
        "CVG_CD_SK",
        "CVG_ATTR_SK",
        "REC_DROP_IND",
        "REC_DROP_RSN_DESC",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "CVG_ATTR_CHCKSUM",
        "COMP_DED",
        "COLL_DED",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT",
        "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "DERIVED_RDRVR_AGE",
        "FINAL_RDRVR_AGE",
        "PNI_AGE",
        "LOB",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED"
    )
except Exception as e:
    logger.warning(f"S3 parquet read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_CATALOG_DB, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_cat = dyf.toDF()
        df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan = df_cat.selectExpr(
            "NISS_APRM_DETL_SK",
            "CLNDR_YR",
            "CALL_YR",
            "NAIC_CMPNY_CD",
            "NISS_CMPNY_CD",
            "ST_NM",
            "ST_CD",
            "NISS_ST_CD",
            "ST_ABBR",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "GA_ADDED_AT_FAULT_IND",
            "FA2_PLCY_IND",
            "UM_UMI_STACKING",
            "PIP_WVR_WL_IND",
            "PIP_MED_SEC_IND",
            "PIP_LOSS_INCOME_IND",
            "MI_PPO_IND",
            "PRD_GRP_CD",
            "NJ_HLTH_INSR_PRIM",
            "NJ_EXTR_PIP_PKG",
            "NJ_RESDNC_RLTNSHP_PIP_IND",
            "NY_SSL_IND",
            "NY_FULL_CVG_GLASS_COMP_IND",
            "GRGNG_ZIP_5",
            "NISS_TERR_CD",
            "RATNG_CMPY_CD",
            "MLT_CAR_IND",
            "RT_CLS",
            "AGE",
            "GENDR",
            "MRTL_STAT",
            "AUTO_USE_CD",
            "MILES_TO_WRK",
            "GOOD_STDNT_IND",
            "DRVR_TRNG_IND",
            "SOI_TYP",
            "PHY_DMG_IND",
            "NJ_RATD_PNTS",
            "VEH_MDL_YR",
            "NJ_EXCPTION_CD",
            "NJ_FGVN_PNTS",
            "PASSV_RESTRA_DISC",
            "SNR_DRVR_IND",
            "DEFNS_DRVR_DISC_IND",
            "ANTI_THFT_DISC",
            "DAY_TM_RUN_LIGHTS",
            "LMT_TORT",
            "ANNL_STMNT_LOB_CD",
            "CVG_TYP_IND",
            "CVG_EXPS_VAL",
            "TTL_WRITTN_PREM_AMT",
            "LINE_CD",
            "ACCDNT_YR",
            "NISS_CVG_CD",
            "RTNG_ZNE_CD",
            "TERM_ZNE_CD",
            "NISS_CLASS_CD",
            "NISS_ELIG_PNTS_CD",
            "NISS_AGE_GRP_CD",
            "NISS_CMMCL_IND_CD",
            "NISS_EXCPN_CD",
            "NISS_FGVNS_CD",
            "NISS_PASSV_RESTRA_CD",
            "NISS_DEFNS_DRVR_CRD_CD",
            "NISS_ANTI_THFT_DVC_CD",
            "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
            "NISS_PLCY_LMT_CD",
            "NISS_DEDUC_CD",
            "NISS_SSL_LIAB_CD",
            "NISS_SUBLOB_CD",
            "NISS_TYP_LOSS_CD",
            "NISS_LIAB_OR_NO_FAULT_CD",
            "NISS_ANNL_STMNT_LOB_CD",
            "NISS_PD_LOSS",
            "NISS_PD_ALLOC_ADJUS_EXPNS",
            "NISS_OUTSTNDG_LOSS",
            "NISS_NO_PD_CLMS",
            "NISS_NO_OUTSTND_CLMS",
            "RSVD_NISS_USE",
            "NISS_RSVD_CMPNY_USE",
            "NISS_MNFCTRS_MDL_YR",
            "CR_BY_MAPNG_ID",
            "DW_CR_TMSP",
            "UPD_BY_MAPNG_ID",
            "DW_UPD_TMSP",
            "WRK_FLOW_RUN_ID",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "EXPS_VAL_ROLLED",
            "CVG_CNT_IND",
            "CVG_CNT",
            "CVG_CD_SK",
            "CVG_ATTR_SK",
            "REC_DROP_IND",
            "REC_DROP_RSN_DESC",
            "REC_EXCPN_IND",
            "REC_EXCPN_RSN_DESC",
            "CVG_ATTR_CHCKSUM",
            "COMP_DED",
            "COLL_DED",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM",
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "DERIVED_RDRVR_AGE",
            "FINAL_RDRVR_AGE",
            "PNI_AGE",
            "LOB",
            "PRINCIPAL_OPRT",
            "SOURCE_IND_DERIVED"
        )
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog after S3 fallback: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (SQL override against staged table)
# -----------------------------------------------------------------------------
try:
    # register the staged upstream dataframe as the bare view the override expects
    df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT
    CVG_ATTR_SK,
    CASE WHEN CVG_TYP_IND='B' THEN '?-?-?' ELSE CVG_TYP_CD END CVG_TYP_CD,
    CVG_TYP_IND,
    CASE WHEN CVG_TYP_IND='B' THEN 1 ELSE 99 END CVG_TYP_Priority,
    SUM(CVG_EXPS_VAL) CVG_EXPS_VAL
FROM WRK_BIRP_NISS_APRM_DETL
WHERE 1=1
AND SOURCE_IND_DERIVED!='TOGGLE AUTO'
GROUP BY
  CVG_ATTR_SK,
  CASE WHEN CVG_TYP_IND='B' THEN '?-?-?' ELSE CVG_TYP_CD END,
  CVG_TYP_IND,
  CASE WHEN CVG_TYP_IND='B' THEN 1 ELSE 99 END
ORDER BY CVG_ATTR_SK, CVG_TYP_Priority, CVG_EXPS_VAL DESC
"""

    logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged view via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_pascal = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_Passthru - explicit passthrough projection
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_Passthru passthrough projection")
    df_EXP_Passthru_peaceful_kepler = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_pascal.selectExpr(
        "CVG_ATTR_SK",
        "CVG_TYP_CD",
        "CVG_TYP_IND",
        "CVG_TYP_Priority",
        "CVG_EXPS_VAL"
    )
except Exception as e:
    logger.error(f"Failed EXP_Passthru projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Aggregator: AGG_RetainFirstValueOnly - FIRST CVG_EXPS_VAL per CVG_ATTR_SK
# -----------------------------------------------------------------------------
try:
    logger.info("Aggregating FIRST CVG_EXPS_VAL per CVG_ATTR_SK")
    df_AGG_RetainFirstValueOnly_eager_planck = (
        df_EXP_Passthru_peaceful_kepler
        .groupBy("CVG_ATTR_SK")
        .agg(first(col("CVG_EXPS_VAL"), ignorenulls=True).alias("CVG_EXPS_VAL"))
    )
except Exception as e:
    logger.error(f"Failed AGG_RetainFirstValueOnly aggregation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_PassThrough - compute rolled exposure (v_CVG_EXPS_VAL = ROUND(CVG_EXPS_VAL * 12))
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting required columns for EXP_PassThrough")
    df_EXP_PassThrough_gentle_heisenberg_sel = df_AGG_RetainFirstValueOnly_eager_planck.selectExpr(
        "CVG_ATTR_SK",
        "CVG_EXPS_VAL"
    )
except Exception as e:
    logger.error(f"Failed selecting columns for EXP_PassThrough: {e}", exc_info=True)
    raise

try:
    logger.info("Computing v_CVG_EXPS_VAL = ROUND(CVG_EXPS_VAL * 12)")
    df_EXP_PassThrough_gentle_heisenberg_v = df_EXP_PassThrough_gentle_heisenberg_sel.withColumn(
        "v_CVG_EXPS_VAL",
        spark_round((col("CVG_EXPS_VAL") * F.lit(12)))
    )
except Exception as e:
    logger.error(f"Failed computing v_CVG_EXPS_VAL in EXP_PassThrough: {e}", exc_info=True)
    raise

try:
    logger.info("Creating EXPS_VAL_ROLLED from v_CVG_EXPS_VAL and dropping intermediate column")
    df_EXP_PassThrough_gentle_heisenberg = (
        df_EXP_PassThrough_gentle_heisenberg_v
        .withColumn("EXPS_VAL_ROLLED", col("v_CVG_EXPS_VAL"))
        .drop("v_CVG_EXPS_VAL")
    )
except Exception as e:
    logger.error(f"Failed creating EXPS_VAL_ROLLED in EXP_PassThrough: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_Passthru_Tgt - prepare target row, inject NULL for missing NISS_APRM_DETL_SK
# -----------------------------------------------------------------------------
try:
    logger.info("Preparing target passthrough and injecting NULL for missing NISS_APRM_DETL_SK (lookup missing)")
    # Select EXPS_VAL_ROLLED then add NISS_APRM_DETL_SK as NULL (bigint) because the lookup was missing from export
    df_EXP_Passthru_Tgt_amazing_leibniz = (
        df_EXP_PassThrough_gentle_heisenberg
        .selectExpr("EXPS_VAL_ROLLED")
        .withColumn("NISS_APRM_DETL_SK", lit(None).cast("bigint"))
        .select("NISS_APRM_DETL_SK", "EXPS_VAL_ROLLED")
    )
except Exception as e:
    logger.error(f"Failed EXP_Passthru_Tgt preparation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Update Strategy: Upd_CVG_ATTR_SK - derive dd_op marker and drop REJECT rows
# -----------------------------------------------------------------------------
try:
    logger.info("Deriving Update Strategy marker dd_op for Upd_CVG_ATTR_SK (DD_UPDATE -> 'UPDATE')")
    df_Upd_CVG_ATTR_SK_modest_curie = (
        df_EXP_Passthru_Tgt_amazing_leibniz.withColumn("dd_op", lit("UPDATE"))
    )
    # remove any REJECT rows (none expected since all are UPDATE)
    df_Upd_CVG_ATTR_SK_modest_curie = df_Upd_CVG_ATTR_SK_modest_curie.filter(col("dd_op") != "REJECT")
except Exception as e:
    logger.error(f"Failed deriving dd_op in Upd_CVG_ATTR_SK: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output (UPDATE): FDR_LIB_WRK_BIRP_NISS_APRM_DETL - apply load-modify-store-back to Snowflake target
# -----------------------------------------------------------------------------
try:
    logger.info("Loading current full target WRK_BIRP_NISS_APRM_DETL from Snowflake for Update Strategy apply")
    df_target_existing = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading existing WRK_BIRP_NISS_APRM_DETL from Snowflake: {e}", exc_info=True)
    raise

try:
    logger.info("Computing changed keys from incoming Update Strategy rows")
    changed_keys_df = (
        df_Upd_CVG_ATTR_SK_modest_curie
        .filter(col("dd_op").isin("INSERT", "UPDATE", "DELETE"))
        .select("NISS_APRM_DETL_SK")
        .distinct()
    )
except Exception as e:
    logger.error(f"Failed computing changed keys for Update Strategy apply: {e}", exc_info=True)
    raise

try:
    logger.info("Anti-joining existing target to remove rows being updated/deleted")
    existing_minus_changed = df_target_existing.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")
except Exception as e:
    logger.error(f"Failed anti-joining existing target with changed keys: {e}", exc_info=True)
    raise

try:
    logger.info("Extracting INSERT/UPDATE rows from incoming payload to union back into the full target")
    rows_to_upsert = (
        df_Upd_CVG_ATTR_SK_modest_curie
        .filter(col("dd_op").isin("INSERT", "UPDATE"))
        .drop("dd_op")
    )
except Exception as e:
    logger.error(f"Failed extracting rows to upsert for Update Strategy apply: {e}", exc_info=True)
    raise

try:
    logger.info("Unioning remaining existing rows with upsert rows to produce the complete target dataset")
    df_combined_full = existing_minus_changed.unionByName(rows_to_upsert, allowMissingColumns=True)
except Exception as e:
    logger.error(f"Failed unioning rows for Update Strategy apply: {e}", exc_info=True)
    raise

try:
    logger.info("Writing combined full WRK_BIRP_NISS_APRM_DETL back to Snowflake (overwrite). NOTE: full-table overwrite may be expensive for large targets")
    # DataFrameWriter.jdbc expects properties dict for credentials
    df_combined_full.write.jdbc(
        url=SNOWFLAKE_URL,
        table="FDR.WRK_BIRP_NISS_APRM_DETL",
        mode="overwrite",
        properties={"user": SNOWFLAKE_USER, "password": SNOWFLAKE_PASSWORD},
    )
    # expose the final dataframe variable for lineage/consumers
    df_WRK_BIRP_NISS_APRM_DETL_modest_bohr = df_combined_full
except Exception as e:
    logger.error(f"Failed writing combined WRK_BIRP_NISS_APRM_DETL back to Snowflake: {e}", exc_info=True)
    raise



job.commit()
