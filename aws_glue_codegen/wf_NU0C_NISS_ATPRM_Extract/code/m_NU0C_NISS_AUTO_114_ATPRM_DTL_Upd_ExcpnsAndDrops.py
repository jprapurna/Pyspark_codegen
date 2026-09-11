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


# Top-of-script placeholders for environment / mapping parameters
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
SOURCE_DATABASE = "REPLACE_WITH_GLUE_CATALOG_DATABASE"
PM_BAD_FILE_DIR = "REPLACE_WITH_PM_BAD_FILE_DIR"

# -----------------------------------------------------------------
# FDR_LIB_WRK_BIRP_NISS_APRM_DETL2 (Source) — S3-first read with fallback
# Project only NISS_APRM_DETL_SK
# -----------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_cool_curie = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    ).selectExpr("NISS_APRM_DETL_SK")
except Exception as e:
    logger.warning("Staged S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable; falling back to Glue Catalog/source read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL" % SOURCE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=SOURCE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_cool_curie = dyf.toDF().selectExpr("NISS_APRM_DETL_SK")
    except Exception as e2:
        logger.error(f"Failed fallback read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------
# Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (Source) — S3-first read with fallback
# Project the explicit list of fields into the dataframe
# -----------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL_2 from S3 (Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2)")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_sharp_hume = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_2/"
    ).selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_CMPNY_CD",
        "NISS_ST_CD",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "GRGNG_ZIP_5",
        "NISS_CLASS_CD",
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
        "NISS_LIAB_OR_NO_FAULT_CD",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "AGE",
        "ACCTNG_LOB",
        "NUM_OF_CARS_IN_HH"
    )
except Exception as e:
    logger.warning("Staged S3 path for WRK_BIRP_NISS_APRM_DETL_2 not found or unreadable; falling back to Glue Catalog/source read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL_2 from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL_2" % SOURCE_DATABASE)
        dyf2 = glueContext.create_dynamic_frame.from_catalog(database=SOURCE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL_2")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_sharp_hume = dyf2.toDF().selectExpr(
            "NISS_APRM_DETL_SK",
            "NISS_CMPNY_CD",
            "NISS_ST_CD",
            "NISS_CVG_CD",
            "NISS_TERR_CD",
            "GRGNG_ZIP_5",
            "NISS_CLASS_CD",
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
            "NISS_LIAB_OR_NO_FAULT_CD",
            "REC_EXCPN_IND",
            "REC_EXCPN_RSN_DESC",
            "AGE",
            "ACCTNG_LOB",
            "NUM_OF_CARS_IN_HH"
        )
    except Exception as e2:
        logger.error(f"Failed fallback read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------
# SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (Application Source Qualifier)
# SQL Override against FDR.WRK_BIRP_NISS_APRM_DETL — staged case: register upstream DF as temp view and run spark.sql
# The override projects only NISS_APRM_DETL_SK
# -----------------------------------------------------------------
sql_query = f"""SELECT DET.NISS_APRM_DETL_SK
FROM WRK_BIRP_NISS_APRM_DETL DET
WHERE DET.CVG_ATTR_SK IN (
    SELECT DETL.CVG_ATTR_SK
    FROM WRK_BIRP_NISS_APRM_DETL DETL
    WHERE DETL.REC_EXCPN_IND='Y'
    GROUP BY DETL.CVG_ATTR_SK
    HAVING SUM(DETL.TTL_WRITTN_PREM_AMT) = 0 AND SUM(DETL.EXPS_VAL_ROLLED) = 0
)
"""
try:
    logger.info("Running SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops via spark.sql against temp view WRK_BIRP_NISS_APRM_DETL")
    # upstream staged dataframe must be registered as the bare table name
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_cool_curie.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_affectionate_hawking = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops SQL override: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (Application Source Qualifier)
# Staged case: register the shortcut input dataframe as a temp view and run override via spark.sql
# -----------------------------------------------------------------
sql_query = f"""SELECT DETL.NISS_APRM_DETL_SK,
DETL.NISS_CMPNY_CD,
DETL.NISS_ST_CD,
DETL.NISS_CVG_CD,
DETL.NISS_TERR_CD,
DETL.GRGNG_ZIP_5,
DETL.NISS_CLASS_CD,
DETL.NISS_EXCPN_CD,
DETL.NISS_FGVNS_CD,
DETL.NISS_PASSV_RESTRA_CD,
DETL.NISS_DEFNS_DRVR_CRD_CD,
DETL.NISS_ANTI_THFT_DVC_CD,
DETL.NISS_DAY_TM_RUN_LAMPS_DISC_CD,
DETL.NISS_PLCY_LMT_CD,
DETL.NISS_DEDUC_CD,
DETL.NISS_SSL_LIAB_CD,
DETL.NISS_SUBLOB_CD,
DETL.NISS_LIAB_OR_NO_FAULT_CD,
DETL.REC_EXCPN_IND,
DETL.REC_EXCPN_RSN_DESC,
DETL.AGE,
DETL.ACCTNG_LOB,
DETL.NUM_OF_CARS_IN_HH
FROM WRK_BIRP_NISS_APRM_DETL DETL
"""
try:
    logger.info("Running SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 via spark.sql against temp view WRK_BIRP_NISS_APRM_DETL")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_sharp_hume.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_happy_feynman = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 SQL override: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# EXP_PassThrough_Src1: derive REC_DROP_IND='Y', REC_DROP_RSN_DESC='997' and pass through NISS_APRM_DETL_SK
# -----------------------------------------------------------------
try:
    logger.info("Transforming EXP_PassThrough_Src1: add REC_DROP_IND='Y' and REC_DROP_RSN_DESC='997'")
    df_EXP_PassThrough_Src1_blissful_fermat = df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_affectionate_hawking.selectExpr(
        "NISS_APRM_DETL_SK",
        "'Y' AS REC_DROP_IND",
        "'997' AS REC_DROP_RSN_DESC"
    )
except Exception as e:
    logger.error(f"Failed transforming EXP_PassThrough_Src1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# EXP_PassThrough_Src: pure passthrough projection enumerating every output column explicitly
# -----------------------------------------------------------------
try:
    logger.info("Projecting EXP_PassThrough_Src explicit passthrough columns")
    df_EXP_PassThrough_Src_clever_noether = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_happy_feynman.selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_CMPNY_CD",
        "NISS_ST_CD",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "GRGNG_ZIP_5",
        "NISS_CLASS_CD",
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
        "NISS_LIAB_OR_NO_FAULT_CD",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "AGE",
        "ACCTNG_LOB AS ACCNTG_LOB",
        "NUM_OF_CARS_IN_HH"
    )
except Exception as e:
    logger.error(f"Failed projecting EXP_PassThrough_Src: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Upd_RecDrop_Ind (Update Strategy)
# - mark all rows as UPDATE (DD_UPDATE), drop REJECT rows, then load-modify-store-back to
#   FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (S3 overwrite)
# -----------------------------------------------------------------
try:
    logger.info("Applying Update Strategy Upd_RecDrop_Ind: mark dd_op and prepare changed set")
    # add dd_op column via selectExpr to avoid extra imports
    df_with_dd = df_EXP_PassThrough_Src1_blissful_fermat.selectExpr("*", "'UPDATE' AS dd_op")
    # drop REJECT rows if any
    df_changed = df_with_dd.filter("dd_op != 'REJECT'")

    # Prepare for load-modify-store-back against s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops/
    target_path = f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops/"

    try:
        logger.info("Reading existing target for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops from %s" % target_path)
        existing_df = spark.read.parquet(target_path)
    except Exception:
        logger.warning("Existing target for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops not found; starting from empty dataframe")
        existing_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_changed.schema)

    # identify keys being changed
    changed_keys_df = df_changed.selectExpr("NISS_APRM_DETL_SK").distinct()
    # anti-join to drop rows from existing that are being updated/deleted
    existing_anti = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

    # rows to insert/update (DELETE rows would be excluded; here dd_op is UPDATE)
    rows_to_upsert = df_changed.filter("dd_op IN ('INSERT','UPDATE')").drop("dd_op")

    # union (allow missing columns — changed rows may only contain subset)
    combined = existing_anti.unionByName(rows_to_upsert, allowMissingColumns=True)

    # persist combined result back to target path (overwrite)
    try:
        logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops to S3 as parquet (overwrite)")
        combined.write.mode("overwrite").parquet(target_path)
    except Exception as e:
        logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops to S3: {e}", exc_info=True)
        raise

    # assign node output dataframe variable to the final combined result for downstream consumption
    df_Upd_RecDrop_Ind_adoring_lovelace = combined
except Exception as e:
    logger.error(f"Upd_RecDrop_Ind failed: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# EXP_Gen_Exceptions: compute many local reason fragments and derive REC_EXCPN_RSN_DESC and REC_EXCPN_IND
# Implemented as two selectExpr chains so aliases can be reused
# -----------------------------------------------------------------
try:
    logger.info("Computing EXP_Gen_Exceptions (derive REC_EXCPN_RSN_DESC and REC_EXCPN_IND)")
    # first step: compute individual reason fragments
    df_tmp_ex = df_EXP_PassThrough_Src_clever_noether.selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_CMPNY_CD",
        "NISS_ST_CD",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "GRGNG_ZIP_5",
        "NISS_CLASS_CD",
        "NISS_EXCPN_CD",
        "NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD",
        "NISS_LIAB_OR_NO_FAULT_CD",
        "NUM_OF_CARS_IN_HH",
        "ACCNTG_LOB",
        "AGE",
        "REC_EXCPN_IND AS i_REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC AS i_REC_EXCPN_RSN_DESC",
        "CASE WHEN substr(trim(NISS_CMPNY_CD),1,1) = '?' THEN 'NISS_CMPNY_CD;' ELSE '' END AS NISS_CMPNY_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_ST_CD),1,1) = '?' THEN 'NISS_ST_CD;' ELSE '' END AS NISS_ST_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_CVG_CD),1,1) = '?' THEN 'NISS_CVG_CD;' WHEN i_REC_EXCPN_IND = 'Y' AND substr(trim(NISS_CVG_CD),1,1) = 'E' THEN 'Dflt NISS_CVG_CD;' ELSE '' END AS NISS_CVG_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_TERR_CD),1,1) = '?' THEN 'NISS_TERR_CD;' ELSE '' END AS NISS_TERR_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(GRGNG_ZIP_5),1,1) = '?' OR GRGNG_ZIP_5 = '00000' THEN 'GRGNG_ZIP;' ELSE '' END AS GRGNG_ZIP_5_RecExcpnRsn",
        "CASE WHEN i_REC_EXCPN_IND = 'Y' AND instr(i_REC_EXCPN_RSN_DESC,'DEFAULT_CLASS_CD') != 0 THEN 'DEFAULT_CLASS_CD;' WHEN substr(trim(NISS_CLASS_CD),1,1) = '?' THEN '106:DEFAULT_CLASS_CD;' WHEN substr(trim(NISS_CLASS_CD),1,1) = 'E' THEN '118:DEFAULT_CLASS_CD;' ELSE '' END AS NISS_CLASS_CD_RecExcpnRsn",
        "CASE WHEN instr(i_REC_EXCPN_RSN_DESC,'assigned default zip code') != 0 THEN 'assigned default zip code;' ELSE '' END AS NISS_DFLT_ZIP_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_EXCPN_CD),1,1) = '?' THEN 'NISS_EXCPN_CD;' ELSE '' END AS NISS_EXCPN_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_DEDUC_CD),1,1) = '?' THEN 'DEDUC_CD;' WHEN substr(trim(NISS_DEDUC_CD),1,1) = 'E' THEN 'DFLT_DEDUC_CD;' ELSE '' END AS NISS_DEDUC_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_SSL_LIAB_CD),1,1) = '?' THEN 'SSL_LIAB_CD;' ELSE '' END AS NISS_SSL_LIAB_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_SUBLOB_CD),1,1) = '?' THEN 'SUBLOB_CD;' ELSE '' END AS NISS_SUBLOB_CD_RecExcpnRsn",
        "CASE WHEN substr(trim(NISS_LIAB_OR_NO_FAULT_CD),1,1) = '?' THEN 'IAB_OR_NO_FAULT_CD;' ELSE '' END AS NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn",
        "CASE WHEN NISS_ST_CD = '09' AND NUM_OF_CARS_IN_HH IS NULL THEN 'BLANK VEHICLE COUNT;' ELSE '' END AS NUM_OF_CARS_IN_HH_RecExcpnRsn",
        "CASE WHEN trim(ACCNTG_LOB) = '' OR ACCNTG_LOB IS NULL OR ACCNTG_LOB = ' ' THEN 'Blank ACCNTG_LOB;' ELSE '' END AS ACCNTG_LOB_RecExcpnRsn",
        "CASE WHEN cast(AGE as int) > 120 OR cast(AGE as int) < 14 OR trim(AGE) = '' THEN 'Invalid Rated Driver Age;' ELSE '' END AS AGE_RecExcpnRsn"
    )

    # second step: concatenate into v_REC_EXCPN_RSN_DESC and compute REC_EXCPN_IND
    df_EXP_Gen_Exceptions_blissful_babbage = df_tmp_ex.selectExpr(
        "NISS_APRM_DETL_SK",
        "(NISS_DFLT_ZIP_RecExcpnRsn || NISS_CMPNY_CD_RecExcpnRsn || NISS_ST_CD_RecExcpnRsn || NISS_CVG_CD_RecExcpnRsn || NISS_TERR_CD_RecExcpnRsn || GRGNG_ZIP_5_RecExcpnRsn || NISS_CLASS_CD_RecExcpnRsn || NISS_EXCPN_CD_RecExcpnRsn || NISS_FGVNS_CD_RecExcpnRsn || NISS_PASSV_RESTRA_CD_RecExcpnRsn || NISS_DEDUC_CD_RecExcpnRsn || NISS_ANTI_THFT_DVC_CD_RecExcpnRsn || NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn || NISS_PLCY_LMT_CD_RecExcpnRsn || NISS_DEDUC_CD_RecExcpnRsn || NISS_SSL_LIAB_CD_RecExcpnRsn || NISS_SUBLOB_CD_RecExcpnRsn || NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn || AGE_RecExcpnRsn || NUM_OF_CARS_IN_HH_RecExcpnRsn || ACCNTG_LOB_RecExcpnRsn) AS REC_EXCPN_RSN_DESC",
        "CASE WHEN trim(i_REC_EXCPN_IND) = '' AND (NISS_DFLT_ZIP_RecExcpnRsn || NISS_CMPNY_CD_RecExcpnRsn || NISS_ST_CD_RecExcpnRsn || NISS_CVG_CD_RecExcpnRsn || NISS_TERR_CD_RecExcpnRsn || GRGNG_ZIP_5_RecExcpnRsn || NISS_CLASS_CD_RecExcpnRsn || NISS_EXCPN_CD_RecExcpnRsn || NISS_FGVNS_CD_RecExcpnRsn || NISS_PASSV_RESTRA_CD_RecExcpnRsn || NISS_DEDUC_CD_RecExcpnRsn || NISS_ANTI_THFT_DVC_CD_RecExcpnRsn || NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn || NISS_PLCY_LMT_CD_RecExcpnRsn || NISS_DEDUC_CD_RecExcpnRsn || NISS_SSL_LIAB_CD_RecExcpnRsn || NISS_SUBLOB_CD_RecExcpnRsn || NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn || AGE_RecExcpnRsn || NUM_OF_CARS_IN_HH_RecExcpnRsn || ACCNTG_LOB_RecExcpnRsn) = '' THEN '' ELSE 'Y' END AS REC_EXCPN_IND"
    )
except Exception as e:
    logger.error(f"Failed computing EXP_Gen_Exceptions: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# EXP_Gen_RecDrops: compute v_REC_DROP_RSN_DESC and REC_DROP_IND
# Use two-step selectExpr chain for alias reuse
# -----------------------------------------------------------------
try:
    logger.info("Computing EXP_Gen_RecDrops (derive REC_DROP_RSN_DESC and REC_DROP_IND)")
    df_tmp_rd = df_EXP_PassThrough_Src_clever_noether.selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_CVG_CD",
        "NISS_ST_CD",
        "NISS_CLASS_CD",
        "NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD",
        "ACCNTG_LOB",
        "NUM_OF_CARS_IN_HH",
        "CASE WHEN substr(trim(NISS_CLASS_CD),1,1) = '?' THEN 'INVALID CLASS_CD;' ELSE '' END AS NISS_CLASS_CD_RecDropRsn",
        "CASE WHEN NISS_CVG_CD IN ('na','n/a') OR substr(trim(NISS_CVG_CD),1,1) = '?' THEN '999;' ELSE '' END AS NISS_CVG_CD_RecDropRsn",
        "CASE WHEN NISS_PLCY_LMT_CD IN ('na','??') THEN '998;' ELSE '' END AS NISS_PLCY_LMT_CD_RecDropRsn",
        "CASE WHEN NISS_DEDUC_CD = '??' THEN 'INVALID DEDUC CD;' ELSE '' END AS NISS_DEDUC_CD_RecDropRsn",
        "CASE WHEN trim(ACCNTG_LOB) = '' OR ACCNTG_LOB IS NULL OR ACCNTG_LOB = ' ' THEN 'Blank ACCNTG_LOB;' ELSE '' END AS ACCNTG_LOB_RecDropRsn",
        "CASE WHEN NISS_ST_CD = '??' THEN 'INVALID ST_CD;' ELSE '' END AS NISS_ST_CD_RecDropRsn",
        "CASE WHEN NISS_ST_CD = '09' AND NUM_OF_CARS_IN_HH IS NULL THEN 'Blank Vehicle Count;' ELSE '' END AS NO_OF_CARS_HH_RecDropRsn"
    )

    df_EXP_Gen_RecDrops_romantic_schrodinger = df_tmp_rd.selectExpr(
        "NISS_APRM_DETL_SK",
        "(NISS_CVG_CD_RecDropRsn || NISS_PLCY_LMT_CD_RecDropRsn || NISS_ST_CD_RecDropRsn || NISS_CLASS_CD_RecDropRsn || NISS_DEDUC_CD_RecDropRsn || ACCNTG_LOB_RecDropRsn || NO_OF_CARS_HH_RecDropRsn) AS REC_DROP_RSN_DESC",
        "CASE WHEN (NISS_CVG_CD_RecDropRsn || NISS_PLCY_LMT_CD_RecDropRsn || NISS_ST_CD_RecDropRsn || NISS_CLASS_CD_RecDropRsn || NISS_DEDUC_CD_RecDropRsn || ACCNTG_LOB_RecDropRsn || NO_OF_CARS_HH_RecDropRsn) = '' THEN '' ELSE 'Y' END AS REC_DROP_IND"
    )
except Exception as e:
    logger.error(f"Failed computing EXP_Gen_RecDrops: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# EXP_Dummy: join three upstream streams on NISS_APRM_DETL_SK and project the combined columns
# - base: df_EXP_PassThrough_Src_clever_noether
# - left join REC_DROP derivations and REC_EXCPN derivations
# -----------------------------------------------------------------
try:
    logger.info("Joining streams in EXP_Dummy to assemble REC_DROP and REC_EXCPN indicators and reasons")
    df_base = df_EXP_PassThrough_Src_clever_noether.alias("base")
    df_rd = df_EXP_Gen_RecDrops_romantic_schrodinger.alias("rd")
    df_ex = df_EXP_Gen_Exceptions_blissful_babbage.alias("ex")

    # ensure the recdrops and exceptions have the join key; they do per earlier transformations
    joined = df_base.join(df_rd, on=["NISS_APRM_DETL_SK"], how="left").join(df_ex, on=["NISS_APRM_DETL_SK"], how="left")

    # project final five columns using coalesce to prefer recdrops/excpn outputs where present
    df_EXP_Dummy_dazzling_noether = joined.selectExpr(
        "NISS_APRM_DETL_SK",
        "coalesce(rd.REC_DROP_IND, base.REC_DROP_IND, '') AS REC_DROP_IND",
        "coalesce(rd.REC_DROP_RSN_DESC, '') AS REC_DROP_RSN_DESC",
        "coalesce(ex.REC_EXCPN_IND, '') AS REC_EXCPN_IND",
        "coalesce(ex.REC_EXCPN_RSN_DESC, '') AS REC_EXCPN_RSN_DESC"
    )
except Exception as e:
    logger.error(f"Failed building EXP_Dummy: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# FIL_OnlyExceptions_or_Drops: filter to only rows where REC_DROP_IND='Y' OR REC_EXCPN_IND='Y'
# Project explicit five columns
# -----------------------------------------------------------------
try:
    logger.info("Filtering only exceptions or drops in FIL_OnlyExceptions_or_Drops")
    df_FIL_OnlyExceptions_or_Drops_nostalgic_boltzmann = (
        df_EXP_Dummy_dazzling_noether.filter("REC_DROP_IND = 'Y' OR REC_EXCPN_IND = 'Y'")
        .selectExpr(
            "REC_DROP_RSN_DESC",
            "NISS_APRM_DETL_SK",
            "REC_DROP_IND",
            "REC_EXCPN_IND",
            "REC_EXCPN_RSN_DESC"
        )
    )
except Exception as e:
    logger.error(f"Failed filtering FIL_OnlyExceptions_or_Drops: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Upd_CVG_CD_SK (Update Strategy)
# - mark dd_op as UPDATE, drop REJECT rows, apply load-modify-store-back to
#   FDR_LIB_WRK_BIRP_NISS_APRM_DETL (S3 overwrite)
# -----------------------------------------------------------------
try:
    logger.info("Applying Update Strategy Upd_CVG_CD_SK: mark dd_op and prepare changed set")
    df_with_dd2 = df_FIL_OnlyExceptions_or_Drops_nostalgic_boltzmann.selectExpr("*", "'UPDATE' AS dd_op")
    df_changed2 = df_with_dd2.filter("dd_op != 'REJECT'")

    target_path2 = f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/"

    try:
        logger.info("Reading existing target for FDR_LIB_WRK_BIRP_NISS_APRM_DETL from %s" % target_path2)
        existing_df2 = spark.read.parquet(target_path2)
    except Exception:
        logger.warning("Existing target for FDR_LIB_WRK_BIRP_NISS_APRM_DETL not found; starting from empty dataframe")
        existing_df2 = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_changed2.schema)

    changed_keys_df2 = df_changed2.selectExpr("NISS_APRM_DETL_SK").distinct()
    existing_anti2 = existing_df2.join(changed_keys_df2, on=["NISS_APRM_DETL_SK"], how="left_anti")

    rows_to_upsert2 = df_changed2.filter("dd_op IN ('INSERT','UPDATE')").drop("dd_op")

    combined2 = existing_anti2.unionByName(rows_to_upsert2, allowMissingColumns=True)

    try:
        logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
        combined2.write.mode("overwrite").parquet(target_path2)
    except Exception as e:
        logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
        raise

    df_Upd_CVG_CD_SK_awesome_curie = combined2
except Exception as e:
    logger.error(f"Upd_CVG_CD_SK failed: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (Output)
# Final target write for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: write incoming dataframe as parquet (overwrite)
# The mapping's Update Strategy already performed the load-modify-store-back; emit the explicit Output write as well (idempotent overwrite)
# -----------------------------------------------------------------
try:
    logger.info("Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops to S3 as parquet (overwrite)")
    # Use the dataframe produced by the Update Strategy
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_daring_ramanujan = df_Upd_RecDrop_Ind_adoring_lovelace
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_daring_ramanujan.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops/"
    )
except Exception as e:
    logger.error(f"Failed writing Output FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Output)
# Final write for the FDR_LIB_WRK_BIRP_NISS_APRM_DETL target: write the incoming dataframe as Parquet (overwrite)
# -----------------------------------------------------------------
try:
    logger.info("Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_hume = df_Upd_CVG_CD_SK_awesome_curie
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_hume.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.error(f"Failed writing Output FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
