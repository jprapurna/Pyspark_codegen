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

S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
REPLACE_WITH_GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL - try S3-first for WRK_ intermediate
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 parquet successfully")
except Exception as e:
    logger.warning(
        "Failed to read WRK_BIRP_NISS_APRM_DETL from s3 - falling back to Glue Catalog read: %s", e
    )
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=REPLACE_WITH_GLUE_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL",
        )
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman = dyf.toDF()
    except Exception as e2:
        logger.error(
            f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}",
            exc_info=True,
        )
        raise

# Register the staged source as a temp view for the SQL override below
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Registered WRK_BIRP_NISS_APRM_DETL temp view from staged dataframe")
except Exception as e:
    logger.error(f"Failed creating temp view for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: SQL Override executed against the staged temp view
sql_query = f"""SELECT NISS_APRM_DETL_SK,
(CLNDR_YR || CALL_YR || NISS_CMPNY_CD || NISS_ST_CD || GRGNG_ZIP_5 || NISS_TERR_CD || LINE_CD || ACCDNT_YR || NISS_CVG_CD || RTNG_ZNE_CD || TERM_ZNE_CD || NISS_CLASS_CD || NISS_ELIG_PNTS_CD || NISS_AGE_GRP_CD || NISS_CMMCL_IND_CD || NISS_EXCPN_CD || NISS_FGVNS_CD || NISS_PASSV_RESTRA_CD || NISS_DEFNS_DRVR_CRD_CD || NISS_ANTI_THFT_DVC_CD || NISS_DAY_TM_RUN_LAMPS_DISC_CD || NISS_PLCY_LMT_CD || NISS_DEDUC_CD || NISS_SSL_LIAB_CD || NISS_SUBLOB_CD || NISS_TYP_LOSS_CD || NISS_LIAB_OR_NO_FAULT_CD || NISS_ANNL_STMNT_LOB_CD || NISS_PD_LOSS || NISS_PD_ALLOC_ADJUS_EXPNS || NISS_OUTSTNDG_LOSS || NISS_NO_PD_CLMS || NISS_NO_OUTSTND_CLMS || RSVD_NISS_USE || NISS_RSVD_CMPNY_USE || NISS_MNFCTRS_MDL_YR) as CVG_ATTR
FROM (

SELECT NISS_APRM_DETL_SK,
CASE WHEN (LTRIM(RTRIM(CLNDR_YR)) IS NULL OR  LTRIM(RTRIM(CLNDR_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CLNDR_YR)) END AS CLNDR_YR,
CASE WHEN (LTRIM(RTRIM(CALL_YR)) IS NULL  OR LTRIM(RTRIM(CALL_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CALL_YR)) END AS CALL_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CMPNY_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMPNY_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMPNY_CD)) END AS NISS_CMPNY_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ST_CD)) IS NULL OR LTRIM(RTRIM(NISS_ST_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ST_CD)) END AS NISS_ST_CD,
CASE WHEN (LTRIM(RTRIM(GRGNG_ZIP_5)) IS NULL OR LTRIM(RTRIM(GRGNG_ZIP_5)) ='') THEN ' ' ELSE LTRIM(RTRIM(GRGNG_ZIP_5)) END AS GRGNG_ZIP_5,
CASE WHEN (LTRIM(RTRIM(NISS_TERR_CD)) IS NULL OR LTRIM(RTRIM(NISS_TERR_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TERR_CD)) END AS NISS_TERR_CD,
CASE WHEN (LTRIM(RTRIM(LINE_CD)) IS NULL OR LTRIM(RTRIM(LINE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(LINE_CD)) END AS LINE_CD,
CASE WHEN (LTRIM(RTRIM(ACCDNT_YR)) IS NULL OR LTRIM(RTRIM(ACCDNT_YR)) ='') THEN ' ' ELSE LTRIM(RTRIM(ACCDNT_YR)) END AS ACCDNT_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CVG_CD)) IS NULL  OR LTRIM(RTRIM(NISS_CVG_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CVG_CD)) END AS NISS_CVG_CD,
CASE WHEN (LTRIM(RTRIM(RTNG_ZNE_CD)) IS NULL OR LTRIM(RTRIM(RTNG_ZNE_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(RTNG_ZNE_CD)) END AS RTNG_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(TERM_ZNE_CD)) IS NULL OR LTRIM(RTRIM(TERM_ZNE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(TERM_ZNE_CD)) END AS TERM_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CLASS_CD)) IS NULL OR LTRIM(RTRIM(NISS_CLASS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CLASS_CD)) END AS NISS_CLASS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) IS NULL OR LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) END AS NISS_ELIG_PNTS_CD,
TO_CHAR(CASE WHEN NISS_AGE_GRP_CD IS NULL THEN 0 ELSE NISS_AGE_GRP_CD END) AS NISS_AGE_GRP_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CMMCL_IND_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMMCL_IND_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMMCL_IND_CD)) END AS NISS_CMMCL_IND_CD,
CASE WHEN (LTRIM(RTRIM(NISS_EXCPN_CD)) IS NULL OR LTRIM(RTRIM(NISS_EXCPN_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_EXCPN_CD)) END AS NISS_EXCPN_CD,
CASE WHEN (LTRIM(RTRIM(NISS_FGVNS_CD)) IS NULL OR LTRIM(RTRIM(NISS_FGVNS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_FGVNS_CD)) END AS NISS_FGVNS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) IS NULL OR LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) END AS NISS_PASSV_RESTRA_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) END AS NISS_DEFNS_DRVR_CRD_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) END AS NISS_ANTI_THFT_DVC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) END AS NISS_DAY_TM_RUN_LAMPS_DISC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PLCY_LMT_CD)) IS NULL OR LTRIM(RTRIM(NISS_PLCY_LMT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PLCY_LMT_CD)) END AS NISS_PLCY_LMT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEDUC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEDUC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEDUC_CD)) END AS NISS_DEDUC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SSL_LIAB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SSL_LIAB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SSL_LIAB_CD)) END AS NISS_SSL_LIAB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SUBLOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SUBLOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SUBLOB_CD)) END AS NISS_SUBLOB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_TYP_LOSS_CD)) IS NULL OR LTRIM(RTRIM(NISS_TYP_LOSS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TYP_LOSS_CD)) END AS NISS_TYP_LOSS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) IS NULL OR LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) END AS NISS_LIAB_OR_NO_FAULT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) END AS NISS_ANNL_STMNT_LOB_CD,
TO_CHAR(CASE WHEN NISS_PD_LOSS IS NULL THEN 0 ELSE NISS_PD_LOSS END) AS NISS_PD_LOSS,
TO_CHAR(CASE WHEN NISS_PD_ALLOC_ADJUS_EXPNS IS NULL THEN 0 ELSE NISS_PD_ALLOC_ADJUS_EXPNS END) AS NISS_PD_ALLOC_ADJUS_EXPNS,
TO_CHAR(CASE WHEN NISS_OUTSTNDG_LOSS IS NULL THEN 0 ELSE NISS_OUTSTNDG_LOSS END) AS NISS_OUTSTNDG_LOSS,
TO_CHAR(CASE WHEN NISS_NO_PD_CLMS IS NULL THEN 0 ELSE NISS_NO_PD_CLMS END) AS NISS_NO_PD_CLMS,
TO_CHAR(CASE WHEN NISS_NO_OUTSTND_CLMS IS NULL THEN 0 ELSE NISS_NO_OUTSTND_CLMS END) AS NISS_NO_OUTSTND_CLMS,
CASE WHEN (LTRIM(RTRIM(RSVD_NISS_USE)) IS NULL OR LTRIM(RTRIM(RSVD_NISS_USE)) ='') THEN 0 ELSE LTRIM(RTRIM(RSVD_NISS_USE)) END AS RSVD_NISS_USE,
CASE WHEN (LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) IS NULL OR LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) END AS NISS_RSVD_CMPNY_USE,
TO_CHAR(CASE WHEN NISS_MNFCTRS_MDL_YR IS NULL THEN 0 ELSE NISS_MNFCTRS_MDL_YR END) AS NISS_MNFCTRS_MDL_YR

FROM WRK_BIRP_NISS_APRM_DETL
ORDER BY
NISS_APRM_DETL_SK,
CLNDR_YR,
CALL_YR,
NISS_CMPNY_CD,
NISS_ST_CD,
GRGNG_ZIP_5,
NISS_TERR_CD,
LINE_CD,
ACCDNT_YR,
NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
NISS_CLASS_CD,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR
"""

try:
    logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override against staged temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_mendel = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL override: {e}", exc_info=True)
    raise

# EXP_Gen_CvgAttrCheckSum: compute MD5 checksum of CVG_ATTR and pass through NISS_APRM_DETL_SK
try:
    logger.info("Computing CVG_ATTR_CHCKSUM via MD5(CVG_ATTR)")
    df_EXP_Gen_CvgAttrCheckSum_cool_heisenberg = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_mendel.selectExpr(
        "NISS_APRM_DETL_SK",
        "md5(CVG_ATTR) AS CVG_ATTR_CHCKSUM"
    )
except Exception as e:
    logger.error(f"Failed computing CVG_ATTR_CHCKSUM: {e}", exc_info=True)
    raise

# SRT_OrderByChkSum: deterministic ordering by CVG_ATTR_CHCKSUM asc, NISS_APRM_DETL_SK asc
try:
    logger.info("Sorting by CVG_ATTR_CHCKSUM, NISS_APRM_DETL_SK")
    df_SRT_OrderByChkSum_sharp_curie = df_EXP_Gen_CvgAttrCheckSum_cool_heisenberg.orderBy(
        ["CVG_ATTR_CHCKSUM", "NISS_APRM_DETL_SK"], ascending=[True, True]
    )
except Exception as e:
    logger.error(f"Failed sorting rows: {e}", exc_info=True)
    raise

# EXP_Gen_CvgAttrSK: detect changes via lag and produce cumulative surrogate CVG_ATTR_SK starting at 1
try:
    logger.info("Generating CVG_ATTR_SK using lag() and cumulative sum over ordered window")
    w_order = Window.orderBy(F.col("CVG_ATTR_CHCKSUM"), F.col("NISS_APRM_DETL_SK"))
    # previous checksum
    df_tmp = df_SRT_OrderByChkSum_sharp_curie.select("NISS_APRM_DETL_SK", "CVG_ATTR_CHCKSUM")
    df_tmp = df_tmp.withColumn("_prev_csum", F.lag(F.col("CVG_ATTR_CHCKSUM")).over(w_order))
    # NewVal indicator: 1 when previous is null (first row) OR different from current, else 0
    df_tmp = df_tmp.withColumn(
        "_newval",
        F.when(F.col("_prev_csum").isNull() | (F.col("_prev_csum") != F.col("CVG_ATTR_CHCKSUM")), F.lit(1)).otherwise(F.lit(0))
    )
    # cumulative sum to generate surrogate starting at 1 for the first observed group
    w_cum = Window.orderBy(F.col("CVG_ATTR_CHCKSUM"), F.col("NISS_APRM_DETL_SK")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_tmp = df_tmp.withColumn("CVG_ATTR_SK", F.sum(F.col("_newval")).over(w_cum))
    # ensure CVG_ATTR_SK is bigint (cast)
    df_EXP_Gen_CvgAttrSK_cool_babbage = df_tmp.select(
        F.col("NISS_APRM_DETL_SK"),
        F.col("CVG_ATTR_CHCKSUM"),
        F.col("CVG_ATTR_SK").cast("long")
    )
except Exception as e:
    logger.error(f"Failed generating CVG_ATTR_SK: {e}", exc_info=True)
    raise

# Upd_CVG_ATTR_SK: mark dd_op = 'UPDATE' for all rows, drop REJECT (none expected), then apply load-modify-store-back against FDR_LIB_WRK_BIRP_NISS_APRM_DETL
try:
    logger.info("Applying Update Strategy: marking rows as UPDATE and preparing apply")
    df_Upd_CVG_ATTR_SK_hopeful_einstein = df_EXP_Gen_CvgAttrSK_cool_babbage.withColumn("dd_op", F.lit("UPDATE"))
    # drop REJECT rows if any
    df_Upd_CVG_ATTR_SK_hopeful_einstein = df_Upd_CVG_ATTR_SK_hopeful_einstein.filter(F.col("dd_op") != "REJECT")
except Exception as e:
    logger.error(f"Failed preparing Update Strategy marker column: {e}", exc_info=True)
    raise

# load current target table (prefer S3 parquet snapshot), then anti-join out changed keys and union in updated rows
target_s3_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
target_is_s3 = False
try:
    logger.info("Attempting to read existing target WRK_BIRP_NISS_APRM_DETL from S3 for update-apply")
    existing_target_df = spark.read.parquet(target_s3_path)
    target_is_s3 = True
    logger.info("Read existing target from S3 successfully")
except Exception as e:
    logger.warning("Failed to read existing target from S3, falling back to Snowflake JDBC read: %s", e)
    try:
        logger.info("Reading existing target FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake via JDBC")
        existing_target_df = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
            .load()
        )
        target_is_s3 = False
    except Exception as e2:
        logger.error(f"Failed reading existing target from Snowflake: {e2}", exc_info=True)
        raise

try:
    logger.info("Computing keys of changed rows for anti-join")
    changed_keys_df = df_Upd_CVG_ATTR_SK_hopeful_einstein.select("NISS_APRM_DETL_SK").distinct()
    # remove rows being updated/deleted from existing target
    surviving_df = existing_target_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")
    # only keep INSERT/UPDATE rows to union back (we have only UPDATE here)
    rows_to_upsert = df_Upd_CVG_ATTR_SK_hopeful_einstein.filter(F.col("dd_op").isin(["INSERT", "UPDATE"]))
    # align schemas: unionByName allowing missing columns
    from functools import reduce
    combined_df = surviving_df.unionByName(rows_to_upsert, allowMissingColumns=True)
except Exception as e:
    logger.error(f"Failed during load-modify steps for Update Strategy: {e}", exc_info=True)
    raise

# write back the full combined result to the same physical target
try:
    if target_is_s3:
        logger.info("Writing combined target dataframe back to S3 parquet (overwrite)")
        combined_df.write.mode("overwrite").parquet(target_s3_path)
    else:
        logger.info("Writing combined target dataframe back to Snowflake via JDBC (overwrite)")
        # Use JDBC write properties for Snowflake
        combined_df.write.format("jdbc").option("url", SNOWFLAKE_URL).option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL").option("user", SNOWFLAKE_USER).option("password", SNOWFLAKE_PASSWORD).mode("overwrite").save()
except Exception as e:
    logger.error(f"Failed writing applied updates back to target: {e}", exc_info=True)
    raise

# Output node: assign and write parquet snapshot for downstream reuse
try:
    logger.info("Assigning final output dataframe for FDR_LIB_WRK_BIRP_NISS_APRM_DETL and writing parquet snapshot to S3 (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_pasteur = df_Upd_CVG_ATTR_SK_hopeful_einstein
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_pasteur.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL snapshot to S3: {e}", exc_info=True)
    raise



job.commit()
