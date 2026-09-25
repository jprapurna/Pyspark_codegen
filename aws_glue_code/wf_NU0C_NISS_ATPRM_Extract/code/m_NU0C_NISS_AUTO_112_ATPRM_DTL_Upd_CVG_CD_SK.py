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
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (try S3-first for staged WRK_ table, fall back to Glue Catalog)
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 as parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 (staged) into df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo")
except Exception as e:
    logger.warning("Failed to read WRK_BIRP_NISS_APRM_DETL from S3; falling back to Glue Catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=%s" % (GLUE_DATABASE, "WRK_BIRP_NISS_APRM_DETL"))
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed fallback read of WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# register temp view for use by the SQL override in the SQ node
try:
    logger.info("Registering temp view WRK_BIRP_NISS_APRM_DETL for staged source dataframe")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed creating temp view WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# This mapping's SQL Override references the staged table WRK_BIRP_NISS_APRM_DETL; run it via spark.sql against the temp view.
sql_query = f"""SELECT DISTINCT DETL.NISS_APRM_DETL_SK, DETL.CVG_ATTR_SK, Y.CVG_CD_ATTR_SK
FROM (
  SELECT DISTINCT CVG_ATTR_SK, CVG_TYP_CD, CVG_TYP_IND, CVG_CD_ATTR, ROW_NUMBER() OVER ( ORDER BY CVG_CD_ATTR) AS CVG_CD_ATTR_SK
  FROM (
    SELECT DISTINCT CVG_ATTR_SK, CVG_TYP_CD, CVG_TYP_IND,
      (CLNDR_YR || REG_PRD_YR || NISS_CMPNY_CD || NISS_ST_CD || GRGNG_ZIP_5 || NISS_TERR_CD || LINE_CD || ACCDNT_YR || NISS_CVG_CD || RTNG_ZNE_CD || TERM_ZNE_CD || NISS_CLASS_CD || NISS_ELIG_PNTS_CD || NISS_AGE_GRP_CD || NISS_CMMCL_IND_CD || NISS_EXCPN_CD || NISS_FGVNS_CD || NISS_PASSV_RESTRA_CD || NISS_DEFNS_DRVR_CRD_CD || NISS_ANTI_THFT_DVC_CD || NISS_DAY_TM_RUN_LAMPS_DISC_CD || NISS_PLCY_LMT_CD || NISS_DEDUC_CD || NISS_SSL_LIAB_CD || NISS_SUBLOB_CD || NISS_TYP_LOSS_CD || NISS_LIAB_OR_NO_FAULT_CD || NISS_ANNL_STMNT_LOB_CD || NISS_PD_LOSS || NISS_PD_ALLOC_ADJUS_EXPNS || NISS_OUTSTNDG_LOSS || NISS_NO_PD_CLMS || NISS_NO_OUTSTND_CLMS || RSVD_NISS_USE || NISS_RSVD_CMPNY_USE || NISS_MNFCTRS_MDL_YR || CVG_TYP_CD || CVG_TYP_IND) AS CVG_CD_ATTR
    FROM (
      SELECT DISTINCT
        CVG_ATTR_SK,
        CASE WHEN (LTRIM(RTRIM(CLNDR_YR)) IS NULL OR LTRIM(RTRIM(CLNDR_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CLNDR_YR)) END AS CLNDR_YR,
        CASE WHEN (LTRIM(RTRIM(REG_PRD_YR)) IS NULL  OR LTRIM(RTRIM(REG_PRD_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(REG_PRD_YR)) END AS REG_PRD_YR,
        CASE WHEN (LTRIM(RTRIM(NISS_CMPNY_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMPNY_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMPNY_CD)) END AS NISS_CMPNY_CD,
        CASE WHEN (LTRIM(RTRIM(NISS_ST_CD)) IS NULL OR LTRIM(RTRIM(NISS_ST_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ST_CD)) END AS NISS_ST_CD,
        CASE WHEN (LTRIM(RTRIM(GRGNG_ZIP_5)) IS NULL OR LTRIM(RTRIM(GRGNG_ZIP_5)) ='') THEN ' ' ELSE LTRIM(RTRIM(GRGNG_ZIP_5)) END AS GRGNG_ZIP_5,
        CASE WHEN (LTRIM(RTRIM(NISS_TERR_CD)) IS NULL OR LTRIM(RTRIM(NISS_TERR_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TERR_CD)) END AS NISS_TERR_CD,
        CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD,
        CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND,
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
        CASE WHEN NISS_PD_LOSS IS NULL THEN 0 ELSE NISS_PD_LOSS END AS NISS_PD_LOSS,
        CASE WHEN NISS_PD_ALLOC_ADJUS_EXPNS IS NULL THEN 0 ELSE NISS_PD_ALLOC_ADJUS_EXPNS END AS NISS_PD_ALLOC_ADJUS_EXPNS,
        CASE WHEN NISS_OUTSTNDG_LOSS IS NULL THEN 0 ELSE NISS_OUTSTNDG_LOSS END AS NISS_OUTSTNDG_LOSS,
        CASE WHEN NISS_NO_PD_CLMS IS NULL THEN 0 ELSE NISS_NO_PD_CLMS END AS NISS_NO_PD_CLMS,
        CASE WHEN NISS_NO_OUTSTND_CLMS IS NULL THEN 0 ELSE NISS_NO_OUTSTND_CLMS END AS NISS_NO_OUTSTND_CLMS,
        CASE WHEN (LTRIM(RTRIM(RSVD_NISS_USE)) IS NULL OR LTRIM(RTRIM(RSVD_NISS_USE)) ='') THEN 0 ELSE LTRIM(RTRIM(RSVD_NISS_USE)) END AS RSVD_NISS_USE,
        CASE WHEN (LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) IS NULL OR LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) END AS NISS_RSVD_CMPNY_USE,
        CASE WHEN NISS_MNFCTRS_MDL_YR IS NULL THEN 0 ELSE NISS_MNFCTRS_MDL_YR END AS NISS_MNFCTRS_MDL_YR
      FROM WRK_BIRP_NISS_APRM_DETL
      ORDER BY 
        CLNDR_YR,
        REG_PRD_YR,
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
    ) A
  ) X
) Y,
(SELECT NISS_APRM_DETL_SK, CVG_ATTR_SK, CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD, CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND FROM WRK_BIRP_NISS_APRM_DETL) DETL
WHERE Y.CVG_ATTR_SK = DETL.CVG_ATTR_SK
AND LTRIM(RTRIM(Y.CVG_TYP_CD)) = LTRIM(RTRIM(DETL.CVG_TYP_CD))
AND LTRIM(RTRIM(Y.CVG_TYP_IND)) = LTRIM(RTRIM(DETL.CVG_TYP_IND))
ORDER BY DETL.NISS_APRM_DETL_SK, DETL.CVG_ATTR_SK
"""

try:
    logger.info("Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql against staged temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_heisenberg = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# Expression: EXP_To_generate_CVG_CD_SK (pass-through projection of the three ports)
try:
    logger.info("Applying projection EXP_To_generate_CVG_CD_SK to expose NISS_APRM_DETL_SK, CVG_ATTR_SK, CVG_CD_ATTR_SK")
    df_EXP_To_generate_CVG_CD_SK_admiring_hawking = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_heisenberg.selectExpr(
        "NISS_APRM_DETL_SK",
        "CVG_ATTR_SK",
        "CVG_CD_ATTR_SK"
    )
except Exception as e:
    logger.error(f"Failed applying expression EXP_To_generate_CVG_CD_SK: {e}", exc_info=True)
    raise

# Update Strategy: Upd_CVG_CD_SK - derive dd_op, drop REJECT, and fully apply updates to Snowflake target via load-modify-store-back
try:
    logger.info("Deriving dd_op column for Upd_CVG_CD_SK (mapping config: DD_UPDATE -> mark rows as UPDATE)")
    df_with_dd = df_EXP_To_generate_CVG_CD_SK_admiring_hawking.withColumn("dd_op", lit('UPDATE'))
    # drop rejected rows if any (none expected for DD_UPDATE-only mapping)
    df_with_dd = df_with_dd.filter(col("dd_op") != 'REJECT')
except Exception as e:
    logger.error(f"Failed deriving dd_op for Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# Identify changed keys (rows marked UPDATE or DELETE) - primary key = NISS_APRM_DETL_SK
try:
    logger.info("Building changed keys dataframe for Upd_CVG_CD_SK")
    changed_keys_df = (
        df_with_dd
        .filter(col("dd_op").isin('UPDATE', 'DELETE'))
        .select("NISS_APRM_DETL_SK")
        .distinct()
    )
except Exception as e:
    logger.error(f"Failed building changed keys for Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# Read current full target table from Snowflake via JDBC
try:
    logger.info("Reading existing target table FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake for load-modify-store-back")
    df_existing_target = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading existing target FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake: {e}", exc_info=True)
    raise

# Remove existing rows that will be updated/deleted
try:
    logger.info("Anti-joining existing target to remove rows that will be updated/deleted")
    if changed_keys_df.rdd.isEmpty():
        # no changed keys - keep existing as-is
        remaining_existing = df_existing_target
    else:
        remaining_existing = df_existing_target.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")
except Exception as e:
    logger.error(f"Failed computing remaining_existing for Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# Prepare rows to insert/update (INSERT and UPDATE rows) - DELETE rows are excluded
try:
    logger.info("Preparing rows to upsert (INSERT/UPDATE) for Upd_CVG_CD_SK")
    rows_to_upsert = (
        df_with_dd
        .filter(col("dd_op").isin('INSERT', 'UPDATE'))
        .drop("dd_op")
    )
except Exception as e:
    logger.error(f"Failed preparing rows_to_upsert for Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# Union remaining_existing with rows_to_upsert to form the new full table
try:
    logger.info("Unioning remaining_existing with rows_to_upsert to form new full table for target")
    # allowMissingColumns=True so upsert rows that only contain the changed columns align with full target schema
    from functools import reduce
    if 'rows_to_upsert' in locals() and rows_to_upsert.rdd.isEmpty():
        new_full = remaining_existing
    else:
        new_full = remaining_existing.unionByName(rows_to_upsert, allowMissingColumns=True)
except Exception as e:
    logger.error(f"Failed constructing new full target dataframe for Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# Write the new full table back to Snowflake (overwrite) to apply updates in full within this job
try:
    logger.info("Writing new full table back to Snowflake target FDR.WRK_BIRP_NISS_APRM_DETL (overwrite) as part of Update Strategy apply")
    # Use DataFrameWriter.jdbc to ensure credentials are passed via properties
    new_full.write.jdbc(SNOWFLAKE_URL, "FDR.WRK_BIRP_NISS_APRM_DETL", mode='overwrite', properties={"user": SNOWFLAKE_USER, "password": SNOWFLAKE_PASSWORD})
except Exception as e:
    logger.error(f"Failed writing new full table back to Snowflake for FDR.WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# Assign the final combined dataframe to the Update Strategy node's df_name
try:
    logger.info("Assigning final applied dataframe to df_Upd_CVG_CD_SK_nostalgic_lovelace")
    df_Upd_CVG_CD_SK_nostalgic_lovelace = new_full
except Exception as e:
    logger.error(f"Failed assigning df_Upd_CVG_CD_SK_nostalgic_lovelace: {e}", exc_info=True)
    raise

# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> persist final full table to S3 as parquet for downstream reuse
# write intermediate WRK_ table as parquet to S3 (overwrite)
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
    df_WRK_BIRP_NISS_APRM_DETL_brave_euclid = df_Upd_CVG_CD_SK_nostalgic_lovelace
    df_WRK_BIRP_NISS_APRM_DETL_brave_euclid.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
