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

from pyspark.sql.functions import lit

# Top-of-script placeholder constants (replace before running)
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
FILTER_COND_FNL = "REPLACE_WITH_FILTER_COND_FNL_VALUE"
SOURCE_DATABASE = "REPLACE_WITH_SOURCE_DATABASE"
SOURCE_TABLE = "REPLACE_WITH_SOURCE_TABLE"

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL -> df_WRK_BIRP_NISS_APRM_DETL_nice_darwin
# Try S3-first for workflow-staged parquet; on failure fall back to Glue Catalog read
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    df_WRK_BIRP_NISS_APRM_DETL_nice_darwin = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from staged S3 parquet")
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not available on S3; falling back to Glue Catalog read: %s", e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog database=%s table=%s", SOURCE_DATABASE, SOURCE_TABLE)
        dyf = glueContext.create_dynamic_frame_from_catalog(database=SOURCE_DATABASE, table_name=SOURCE_TABLE)
        df_WRK_BIRP_NISS_APRM_DETL_nice_darwin = dyf.toDF()
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# Staged case: register the upstream staged DF as a temp view and run the SQL override
# -----------------------------------------------------------------------------
try:
    logger.info("Registering staged DF as temp view 'WRK_BIRP_NISS_APRM_DETL' for SQ override and executing SQL override")
    df_WRK_BIRP_NISS_APRM_DETL_nice_darwin.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT
    CLNDR_YR,
    CALL_YR,
    NISS_CMPNY_CD,
    ST_NM,
    ST_CD,
    ST_ABBR,
    NISS_ST_CD,
    LINE_CD,
    ACCDNT_YR,
    CASE
      WHEN NISS_CVG_CD = 'E16' THEN '616'
      WHEN NISS_CVG_CD = 'E36' THEN '636'
      WHEN NISS_CVG_CD = 'E56' THEN '656'
      WHEN NISS_CVG_CD = 'E76' THEN '676'
      WHEN NISS_CVG_CD = 'E85' THEN '085'
      WHEN NISS_CVG_CD = 'E14' THEN '614'
      WHEN NISS_CVG_CD = 'E81' THEN '081'
      WHEN NISS_CVG_CD = 'E01' THEN '001'
      WHEN NISS_CVG_CD = 'E34' THEN '034'
      WHEN NISS_CVG_CD = 'E33' THEN '033'
      WHEN NISS_CVG_CD = 'E71' THEN '071'
      WHEN NISS_CVG_CD = 'E77' THEN '077'
      WHEN NISS_CVG_CD = 'E55' THEN '055'
      WHEN NISS_CVG_CD = 'E33' THEN '733'
      WHEN NISS_CVG_CD = 'E99' THEN '099'
      ELSE NISS_CVG_CD
    END AS NISS_CVG_CD,
    NISS_TERR_CD,
    RTNG_ZNE_CD,
    TERM_ZNE_CD,
    GRGNG_ZIP_5,
    CASE
      WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1202' THEN '120200'
      WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1212' THEN '121200'
      WHEN SUBSTR(NISS_CLASS_CD,1,6)= 'E88712' THEN '887120'
      WHEN LEFT(NISS_CLASS_CD,4)= 'E202' THEN '1202' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E600' THEN '1600' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E602' THEN '1602' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E620' THEN '1620' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E622' THEN '1622' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E630' THEN '1630' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E632' THEN '1632' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E610' THEN '1610' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E612' THEN '1612' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E696' THEN '1696' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E698' THEN '1698' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E124' THEN '8124' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E554' THEN '8554' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E214' THEN '8214' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E211' THEN '8211' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E611' THEN '8611' || RIGHT(NISS_CLASS_CD,2)
      WHEN LEFT(NISS_CLASS_CD,4)= 'E160' THEN '1600' || RIGHT(NISS_CLASS_CD,2)
      ELSE NISS_CLASS_CD
    END AS NISS_CLASS_CD,
    NISS_ELIG_PNTS_CD,
    NISS_AGE_GRP_CD,
    NISS_CMMCL_IND_CD,
    NISS_EXCPN_CD,
    NISS_FGVNS_CD,
    NISS_PASSV_RESTRA_CD,
    NISS_DEFNS_DRVR_CRD_CD,
    NISS_ANTI_THFT_DVC_CD,
    NISS_DAY_TM_RUN_LAMPS_DISC_CD,
    CASE
      WHEN NISS_PLCY_LMT_CD = 'E2' THEN '02'
      ELSE NISS_PLCY_LMT_CD
    END AS NISS_PLCY_LMT_CD,
    CASE
      WHEN NISS_DEDUC_CD = 'E0' THEN '01'
      WHEN NISS_DEDUC_CD = 'E1' THEN '01'
      WHEN NISS_DEDUC_CD = 'E7' THEN '07'
      ELSE NISS_DEDUC_CD
    END AS NISS_DEDUC_CD,
    NISS_SSL_LIAB_CD,
    NISS_SUBLOB_CD,
    NISS_TYP_LOSS_CD,
    NISS_LIAB_OR_NO_FAULT_CD,
    NISS_ANNL_STMNT_LOB_CD,
    SUM(EXPS_VAL_ROLLED) AS CVG_EXPS_VAL,
    SUM(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
    NISS_PD_LOSS,
    NISS_PD_ALLOC_ADJUS_EXPNS,
    NISS_OUTSTNDG_LOSS,
    NISS_NO_PD_CLMS,
    NISS_NO_OUTSTND_CLMS,
    RSVD_NISS_USE,
    NISS_RSVD_CMPNY_USE,
    NISS_MNFCTRS_MDL_YR,
    99999 AS CVG_ATTR_SK
FROM WRK_BIRP_NISS_APRM_DETL
WHERE {FILTER_COND_FNL}
GROUP BY
  CLNDR_YR,
  CALL_YR,
  NISS_CMPNY_CD,
  ST_NM,
  ST_ABBR,
  NISS_ST_CD,
  LINE_CD,
  ACCDNT_YR,
  CASE
    WHEN NISS_CVG_CD = 'E16' THEN '616'
    WHEN NISS_CVG_CD = 'E36' THEN '636'
    WHEN NISS_CVG_CD = 'E56' THEN '656'
    WHEN NISS_CVG_CD = 'E76' THEN '676'
    WHEN NISS_CVG_CD = 'E85' THEN '085'
    WHEN NISS_CVG_CD = 'E14' THEN '614'
    WHEN NISS_CVG_CD = 'E81' THEN '081'
    WHEN NISS_CVG_CD = 'E01' THEN '001'
    WHEN NISS_CVG_CD = 'E34' THEN '034'
    WHEN NISS_CVG_CD = 'E33' THEN '033'
    WHEN NISS_CVG_CD = 'E71' THEN '071'
    WHEN NISS_CVG_CD = 'E77' THEN '077'
    WHEN NISS_CVG_CD = 'E55' THEN '055'
    WHEN NISS_CVG_CD = 'E33' THEN '733'
    WHEN NISS_CVG_CD = 'E99' THEN '099'
    ELSE NISS_CVG_CD
  END,
  NISS_TERR_CD,
  RTNG_ZNE_CD,
  TERM_ZNE_CD,
  GRGNG_ZIP_5,
  CASE
    WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1202' THEN '120200'
    WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1212' THEN '121200'
    WHEN SUBSTR(NISS_CLASS_CD,1,6)= 'E88712' THEN '887120'
    WHEN LEFT(NISS_CLASS_CD,4)= 'E202' THEN '1202' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E600' THEN '1600' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E602' THEN '1602' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E620' THEN '1620' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E622' THEN '1622' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E630' THEN '1630' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E632' THEN '1632' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E610' THEN '1610' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E612' THEN '1612' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E696' THEN '1696' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E698' THEN '1698' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E124' THEN '8124' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E554' THEN '8554' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E214' THEN '8214' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E211' THEN '8211' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E611' THEN '8611' || RIGHT(NISS_CLASS_CD,2)
    WHEN LEFT(NISS_CLASS_CD,4)= 'E160' THEN '1600' || RIGHT(NISS_CLASS_CD,2)
    ELSE NISS_CLASS_CD
  END,
  NISS_ELIG_PNTS_CD,
  NISS_AGE_GRP_CD,
  NISS_CMMCL_IND_CD,
  NISS_EXCPN_CD,
  NISS_FGVNS_CD,
  NISS_PASSV_RESTRA_CD,
  NISS_DEFNS_DRVR_CRD_CD,
  NISS_ANTI_THFT_DVC_CD,
  NISS_DAY_TM_RUN_LAMPS_DISC_CD,
  CASE
    WHEN NISS_PLCY_LMT_CD = 'E2' THEN '02'
    ELSE NISS_PLCY_LMT_CD
  END,
  CASE
    WHEN NISS_DEDUC_CD = 'E0' THEN '01'
    WHEN NISS_DEDUC_CD = 'E1' THEN '01'
    WHEN NISS_DEDUC_CD = 'E7' THEN '07'
    ELSE NISS_DEDUC_CD
  END,
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
  NISS_MNFCTRS_MDL_YR"""

    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_plato = spark.sql(sql_query)
    logger.info("Completed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override (staged temp view)")
except Exception as e:
    logger.error(f"Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru: explicit projection of every OUTPUT/INPUT-OUTPUT port; PM system vars -> NULL
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru projection (explicit column listing)")
    df_EXP_PassThru_upbeat_franklin = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_plato.selectExpr(
        "CLNDR_YR",
        "CALL_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "ST_ABBR",
        "NISS_ST_CD",
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "GRGNG_ZIP_5",
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
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE",
        "NISS_MNFCTRS_MDL_YR",
        "CVG_ATTR_SK"
    )
    # Informatica PM* system variables have no Glue equivalent -> NULLs
    df_EXP_PassThru_upbeat_franklin = (
        df_EXP_PassThru_upbeat_franklin
        .withColumn("MAAPING_NAME", lit(None))
        .withColumn("FOLDER_NAME", lit(None))
        .withColumn("WORKFLOW_NAME", lit(None))
    )
    logger.info("EXP_PassThru projection complete")
except Exception as e:
    logger.error(f"Failed EXP_PassThru projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Mapplet: mplt_FDR_LIB_ABC_MAPPING_AUDIT -> produce single-row audit DF
# -----------------------------------------------------------------------------
try:
    logger.info("Creating mapplet audit dataframe (mplt_FDR_LIB_ABC_MAPPING_AUDIT)")
    # single-row audit DF: CR_BY_MAPNG_ID/UPD_BY_MAPNG_ID/WRK_FLOW_RUN_ID unknown -> NULL; timestamps set to current timestamp()
    df_mplt_ABC_MAPPING_AUDIT_laughing_galileo = (
        spark.range(1)
        .selectExpr(
            "CAST(NULL AS BIGINT) AS CR_BY_MAPNG_ID",
            "current_timestamp() AS DW_CR_TMSP",
            "CAST(NULL AS BIGINT) AS UPD_BY_MAPNG_ID",
            "current_timestamp() AS DW_UPD_TMSP",
            "CAST(NULL AS BIGINT) AS WRK_FLOW_RUN_ID"
        )
    )
    logger.info("Mapplet audit dataframe created")
except Exception as e:
    logger.error(f"Failed creating mapplet audit dataframe: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru_Tgt: attach audit columns (cross-join) and project final OUTPUT ports
# NISS_APRM_FINAL_SK = CVG_ATTR_SK
# -----------------------------------------------------------------------------
try:
    logger.info("Attaching audit columns and projecting final target columns in EXP_PassThru_Tgt")
    # crossJoin the single-row audit dataframe so audit columns are attached to every row
    df_temp = df_EXP_PassThru_upbeat_franklin.crossJoin(df_mplt_ABC_MAPPING_AUDIT_laughing_galileo)

    df_EXP_PassThru_Tgt_calm_plato = df_temp.selectExpr(
        "CVG_ATTR_SK AS NISS_APRM_FINAL_SK",
        "CLNDR_YR",
        "CALL_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "ST_ABBR",
        "NISS_ST_CD",
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "GRGNG_ZIP_5",
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
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
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
        "WRK_FLOW_RUN_ID"
    )
    logger.info("EXP_PassThru_Tgt projection complete")
except Exception as e:
    logger.error(f"Failed EXP_PassThru_Tgt processing: {e}", exc_info=True)
    raise

# Ensure the mapping's expected final dataframe variable name is assigned
try:
    logger.info("Assigning final dataframe variable df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking")
    df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking = df_EXP_PassThru_Tgt_calm_plato
    logger.info("Assigned df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking")
except Exception as e:
    logger.error(f"Failed assigning final dataframe variable: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL -> write parquet to S3 (overwrite)
# write intermediate WRK_ table as parquet to S3 (overwrite)
# -----------------------------------------------------------------------------
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_FINAL to S3 as parquet (overwrite)")
    df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/"
    )
    logger.info("Successfully wrote WRK_BIRP_NISS_APRM_FINAL to s3://%s/WRK_BIRP_NISS_APRM_FINAL/", S3_OUTPUT_BUCKET)
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_FINAL to S3: {e}", exc_info=True)
    raise



job.commit()
