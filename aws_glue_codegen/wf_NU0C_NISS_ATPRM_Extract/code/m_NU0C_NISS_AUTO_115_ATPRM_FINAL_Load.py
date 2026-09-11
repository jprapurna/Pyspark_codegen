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

from pyspark.sql.functions import expr, lit

# Top-level placeholders for mapping parameters / environment values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
FILTER_COND_FNL = "REPLACE_WITH_FILTER_COND_FNL_VALUE"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# -------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# This is a WRK_ intermediate source: try to read the already-staged Parquet first
# -------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3 (staged branch)")
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3, falling back to Glue Catalog read: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database/table")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# Project exactly the columns listed on the Source node
try:
    logger.info("Projecting exact source columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking.selectExpr(
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
    logger.error(f"Failed projecting source columns for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# The SQ has a SQL Override against FDR.WRK_BIRP_NISS_APRM_DETL and the upstream
# WRK_ table is staged in this workflow: register the upstream dataframe as a
# temp view named 'WRK_BIRP_NISS_APRM_DETL' and run the override via spark.sql
# -------------------------------------------------------------------------
try:
    logger.info("Registering staged WRK_BIRP_NISS_APRM_DETL as temp view for SQ override")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_determined_hawking.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""
SELECT
CLNDR_YR,
CALL_YR,
NISS_CMPNY_CD,
ST_NM,
' ' AS ST_CD,
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
ELSE NISS_CVG_CD END NISS_CVG_CD,
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
ELSE NISS_CLASS_CD END NISS_CLASS_CD,
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
ELSE NISS_PLCY_LMT_CD END NISS_PLCY_LMT_CD,
CASE
WHEN NISS_DEDUC_CD = 'E0' THEN '01'
WHEN NISS_DEDUC_CD = 'E1' THEN '01'
WHEN NISS_DEDUC_CD = 'E7' THEN '07'
ELSE NISS_DEDUC_CD END NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
SUM(EXPS_VAL_ROLLED) CVG_EXPS_VAL,
SUM(TTL_WRITTN_PREM_AMT) TTL_WRITTN_PREM_AMT,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR,
99999 as CVG_ATTR_SK
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
ELSE NISS_CVG_CD END,
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
ELSE NISS_CLASS_CD END,
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
ELSE NISS_PLCY_LMT_CD END,
CASE
WHEN NISS_DEDUC_CD = 'E0' THEN '01'
WHEN NISS_DEDUC_CD = 'E1' THEN '01'
WHEN NISS_DEDUC_CD = 'E7' THEN '07'
ELSE NISS_DEDUC_CD END,
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

    logger.info("Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_einstein = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Expression: EXP_PassThru
# Pure passthrough and explicit projection of every port; PM variables -> NULL
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru passthrough projection")
    df_EXP_PassThru_keen_darwin = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_einstein.selectExpr(
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
        "CVG_ATTR_SK",
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
        "NISS_LIAB_OR_NO_FAULT_CD"
    )

    # Informatica-built-in audit/system variables become SQL NULLs -> lit(None)
    df_EXP_PassThru_keen_darwin = df_EXP_PassThru_keen_darwin.withColumn("MAAPING_NAME", lit(None)).withColumn("FOLDER_NAME", lit(None)).withColumn("WORKFLOW_NAME", lit(None))
except Exception as e:
    logger.error(f"Failed applying EXP_PassThru transformations: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Expression: EXP_PassThru_Tgt
# Project every target column explicitly; derive NISS_APRM_FINAL_SK from CVG_ATTR_SK.
# The mapping-audit mapplet was unavailable; the join and audit columns (CR_BY_MAPNG_ID,
# DW_CR_TMSP, UPD_BY_MAPNG_ID, DW_UPD_TMSP, WRK_FLOW_RUN_ID) are omitted/commented out
# per plan rather than fabricated.
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru_Tgt projection to shape target columns")
    # First compute NISS_APRM_FINAL_SK from CVG_ATTR_SK via aliasing in selectExpr
    df_EXP_PassThru_Tgt_mystifying_hopper = df_EXP_PassThru_keen_darwin.selectExpr(
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
        "NISS_MNFCTRS_MDL_YR"
        # NOTE: Audit/mapplet columns CR_BY_MAPNG_ID, DW_CR_TMSP, UPD_BY_MAPNG_ID,
        # DW_UPD_TMSP, WRK_FLOW_RUN_ID are omitted because the mapping-audit mapplet
        # input was not available in the lineage for this mapping. Restore them
        # when the audit mapplet becomes available.
    )
except Exception as e:
    logger.error(f"Failed applying EXP_PassThru_Tgt projection: {e}", exc_info=True)
    raise

# Set the output df name expected by downstream / target
df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_affectionate_turing = df_EXP_PassThru_Tgt_mystifying_hopper

# -------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL
# Write final dataframe as parquet to S3 (overwrite)
# -------------------------------------------------------------------------
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_FINAL to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_affectionate_turing.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_FINAL to S3: {e}", exc_info=True)
    raise


job.commit()
