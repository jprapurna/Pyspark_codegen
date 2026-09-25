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

import pandas as pd
from pyspark.sql.functions import trim, substr, when, expr, col, split, element_at, trim, regexp_replace, coalesce, concat_ws, lit, size
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# placeholder constants (replace before running)
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"
OUTPUT_FILENAME_SUM = "REPLACE_WITH_OutputFile_PremRpt3_Sum"
OUTPUT_FILENAME_DETAIL = "REPLACE_WITH_OutputFile_PremRpt1_Det"
OUTPUT_FILENAME_XCPN = "REPLACE_WITH_OutputFile_PremRpt_Exp"
# placeholders for this mapping's final outputs (replace before running)
OUTPUT_FILENAME_FNLCsv = "REPLACE_WITH_OutputFile_PremRpt2_FnlCsv"
OUTPUT_FILENAME_FNLTxt = "REPLACE_WITH_OutputFile_PremRpt2_FnlTxt"
LOOKUP_GLUE_DB = "REPLACE_WITH_GLUE_DB"
LOOKUP_TABLE = "REPLACE_WITH_LOOKUP_TABLE"

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 (WRK_ intermediate - try S3 first)
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 as staged parquet")
    df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 (staged)")
except Exception as e:
    logger.warning("S3 staged path for WRK_BIRP_NISS_APRM_DETL not available, falling back to Glue catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame.from_catalog(database="REPLACE_WITH_SOURCE_DB", table_name="WRK_BIRP_NISS_APRM_DETL")
        df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck = dyf.toDF()
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue catalog: {e}", exc_info=True)
        raise

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL (another shortcut/read of same WRK_ - try S3 first)
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 as staged parquet (shortcut) ")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 (staged) for shortcut")
except Exception as e:
    logger.warning("S3 staged path for WRK_BIRP_NISS_APRM_DETL (shortcut) not available, falling back to Glue catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL (shortcut) from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame.from_catalog(database="REPLACE_WITH_SOURCE_DB", table_name="WRK_BIRP_NISS_APRM_DETL")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace = dyf.toDF()
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL (shortcut) from Glue catalog: {e}", exc_info=True)
        raise

# Source: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL (WRK_ final intermediate - S3-first)
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_FINAL from S3 as staged parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_FINAL from S3 (staged)")
except Exception as e:
    logger.warning("S3 staged path for WRK_BIRP_NISS_APRM_FINAL not available, falling back to Glue catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_FINAL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame.from_catalog(database="REPLACE_WITH_SOURCE_DB", table_name="WRK_BIRP_NISS_APRM_FINAL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon = dyf.toDF()
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from Glue catalog: {e}", exc_info=True)
        raise

# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY: STAGED case -> register staged temp view and run rewritten SQL override
sql_query = f"""SELECT 
    NISS_CMPNY_CD,
    CLNDR_YR,
    NISS_ST_CD,
    ACCDNT_YR,
    NISS_CVG_CD,
    NISS_CLASS_CD,
    NISS_SUBLOB_CD,
    NISS_TYP_LOSS_CD,
    NISS_ANNL_STMNT_LOB_CD,
    SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
    SUM(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
    NISS_PD_LOSS,
    NISS_PD_ALLOC_ADJUS_EXPNS,
    NISS_OUTSTNDG_LOSS,
    NISS_NO_PD_CLMS,
    NISS_NO_OUTSTND_CLMS,
    NISS_TERR_CD,
    COUNT(*) AS REC_COUNT

FROM WRK_BIRP_NISS_APRM_FINAL
--WHERE  NISS_ST_CD IN('39','31','16')
GROUP BY 
NISS_CMPNY_CD,
CLNDR_YR,
NISS_ST_CD,
ACCDNT_YR,
NISS_CVG_CD,
NISS_CLASS_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_TERR_CD"""
try:
    # the staged temp view is already available in df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")
    logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY against staged temp view WRK_BIRP_NISS_APRM_FINAL")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_heroic_archimedes = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing staged SQL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY: {e}", exc_info=True)
    raise

# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: STAGED -> register temp view and run rewritten SQL override
sql_query = f"""SELECT
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
PRD_GRP_CD,
NJ_HLTH_INSR_PRIM,
NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
NISS_TERR_CD,
RATNG_CMPY_CD,
MLT_CAR_IND,
RT_CLS,
AGE,
GENDR,
MRTL_STAT,
AUTO_USE_CD,
MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP,
PHY_DMG_IND,
NJ_RATD_PNTS,
VEH_MDL_YR,
NJ_EXCPTION_CD,
NJ_FGVN_PNTS,
PASSV_RESTRA_DISC,
SNR_DRVR_IND,
DEFNS_DRVR_DISC_IND,
ANTI_THFT_DISC,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
CVG_TYP_IND,
SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
SUM(TTL_WRITTN_PREM_AMT) TTL_WRITTN_PREM_AMT,
LINE_CD,
ACCDNT_YR,
CASE 
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
ELSE NISS_CVG_CD END NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN substr(NISS_CLASS_CD,1,1)='E' THEN '933100' ELSE NISS_CLASS_CD END NISS_CLASS_CD,
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
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
SUM(EXPS_VAL_ROLLED) AS EXPS_VAL_ROLLED,
REC_DROP_IND,
REC_DROP_RSN_DESC,
REC_EXCPN_IND,
COMP_DED,
COLL_DED,
NUM_OF_CARS_IN_HH,
RDRVR_DT_OF_BRTH,
TO_CHAR(TERM_STRT_DT,'MM/DD/YYYY') TERM_STRT_DT,
SRC_SYS_CD,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE,
PNI_AGE,
LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED

FROM WRK_BIRP_NISS_APRM_DETL

--WHERE NISS_APRM_DETL_SK IN (...) or other filters
GROUP BY 
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_TYP_IND,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
PRD_GRP_CD,
NJ_HLTH_INSR_PRIM,
NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
NISS_TERR_CD,
RATNG_CMPY_CD,
MLT_CAR_IND,
RT_CLS,
AGE,
GENDR,
MRTL_STAT,
AUTO_USE_CD,
MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP,
PHY_DMG_IND,
NJ_RATD_PNTS,
VEH_MDL_YR,
NJ_EXCPTION_CD,
NJ_FGVN_PNTS,
PASSV_RESTRA_DISC,
SNR_DRVR_IND,
DEFNS_DRVR_DISC_IND,
ANTI_THFT_DISC,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
LINE_CD,
ACCDNT_YR,
CASE 
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
ELSE NISS_CVG_CD END,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN substr(NISS_CLASS_CD,1,1)='E' THEN '933100' ELSE NISS_CLASS_CD END,
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
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
REC_EXCPN_IND,
REC_DROP_IND,
REC_DROP_RSN_DESC,
COMP_DED,
COLL_DED,
NUM_OF_CARS_IN_HH,
TERM_STRT_DT,
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE,
LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED
"""
try:
    # create temp view from staged detail shortcut
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL against staged temp view WRK_BIRP_NISS_APRM_DETL")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing staged SQL for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception: STAGED -> temp view + SQL
sql_query = f"""SELECT
DETL.CLNDR_YR,
DETL.CALL_YR,
DETL.NAIC_CMPNY_CD,
DETL.NISS_CMPNY_CD,
DETL.ST_NM,
DETL.ST_CD,
DETL.NISS_ST_CD,
DETL.ST_ABBR,
DETL.ACCTNG_LOB,
DETL.CVG_TYP_CD,
DETL.CVG_AMT,
DETL.BI_LMT,
DETL.GA_ADDED_AT_FAULT_IND,
DETL.FA2_PLCY_IND,
DETL.UM_UMI_STACKING,
DETL.PIP_WVR_WL_IND,
DETL.PIP_MED_SEC_IND,
DETL.PIP_LOSS_INCOME_IND,
DETL.MI_PPO_IND,
DETL.PRD_GRP_CD,
DETL.NJ_HLTH_INSR_PRIM,
DETL.NJ_EXTR_PIP_PKG,
DETL.NJ_RESDNC_RLTNSHP_PIP_IND,
DETL.NY_SSL_IND,
DETL.NY_FULL_CVG_GLASS_COMP_IND,
DETL.GRGNG_ZIP_5,
DETL.NISS_TERR_CD,
DETL.RATNG_CMPY_CD,
DETL.MLT_CAR_IND,
DETL.RT_CLS,
DETL.AGE,
DETL.GENDR,
DETL.MRTL_STAT,
DETL.AUTO_USE_CD,
DETL.MILES_TO_WRK,
DETL.GOOD_STDNT_IND,
DETL.DRVR_TRNG_IND,
DETL.SOI_TYP,
DETL.PHY_DMG_IND,
DETL.NJ_RATD_PNTS,
DETL.VEH_MDL_YR,
DETL.NJ_EXCPTION_CD,
DETL.NJ_FGVN_PNTS,
DETL.PASSV_RESTRA_DISC,
DETL.SNR_DRVR_IND,
DETL.DEFNS_DRVR_DISC_IND,
DETL.ANTI_THFT_DISC,
DETL.DAY_TM_RUN_LIGHTS,
DETL.LMT_TORT,
DETL.CVG_EXPS_VAL,
DETL.TTL_WRITTN_PREM_AMT,
DETL.LINE_CD,
DETL.ACCDNT_YR,
CASE 
WHEN DETL.NISS_CVG_CD = 'E16' THEN '616'
WHEN DETL.NISS_CVG_CD = 'E36' THEN '636'
WHEN DETL.NISS_CVG_CD = 'E56' THEN '656'
WHEN DETL.NISS_CVG_CD = 'E76' THEN '676'
ELSE DETL.NISS_CVG_CD END NISS_CVG_CD,
DETL.RTNG_ZNE_CD,
DETL.TERM_ZNE_CD,
CASE WHEN substr(DETL.NISS_CLASS_CD,1,1)='E' THEN '933100' ELSE DETL.NISS_CLASS_CD END NISS_CLASS_CD,
DETL.NISS_ELIG_PNTS_CD,
DETL.NISS_AGE_GRP_CD,
DETL.NISS_CMMCL_IND_CD,
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
DETL.NISS_TYP_LOSS_CD,
DETL.NISS_LIAB_OR_NO_FAULT_CD,
DETL.NISS_ANNL_STMNT_LOB_CD,
DETL.NISS_PD_LOSS,
DETL.NISS_PD_ALLOC_ADJUS_EXPNS,
DETL.NISS_OUTSTNDG_LOSS,
DETL.NISS_NO_PD_CLMS,
DETL.NISS_NO_OUTSTND_CLMS,
DETL.NISS_MNFCTRS_MDL_YR,
DETL.NJ_NO_LWST_LMT_IND,
DETL.NJ_NMD_DRVR_EXCL_IND,
DETL.REC_DROP_IND,
DETL.REC_EXCPN_IND,
DETL.REC_EXCPN_RSN_DESC,
DETL.COMP_DED,
DETL.COLL_DED,
DETL.NUM_OF_CARS_IN_HH,
DETL.RDRVR_DT_OF_BRTH,
TO_CHAR(DETL.TERM_STRT_DT,'MM/DD/YYYY') TERM_STRT_DT,
DETL.SRC_SYS_CD,
DETL.DERIVED_RDRVR_AGE,
DETL.FINAL_RDRVR_AGE,
DETL.PNI_AGE,
DETL.LOB,
DETL.PRINCIPAL_OPRT,
DETL.SOURCE_IND_DERIVED

FROM WRK_BIRP_NISS_APRM_DETL DETL

WHERE DETL.REC_EXCPN_IND='Y'"""
try:
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception against staged temp view WRK_BIRP_NISS_APRM_DETL")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception_focused_ramanujan = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing staged SQL for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception: {e}", exc_info=True)
    raise

# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: STAGED -> register and run SQL override; log that PreSQL is not executed against staged DF
sql_query = f"""SELECT FNL.NISS_CMPNY_CD,
FNL.CLNDR_YR,
FNL.CALL_YR,
FNL.NISS_ST_CD,
FNL.LINE_CD,
FNL.ACCDNT_YR,
FNL.NISS_CVG_CD,
FNL.NISS_TERR_CD,
FNL.RTNG_ZNE_CD,
FNL.TERM_ZNE_CD,
FNL.GRGNG_ZIP_5,
FNL.NISS_CLASS_CD,
FNL.NISS_ELIG_PNTS_CD,
FNL.NISS_AGE_GRP_CD,
FNL.NISS_MNFCTRS_MDL_YR,
FNL.NISS_CMMCL_IND_CD,
FNL.NISS_EXCPN_CD,
FNL.NISS_FGVNS_CD,
FNL.NISS_PASSV_RESTRA_CD,
FNL.NISS_DEFNS_DRVR_CRD_CD,
FNL.NISS_ANTI_THFT_DVC_CD,
FNL.NISS_DAY_TM_RUN_LAMPS_DISC_CD,
FNL.NISS_PLCY_LMT_CD,
FNL.NISS_DEDUC_CD,
FNL.NISS_SSL_LIAB_CD,
FNL.NISS_SUBLOB_CD,
FNL.NISS_TYP_LOSS_CD,
FNL.NISS_LIAB_OR_NO_FAULT_CD,
FNL.NISS_ANNL_STMNT_LOB_CD,
FNL.CVG_EXPS_VAL,
FNL.TTL_WRITTN_PREM_AMT,
FNL.NISS_PD_LOSS,
FNL.NISS_PD_ALLOC_ADJUS_EXPNS,
FNL.NISS_OUTSTNDG_LOSS,
FNL.NISS_NO_PD_CLMS,
FNL.NISS_NO_OUTSTND_CLMS,
FNL.RSVD_NISS_USE,
FNL.NISS_RSVD_CMPNY_USE

FROM WRK_BIRP_NISS_APRM_FINAL FNL
--WHERE  FNL.NISS_ST_CD IN('39','31','16')"""
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")
    logger.info("Note: session-level PreSQL statements from the original session are not re-run against the staged DataFrame. Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL against staged view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing staged SQL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: {e}", exc_info=True)
    raise

# EX_PASS_SUMRY: expression transformations over summary ASQ
try:
    logger.info("Applying EX_PASS_SUMRY transformations")
    df_EX_PASS_SUMRY_amazing_hopper = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_heroic_archimedes.selectExpr(
        "NISS_CMPNY_CD",
        "substr(trim(CLNDR_YR),3,2) as o_CLNDR_YR",
        "NISS_ST_CD",
        "ACCDNT_YR",
        "CASE WHEN trim(NISS_CVG_CD) = '' OR NISS_CVG_CD IS NULL THEN '???' ELSE NISS_CVG_CD END AS o_NISS_CVG_CD",
        "NISS_TERR_CD",
        "CASE WHEN trim(NISS_CLASS_CD) = '' OR NISS_CLASS_CD IS NULL THEN '??????' ELSE NISS_CLASS_CD END AS o_NISS_CLASS_CD",
        "NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD",
        "NISS_ANNL_STMNT_LOB_CD",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "TALLY"
    )
except Exception as e:
    logger.error(f"Failed EX_PASS_SUMRY transformation: {e}", exc_info=True)
    raise

# EXP_GenErrReason: tokenize REC_DROP_RSN_DESC, attempt lookup via Glue-catalog lookup table (placeholder), broadcast join, and produce normalized REC_DROP_RSN_DESC
try:
    logger.info("Applying EXP_GenErrReason tokenization and lookup normalization")
    df_tmp = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper.withColumn("i_REC_DROP_RSN_DESC", col("REC_DROP_RSN_DESC"))

    # split into tokens (up to 4)
    tokens_col = split(coalesce(col("i_REC_DROP_RSN_DESC"), lit("")), ';')
    df_tmp = df_tmp.withColumn("rec_tok1", trim(element_at(tokens_col, 1)))
    df_tmp = df_tmp.withColumn("rec_tok2", trim(element_at(tokens_col, 2)))
    df_tmp = df_tmp.withColumn("rec_tok3", trim(element_at(tokens_col, 3)))
    df_tmp = df_tmp.withColumn("rec_tok4", trim(element_at(tokens_col, 4)))

    # attempt to read lookup from Glue catalog; this is a placeholder and should be set to the real DB/table
    try:
        dyf_lkp = glueContext.create_dynamic_frame.from_catalog(database=LOOKUP_GLUE_DB, table_name=LOOKUP_TABLE)
        df_lkp = dyf_lkp.toDF()
        # assume lookup has columns 'ERR_CODE' and 'ERR_DESC' (adjust if different)
        df_lkp = df_lkp.selectExpr("ERR_CODE as lkp_code", "ERR_DESC as lkp_desc")
    except Exception as e:
        logger.warning("Lookup table read failed or not configured (LOOKUP_GLUE_DB/LOOKUP_TABLE). Continuing without enrichment: %s" % e)
        # create empty lookup DF with appropriate schema to allow broadcast joins to run but yield no matches
        df_lkp = spark.createDataFrame([], schema="lkp_code string, lkp_desc string")

    # broadcast lookup for joins
    from pyspark.sql.functions import broadcast
    lkp_b = broadcast(df_lkp)

    # join tokens to lookup to get normalized descriptions
    df_tmp = df_tmp.join(lkp_b, df_tmp.rec_tok1 == lkp_b.lkp_code, how='left')
    df_tmp = df_tmp.withColumn("rec_desc1", coalesce(col("lkp_desc"), col("rec_tok1"))).drop("lkp_code", "lkp_desc")

    # re-read lookup for second token (join again)
    df_tmp = df_tmp.join(lkp_b.withColumnRenamed("lkp_code","lkp_code2").withColumnRenamed("lkp_desc","lkp_desc2"), df_tmp.rec_tok2 == col("lkp_code2"), how='left')
    df_tmp = df_tmp.withColumn("rec_desc2", coalesce(col("lkp_desc2"), col("rec_tok2"))).drop("lkp_code2","lkp_desc2")

    df_tmp = df_tmp.join(lkp_b.withColumnRenamed("lkp_code","lkp_code3").withColumnRenamed("lkp_desc","lkp_desc3"), df_tmp.rec_tok3 == col("lkp_code3"), how='left')
    df_tmp = df_tmp.withColumn("rec_desc3", coalesce(col("lkp_desc3"), col("rec_tok3"))).drop("lkp_code3","lkp_desc3")

    df_tmp = df_tmp.join(lkp_b.withColumnRenamed("lkp_code","lkp_code4").withColumnRenamed("lkp_desc","lkp_desc4"), df_tmp.rec_tok4 == col("lkp_code4"), how='left')
    df_tmp = df_tmp.withColumn("rec_desc4", coalesce(col("lkp_desc4"), col("rec_tok4"))).drop("lkp_code4","lkp_desc4")

    # combine normalized descriptions into one REC_DROP_RSN_DESC (only non-empty tokens)
    df_tmp = df_tmp.withColumn(
        "REC_DROP_RSN_DESC",
        concat_ws(";",
                  when(col("rec_desc1").isNotNull() & (col("rec_desc1") != ""), col("rec_desc1")).otherwise(lit(None)),
                  when(col("rec_desc2").isNotNull() & (col("rec_desc2") != ""), col("rec_desc2")).otherwise(lit(None)),
                  when(col("rec_desc3").isNotNull() & (col("rec_desc3") != ""), col("rec_desc3")).otherwise(lit(None)),
                  when(col("rec_desc4").isNotNull() & (col("rec_desc4") != ""), col("rec_desc4")).otherwise(lit(None))
                 )
    )

    # Keep original columns plus i_REC_DROP_RSN_DESC and normalized REC_DROP_RSN_DESC
    df_EXP_GenErrReason_eager_tesla = df_tmp

except Exception as e:
    logger.error(f"Failed EXP_GenErrReason processing: {e}", exc_info=True)
    raise

# EXP_PASS_DETL: combine detail ASQ with normalized REC_DROP_RSN_DESC and compute derived outputs
try:
    logger.info("Applying EXP_PASS_DETL - passing through detail fields and computing small derived outputs")
    d = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper.alias("d")
    e = df_EXP_GenErrReason_eager_tesla.alias("e")

    # join on the raw record-drop description to bring in the normalized REC_DROP_RSN_DESC
    joined = d.join(e.select(col("i_REC_DROP_RSN_DESC"), col("REC_DROP_RSN_DESC").alias("REC_DROP_RSN_DESC_NORM")),
                    on=(d.REC_DROP_RSN_DESC == e.i_REC_DROP_RSN_DESC), how='left')

    df_EXP_PASS_DETL_optimistic_lovelace = joined.select(
        col("GRGNG_ZIP_5"),
        col("NISS_TERR_CD"),
        when((trim(col("NISS_TERR_CD")).eqNullSafe(lit(""))) | col("NISS_TERR_CD").isNull(), lit('???')).otherwise(col("NISS_TERR_CD")).alias("o_NISS_TERR_CD"),
        col("RATNG_CMPY_CD"),
        col("MLT_CAR_IND"),
        col("RT_CLS"),
        col("AGE"),
        col("GENDR"),
        col("MRTL_STAT"),
        col("AUTO_USE_CD"),
        col("MILES_TO_WRK"),
        col("GOOD_STDNT_IND"),
        col("DRVR_TRNG_IND"),
        col("SOI_TYP"),
        col("PHY_DMG_IND"),
        col("NJ_RATD_PNTS"),
        col("VEH_MDL_YR"),
        col("NJ_EXCPTION_CD"),
        col("NJ_FGVN_PNTS"),
        # NJ flag derivations (inputs named i_NJ_... as per source logic)
        when(col("i_NJ_NO_LWST_LMT_IND") == 1, lit('Y')).otherwise(lit('N')).alias("NJ_NO_LWST_LMT_IND"),
        when(col("i_NJ_NMD_DRVR_EXCL_IND") == 1, lit('Y')).otherwise(lit('N')).alias("NJ_NMD_DRVR_EXCL_IND"),
        # numeric amount cleaning
        regexp_replace(col("CVG_AMT"), ',', '').alias("o_CVG_AMT"),
        regexp_replace(col("BI_LMT"), ',', '').alias("o_BI_LMT"),
        col("CLNDR_YR"),
        substr(trim(col("CLNDR_YR")), 3, 2).alias("o_CLNDR_YR"),
        col("CALL_YR"),
        substr(trim(col("CALL_YR")), 3, 2).alias("o_CALL_YR"),
        col("NAIC_CMPNY_CD"),
        col("NISS_CMPNY_CD"),
        col("ST_NM"),
        col("ST_CD"),
        col("NISS_ST_CD"),
        col("ST_ABBR"),
        col("ACCTNG_LOB"),
        col("CVG_TYP_CD"),
        col("CVG_TYP_IND"),
        col("CVG_EXPS_VAL"),
        col("TTL_WRITTN_PREM_AMT"),
        col("LINE_CD"),
        col("ACCDNT_YR"),
        col("NISS_CVG_CD"),
        col("RTNG_ZNE_CD"),
        col("TERM_ZNE_CD"),
        col("NISS_CLASS_CD"),
        col("NISS_ELIG_PNTS_CD"),
        col("NISS_AGE_GRP_CD"),
        col("NISS_CMMCL_IND_CD"),
        col("NISS_EXCPN_CD"),
        col("NISS_FGVNS_CD"),
        col("NISS_PASSV_RESTRA_CD"),
        col("NISS_DEFNS_DRVR_CRD_CD"),
        col("NISS_ANTI_THFT_DVC_CD"),
        col("NISS_DAY_TM_RUN_LAMPS_DISC_CD"),
        col("NISS_PLCY_LMT_CD"),
        col("NISS_DEDUC_CD"),
        col("NISS_SSL_LIAB_CD"),
        col("NISS_SUBLOB_CD"),
        col("NISS_TYP_LOSS_CD"),
        col("NISS_LIAB_OR_NO_FAULT_CD"),
        col("NISS_ANNL_STMNT_LOB_CD"),
        col("NISS_PD_LOSS"),
        col("NISS_PD_ALLOC_ADJUS_EXPNS"),
        col("NISS_OUTSTNDG_LOSS"),
        col("NISS_NO_PD_CLMS"),
        col("NISS_NO_OUTSTND_CLMS"),
        col("REC_EXCPN_IND"),
        col("COMP_DED"),
        col("COLL_DED"),
        col("NUM_OF_CARS_IN_HH"),
        col("TERM_STRT_DT"),
        col("RDRVR_DT_OF_BRTH"),
        col("SRC_SYS_CD"),
        col("PNI_AGE"),
        col("DERIVED_RDRVR_AGE"),
        col("FINAL_RDRVR_AGE"),
        col("LOB"),
        col("PRINCIPAL_OPRT"),
        col("SOURCE_IND_DERIVED"),
        # bring in normalized REC_DROP_RSN_DESC from the EXP_GenErrReason frame
        col("REC_DROP_RSN_DESC_NORM").alias("REC_DROP_RSN_DESC")
    )

except Exception as e:
    logger.error(f"Failed EXP_PASS_DETL transformation: {e}", exc_info=True)
    raise

# EXP_PASS_TRGT: pass-through and light derivations for exception ASQ
try:
    logger.info("Applying EXP_PASS_TRGT transformations for exception target")
    df_EXP_PASS_TRGT_calm_dirac = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception_focused_ramanujan.selectExpr(
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "CASE WHEN trim(NISS_CVG_CD) = '' OR NISS_CVG_CD IS NULL THEN '??????' ELSE NISS_CVG_CD END AS O_CVG_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "NISS_CLASS_CD",
        "CASE WHEN trim(NISS_CLASS_CD) = '' OR NISS_CLASS_CD IS NULL THEN '??????' ELSE NISS_CLASS_CD END AS O_CLASS_CD",
        "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD",
        "NISS_MNFCTRS_MDL_YR",
        "substr(trim(to_char(NISS_MNFCTRS_MDL_YR)),1,2) as v_NISS_MNFCTRS_MDL_YR",
        "CASE WHEN length(trim(to_char(NISS_MNFCTRS_MDL_YR))) = 2 THEN trim(to_char(NISS_MNFCTRS_MDL_YR)) ELSE concat('0', trim(to_char(NISS_MNFCTRS_MDL_YR))) END as o_NISS_MNFCTRS_MDL_YR",
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
        "CLNDR_YR",
        "substr(trim(CLNDR_YR),3,2) as o_CLNDR_YR",
        "CALL_YR",
        "substr(trim(CALL_YR),3,2) as o_CALL_YR",
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
        "REC_DROP_IND",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "COMP_DED",
        "COLL_DED",
        "NUM_OF_CARS_IN_HH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "PNI_AGE",
        "DERIVED_RDRVR_AGE",
        "FINAL_RDRVR_AGE",
        "LOB",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED"
    )
except Exception as e:
    logger.error(f"Failed EXP_PASS_TRGT transformation: {e}", exc_info=True)
    raise

# EXP_defaults: simple pass-through projection from final ASQ
try:
    logger.info("Applying EXP_defaults pass-through projection")
    df_EXP_defaults_pensive_maxwell = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger.selectExpr(
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "CALL_YR",
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
        "NISS_MNFCTRS_MDL_YR",
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
        "NISS_RSVD_CMPNY_USE"
    )
except Exception as e:
    logger.error(f"Failed EXP_defaults projection: {e}", exc_info=True)
    raise

# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary -> flat CSV to PM_TARGET_FILE_DIR
try:
    logger.info("Writing flat-file target ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary to local/remote file via pandas")
    pdf = df_EX_PASS_SUMRY_amazing_hopper.toPandas()
    out_path = f"{PM_TARGET_FILE_DIR}/{OUTPUT_FILENAME_SUM}"
    pdf.to_csv(out_path, index=False, mode='w')
    logger.info(f"Wrote summary flat file to {out_path}")
    # expose a dataframe variable name for downstream/lineage
    df_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary_nostalgic_pasteur = df_EX_PASS_SUMRY_amazing_hopper
except Exception as e:
    logger.error(f"Failed writing summary flat file: {e}", exc_info=True)
    raise

# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail -> flat CSV
try:
    logger.info("Writing flat-file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail to local/remote file via pandas")
    pdf = df_EXP_PASS_DETL_optimistic_lovelace.toPandas()
    out_path = f"{PM_TARGET_FILE_DIR}/{OUTPUT_FILENAME_DETAIL}"
    pdf.to_csv(out_path, index=False, mode='w')
    logger.info(f"Wrote detail flat file to {out_path}")
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail_careful_turing = df_EXP_PASS_DETL_optimistic_lovelace
except Exception as e:
    logger.error(f"Failed writing detail flat file: {e}", exc_info=True)
    raise

# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn -> flat CSV
try:
    logger.info("Writing flat-file target ff_BIRP_NU0C_NISS_ATPRM_RptXcpn to local/remote file via pandas")
    pdf = df_EXP_PASS_TRGT_calm_dirac.toPandas()
    out_path = f"{PM_TARGET_FILE_DIR}/{OUTPUT_FILENAME_XCPN}"
    pdf.to_csv(out_path, index=False, mode='w')
    logger.info(f"Wrote exception flat file to {out_path}")
    df_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn_tender_euclid = df_EXP_PASS_TRGT_calm_dirac
except Exception as e:
    logger.error(f"Failed writing exception flat file: {e}", exc_info=True)
    raise


# EXP_PASS_FINAL_Csv: compute trimmed year substrings, normalized code fields, and manufacturer-year padding
try:
    logger.info("Applying EXP_PASS_FINAL_Csv transformations")
    # first stage: compute any local stringified manufacturer-year variable so it can be referenced later
    df_exp_stage = df_EXP_defaults_pensive_maxwell.selectExpr(
        "'' as Blank",
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "CALL_YR",
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
        "NISS_MNFCTRS_MDL_YR",
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
        # local variable equivalent - stringified, trimmed manufacturer model year
        "LTRIM(RTRIM(CAST(NISS_MNFCTRS_MDL_YR AS STRING))) AS v_NISS_MNFCTRS_MDL_YR"
    )

    # second stage: final projection referencing the local variable above
    df_EXP_PASS_FINAL_Csv_jovial_nash = df_exp_stage.selectExpr(
        "Blank",
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "substr(trim(CLNDR_YR),3,2) as o_CLNDR_YR",
        "CALL_YR",
        "substr(trim(CALL_YR),3,2) as o_REG_PRD_YR",
        "NISS_ST_CD",
        "LINE_CD",
        "ACCDNT_YR",
        # normalized coverage code
        "CASE WHEN trim(NISS_CVG_CD) = '' OR NISS_CVG_CD IS NULL THEN '???' ELSE NISS_CVG_CD END AS o_NISS_CVG_CD",
        # normalized territory
        "CASE WHEN trim(NISS_TERR_CD) = '' OR NISS_TERR_CD IS NULL THEN '???' ELSE NISS_TERR_CD END AS o_NISS_TERR_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "GRGNG_ZIP_5",
        # normalized class
        "CASE WHEN trim(NISS_CLASS_CD) = '' OR NISS_CLASS_CD IS NULL THEN '??????' ELSE NISS_CLASS_CD END AS o_NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD",
        # manufacturer model year padded to 2 chars
        "CASE WHEN length(trim(v_NISS_MNFCTRS_MDL_YR)) = 2 THEN v_NISS_MNFCTRS_MDL_YR ELSE concat('0', v_NISS_MNFCTRS_MDL_YR) END AS o_NISS_MNFCTRS_MDL_YR",
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
        "NISS_RSVD_CMPNY_USE"
    )

except Exception as e:
    logger.error(f"Failed EXP_PASS_FINAL_Csv transformation: {e}", exc_info=True)
    raise


# EXP_PASS_FINAL_Txt: compute local rounding/string variables and produce formatted/padded string outputs suitable for flat fixed-width/text layout
try:
    logger.info("Applying EXP_PASS_FINAL_Txt transformations")
    # first stage: compute local variables used by multiple derived ports
    df_txt_stage = df_EXP_defaults_pensive_maxwell.selectExpr(
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "CALL_YR",
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
        "NISS_MNFCTRS_MDL_YR",
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
        # numeric/string local variables: round then cast to string so last-digit mapping can run
        "CAST(ROUND(CAST(CVG_EXPS_VAL AS DOUBLE)) AS STRING) AS v_CVG_EXPS_VAL",
        "CAST(ROUND(CAST(TTL_WRITTN_PREM_AMT AS DOUBLE)) AS STRING) AS v_TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE",
        # local v for manufacturer year string
        "LTRIM(RTRIM(CAST(NISS_MNFCTRS_MDL_YR AS STRING))) AS v_NISS_MNFCTRS_MDL_YR"
    )

    # second stage: produce final formatted fields referencing local variables
    df_EXP_PASS_FINAL_Txt_magical_faraday = df_txt_stage.selectExpr(
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "substr(trim(CLNDR_YR),3,2) as o_CLNDR_YR",
        "CALL_YR",
        "substr(trim(CALL_YR),3,2) as o_REG_PRD_YR",
        "NISS_ST_CD",
        "LINE_CD",
        "ACCDNT_YR",
        # normalized coverage code
        "CASE WHEN trim(NISS_CVG_CD) = '' OR NISS_CVG_CD IS NULL THEN '???' ELSE NISS_CVG_CD END AS o_NISS_CVG_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "GRGNG_ZIP_5",
        # normalized class
        "CASE WHEN trim(NISS_CLASS_CD) = '' OR NISS_CLASS_CD IS NULL THEN '??????' ELSE NISS_CLASS_CD END AS o_NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD",
        # padded manufacturer model year
        "CASE WHEN length(trim(v_NISS_MNFCTRS_MDL_YR)) = 2 THEN v_NISS_MNFCTRS_MDL_YR ELSE concat('0', v_NISS_MNFCTRS_MDL_YR) END AS o_NISS_MNFCTRS_MDL_YR",
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
        # CVG_EXPS_VAL formatting: handle negative sign branch and last-digit mapping, then left-pad to 8
        "lpad( CASE WHEN substr(ltrim(rtrim(v_CVG_EXPS_VAL)),1,1) = '-' THEN concat( substr(v_CVG_EXPS_VAL,2, length(ltrim(rtrim(v_CVG_EXPS_VAL)))-2), \
            CASE substr(ltrim(rtrim(v_CVG_EXPS_VAL)), length(ltrim(rtrim(v_CVG_EXPS_VAL))), 1) \
                 WHEN '1' THEN 'J' WHEN '2' THEN 'K' WHEN '3' THEN 'L' WHEN '4' THEN 'M' WHEN '5' THEN 'N' WHEN '6' THEN 'O' WHEN '7' THEN 'P' WHEN '8' THEN 'Q' WHEN '9' THEN 'R' ELSE '}' END ) \
            ELSE v_CVG_EXPS_VAL END, 8, '0') AS CVG_EXPS_VAL",
        # TTL_WRITTN_PREM_AMT with identical formatting rules
        "lpad( CASE WHEN substr(ltrim(rtrim(v_TTL_WRITTN_PREM_AMT)),1,1) = '-' THEN concat( substr(ltrim(rtrim(v_TTL_WRITTN_PREM_AMT)),2, length(ltrim(rtrim(v_TTL_WRITTN_PREM_AMT)))-2), \
            CASE substr(ltrim(rtrim(v_TTL_WRITTN_PREM_AMT)), length(ltrim(rtrim(v_TTL_WRITTN_PREM_AMT))), 1) \
                 WHEN '1' THEN 'J' WHEN '2' THEN 'K' WHEN '3' THEN 'L' WHEN '4' THEN 'M' WHEN '5' THEN 'N' WHEN '6' THEN 'O' WHEN '7' THEN 'P' WHEN '8' THEN 'Q' WHEN '9' THEN 'R' ELSE '}' END ) \
            ELSE v_TTL_WRITTN_PREM_AMT END, 8, '0') AS TTL_WRITTN_PREM_AMT",
        # left-pad numeric output fields as strings to required widths
        "lpad(CAST(NISS_PD_LOSS AS STRING),8,'0') AS o_NISS_PD_LOSS",
        "lpad(CAST(NISS_PD_ALLOC_ADJUS_EXPNS AS STRING),8,'0') AS o_NISS_PD_ALLOC_ADJUS_EXPNS",
        "lpad(CAST(NISS_OUTSTNDG_LOSS AS STRING),8,'0') AS o_NISS_OUTSTNDG_LOSS",
        "lpad(CAST(NISS_NO_PD_CLMS AS STRING),5,'0') AS o_NISS_NO_PD_CLMS",
        "lpad(CAST(NISS_NO_OUTSTND_CLMS AS STRING),5,'0') AS o_NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE"
    )

except Exception as e:
    logger.error(f"Failed EXP_PASS_FINAL_Txt transformation: {e}", exc_info=True)
    raise


# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Csv -> flat CSV to PM_TARGET_FILE_DIR
try:
    logger.info("Writing flat-file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final (CSV) to local/remote file via pandas")
    pdf = df_EXP_PASS_FINAL_Csv_jovial_nash.toPandas()
    out_path = f"{PM_TARGET_FILE_DIR}/{OUTPUT_FILENAME_FNLCsv}"
    pdf.to_csv(out_path, index=False, mode='w')
    logger.info(f"Wrote final CSV flat file to {out_path}")
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_fierce_heisenberg = df_EXP_PASS_FINAL_Csv_jovial_nash
except Exception as e:
    logger.error(f"Failed writing final CSV flat file: {e}", exc_info=True)
    raise

# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt -> flat text to PM_TARGET_FILE_DIR (no header)
try:
    logger.info("Writing flat-file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final (TXT) to local/remote file via pandas")
    pdf = df_EXP_PASS_FINAL_Txt_magical_faraday.toPandas()
    out_path = f"{PM_TARGET_FILE_DIR}/{OUTPUT_FILENAME_FNLTxt}"
    # original session had 'No Header' for the TXT variant
    pdf.to_csv(out_path, index=False, header=False, mode='w')
    logger.info(f"Wrote final TXT flat file to {out_path}")
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt_careful_socrates = df_EXP_PASS_FINAL_Txt_magical_faraday
except Exception as e:
    logger.error(f"Failed writing final TXT flat file: {e}", exc_info=True)
    raise



job.commit()
