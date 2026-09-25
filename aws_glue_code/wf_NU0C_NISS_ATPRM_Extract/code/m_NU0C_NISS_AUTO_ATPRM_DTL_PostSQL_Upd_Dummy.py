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

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (WRK_ intermediate) - try S3 parquet first, then fall back to Glue Catalog
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL_1 from S3 parquet")
    df_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_1/")
        .selectExpr(
            "NISS_APRM_DETL_SK",
            "ST_ABBR",
            "ACCTNG_LOB",
            "BI_LMT",
            "PRD_GRP_CD",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "CVG_TYP_CD",
        )
    )
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL_1 not available on S3, falling back to Glue Catalog read")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = (
            dyf.toDF()
            .selectExpr(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "BI_LMT",
                "PRD_GRP_CD",
                "NJ_NO_LWST_LMT_IND",
                "NJ_NMD_DRVR_EXCL_IND",
                "CVG_TYP_CD",
            )
        )
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback: {e}", exc_info=True)
        raise

# Source Qualifier: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (session-level SQL Override; default/non-staged case)
# Note: PreSQL/PostSQL are defined in the session and will run on Snowflake side as part of the JDBC read operation.
sql_query = f"""--This is a dummy session that does not read any rows
SELECT 
NISS_APRM_DETL_SK,
ST_ABBR,
ACCTNG_LOB,
BI_LMT,
PRD_GRP_CD,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
CVG_TYP_CD

FROM FDR.WRK_BIRP_NISS_APRM_DETL
WHERE 1=2"""

try:
    logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake via JDBC override query")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake: {e}", exc_info=True)
    raise

# EXPTRANS: pure passthrough of the SQ columns
try:
    logger.info("Applying EXPTRANS passthrough projection")
    df_EXPTRANS_humble_socrates = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid.selectExpr(
        "NISS_APRM_DETL_SK",
        "ST_ABBR",
        "ACCTNG_LOB",
        "BI_LMT",
        "PRD_GRP_CD",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "CVG_TYP_CD",
    )
except Exception as e:
    logger.error(f"Failed applying EXPTRANS projection: {e}", exc_info=True)
    raise

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL: write target as S3 parquet (strip FDR_LIB_ prefix => WRK_BIRP_NISS_APRM_DETL)
# Note: design-time writer was Snowflake, but for Glue lineage and downstream jobs we persist as parquet to S3.
try:
    logger.info("Projecting target columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL and writing to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_pascal = df_EXPTRANS_humble_socrates.selectExpr(
        # available upstream passthrough columns
        "NISS_APRM_DETL_SK",
        "CLNDR_YR AS CLNDR_YR" if False else "NULL AS CLNDR_YR",
        "CALL_YR AS CALL_YR" if False else "NULL AS CALL_YR",
        "NAIC_CMPNY_CD AS NAIC_CMPNY_CD" if False else "NULL AS NAIC_CMPNY_CD",
        "NISS_CMPNY_CD AS NISS_CMPNY_CD" if False else "NULL AS NISS_CMPNY_CD",
        "ST_NM AS ST_NM" if False else "NULL AS ST_NM",
        "ST_CD AS ST_CD" if False else "NULL AS ST_CD",
        "NISS_ST_CD AS NISS_ST_CD" if False else "NULL AS NISS_ST_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT AS CVG_AMT" if False else "NULL AS CVG_AMT",
        "BI_LMT",
        "GA_ADDED_AT_FAULT_IND AS GA_ADDED_AT_FAULT_IND" if False else "NULL AS GA_ADDED_AT_FAULT_IND",
        "FA2_PLCY_IND AS FA2_PLCY_IND" if False else "NULL AS FA2_PLCY_IND",
        "UM_UMI_STACKING AS UM_UMI_STACKING" if False else "NULL AS UM_UMI_STACKING",
        "PIP_WVR_WL_IND AS PIP_WVR_WL_IND" if False else "NULL AS PIP_WVR_WL_IND",
        "PIP_MED_SEC_IND AS PIP_MED_SEC_IND" if False else "NULL AS PIP_MED_SEC_IND",
        "PIP_LOSS_INCOME_IND AS PIP_LOSS_INCOME_IND" if False else "NULL AS PIP_LOSS_INCOME_IND",
        "MI_PPO_IND AS MI_PPO_IND" if False else "NULL AS MI_PPO_IND",
        "PRD_GRP_CD",
        "NJ_HLTH_INSR_PRIM AS NJ_HLTH_INSR_PRIM" if False else "NULL AS NJ_HLTH_INSR_PRIM",
        "NJ_EXTR_PIP_PKG AS NJ_EXTR_PIP_PKG" if False else "NULL AS NJ_EXTR_PIP_PKG",
        "NJ_RESDNC_RLTNSHP_PIP_IND AS NJ_RESDNC_RLTNSHP_PIP_IND" if False else "NULL AS NJ_RESDNC_RLTNSHP_PIP_IND",
        "NY_SSL_IND AS NY_SSL_IND" if False else "NULL AS NY_SSL_IND",
        "NY_FULL_CVG_GLASS_COMP_IND AS NY_FULL_CVG_GLASS_COMP_IND" if False else "NULL AS NY_FULL_CVG_GLASS_COMP_IND",
        "GRGNG_ZIP_5 AS GRGNG_ZIP_5" if False else "NULL AS GRGNG_ZIP_5",
        "NISS_TERR_CD AS NISS_TERR_CD" if False else "NULL AS NISS_TERR_CD",
        "RATNG_CMPY_CD AS RATNG_CMPY_CD" if False else "NULL AS RATNG_CMPY_CD",
        "MLT_CAR_IND AS MLT_CAR_IND" if False else "NULL AS MLT_CAR_IND",
        "RT_CLS AS RT_CLS" if False else "NULL AS RT_CLS",
        "AGE AS AGE" if False else "NULL AS AGE",
        "GENDR AS GENDR" if False else "NULL AS GENDR",
        "MRTL_STAT AS MRTL_STAT" if False else "NULL AS MRTL_STAT",
        "AUTO_USE_CD AS AUTO_USE_CD" if False else "NULL AS AUTO_USE_CD",
        "MILES_TO_WRK AS MILES_TO_WRK" if False else "NULL AS MILES_TO_WRK",
        "GOOD_STDNT_IND AS GOOD_STDNT_IND" if False else "NULL AS GOOD_STDNT_IND",
        "DRVR_TRNG_IND AS DRVR_TRNG_IND" if False else "NULL AS DRVR_TRNG_IND",
        "SOI_TYP AS SOI_TYP" if False else "NULL AS SOI_TYP",
        "PHY_DMG_IND AS PHY_DMG_IND" if False else "NULL AS PHY_DMG_IND",
        "NJ_RATD_PNTS AS NJ_RATD_PNTS" if False else "NULL AS NJ_RATD_PNTS",
        "VEH_MDL_YR AS VEH_MDL_YR" if False else "NULL AS VEH_MDL_YR",
        "NJ_EXCPTION_CD AS NJ_EXCPTION_CD" if False else "NULL AS NJ_EXCPTION_CD",
        "NJ_FGVN_PNTS AS NJ_FGVN_PNTS" if False else "NULL AS NJ_FGVN_PNTS",
        "PASSV_RESTRA_DISC AS PASSV_RESTRA_DISC" if False else "NULL AS PASSV_RESTRA_DISC",
        "SNR_DRVR_IND AS SNR_DRVR_IND" if False else "NULL AS SNR_DRVR_IND",
        "DEFNS_DRVR_DISC_IND AS DEFNS_DRVR_DISC_IND" if False else "NULL AS DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC AS ANTI_THFT_DISC" if False else "NULL AS ANTI_THFT_DISC",
        "DAY_TM_RUN_LIGHTS AS DAY_TM_RUN_LIGHTS" if False else "NULL AS DAY_TM_RUN_LIGHTS",
        "LMT_TORT AS LMT_TORT" if False else "NULL AS LMT_TORT",
        "ANNL_STMNT_LOB_CD AS ANNL_STMNT_LOB_CD" if False else "NULL AS ANNL_STMNT_LOB_CD",
        "CVG_TYP_IND AS CVG_TYP_IND" if False else "NULL AS CVG_TYP_IND",
        "CVG_EXPS_VAL AS CVG_EXPS_VAL" if False else "NULL AS CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT AS TTL_WRITTN_PREM_AMT" if False else "NULL AS TTL_WRITTN_PREM_AMT",
        "LINE_CD AS LINE_CD" if False else "NULL AS LINE_CD",
        "ACCDNT_YR AS ACCDNT_YR" if False else "NULL AS ACCDNT_YR",
        "NISS_CVG_CD AS NISS_CVG_CD" if False else "NULL AS NISS_CVG_CD",
        "RTNG_ZNE_CD AS RTNG_ZNE_CD" if False else "NULL AS RTNG_ZNE_CD",
        "TERM_ZNE_CD AS TERM_ZNE_CD" if False else "NULL AS TERM_ZNE_CD",
        "NISS_CLASS_CD AS NISS_CLASS_CD" if False else "NULL AS NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD AS NISS_ELIG_PNTS_CD" if False else "NULL AS NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD AS NISS_AGE_GRP_CD" if False else "NULL AS NISS_AGE_GRP_CD",
        "NISS_CMMCL_IND_CD AS NISS_CMMCL_IND_CD" if False else "NULL AS NISS_CMMCL_IND_CD",
        "NISS_EXCPN_CD AS NISS_EXCPN_CD" if False else "NULL AS NISS_EXCPN_CD",
        "NISS_FGVNS_CD AS NISS_FGVNS_CD" if False else "NULL AS NISS_FGVNS_CD",
        "NISS_PASSV_RESTRA_CD AS NISS_PASSV_RESTRA_CD" if False else "NULL AS NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD AS NISS_DEFNS_DRVR_CRD_CD" if False else "NULL AS NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD AS NISS_ANTI_THFT_DVC_CD" if False else "NULL AS NISS_ANTI_THFT_DVC_CD",
        "NISS_DAY_TM_RUN_LAMPS_DISC_CD AS NISS_DAY_TM_RUN_LAMPS_DISC_CD" if False else "NULL AS NISS_DAY_TM_RUN_LAMPS_DISC_CD",
        "NISS_PLCY_LMT_CD AS NISS_PLCY_LMT_CD" if False else "NULL AS NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD AS NISS_DEDUC_CD" if False else "NULL AS NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD AS NISS_SSL_LIAB_CD" if False else "NULL AS NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD AS NISS_SUBLOB_CD" if False else "NULL AS NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD AS NISS_TYP_LOSS_CD" if False else "NULL AS NISS_TYP_LOSS_CD",
        "NISS_LIAB_OR_NO_FAULT_CD AS NISS_LIAB_OR_NO_FAULT_CD" if False else "NULL AS NISS_LIAB_OR_NO_FAULT_CD",
        "NISS_ANNL_STMNT_LOB_CD AS NISS_ANNL_STMNT_LOB_CD" if False else "NULL AS NISS_ANNL_STMNT_LOB_CD",
        "NISS_PD_LOSS AS NISS_PD_LOSS" if False else "NULL AS NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS AS NISS_PD_ALLOC_ADJUS_EXPNS" if False else "NULL AS NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS AS NISS_OUTSTNDG_LOSS" if False else "NULL AS NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS AS NISS_NO_PD_CLMS" if False else "NULL AS NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS AS NISS_NO_OUTSTND_CLMS" if False else "NULL AS NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE AS RSVD_NISS_USE" if False else "NULL AS RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE AS NISS_RSVD_CMPNY_USE" if False else "NULL AS NISS_RSVD_CMPNY_USE",
        "NISS_MNFCTRS_MDL_YR AS NISS_MNFCTRS_MDL_YR" if False else "NULL AS NISS_MNFCTRS_MDL_YR",
        "CR_BY_MAPNG_ID AS CR_BY_MAPNG_ID" if False else "NULL AS CR_BY_MAPNG_ID",
        "DW_CR_TMSP AS DW_CR_TMSP" if False else "NULL AS DW_CR_TMSP",
        "UPD_BY_MAPNG_ID AS UPD_BY_MAPNG_ID" if False else "NULL AS UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP AS DW_UPD_TMSP" if False else "NULL AS DW_UPD_TMSP",
        "WRK_FLOW_RUN_ID AS WRK_FLOW_RUN_ID" if False else "NULL AS WRK_FLOW_RUN_ID",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "EXPS_VAL_ROLLED AS EXPS_VAL_ROLLED" if False else "NULL AS EXPS_VAL_ROLLED",
        "CVG_CNT_IND AS CVG_CNT_IND" if False else "NULL AS CVG_CNT_IND",
        "CVG_CNT AS CVG_CNT" if False else "NULL AS CVG_CNT",
        "CVG_CD_SK AS CVG_CD_SK" if False else "NULL AS CVG_CD_SK",
        "CVG_ATTR_SK AS CVG_ATTR_SK" if False else "NULL AS CVG_ATTR_SK",
        "REC_DROP_IND AS REC_DROP_IND" if False else "NULL AS REC_DROP_IND",
        "REC_DROP_RSN_DESC AS REC_DROP_RSN_DESC" if False else "NULL AS REC_DROP_RSN_DESC",
        "REC_EXCPN_IND AS REC_EXCPN_IND" if False else "NULL AS REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC AS REC_EXCPN_RSN_DESC" if False else "NULL AS REC_EXCPN_RSN_DESC",
        "CVG_ATTR_CHCKSUM AS CVG_ATTR_CHCKSUM" if False else "NULL AS CVG_ATTR_CHCKSUM",
        "COMP_DED AS COMP_DED" if False else "NULL AS COMP_DED",
        "COLL_DED AS COLL_DED" if False else "NULL AS COLL_DED",
        "PLCY_CNTRCT_NUM AS PLCY_CNTRCT_NUM" if False else "NULL AS PLCY_CNTRCT_NUM",
        "UNIT_NUM AS UNIT_NUM" if False else "NULL AS UNIT_NUM",
        "EFF_DT AS EFF_DT" if False else "NULL AS EFF_DT",
        "NUM_OF_CARS_IN_HH AS NUM_OF_CARS_IN_HH" if False else "NULL AS NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH AS RDRVR_DT_OF_BRTH" if False else "NULL AS RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT AS TERM_STRT_DT" if False else "NULL AS TERM_STRT_DT",
        "SRC_SYS_CD AS SRC_SYS_CD" if False else "NULL AS SRC_SYS_CD",
        "DERIVED_RDRVR_AGE AS DERIVED_RDRVR_AGE" if False else "NULL AS DERIVED_RDRVR_AGE",
        "FINAL_RDRVR_AGE AS FINAL_RDRVR_AGE" if False else "NULL AS FINAL_RDRVR_AGE",
        "PNI_AGE AS PNI_AGE" if False else "NULL AS PNI_AGE",
        "LOB AS LOB" if False else "NULL AS LOB",
        "PRINCIPAL_OPRT AS PRINCIPAL_OPRT" if False else "NULL AS PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED AS SOURCE_IND_DERIVED" if False else "NULL AS SOURCE_IND_DERIVED",
    )

    # write intermediate WRK_ table as parquet to S3 (overwrite)
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_pascal.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Successfully wrote WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
except Exception as e:
    logger.error(f"Failed projecting/writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
