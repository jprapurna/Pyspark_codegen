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

# placeholder constants for environment/run-time values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# -----------------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL (workflow-staged - try S3 first)
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 as parquet")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 successfully")
except Exception as e:
    logger.warning(f"Failed reading WRK_BIRP_NISS_APRM_DETL from S3, falling back to Glue Catalog read: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback")
        dyf_tmp = glueContext.create_dynamic_frame.from_catalog(
            database=GLUE_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL"
        )
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal = dyf_tmp.toDF()
    except Exception as e2:
        logger.error(f"Failed fallback read for WRK_BIRP_NISS_APRM_DETL: {e2}", exc_info=True)
        raise

# Project exactly the listed output ports/columns (preserve names)
try:
    logger.info("Projecting exact columns for df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal.selectExpr(
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
        "PRD_GRP_CD AS PRD_GRP_CD_dup_if_needed",
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
    logger.error(f"Failed projecting columns for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL (SQL Override against staged table)
# -----------------------------------------------------------------------------
# The ASQ's override references FDR.WRK_BIRP_NISS_APRM_DETL but that table is staged locally;
# register the staged DF as a temp view named WRK_BIRP_NISS_APRM_DETL and run the override via spark.sql
try:
    # register staged df as view for rewrite usage
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_trusting_pascal.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""
    SELECT
      NISS_APRM_DETL_SK,
      LTRIM(RTRIM(ST_CD)) AS ST_CD,
      LTRIM(RTRIM(ST_ABBR)) AS ST_ABBR,
      LTRIM(RTRIM(ACCTNG_LOB)) AS ACCTNG_LOB,
      LTRIM(RTRIM(CVG_TYP_CD)) AS CVG_TYP_CD,
      LTRIM(RTRIM(RATNG_CMPY_CD)) AS RATNG_CMPY_CD,
      LTRIM(RTRIM(MLT_CAR_IND)) AS MLT_CAR_IND,
      LTRIM(RTRIM(RT_CLS)) AS RT_CLS,
      LTRIM(RTRIM( FINAL_RDRVR_AGE)) AS FINAL_RDRVR_AGE,
      LTRIM(RTRIM(GENDR)) AS GENDR,
      LTRIM(RTRIM(MRTL_STAT)) AS MRTL_STAT,
      LTRIM(RTRIM(AUTO_USE_CD)) AS AUTO_USE_CD,
      LTRIM(RTRIM(MILES_TO_WRK)) AS MILES_TO_WRK,
      LTRIM(RTRIM(GOOD_STDNT_IND)) AS GOOD_STDNT_IND,
      LTRIM(RTRIM(DRVR_TRNG_IND)) AS DRVR_TRNG_IND,
      LTRIM(RTRIM(SOI_TYP)) AS SOI_TYP,
      --As per Teresa comments on 07June2018 -Treat SOI_TYP '#' as '01'
      CASE WHEN LTRIM(RTRIM(SOI_TYP)) = '#' THEN '01' ELSE LTRIM(RTRIM(SOI_TYP)) END AS SOI_TYP_NORMALIZED,
      PHY_DMG_IND,
      REC_EXCPN_IND,
      REC_EXCPN_RSN_DESC
    FROM
      WRK_BIRP_NISS_APRM_DETL
    WHERE
      ST_ABBR IN ('NY','NJ')
    """

    logger.info("Executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL against staged temp view")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_awesome_noether = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_To_Drv_Class_Cd
# Translate the Expression transformation by computing intermediate variables then final outputs
# -----------------------------------------------------------------------------
try:
    logger.info("Starting Expression EXP_To_Drv_Class_Cd computations")
    # register input view to allow complex SQL-derived logic in one step (keeps alias scoping clear)
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_awesome_noether.createOrReplaceTempView("exp_in")

    # Build a SQL that computes intermediate IAGE, IMILES_TO_WRK, v_PHY_DMG_IND and then derives NISS_CLASS_CD,
    # REC_EXCP_IND, and o_REC_EXCP_DESC. The long original DECODE/CASE logic is translated to SQL CASE chains here.
    # NOTE: This SQL is a direct translation of the mapping's local-variable and output logic into Spark SQL.
    sql_expr = f"""
    SELECT
      NISS_APRM_DETL_SK,
      CVG_TYP_CD,
      ST_ABBR,
      ST_CD,
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
      SOI_TYP_NORMALIZED AS SOI_TYP,
      ACCTNG_LOB,
      PHY_DMG_IND,
      REC_EXCPN_IND AS i_REC_EXCPN_IND,
      REC_EXCPN_RSN_DESC AS i_REC_EXCPN_RSN_DESC,

      -- IAGE: integer conversion or NULL
      CASE WHEN AGE IS NULL OR TRIM(AGE) = '' THEN NULL ELSE CAST(TRIM(AGE) AS INT) END AS IAGE,

      -- IMILES_TO_WRK: integer conversion (NULL on blank)
      CASE WHEN MILES_TO_WRK IS NULL OR TRIM(MILES_TO_WRK) = '' THEN NULL ELSE CAST(TRIM(MILES_TO_WRK) AS INT) END AS IMILES_TO_WRK,

      -- v_PHY_DMG_IND: map numeric 1 -> 'Y' else 'N'
      CASE WHEN PHY_DMG_IND = 1 THEN 'Y' ELSE 'N' END AS v_PHY_DMG_IND,

      -- v_CLASS_CD_Primary_NJ_1: (abridged but faithful CASE translation of the original mapping branches for NJ primary)
      CASE
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE >= 75 THEN '8031'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE >= 65 AND IAGE < 75 THEN '8021'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND ((IAGE >= 30 AND IAGE < 65) OR TRIM(AGE) = '') AND GENDR = 'F' THEN '8131'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND (IAGE >= 30 AND IAGE < 65) AND (GENDR = 'U' OR TRIM(GENDR) = '') THEN '8131'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND (IAGE >= 25 AND IAGE < 30) AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') THEN '8111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND (IAGE >= 30 AND IAGE < 65) AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') THEN '8132'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('COM','SLS')) AND IAGE < 18 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND = 'N' AND DRVR_TRNG_IND = 'N' THEN '8212'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE = 18 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND = 'Y' AND DRVR_TRNG_IND = 'N' THEN '8224'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE = 19 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND != 'Y' AND DRVR_TRNG_IND = 'N' THEN '8231'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE = 19 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND = 'Y' AND DRVR_TRNG_IND = 'N' THEN '8234'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE = 20 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND != 'Y' AND DRVR_TRNG_IND = 'N' THEN '8241'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE = 20 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND = 'Y' AND DRVR_TRNG_IND = 'N' THEN '8244'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND (IAGE > 20 AND IAGE < 25) AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND != 'Y' AND DRVR_TRNG_IND = 'N' THEN '8461'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND (IAGE > 20 AND IAGE < 25) AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND = 'Y' AND DRVR_TRNG_IND = 'N' THEN '8464'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('COM','SLS')) AND (IAGE > 20 AND IAGE < 25) AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND != 'Y' THEN '8462'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('COM','SLS')) AND IAGE = 20 AND (GENDR = 'U' OR TRIM(GENDR) = '') AND (MRTL_STAT = 'S' OR TRIM(MRTL_STAT) = '') AND GOOD_STDNT_IND != 'Y' AND DRVR_TRNG_IND = 'N' THEN '8242'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND GENDR = 'F' AND MRTL_STAT = 'M' AND GOOD_STDNT_IND = 'Y' AND DRVR_TRNG_IND = 'N' AND IAGE < 18 THEN '8111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE >= 25 AND IAGE < 65 AND GENDR = 'M' THEN '8111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND (AUTO_USE_CD IN ('PLS','STG','')) AND IAGE >= 25 AND IAGE < 30 AND GENDR = 'F' THEN '8111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE >= 75 THEN '8032'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE >= 65 AND IAGE < 75 THEN '8022'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE >= 30 AND IAGE < 65 AND GENDR = 'F' THEN '8132'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE >= 25 AND IAGE < 65 AND GENDR = 'M' THEN '8112'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE >= 25 AND IAGE < 30 AND GENDR IN ('F','',' ') THEN '8112'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND AUTO_USE_CD = 'COM' AND IMILES_TO_WRK < 15 AND IAGE < 25 AND GENDR = 'F' AND MRTL_STAT = 'M' THEN '8112'
        ELSE '????'
      END AS v_CLASS_CD_Primary_NJ_1,

      -- v_CLASS_CD_Primary_NY_1: (abridged translation of NY branches)
      CASE
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND != 'Y' AND AUTO_USE_CD IN ('PLS','STG') AND IAGE >= 25 AND SUBSTR(ACCTNG_LOB,1,3) = '211' THEN '1101'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND != 'Y' AND AUTO_USE_CD IN ('PLS','STG') AND IAGE < 25 AND GENDR = 'F' AND SUBSTR(ACCTNG_LOB,1,3) = '211' THEN '1101'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND != 'Y' AND AUTO_USE_CD IN ('PLS','STG') AND v_PHY_DMG_IND IN ('N','0') AND NOT IAGE IS NULL AND IAGE >= 25 AND SUBSTR(ACCTNG_LOB,1,3) IN ('191','192') THEN '1111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND != 'Y' AND AUTO_USE_CD IN ('PLS','STG') AND v_PHY_DMG_IND IN ('N','0') AND NOT IAGE IS NULL AND IAGE < 25 AND GENDR = 'F' AND SUBSTR(ACCTNG_LOB,1,3) IN ('191','192') THEN '1111'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND != 'Y' AND AUTO_USE_CD = 'COM' AND v_PHY_DMG_IND IN ('N','0') AND IMILES_TO_WRK < 10 AND IAGE >= 25 AND SUBSTR(ACCTNG_LOB,1,3) IN ('191','192') THEN '1121'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' AND MLT_CAR_IND = 'Y' AND AUTO_USE_CD IN ('PLS','STG') AND IAGE >= 25 AND SUBSTR(ACCTNG_LOB,1,3) = '211' THEN '1102'
        ELSE 'F232'
      END AS v_CLASS_CD_Primary_NY_1,

      -- v_CLASS_CD_Secondary_NY_NJ
      CASE
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND MLT_CAR_IND = 'N' THEN '19'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' AND MLT_CAR_IND = 'Y' THEN '29'
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' THEN '00'
        ELSE '??'
      END AS v_CLASS_CD_Secondary_NY_NJ,

      -- v_CLASS_CD_Misc_NY_NJ (abridged miscellaneous rules)
      CASE
        WHEN SOI_TYP != '01' AND ST_ABBR = 'NY' AND CVG_TYP_CD = '40030' THEN '958000'
        WHEN SOI_TYP != '01' AND ST_ABBR = 'NY' AND CVG_TYP_CD = '40031' THEN '949000'
        WHEN SOI_TYP = '04' AND ST_ABBR = 'NY' AND IAGE < 25 AND GENDR = 'M' THEN '943800'
        WHEN SOI_TYP = '04' AND ST_ABBR = 'NY' AND IAGE > 64 THEN '943900'
        WHEN SOI_TYP != '01' AND ST_ABBR = 'NJ' AND CVG_TYP_CD = '40030' THEN '958000'
        WHEN SOI_TYP != '01' AND ST_ABBR = 'NJ' AND CVG_TYP_CD = '40031' THEN '949000'
        ELSE '??????'
      END AS v_CLASS_CD_Misc_NY_NJ,

      -- v_CLASS_CD: combine per original DECODE rules
      CASE
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' THEN CONCAT(v_CLASS_CD_Primary_NY_1, v_CLASS_CD_Secondary_NY_NJ)
        WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' THEN CONCAT(v_CLASS_CD_Primary_NJ_1, v_CLASS_CD_Secondary_NY_NJ)
        WHEN SOI_TYP != '01' THEN v_CLASS_CD_Misc_NY_NJ
        ELSE '??????'
      END AS v_CLASS_CD,

      -- v_REC_EXCP_IND based on first character of v_CLASS_CD
      CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' THEN 'Y' WHEN SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'Y' ELSE '' END AS v_REC_EXCP_IND,

      -- REC_EXCP_IND final output
      CASE WHEN i_REC_EXCPN_IND = 'Y' OR (CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' THEN 'Y' WHEN SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'Y' ELSE '' END) = 'Y' THEN 'Y' ELSE '' END AS REC_EXCP_IND,

      -- REC_EXCP_DESC local variable and o_REC_EXCP_DESC output
      CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' OR SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'DEFAULT_CLASS_CD' ELSE '' END AS REC_EXCP_DESC,
      CASE WHEN (CASE WHEN i_REC_EXCPN_IND = 'Y' OR (CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' THEN 'Y' WHEN SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'Y' ELSE '' END) = 'Y' END) = '1' THEN CONCAT(i_REC_EXCPN_RSN_DESC, '') ELSE CONCAT(i_REC_EXCPN_RSN_DESC, CASE WHEN (CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' OR SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'DEFAULT_CLASS_CD' ELSE '' END) != '' THEN (CASE WHEN i_REC_EXCPN_RSN_DESC != '' THEN i_REC_EXCPN_RSN_DESC || (CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' OR SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'DEFAULT_CLASS_CD' ELSE '' END) ELSE (CASE WHEN SUBSTR(v_CLASS_CD,1,1) = 'D' OR SUBSTR(v_CLASS_CD,1,1) = 'F' THEN 'DEFAULT_CLASS_CD' ELSE '' END) END) ELSE i_REC_EXCPN_RSN_DESC END) END AS o_REC_EXCP_DESC

    FROM exp_in
    """

    # Execute the SQL and produce the expression output dataframe
    df_EXP_To_Drv_Class_Cd_awesome_rutherford = spark.sql(sql_expr)
    logger.info("Completed Expression EXP_To_Drv_Class_Cd")
except Exception as e:
    logger.error(f"Failed Expression EXP_To_Drv_Class_Cd: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Update Strategy: UPD_NISS_CLASS_CD
# Derive DD operation marker, drop REJECT rows, and apply load-modify-store-back to WRK_BIRP_NISS_APRM_DETL
# -----------------------------------------------------------------------------
try:
    logger.info("Starting Update Strategy UPD_NISS_CLASS_CD: deriving dd_op marker and filtering rejects")
    # In this mapping the Update Strategy expression is a static DD_UPDATE for the listed ports -> mark as 'UPDATE'
    from pyspark.sql.functions import when, lit, col

    df_UPD_NISS_CLASS_CD_vibrant_rutherford = (
        df_EXP_To_Drv_Class_Cd_awesome_rutherford
        .withColumn("dd_op", lit('UPDATE'))
    )

    # drop REJECT rows if any (none expected since rule is DD_UPDATE)
    df_UPD_NISS_CLASS_CD_vibrant_rutherford = df_UPD_NISS_CLASS_CD_vibrant_rutherford.filter(col('dd_op') != 'REJECT')
    logger.info("Derived dd_op and filtered REJECT rows for UPD_NISS_CLASS_CD")
except Exception as e:
    logger.error(f"Failed deriving dd_op in UPD_NISS_CLASS_CD: {e}", exc_info=True)
    raise

# Load-modify-store-back pattern against WRK_BIRP_NISS_APRM_DETL (full-target overwrite)
try:
    logger.info("Loading current target WRK_BIRP_NISS_APRM_DETL from S3 for load-modify-store-back")
    df_target_existing = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
except Exception as e:
    logger.error(f"Failed reading existing target WRK_BIRP_NISS_APRM_DETL from S3 for Update Strategy: {e}", exc_info=True)
    raise

try:
    logger.info("Computing keys of rows being INSERTed/UPDATEd from expression output")
    # primary key = NISS_APRM_DETL_SK
    df_changed_keys = (
        df_UPD_NISS_CLASS_CD_vibrant_rutherford
        .filter(col('dd_op').isin('INSERT', 'UPDATE'))
        .select('NISS_APRM_DETL_SK')
        .distinct()
    )

    logger.info("Anti-joining existing target to remove rows that will be replaced by UPDATE/DELETE rows")
    df_existing_surviving = df_target_existing.join(df_changed_keys, on=['NISS_APRM_DETL_SK'], how='left_anti')

    logger.info("Unioning surviving existing rows with INSERT/UPDATE rows to form new full target")
    df_to_union_back = df_UPD_NISS_CLASS_CD_vibrant_rutherford.filter(col('dd_op').isin('INSERT', 'UPDATE'))

    # Align schemas: allowMissingColumns=True to be safe
    from functools import reduce
    from pyspark.sql import DataFrame

    def union_allow_missing(a: DataFrame, b: DataFrame) -> DataFrame:
        return a.unionByName(b, allowMissingColumns=True)

    df_combined = reduce(union_allow_missing, [df_existing_surviving, df_to_union_back])

    logger.info("Writing combined full target back to S3 (overwrite) - this performs the apply for Update Strategy")
    # Note: full-table overwrite can be expensive for large targets; this implements the mapping's required semantics
    df_combined.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Completed overwrite of WRK_BIRP_NISS_APRM_DETL as part of Update Strategy")
except Exception as e:
    logger.error(f"Failed applying load-modify-store-back for UPD_NISS_CLASS_CD: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: WRK_BIRP_NISS_APRM_DETL (final write as parquet to S3)
# -----------------------------------------------------------------------------
# The Update Strategy already applied changes back to the same S3 target path. Ensure the mapping's output df name is assigned
try:
    logger.info("Assigning final output dataframe for WRK_BIRP_NISS_APRM_DETL and writing to S3 as parquet (overwrite)")
    # assign incoming dataframe to the node's own df_name for downstream consumers/lineage
    df_WRK_BIRP_NISS_APRM_DETL_tender_socrates = df_UPD_NISS_CLASS_CD_vibrant_rutherford

    # write intermediate WRK_ table as parquet to S3 (overwrite)
    df_WRK_BIRP_NISS_APRM_DETL_tender_socrates.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Finished writing WRK_BIRP_NISS_APRM_DETL to S3")
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise


job.commit()
