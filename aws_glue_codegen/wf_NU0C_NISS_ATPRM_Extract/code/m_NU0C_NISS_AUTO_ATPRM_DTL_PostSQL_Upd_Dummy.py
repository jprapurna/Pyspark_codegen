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

# -------------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1
# Attempt an S3-first read of the intermediate table WRK_BIRP_NISS_APRM_DETL_1.
# If the S3 path is missing, fall back to the Glue Data Catalog read.
try:
    logger.info("Attempting S3-first read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (WRK_BIRP_NISS_APRM_DETL_1)")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_heisenberg = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_1/")
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL_1 from S3 successfully")
except Exception as e:
    # Allowed fallback: try Glue Catalog on S3 read failure
    logger.warning("S3 path for WRK_BIRP_NISS_APRM_DETL_1 not available, falling back to Glue Data Catalog read")
    try:
        logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL_1")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_heisenberg = dyf.toDF()
        logger.info("Read WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog successfully")
    except Exception as e:
        logger.error(f"Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog: {e}", exc_info=True)
        raise

# -------------------------------------------------------------------------
# Source: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (Application Source Qualifier with SQL Override)
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (bypassed — SQL Override below reads it directly)
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
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_busy_ramanujan = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
    logger.info("Read SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake successfully")
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# EXPTRANS: explicit passthrough projection of the SQ outputs
try:
    logger.info("Applying EXPTRANS explicit projection")
    df_EXPTRANS_busy_hopper = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_busy_ramanujan.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_ABBR',
        'ACCTNG_LOB',
        'BI_LMT',
        'PRD_GRP_CD',
        'NJ_NO_LWST_LMT_IND',
        'NJ_NMD_DRVR_EXCL_IND',
        'CVG_TYP_CD'
    )
    logger.info("EXPTRANS projection applied successfully")
except Exception as e:
    logger.error(f"Failed applying EXPTRANS projection: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL -> write as parquet to S3 (strip 'FDR_LIB_' prefix for path)
try:
    logger.info("Projecting final columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL and writing to S3 as parquet (overwrite)")
    # Build explicit projection: use available upstream columns where present; emit NULL for any column not produced by the upstream SQ.
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_focused_archimedes = df_EXPTRANS_busy_hopper.selectExpr(
        'NISS_APRM_DETL_SK',
        "NULL AS CLNDR_YR",
        "NULL AS CALL_YR",
        "NULL AS NAIC_CMPNY_CD",
        "NULL AS NISS_CMPNY_CD",
        "NULL AS ST_NM",
        "NULL AS ST_CD",
        "NULL AS NISS_ST_CD",
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        "NULL AS CVG_AMT",
        'BI_LMT',
        "NULL AS GA_ADDED_AT_FAULT_IND",
        "NULL AS FA2_PLCY_IND",
        "NULL AS UM_UMI_STACKING",
        "NULL AS PIP_WVR_WL_IND",
        "NULL AS PIP_MED_SEC_IND",
        "NULL AS PIP_LOSS_INCOME_IND",
        "NULL AS MI_PPO_IND",
        'PRD_GRP_CD',
        "NULL AS NJ_HLTH_INSR_PRIM",
        "NULL AS NJ_EXTR_PIP_PKG",
        "NULL AS NJ_RESDNC_RLTNSHP_PIP_IND",
        "NULL AS NY_SSL_IND",
        "NULL AS NY_FULL_CVG_GLASS_COMP_IND",
        "NULL AS GRGNG_ZIP_5",
        "NULL AS NISS_TERR_CD",
        "NULL AS RATNG_CMPY_CD",
        "NULL AS MLT_CAR_IND",
        "NULL AS RT_CLS",
        "NULL AS AGE",
        "NULL AS GENDR",
        "NULL AS MRTL_STAT",
        "NULL AS AUTO_USE_CD",
        "NULL AS MILES_TO_WRK",
        "NULL AS GOOD_STDNT_IND",
        "NULL AS DRVR_TRNG_IND",
        "NULL AS SOI_TYP",
        "NULL AS PHY_DMG_IND",
        "NULL AS NJ_RATD_PNTS",
        "NULL AS VEH_MDL_YR",
        "NULL AS NJ_EXCPTION_CD",
        "NULL AS NJ_FGVN_PNTS",
        "NULL AS PASSV_RESTRA_DISC",
        "NULL AS SNR_DRVR_IND",
        "NULL AS DEFNS_DRVR_DISC_IND",
        "NULL AS ANTI_THFT_DISC",
        "NULL AS DAY_TM_RUN_LIGHTS",
        "NULL AS LMT_TORT",
        "NULL AS ANNL_STMNT_LOB_CD",
        "NULL AS CVG_TYP_IND",
        "NULL AS CVG_EXPS_VAL",
        "NULL AS TTL_WRITTN_PREM_AMT",
        "NULL AS LINE_CD",
        "NULL AS ACCDNT_YR",
        "NULL AS NISS_CVG_CD",
        "NULL AS RTNG_ZNE_CD",
        "NULL AS TERM_ZNE_CD",
        "NULL AS NISS_CLASS_CD",
        "NULL AS NISS_ELIG_PNTS_CD",
        "NULL AS NISS_AGE_GRP_CD",
        "NULL AS NISS_CMMCL_IND_CD",
        "NULL AS NISS_EXCPN_CD",
        "NULL AS NISS_FGVNS_CD",
        "NULL AS NISS_PASSV_RESTRA_CD",
        "NULL AS NISS_DEFNS_DRVR_CRD_CD",
        "NULL AS NISS_ANTI_THFT_DVC_CD",
        "NULL AS NISS_DAY_TM_RUN_LAMPS_DISC_CD",
        "NULL AS NISS_PLCY_LMT_CD",
        "NULL AS NISS_DEDUC_CD",
        "NULL AS NISS_SSL_LIAB_CD",
        "NULL AS NISS_SUBLOB_CD",
        "NULL AS NISS_TYP_LOSS_CD",
        "NULL AS NISS_LIAB_OR_NO_FAULT_CD",
        "NULL AS NISS_ANNL_STMNT_LOB_CD",
        "NULL AS NISS_PD_LOSS",
        "NULL AS NISS_PD_ALLOC_ADJUS_EXPNS",
        "NULL AS NISS_OUTSTNDG_LOSS",
        "NULL AS NISS_NO_PD_CLMS",
        "NULL AS NISS_NO_OUTSTND_CLMS",
        "NULL AS RSVD_NISS_USE",
        "NULL AS NISS_RSVD_CMPNY_USE",
        "NULL AS NISS_MNFCTRS_MDL_YR",
        "NULL AS CR_BY_MAPNG_ID",
        "NULL AS DW_CR_TMSP",
        "NULL AS UPD_BY_MAPNG_ID",
        "NULL AS DW_UPD_TMSP",
        "NULL AS WRK_FLOW_RUN_ID",
        'NJ_NO_LWST_LMT_IND',
        'NJ_NMD_DRVR_EXCL_IND',
        "NULL AS EXPS_VAL_ROLLED",
        "NULL AS CVG_CNT_IND",
        "NULL AS CVG_CNT",
        "NULL AS CVG_CD_SK",
        "NULL AS CVG_ATTR_SK",
        "NULL AS REC_DROP_IND",
        "NULL AS REC_DROP_RSN_DESC",
        "NULL AS REC_EXCPN_IND",
        "NULL AS REC_EXCPN_RSN_DESC",
        "NULL AS CVG_ATTR_CHCKSUM",
        "NULL AS COMP_DED",
        "NULL AS COLL_DED",
        "NULL AS PLCY_CNTRCT_NUM",
        "NULL AS UNIT_NUM",
        "NULL AS EFF_DT",
        "NULL AS NUM_OF_CARS_IN_HH",
        "NULL AS RDRVR_DT_OF_BRTH",
        "NULL AS TERM_STRT_DT",
        "NULL AS SRC_SYS_CD",
        "NULL AS DERIVED_RDRVR_AGE",
        "NULL AS FINAL_RDRVR_AGE",
        "NULL AS PNI_AGE",
        "NULL AS LOB",
        "NULL AS PRINCIPAL_OPRT",
        "NULL AS SOURCE_IND_DERIVED"
    )

    # write intermediate WRK_ table as parquet to S3 (overwrite)
    logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_focused_archimedes.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Write to s3://{}/WRK_BIRP_NISS_APRM_DETL/ completed".format(S3_OUTPUT_BUCKET))
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise


job.commit()
