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

from pyspark.sql.functions import col

# Top-of-script placeholders for environment-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_CATALOG_DATABASE = "REPLACE_WITH_GLUE_CATALOG_DATABASE"

# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL (WRK_ intermediate — try S3 first, fall back to Glue Catalog)
SOURCE_NAME = "WRK_BIRP_TA_NISS_NU0C_APRM_DTL"

try:
    logger.info(f"Attempting to read Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from s3://{S3_OUTPUT_BUCKET}/{SOURCE_NAME}/")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/{SOURCE_NAME}/")
except Exception as e:
    logger.warning(f"Failed reading s3://{S3_OUTPUT_BUCKET}/{SOURCE_NAME}/, falling back to Glue Catalog for {SOURCE_NAME}: {e}")
    try:
        logger.info(f"Reading {SOURCE_NAME} from Glue Data Catalog database {GLUE_CATALOG_DATABASE}, table {SOURCE_NAME}")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_CATALOG_DATABASE, table_name=SOURCE_NAME)
        df_tmp = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading {SOURCE_NAME} from Glue Catalog: {e2}", exc_info=True)
        raise

# Project exactly the ports/columns listed on the Source node (no '*')
cols = [
    "FISC_PER_YR",
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
    "PLCY_IND",
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
    "GRGNG_ZIP",
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
    "WRITTN_PREM_AMT",
    "NJ_NO_LWST_LMT_IND",
    "NJ_NMD_DRVR_EXCL_IND",
    "COMP_DED",
    "COLL_DED",
    "PLCY_CNTRCT_NUM",
    "UNIT_NUM",
    "EFF_DT",
    "NUM_OF_CARS_IN_HH",
    "RDRVR_DT_OF_BRTH",
    "TERM_STRT_DT",
    "SRC_SYS_CD",
    "PNI_AGE",
    "MIS_LOB",
    "PRINCIPAL_OPRT",
    "SOURCE_IND_DERIVED",
    "ANTI_THFT_CTGY_CD",
]

try:
    logger.info("Projecting columns for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL into the mapping dataframe")
    # select the listed columns explicitly (no wildcard)
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin = df_tmp.select(*[col(c) for c in cols])
except Exception as e:
    logger.error(f"Failed projecting columns for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL: {e}", exc_info=True)
    raise


job.commit()
