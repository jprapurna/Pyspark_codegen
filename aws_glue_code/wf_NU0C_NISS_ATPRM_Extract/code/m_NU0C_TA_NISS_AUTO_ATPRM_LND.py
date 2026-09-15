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
SOURCE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
SOURCE_TABLE = "WRK_BIRP_TA_NISS_NU0C_APRM_LND"

# Attempt to read staging WRK_ table from S3 first; if missing, fall back to Glue Data Catalog
try:
    logger.info("Attempting to read WRK_BIRP_TA_NISS_NU0C_APRM_LND from s3 bucket as parquet")
    df_s3_read = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/{SOURCE_TABLE}/")
    df_raw = df_s3_read
except Exception as e:
    logger.warning(
        f"Failed to read WRK_BIRP_TA_NISS_NU0C_APRM_LND from s3://{S3_OUTPUT_BUCKET}/{SOURCE_TABLE}/; falling back to Glue Data Catalog read: {e}"
    )
    try:
        logger.info(f"Reading {SOURCE_TABLE} from Glue Data Catalog (database={SOURCE_DATABASE}, table={SOURCE_TABLE})")
        dynamic_df = glueContext.create_dynamic_frame_from_catalog(database=SOURCE_DATABASE, table_name=SOURCE_TABLE)
        df_catalog = dynamic_df.toDF()
        df_raw = df_catalog
    except Exception as e2:
        logger.error(f"Failed reading {SOURCE_TABLE} from Glue Data Catalog: {e2}", exc_info=True)
        raise

# Project exactly the columns listed on the source node
try:
    logger.info("Projecting exact output ports for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND")
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = df_raw.selectExpr(
        'NISS_APRM_LND_SK',
        'REG_PER_YR',
        'FISC_PER_YR',
        'NAIC_CMPNY_CD',
        'NISS_CMPNY_CD',
        'ST_NM',
        'ST_CD',
        'NISS_ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'BI_LMT',
        'GA_ADDED_AT_FAULT_IND',
        'PLCY_IND',
        'UM_UMI_STACKING',
        'PIP_WVR_WL_IND',
        'PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND',
        'MI_PPO_IND',
        'PRD_GRP_CD',
        'NJ_HLTH_INSR_PRIM',
        'NJ_EXTR_PIP_PKG',
        'NJ_RESDNC_RLTNSHP_PIP_IND',
        'NY_SSL_IND',
        'NY_FULL_CVG_GLASS_COMP_IND',
        'GRGNG_ZIP',
        'NISS_TERR_CD',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'RT_CLS',
        'AGE',
        'GENDR',
        'MRTL_STAT',
        'AUTO_USE_CD',
        'MILES_TO_WRK',
        'GOOD_STDNT_IND',
        'DRVR_TRNG_IND',
        'SOI_TYP',
        'PHY_DMG_IND',
        'NJ_RATD_PNTS',
        'VEH_MDL_YR',
        'NJ_EXCPTION_CD',
        'NJ_FGVN_PNTS',
        'PASSV_RESTRA_DISC',
        'SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND',
        'ANTI_THFT_DISC',
        'DAY_TM_RUN_LIGHTS',
        'LMT_TORT',
        'ANNL_STMNT_LOB_CD',
        'CVG_TYP_IND',
        'CVG_EXPS_VAL',
        'WRITTN_PREM_AMT',
        'CR_BY_MAPNG_ID',
        'DW_CR_TMSP',
        'UPD_BY_MAPNG_ID',
        'DW_UPD_TMSP',
        'WRK_FLOW_RUN_ID',
        'NJ_NO_LWST_LMT_IND',
        'NJ_NMD_DRVR_EXCL_IND',
        'EXPS_VAL_ROLLED',
        'COMP_DED',
        'COLL_DED',
        'PLCY_CNTRCT_NUM',
        'UNIT_NUM',
        'EFF_DT',
        'NUM_OF_CARS_IN_HH',
        'RDRVR_DT_OF_BRTH',
        'TERM_STRT_DT',
        'SRC_SYS_CD',
        'PNI_AGE',
        'MIS_LOB',
        'PRINCIPAL_OPRT',
        'SOURCE_IND_DERIVED'
    )
except Exception as e:
    logger.error(f"Failed projecting columns for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND: {e}", exc_info=True)
    raise


job.commit()
