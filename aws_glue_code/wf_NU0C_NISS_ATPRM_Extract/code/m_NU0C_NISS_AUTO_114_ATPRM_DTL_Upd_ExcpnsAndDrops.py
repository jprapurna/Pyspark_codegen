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

# Top-of-script placeholder constants
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
CATALOG_DATABASE = "REPLACE_WITH_CATALOG_DATABASE"

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL2: staged S3-first read with Glue Catalog fallback, project NISS_APRM_DETL_SK
logger.info("Attempting to read FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged) from S3 first")
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_sleepy_leibniz = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3")
except Exception as e:
    logger.warning(f"Failed reading WRK_BIRP_NISS_APRM_DETL from s3, falling back to Glue catalog: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=CATALOG_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_sleepy_leibniz = dyf.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog: {e2}", exc_info=True)
        raise

# Project only the NISS_APRM_DETL_SK column as required by the downstream lineage
try:
    logger.info("Projecting NISS_APRM_DETL_SK from FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_sleepy_leibniz = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_sleepy_leibniz.selectExpr("NISS_APRM_DETL_SK")
except Exception as e:
    logger.error(f"Failed projecting NISS_APRM_DETL_SK: {e}", exc_info=True)
    raise


# Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2: staged S3-first read with Glue Catalog fallback, project listed columns
logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL_2 (staged) from S3 first")
try:
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_romantic_maxwell = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_2/")
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL_2 from S3")
except Exception as e:
    logger.warning(f"Failed reading WRK_BIRP_NISS_APRM_DETL_2 from s3, falling back to Glue catalog: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL_2 from Glue Data Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=CATALOG_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL_2")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_romantic_maxwell = dyf.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL_2 from Glue Data Catalog")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL_2 from Glue Data Catalog: {e2}", exc_info=True)
        raise

# Project exactly the columns required by this node's output edge
try:
    logger.info("Projecting required columns from Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_romantic_maxwell = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_romantic_maxwell.selectExpr(
        "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
        "NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD",
        "NISS_LIAB_OR_NO_FAULT_CD",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "AGE",
        "ACCTNG_LOB",
        "NUM_OF_CARS_IN_HH",
        "NISS_APRM_DETL_SK",
        "NISS_CMPNY_CD",
        "NISS_ST_CD",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "GRGNG_ZIP_5",
        "NISS_CLASS_CD",
        "NISS_EXCPN_CD",
        "NISS_FGVNS_CD",
        "NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD"
    )
except Exception as e:
    logger.error(f"Failed projecting columns from Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2: {e}", exc_info=True)
    raise


job.commit()
