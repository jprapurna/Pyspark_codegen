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
GLUE_CATALOG_DATABASE = "REPLACE_WITH_GLUE_CATALOG_DATABASE"

# Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: intermediate WRK_ source - S3-first with Glue Catalog fallback
try:
    logger.info(
        "Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 path s3://{}/WRK_BIRP_NISS_APRM_DETL/".format(S3_OUTPUT_BUCKET)
    )
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3")
except Exception as e:
    logger.warning(
        "Failed to read WRK_BIRP_NISS_APRM_DETL from S3; falling back to Glue Catalog read: {0}".format(e)
    )
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=GLUE_CATALOG_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL",
        )
        df_tmp = dyf.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
    except Exception as e:
        logger.error(
            f"Failed reading WRK_BIRP_NISS_APRM_DETL from both S3 and Glue Catalog: {e}",
            exc_info=True,
        )
        raise

# Project exactly the output ports listed on the Source node
try:
    logger.info("Projecting required columns for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_focused_hilbert = df_tmp.select(
        "CVG_TYP_CD",
        "SOI_TYP",
        "NISS_CLASS_CD",
        "REC_DROP_IND",
        "REC_DROP_RSN_DESC",
        "REC_EXCPN_IND",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED",
        "NISS_ST_CD",
        "NISS_APRM_DETL_SK",
        "ST_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "RATNG_CMPY_CD",
        "MLT_CAR_IND",
        "AGE",
        "GENDR",
        "MRTL_STAT",
        "AUTO_USE_CD",
    )
    logger.info("Projection complete for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
except Exception as e:
    logger.error(
        f"Failed projecting columns for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: {e}",
        exc_info=True,
    )
    raise


job.commit()
