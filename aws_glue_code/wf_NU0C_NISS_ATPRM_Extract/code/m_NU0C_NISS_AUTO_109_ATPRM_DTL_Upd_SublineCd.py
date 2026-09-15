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

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL: try to read intermediate WRK_ table from S3 first, fall back to Glue Catalog
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 as parquet")
    df_temp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 succeeded")
except Exception as e:
    logger.warning(f"Reading WRK_BIRP_NISS_APRM_DETL from S3 failed, falling back to Glue Catalog read: {e}", exc_info=True)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_temp = dyf.toDF()
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog succeeded")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# Project only the ports/columns required by downstream nodes
try:
    logger.info("Projecting required columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL into df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = df_temp.select(
        "CVG_TYP_CD",
        "BI_LMT",
        "MI_PPO_IND",
        "LMT_TORT",
        "NISS_APRM_DETL_SK",
        "ST_NM",
        "EFF_DT",
        "ST_ABBR",
        "ACCTNG_LOB",
    )
except Exception as e:
    logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
