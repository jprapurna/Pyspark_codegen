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

# Read staged intermediate WRK_BIRP_NISS_APRM_DETL: try S3 parquet first, then fall back to Glue Catalog
logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3 first")
try:
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 parquet successfully")
except Exception as e:
    logger.warning("Failed to read WRK_BIRP_NISS_APRM_DETL from S3; falling back to Glue Catalog read")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_tmp = dyf.toDF()
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# Project only the columns required downstream: CVG_AMT, NISS_APRM_DETL_SK, ST_ABBR, ACCTNG_LOB, CVG_TYP_CD, NISS_CVG_CD
try:
    logger.info("Projecting required columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann = df_tmp.selectExpr(
        "CVG_AMT",
        "NISS_APRM_DETL_SK",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "NISS_CVG_CD"
    )
    logger.info("Projection complete for FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
