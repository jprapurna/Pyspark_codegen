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

# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (staged WRK_ intermediate read, project CVG_ATTR_CHCKSUM)
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    # project only the CVG_ATTR_CHCKSUM column
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon.selectExpr(
        "CVG_ATTR_CHCKSUM"
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3 and projected CVG_ATTR_CHCKSUM")
except Exception as e:
    # S3-first fallback: log and attempt Glue Catalog read instead of re-raising
    logger.warning("Staged S3 read for WRK_BIRP_NISS_APRM_DETL failed or path not present; falling back to Glue Data Catalog read", exc_info=True)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon = dyf.toDF().selectExpr("CVG_ATTR_CHCKSUM")
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog and projected CVG_ATTR_CHCKSUM")
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog: {e}", exc_info=True)
        raise


job.commit()
