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

# InteractionGroupStatus1: Source
# Attempt S3-first read of the workflow-staged parquet for InteractionGroupStatus,
# falling back to Glue Data Catalog if the S3 path does not exist or the read fails.
# Project exactly the single outgoing field listed on the edge: 'Source_Cd'.
try:
    logger.info(f"Attempting to read InteractionGroupStatus from S3 path s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/")
    # project exactly the outgoing field
    df_InteractionGroupStatus1_jovial_babbage = df_tmp.select("Source_Cd")
    logger.info("Read InteractionGroupStatus from S3 succeeded")
except Exception as e:
    logger.warning(f"S3 read for InteractionGroupStatus failed, falling back to Glue Data Catalog: {e}")
    try:
        logger.info("Reading InteractionGroupStatus from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="InteractionGroupStatus")
        df_InteractionGroupStatus1_jovial_babbage = dyf.toDF().select("Source_Cd")
        logger.info("Read InteractionGroupStatus from Glue Data Catalog succeeded")
    except Exception as e2:
        logger.error(f"Failed reading InteractionGroupStatus from Glue Data Catalog: {e2}", exc_info=True)
        raise


job.commit()
