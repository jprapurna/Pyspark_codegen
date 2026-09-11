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

# Placeholder for any mapping-level parameters or environment values referenced by this mapping
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

try:
    logger.info("Mapping 'm_NU0C_NISS_AUTO_111A_ATPRM_DTL_Upd_CvgAttrChkSum': no nodes found to process (mapping body is empty). Nothing to execute.")
except Exception as e:
    logger.error(f"Unexpected error while announcing empty mapping processing: {e}", exc_info=True)
    raise


job.commit()
