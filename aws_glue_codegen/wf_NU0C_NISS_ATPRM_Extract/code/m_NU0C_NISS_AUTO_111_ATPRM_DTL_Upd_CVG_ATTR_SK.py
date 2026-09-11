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

# Mapping: m_NU0C_NISS_AUTO_111_ATPRM_DTL_Upd_CVG_ATTR_SK
# No nodes were present in the provided mapping plan, so there is no read/transform/write work to perform here.
# Emit a clear log entry so the job run records that this mapping was intentionally a no-op.
try:
    logger.info("Starting mapping m_NU0C_NISS_AUTO_111_ATPRM_DTL_Upd_CVG_ATTR_SK - no nodes to process")
except Exception as e:
    logger.error(f"Unexpected error when logging start of mapping: {e}", exc_info=True)
    raise

try:
    # Nothing to do for this mapping; all work for this workflow either lives in upstream mappings
    # or this mapping was an empty/placeholder mapping in the source metadata.
    logger.info("No processing steps defined for m_NU0C_NISS_AUTO_111_ATPRM_DTL_Upd_CVG_ATTR_SK - exiting cleanly")
except Exception as e:
    logger.error(f"Unexpected error during no-op mapping execution: {e}", exc_info=True)
    raise


job.commit()
