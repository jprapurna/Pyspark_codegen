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

# Mapping: m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED
# NOTE: The provided mapping plan contains no node entries to translate.
# As a result there are no source reads, transformations, or outputs to execute
# for this mapping in this job. This script intentionally performs no data
# operations. If this is unexpected, please re-run the lineage extraction so
# the mapping's nodes_breadth_first() output can be translated into PySpark.

logger.info("No nodes to process for mapping m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED - exiting job body.")


job.commit()
