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

# Mapping: m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED
#
# No nodes were present in the provided MappingIndex plan for this mapping.
# As a result, there is no per-node PySpark/Glue logic to execute here.
# This script body intentionally contains no read/transform/write steps.

logger.info("Mapping 'm_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED' contains no executable nodes; nothing to run.")


job.commit()
