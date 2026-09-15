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

# Shortcut_to_WRK_BIRP_NISS_APRM_DETL: staged-intermediate S3-first read with Glue Catalog fallback
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL parquet from S3")
    # Try reading the parquet that earlier workflow runs may have written
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    # Project exactly the output columns required by this Source node
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage = df_tmp.select(
        "PIP_LOSS_INCOME_IND",
        "MI_PPO_IND",
        "RATNG_CMPY_CD",
        "COMP_DED",
        "COLL_DED",
        "ST_ABBR",
        "FA2_PLCY_IND",
        "UM_UMI_STACKING",
        "PIP_WVR_WL_IND",
        "PIP_MED_SEC_IND",
        "LOB",
        "SOURCE_IND_DERIVED",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "BI_LMT",
        "GA_ADDED_AT_FAULT_IND",
        "NISS_APRM_DETL_SK",
        "ST_NM",
    )
except Exception as e:
    # Allowed fallback: if the staged parquet is not present, fall back to Glue Data Catalog
    logger.warning(
        "Staged parquet for WRK_BIRP_NISS_APRM_DETL not found or failed to read from S3; falling back to Glue Data Catalog source",
        exc_info=True,
    )
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL"
        )
        df_tmp = dyf.toDF()
        # Project exactly the output columns required by this Source node
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage = df_tmp.select(
            "PIP_LOSS_INCOME_IND",
            "MI_PPO_IND",
            "RATNG_CMPY_CD",
            "COMP_DED",
            "COLL_DED",
            "ST_ABBR",
            "FA2_PLCY_IND",
            "UM_UMI_STACKING",
            "PIP_WVR_WL_IND",
            "PIP_MED_SEC_IND",
            "LOB",
            "SOURCE_IND_DERIVED",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "GA_ADDED_AT_FAULT_IND",
            "NISS_APRM_DETL_SK",
            "ST_NM",
        )
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise


job.commit()
