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

REPLACE_WITH_GLUE_DATABASE_VALUE = "REPLACE_WITH_GLUE_DATABASE_VALUE"
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (intermediate WRK_ table — try S3 first, then Glue Catalog)
try:
    logger.info("Attempting to read Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from S3 path before falling back to Glue Catalog")
    try:
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = spark.read.parquet(
            f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_1/"
        )
        # project exactly the output ports from the source
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin.select(
            "NISS_APRM_DETL_SK",
            "ST_ABBR",
            "ACCTNG_LOB",
            "BI_LMT",
            "PRD_GRP_CD",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "CVG_TYP_CD",
        )
        logger.info("Successfully read Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from S3")
    except Exception as e_s3:
        # allowed fallback: S3 path may not exist yet on first run — fall back to Glue Catalog
        logger.warning(
            "Failed to read Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from S3, falling back to Glue Data Catalog: %s", e_s3
        )
        try:
            logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Glue Data Catalog")
            dyf = glueContext.create_dynamic_frame_from_catalog(
                database=REPLACE_WITH_GLUE_DATABASE_VALUE,
                table_name="WRK_BIRP_NISS_APRM_DETL_1",
            )
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = dyf.toDF()
            # project exactly the output ports from the source
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin.select(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "BI_LMT",
                "PRD_GRP_CD",
                "NJ_NO_LWST_LMT_IND",
                "NJ_NMD_DRVR_EXCL_IND",
                "CVG_TYP_CD",
            )
            logger.info("Successfully read Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog")
        except Exception as e_cat:
            logger.error(
                f"Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog: {e_cat}",
                exc_info=True,
            )
            raise
except Exception as e:
    # Any unexpected error in the overall Source handling should be logged and re-raised
    logger.error(f"Unexpected failure while preparing Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1: {e}", exc_info=True)
    raise


job.commit()
