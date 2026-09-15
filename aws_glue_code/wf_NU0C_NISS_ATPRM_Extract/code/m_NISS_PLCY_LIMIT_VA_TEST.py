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

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL: staged S3-first read, fallback to Glue Catalog; project exact output ports
try:
    logger.info("Attempting S3-first read for WRK_BIRP_NISS_APRM_DETL from s3://%s/WRK_BIRP_NISS_APRM_DETL/", S3_OUTPUT_BUCKET)
    try:
        df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 succeeded")
    except Exception as e_s3:
        logger.warning("S3 read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read: %s", e_s3, exc_info=True)
        try:
            logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog (database=%s, table=WRK_BIRP_NISS_APRM_DETL)", GLUE_DATABASE)
            dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
            df_tmp = dyf.toDF()
            logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog succeeded")
        except Exception as e_cat:
            logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e_cat}", exc_info=True)
            raise

    # Project exactly the fields that flow from this Source node
    try:
        logger.info("Projecting required columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman = df_tmp.selectExpr(
            "NISS_APRM_DETL_SK",
            "ST_ABBR",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "NISS_CVG_CD",
            "NISS_ST_CD",
        )
        logger.info("Projection for FDR_LIB_WRK_BIRP_NISS_APRM_DETL completed")
    except Exception as e_proj:
        logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e_proj}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed processing source FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
