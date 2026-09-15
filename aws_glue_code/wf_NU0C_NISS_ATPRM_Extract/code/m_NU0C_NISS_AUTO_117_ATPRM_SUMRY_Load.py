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
GLUE_DB = "REPLACE_WITH_GLUE_DATABASE"

# Source: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL (attempt S3 parquet read first, fallback to Glue Data Catalog)
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_FINAL from S3 parquet first")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/")
    logger.info("Successfully read WRK_BIRP_NISS_APRM_FINAL from S3 parquet")
except Exception as e:
    logger.warning(f"Failed to read WRK_BIRP_NISS_APRM_FINAL from s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/; falling back to Glue Data Catalog: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_FINAL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DB, table_name="WRK_BIRP_NISS_APRM_FINAL")
        df_tmp = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from Glue Data Catalog: {e2}", exc_info=True)
        raise

# project exactly the columns expected by downstream edges
try:
    logger.info("Projecting required columns for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL source")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_nice_einstein = df_tmp.selectExpr(
        'NISS_NO_OUTSTND_CLMS',
        'CLNDR_YR',
        'NISS_CMPNY_CD',
        'ST_ABBR',
        'NISS_ST_CD',
        'ACCDNT_YR',
        'NISS_CVG_CD',
        'NISS_CLASS_CD',
        'NISS_SUBLOB_CD',
        'NISS_TYP_LOSS_CD',
        'NISS_ANNL_STMNT_LOB_CD',
        'CVG_EXPS_VAL',
        'TTL_WRITTN_PREM_AMT',
        'NISS_PD_LOSS',
        'NISS_PD_ALLOC_ADJUS_EXPNS',
        'NISS_OUTSTNDG_LOSS',
        'NISS_TERR_CD',
        'NISS_NO_PD_CLMS'
    )
    logger.info("Projection complete for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL source")
except Exception as e:
    logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: {e}", exc_info=True)
    raise


job.commit()
