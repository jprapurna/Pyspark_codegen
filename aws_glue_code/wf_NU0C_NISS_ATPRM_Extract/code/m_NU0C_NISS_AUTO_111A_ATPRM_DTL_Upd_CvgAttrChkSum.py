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

from pyspark.sql.functions import lit, md5, col

# Top-of-script placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# ---------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# Attempt S3-first read for intermediate WRK_ table, fall back to Glue Catalog if missing
# Project only the port listed on this node's output edge: CVG_ATTR_CHCKSUM
# ---------------------------------------------------------------------------
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from s3 as parquet first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        .select("CVG_ATTR_CHCKSUM")
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from s3 successfully")
except Exception as e:
    # This S3-first fallback is allowed to fall back instead of re-raising per project rules
    logger.warning("Failed reading WRK_BIRP_NISS_APRM_DETL from s3; falling back to Glue Catalog read: %s", e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL", GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_tmp = dyf.toDF()
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = df_tmp.select("CVG_ATTR_CHCKSUM")
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# ---------------------------------------------------------------------------
# Expression: EXP_Gen_CvgAttrCheckSum
# Upstream SQ/df is missing from the mapping export - produce INPUT/OUTPUT ports as NULLs
# NISS_APRM_DETL_SK (INPUT/OUTPUT) -> lit(None)
# CVG_ATTR (INPUT) -> lit(None)
# CVG_ATTR_CHCKSUM = md5(CVG_ATTR)
# ---------------------------------------------------------------------------
try:
    logger.info("Building EXP_Gen_CvgAttrCheckSum: producing NISS_APRM_DETL_SK and CVG_ATTR as NULL and computing CVG_ATTR_CHCKSUM = md5(CVG_ATTR)")
    # Create a single-row dataframe with NULLs to preserve port presence; MD5(NULL) yields NULL
    df_EXP_Gen_CvgAttrCheckSum_eager_newton = (
        spark.createDataFrame([(None, None)], ["NISS_APRM_DETL_SK", "CVG_ATTR"])  # one row with nulls
        .withColumn("CVG_ATTR_CHCKSUM", md5(col("CVG_ATTR")))
        .select("NISS_APRM_DETL_SK", "CVG_ATTR", "CVG_ATTR_CHCKSUM")
    )
    logger.info("EXP_Gen_CvgAttrCheckSum completed successfully")
except Exception as e:
    logger.error(f"Failed building EXP_Gen_CvgAttrCheckSum: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# Update Strategy: Upd_CVG_ATTR_SK
# Mark every incoming row as 'UPDATE', drop REJECT rows (none expected),
# load current target from S3, anti-join out changed keys, union INSERT/UPDATE rows, overwrite target.
# WARNING: this full overwrite can be expensive for large targets.
# ---------------------------------------------------------------------------
try:
    logger.info("Starting Update Strategy Upd_CVG_ATTR_SK: marking rows and preparing to apply changes to WRK_BIRP_NISS_APRM_DETL")
    # mark all rows as UPDATE per node-level Update Strategy (DD_UPDATE)
    df_marked = df_EXP_Gen_CvgAttrCheckSum_eager_newton.withColumn("dd_op", lit("UPDATE"))

    # drop REJECT rows if any
    df_marked = df_marked.filter(col("dd_op") != "REJECT")

    # derive the set of changed keys (INSERT/UPDATE/DELETE). Here all rows are UPDATE.
    changed_keys_df = (
        df_marked.filter(col("dd_op").isin("INSERT", "UPDATE", "DELETE"))
        .select("NISS_APRM_DETL_SK")
        .distinct()
    )

    # read current target full table from S3
    logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL from s3 for load-modify-store-back")
    try:
        existing_target_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        logger.info("Read existing WRK_BIRP_NISS_APRM_DETL from s3 successfully")
    except Exception as e:
        logger.error(f"Failed reading existing WRK_BIRP_NISS_APRM_DETL from s3: {e}", exc_info=True)
        raise

    # anti-join to remove rows that are being changed
    logger.info("Anti-joining existing target against changed keys to remove rows being updated/deleted")
    existing_minus_changed = existing_target_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

    # take only INSERT/UPDATE rows from incoming set (DELETE rows should not be re-inserted)
    incoming_to_apply = df_marked.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

    # union the preserved existing rows with the incoming INSERT/UPDATE rows
    logger.info("Unioning preserved existing rows with incoming INSERT/UPDATE rows")
    try:
        combined_full_df = existing_minus_changed.unionByName(incoming_to_apply, allowMissingColumns=True)
    except Exception as e:
        logger.error(f"Failed unioning dataframes during update strategy apply: {e}", exc_info=True)
        raise

    # write the combined full result back to the SAME S3 path (overwrite)
    logger.info("Writing combined full WRK_BIRP_NISS_APRM_DETL back to s3 (overwrite) as part of Update Strategy")
    try:
        combined_full_df.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        logger.info("Successfully wrote updated WRK_BIRP_NISS_APRM_DETL to s3")
    except Exception as e:
        logger.error(f"Failed writing updated WRK_BIRP_NISS_APRM_DETL to s3: {e}", exc_info=True)
        raise

    # assign the node's output dataframe variable for downstream consumption
    df_Upd_CVG_ATTR_SK_silly_ramanujan = combined_full_df
    logger.info("Update Strategy Upd_CVG_ATTR_SK completed and df_Upd_CVG_ATTR_SK_silly_ramanujan is set")
except Exception as e:
    logger.error(f"Upd_CVG_ATTR_SK failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# Write the WRK_BIRP_NISS_APRM_DETL intermediate as parquet to S3 (overwrite)
# ---------------------------------------------------------------------------
# pass through the df name expected by downstream naming
try:
    df_WRK_BIRP_NISS_APRM_DETL_peaceful_shannon = df_Upd_CVG_ATTR_SK_silly_ramanujan

    logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) from Output node")
    df_WRK_BIRP_NISS_APRM_DETL_peaceful_shannon.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Output write of WRK_BIRP_NISS_APRM_DETL completed successfully")
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise




job.commit()
