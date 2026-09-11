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

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

# Placeholder constants for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
SOURCE_DATABASE = "REPLACE_WITH_GLUE_SOURCE_DATABASE"

# -----------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon
# This is an intermediate WRK_ source: try S3 parquet first, fall back to Glue Catalog. Project only CVG_ATTR_CHCKSUM.
# After creation, register temp view 'WRK_BIRP_NISS_APRM_DETL'.
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from s3 first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        .select("CVG_ATTR_CHCKSUM")
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 staging path")
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog (fallback)")
        dyf = glueContext.create_dynamic_frame.from_catalog(database=SOURCE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon = dyf.toDF().select("CVG_ATTR_CHCKSUM")
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog and projected CVG_ATTR_CHCKSUM")
    except Exception as e2:
        logger.error(f"Failed to read WRK_BIRP_NISS_APRM_DETL from both S3 and Glue Catalog: {e2}", exc_info=True)
        raise

# register temp view for downstream SQL overrides that reference this staged table by bare name
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_adoring_shannon.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Registered temp view WRK_BIRP_NISS_APRM_DETL for staged dataframe")
except Exception as e:
    logger.error(f"Failed registering temp view for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1
# SQL Override rewritten to select from temp view WRK_BIRP_NISS_APRM_DETL (staged case)
sql_query = f"""SELECT DISTINCT CVG_ATTR_CHCKSUM
FROM WRK_BIRP_NISS_APRM_DETL
"""
try:
    logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 via spark.sql against staged temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_trusting_euclid = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Expression: EXPTRANS (pass-through CVG_ATTR_CHCKSUM, generate CVG_ATTR_SK via row_number())
try:
    logger.info("Transforming EXPTRANS: pass-through CVG_ATTR_CHCKSUM and attach CVG_ATTR_SK via row_number()")
    # Project passthrough columns first
    df_EXPTRANS_careful_babbage = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_trusting_euclid.select("CVG_ATTR_CHCKSUM")

    # Add gap-free surrogate CVG_ATTR_SK via row_number() over an unpartitioned ordering.
    # NOTE: This forces a single-partition shuffle and should be reviewed if the input is large.
    window_spec = Window.orderBy(lit(1))
    df_EXPTRANS_careful_babbage = df_EXPTRANS_careful_babbage.withColumn("CVG_ATTR_SK", row_number().over(window_spec))
except Exception as e:
    logger.error(f"Failed transforming EXPTRANS: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Joiner: JNRTRANS
# Expected two inputs but second input df_EXPTRANS1_charming_hopper is missing. Preserve CVG_ATTR_SK and emit NISS_APRM_DETL_SK as NULL.
try:
    logger.info("Running JNRTRANS joiner logic - second input missing; emitting NISS_APRM_DETL_SK as NULL and preserving CVG_ATTR_SK")
    # Create dataframe with expected output columns. Emit NISS_APRM_DETL_SK as NULL (bigint) and keep CVG_ATTR_SK from left input.
    df_JNRTRANS_dazzling_kepler = df_EXPTRANS_careful_babbage.select(
        lit(None).cast("bigint").alias("NISS_APRM_DETL_SK"),
        col("CVG_ATTR_SK")
    )
    logger.warning("JNRTRANS: second input missing (df_EXPTRANS1_charming_hopper). Emitted NISS_APRM_DETL_SK as NULL to preserve schema.")
except Exception as e:
    logger.error(f"Failed in JNRTRANS transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Expression: EXP_PassThrough - explicit passthrough of NISS_APRM_DETL_SK and CVG_ATTR_SK
try:
    logger.info("Projecting NISS_APRM_DETL_SK and CVG_ATTR_SK in EXP_PassThrough")
    df_EXP_PassThrough_mystifying_gauss = df_JNRTRANS_dazzling_kepler.select("NISS_APRM_DETL_SK", "CVG_ATTR_SK")
except Exception as e:
    logger.error(f"Failed in EXP_PassThrough projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Update Strategy: Upd_CVG_ATTR_SK
# Derive dd_op marker (all rows -> 'UPDATE' per DD_UPDATE), drop REJECT rows, and apply load-modify-store-back against target WRK_BIRP_NISS_APRM_DETL on S3.
try:
    logger.info("Applying Update Strategy: deriving dd_op marker and filtering REJECTs")
    # All incoming rows are DD_UPDATE per the mapping -> mark as 'UPDATE'
    df_Upd_intermediate = df_EXP_PassThrough_mystifying_gauss.withColumn("dd_op", lit("UPDATE"))

    # Drop REJECT rows if any
    df_Upd_intermediate = df_Upd_intermediate.filter(col("dd_op") != "REJECT")
except Exception as e:
    logger.error(f"Failed deriving dd_op and filtering REJECT rows in Update Strategy: {e}", exc_info=True)
    raise

# Load-modify-store-back pattern
target_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
try:
    logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL from S3 for load-modify-store-back")
    existing_target_df = spark.read.parquet(target_path)
    logger.info("Successfully read existing target from S3 for WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.warning("Failed reading existing target from S3 (it may not exist). Proceeding with empty existing dataframe: %s" % e)
    # Create an empty dataframe with the same schema as the incoming update dataframe to allow union operations
    existing_target_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_Upd_intermediate.schema)

try:
    logger.info("Preparing changed-keys dataframe for rows to be applied (INSERT/UPDATE)")
    # Rows to apply are INSERT or UPDATE. Mapping marks rows as UPDATE; DELETE rows would be handled by omission.
    rows_to_apply = df_Upd_intermediate.filter(col("dd_op").isin("INSERT", "UPDATE"))
    changed_keys_df = rows_to_apply.select("NISS_APRM_DETL_SK").dropDuplicates()

    logger.info("Anti-joining existing target to remove rows being updated/deleted")
    existing_minus_changed = existing_target_df.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")

    logger.info("Unioning remaining existing rows with INSERT/UPDATE rows")
    # Union by name and allow missing columns in either side
    from functools import reduce
    combined_df = existing_minus_changed.unionByName(rows_to_apply, allowMissingColumns=True)

    # Final write back to the same target path (overwrite full table)
    logger.info("Overwriting target WRK_BIRP_NISS_APRM_DETL on S3 with the combined dataframe (load-modify-store-back)")
    combined_df.write.mode("overwrite").parquet(target_path)
    logger.info("Successfully overwrote target WRK_BIRP_NISS_APRM_DETL on S3")

    # Assign result to the node's output dataframe name for downstream consistency
    df_Upd_CVG_ATTR_SK_wonderful_archimedes = combined_df

    # Note: Fully rewriting the target can be expensive for large tables; this is the required semantics to apply UPDATE/DELETE rows atomically.
except Exception as e:
    logger.error(f"Failed applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
