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


from pyspark.sql.functions import expr, col, row_number, lit, broadcast
from pyspark.sql.window import Window
from functools import reduce

# Placeholder constants
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage
# Attempt S3 parquet read first (WRK_ table staged by upstream mapping), fall back to Glue Catalog
# Project only CVG_ATTR_CHCKSUM as the mapping needs only that column
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    try:
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage = (
            spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
            .selectExpr("CVG_ATTR_CHCKSUM")
        )
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from s3 parquet successfully")
    except Exception as e_s3:
        logger.warning(f"Staged parquet for WRK_BIRP_NISS_APRM_DETL not available on S3, falling back to Glue Catalog: {e_s3}")
        try:
            logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog as fallback")
            dyf_tmp = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage = dyf_tmp.toDF().selectExpr("CVG_ATTR_CHCKSUM")
            logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
        except Exception as e_cat:
            logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e_cat}", exc_info=True)
            raise
except Exception as e:
    logger.error(f"Unexpected error while preparing df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL -> df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether
# Same S3-first staged-source pattern, project CVG_ATTR_CHCKSUM
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3 (second source instance)")
    try:
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether = (
            spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
            .selectExpr("CVG_ATTR_CHCKSUM")
        )
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from s3 parquet successfully (second source instance)")
    except Exception as e_s3_2:
        logger.warning(f"Staged parquet for WRK_BIRP_NISS_APRM_DETL not available on S3 (second source), falling back to Glue Catalog: {e_s3_2}")
        try:
            logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog as fallback (second source)")
            dyf_tmp2 = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether = dyf_tmp2.toDF().selectExpr("CVG_ATTR_CHCKSUM")
            logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully (second source)")
        except Exception as e_cat2:
            logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback (second source): {e_cat2}", exc_info=True)
            raise
except Exception as e:
    logger.error(f"Unexpected error while preparing df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: staged SQL override using the staged temp view
# Original override: SELECT DISTINCT CVG_ATTR_CHCKSUM FROM FDR.WRK_BIRP_NISS_APRM_DETL
# Rewritten to select from the local temp view WRK_BIRP_NISS_APRM_DETL
# -----------------------------------------------------------------------------
try:
    # register the upstream staged dataframe as the bare table name expected by the override
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT DISTINCT CVG_ATTR_CHCKSUM
FROM WRK_BIRP_NISS_APRM_DETL
"""
    logger.info("Executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 via spark.sql")
    try:
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_affectionate_franklin = spark.sql(sql_query)
    except Exception as e_sql1:
        logger.error(f"Failed executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e_sql1}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Failed preparing/executing staged SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Application Source Qualifier) - partition #1 SQL
# Original: SELECT D.NISS_APRM_DETL_SK, D.CVG_ATTR_CHCKSUM FROM FDR.WRK_BIRP_NISS_APRM_DETL D
# Rewritten to run against temp view WRK_BIRP_NISS_APRM_DETL
# -----------------------------------------------------------------------------
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT
    D.NISS_APRM_DETL_SK,
    D.CVG_ATTR_CHCKSUM
FROM WRK_BIRP_NISS_APRM_DETL D
"""
    logger.info("Executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql")
    try:
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_bohr = spark.sql(sql_query)
    except Exception as e_sql2:
        logger.error(f"Failed executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e_sql2}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Failed preparing/executing staged SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXPTRANS: produce CVG_ATTR_SK (gap-free) and passthrough CVG_ATTR_CHCKSUM
# Project first, then attach the sequence via row_number() over Window.orderBy(lit(1))
# Note: this forces a single-partition ordering shuffle; review for large inputs
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting CVG_ATTR_CHCKSUM for EXPTRANS and then adding CVG_ATTR_SK via row_number()")
    # project the incoming column explicitly
    df_exp_projected = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_affectionate_franklin.selectExpr("CVG_ATTR_CHCKSUM")
    # attach gap-free sequence
    window_spec = Window.orderBy(lit(1))
    df_EXPTRANS_charming_hopper = df_exp_projected.withColumn("CVG_ATTR_SK", row_number().over(window_spec))
except Exception as e:
    logger.error(f"Failed computing CVG_ATTR_SK in EXPTRANS: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXPTRANS1: passthrough of NISS_APRM_DETL_SK and CVG_ATTR_CHCKSUM
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting NISS_APRM_DETL_SK and CVG_ATTR_CHCKSUM in EXPTRANS1")
    df_EXPTRANS1_gentle_hawking = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_bohr.selectExpr("NISS_APRM_DETL_SK", "CVG_ATTR_CHCKSUM")
except Exception as e:
    logger.error(f"Failed executing EXPTRANS1 passthrough projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# JNRTRANS: inner join master=df_EXPTRANS_charming_hopper and detail=df_EXPTRANS1_gentle_hawking
# Join condition: master.CVG_ATTR_CHCKSUM = detail.CVG_ATTR_CHCKSUM
# Preserve NISS_APRM_DETL_SK (from detail) and CVG_ATTR_SK (from master)
# -----------------------------------------------------------------------------
try:
    logger.info("Joining master (EXPTRANS) and detail (EXPTRANS1) on CVG_ATTR_CHCKSUM")
    df_JNRTRANS_adoring_shannon = (
        df_EXPTRANS_charming_hopper.alias("m").join(
            df_EXPTRANS1_gentle_hawking.alias("d"),
            col("m.CVG_ATTR_CHCKSUM") == col("d.CVG_ATTR_CHCKSUM"),
            how="inner",
        )
        .select(col("d.NISS_APRM_DETL_SK"), col("m.CVG_ATTR_SK"))
    )
except Exception as e:
    logger.error(f"Failed executing Joiner JNRTRANS: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThrough: passthrough NISS_APRM_DETL_SK and CVG_ATTR_SK
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting passthrough columns in EXP_PassThrough")
    df_EXP_PassThrough_trusting_euclid = df_JNRTRANS_adoring_shannon.selectExpr("NISS_APRM_DETL_SK", "CVG_ATTR_SK")
except Exception as e:
    logger.error(f"Failed executing EXP_PassThrough projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Upd_CVG_ATTR_SK: Update Strategy -> implement as dd_op marker + load-modify-store-back
# Target (logical) table: FDR.WRK_BIRP_NISS_APRM_DETL (the WRK_ name in schema FDR)
# Steps:
#  1. add dd_op column marking 'UPDATE'
#  2. drop REJECT rows
#  3. load existing full target via JDBC
#  4. anti-join existing rows against changed keys
#  5. union anti-joined existing rows with INSERT/UPDATE rows
#  6. write the combined dataframe back to the same target via JDBC overwrite
# -----------------------------------------------------------------------------
try:
    logger.info("Translating Update Strategy: marking rows and preparing load-modify-store-back")
    # 1) Mark dd_op
    df_with_dd = df_EXP_PassThrough_trusting_euclid.withColumn("dd_op", lit("UPDATE"))
    # 2) drop REJECT rows (none expected here, but follow pattern)
    df_with_dd_survivors = df_with_dd.filter(col("dd_op") != "REJECT")

    # 3) load existing full target table from Snowflake via JDBC
    try:
        logger.info("Reading existing target table FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake for Update Strategy apply")
        existing_df = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
            .load()
        )
    except Exception as e_read_target:
        logger.error(f"Failed reading existing target FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake: {e_read_target}", exc_info=True)
        raise

    # 4) Build changed keys dataframe (primary key NISS_APRM_DETL_SK)
    changed_keys_df = df_with_dd_survivors.select("NISS_APRM_DETL_SK").distinct()

    # 5) anti-join to exclude rows being updated/deleted
    try:
        existing_anti = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")
    except Exception as e_anti:
        logger.error(f"Failed during anti-join of existing target against changed keys: {e_anti}", exc_info=True)
        raise

    # rows to re-insert (INSERT/UPDATE) — here we include UPDATE rows
    rows_to_upsert = df_with_dd_survivors.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

    # union the remaining existing rows with the upsert rows; allowMissingColumns to tolerate schema differences
    try:
        combined_df = existing_anti.unionByName(rows_to_upsert, allowMissingColumns=True)
    except Exception as e_union:
        logger.error(f"Failed unioning existing_anti with upsert rows: {e_union}", exc_info=True)
        raise

    # 6) write combined dataframe back to same Snowflake target table (overwrite)
    try:
        logger.info("Writing combined dataframe back to Snowflake table FDR.WRK_BIRP_NISS_APRM_DETL (overwrite) as part of Update Strategy apply")
        combined_df.write.format("jdbc")
        combined_df.write.option("url", SNOWFLAKE_URL)
        combined_df.write.option("user", SNOWFLAKE_USER)
        combined_df.write.option("password", SNOWFLAKE_PASSWORD)
        combined_df.write.option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
        # use mode("overwrite") to replace the full table as per load-modify-store-back pattern
        combined_df.write.mode("overwrite").save()
    except Exception as e_write_back:
        logger.error(f"Failed writing updated table back to Snowflake target FDR.WRK_BIRP_NISS_APRM_DETL: {e_write_back}", exc_info=True)
        raise

    # assign the final combined dataframe to the node's output df_name for downstream lineage
    df_Upd_CVG_ATTR_SK_dazzling_kepler = combined_df
    logger.info("Update Strategy apply completed and assigned to df_Upd_CVG_ATTR_SK_dazzling_kepler")
except Exception as e:
    logger.error(f"Failed executing Update Strategy Upd_CVG_ATTR_SK: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Update target)
# The Update Strategy above already performed the transactional apply back to the Snowflake target.
# For lineage/audit we pass the dataframe through to the Output node variable; do not write again.
# -----------------------------------------------------------------------------
try:
    logger.info("Assigning final dataframe to output df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_mystifying_gauss (transactional apply already performed above)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_mystifying_gauss = df_Upd_CVG_ATTR_SK_dazzling_kepler
except Exception as e:
    logger.error(f"Failed assigning final output dataframe for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise



job.commit()
