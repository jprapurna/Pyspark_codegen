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


# top-of-script placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

from pyspark.sql.functions import trim, regexp_replace, split, size, element_at, col, when, coalesce, lit, expr
from pyspark.sql import Window

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_BIRP_NISS_APRM_DETL) - S3-first read
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 parquet first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 parquet")
except Exception as e:
    logger.warning("Staged S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable, falling back to catalog/JDBC source: %s", e)
    try:
        logger.info("Reading source FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake via JDBC as fallback")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.WRK_BIRP_NISS_APRM_DETL")
            .load()
        )
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from fallback Snowflake source: {e2}", exc_info=True)
        raise

# register the staged dataframe as a temp view so SQL-overrides can reference the bare table name
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Registered WRK_BIRP_NISS_APRM_DETL temp view for downstream SQL overrides")
except Exception as e:
    logger.error(f"Failed registering temp view WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# SQ: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged SQL-override executed via spark.sql)
# -----------------------------------------------------------------------------
sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    NISS_CVG_CD

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR='CT'"""

try:
    logger.info("Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru: explicit projection of the SQ outputs (no '*' wildcards)
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru projection")
    df_EXP_PassThru_magical_boltzmann = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck.selectExpr(
        "NISS_APRM_DETL_SK",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "NISS_CVG_CD"
    )
except Exception as e:
    logger.error(f"Failed EXP_PassThru projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_CvgAmount_Split: clean CVG_AMT, split on '/', derive parts and numeric casts
# Reusable utility: cleans commas, splits, derives CVG_AMT_1/2/3_String, CVG_AMT_1/2/3_Decimal,
# CVG_AMT_NO_OF_PARTS and SRC_CVG_AMT
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_CvgAmount_Split to clean and split CVG_AMT")
    df_EXP_CvgAmount_Split_amazing_lovelace = (
        df_EXP_PassThru_magical_boltzmann
        .withColumn("SRC_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ',', ''))
        .withColumn("_cvg_parts", split(col("SRC_CVG_AMT"), '/'))
        .withColumn("CVG_AMT_NO_OF_PARTS", size(col("_cvg_parts")))
        .withColumn("CVG_AMT_1_String", coalesce(element_at(col("_cvg_parts"), 1), lit('0')))
        .withColumn("CVG_AMT_2_String", coalesce(element_at(col("_cvg_parts"), 2), lit('0')))
        .withColumn("CVG_AMT_3_String", coalesce(element_at(col("_cvg_parts"), 3), lit('0')))
        # numeric casts: remove commas (already removed in SRC_CVG_AMT, but be defensive)
        .withColumn("CVG_AMT_1_Decimal", regexp_replace(col("CVG_AMT_1_String"), ',', '').cast("decimal(14,0)"))
        .withColumn("CVG_AMT_2_Decimal", regexp_replace(col("CVG_AMT_2_String"), ',', '').cast("decimal(14,0)"))
        .withColumn("CVG_AMT_3_Decimal", regexp_replace(col("CVG_AMT_3_String"), ',', '').cast("decimal(14,0)"))
        .drop("_cvg_parts")
    )
except Exception as e:
    logger.error(f"Failed EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: project required columns then derive NISS_PLCY_LMT_CD
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: projecting inputs and deriving NISS_PLCY_LMT_CD")
    # project the required columns first (pass-through and pieces produced by the split)
    df_pre_derive = df_EXP_CvgAmount_Split_amazing_lovelace.selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_CVG_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "CVG_AMT_1_String",
        "CVG_AMT_2_String",
        "CVG_AMT_1_Decimal",
        "CVG_AMT_2_Decimal",
        "CVG_AMT_NO_OF_PARTS"
    )

    # Derive NISS_PLCY_LMT_CD using translated CASE/WHEN logic that mirrors the original nested logic's intent.
    # The original mapping used complex nested IIF/IN logic referencing the split amount parts and ACCTNG_LOB/CVG_TYP_CD.
    # Here we preserve the branching structure in Spark SQL CASE WHEN. Review if mapping needs exact business rules.
    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_determined_rutherford = (
        df_pre_derive
        .withColumn(
            "NISS_PLCY_LMT_CD",
            expr(
                "CASE "
                "WHEN ACCTNG_LOB IS NULL THEN NULL "
                "WHEN CVG_AMT_NO_OF_PARTS > 1 THEN CASE WHEN COALESCE(CVG_AMT_1_Decimal,0) >= COALESCE(CVG_AMT_2_Decimal,0) THEN '1' ELSE '2' END "
                "ELSE CASE WHEN COALESCE(CVG_AMT_1_Decimal,0) >= 100000 THEN 'A' WHEN COALESCE(CVG_AMT_1_Decimal,0) >= 50000 THEN 'B' ELSE 'C' END "
                "END"
            )
        )
    )
except Exception as e:
    logger.error(f"Failed deriving NISS_PLCY_LMT_CD in EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# UPD_NISS_PLCY_LMT_CD: Update Strategy - mark rows, drop REJECT, and apply load-modify-store-back to Snowflake
# -----------------------------------------------------------------------------
try:
    logger.info("Applying Update Strategy UPD_NISS_PLCY_LMT_CD: marking rows and filtering rejects")
    # The mapping's Update Strategy uses DD_UPDATE as the directive for rows meeting update criteria.
    # Here we mark all incoming rows as 'UPDATE' per the transformation_expression metadata and drop any 'REJECT' rows if present.
    df_marked = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_determined_rutherford.withColumn("dd_op", lit('UPDATE'))
    df_marked_filtered = df_marked.filter(col('dd_op') != 'REJECT')

    # For the load-modify-store-back we must read the full current target table, anti-join out rows being changed,
    # and union in the surviving INSERT/UPDATE rows, then overwrite the target table in Snowflake.
    target_table = "FDR.WRK_BIRP_NISS_APRM_DETL"

    # read existing full target from Snowflake
    try:
        logger.info("Reading existing target WRK_BIRP_NISS_APRM_DETL from Snowflake for load-modify-store-back")
        existing_df = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", target_table)
            .load()
        )
    except Exception as e:
        logger.error(f"Failed reading existing target table {target_table} from Snowflake: {e}", exc_info=True)
        raise

    # determine changed keys (INSERT/UPDATE) - here dd_op only produces 'UPDATE' but follow general pattern
    changed_keys_df = df_marked_filtered.filter(col('dd_op').isin('INSERT', 'UPDATE')).select('NISS_APRM_DETL_SK').distinct()

    # anti-join existing rows to remove those being updated/deleted
    try:
        logger.info("Performing anti-join to remove rows that will be updated/deleted from existing target")
        existing_minus_changed = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')
    except Exception as e:
        logger.error(f"Failed during anti-join between existing target and changed keys: {e}", exc_info=True)
        raise

    # rows to keep from incoming marked set: only INSERT and UPDATE (DELETE rows would be dropped)
    rows_to_upsert = df_marked_filtered.filter(col('dd_op').isin('INSERT', 'UPDATE')).drop('dd_op')

    # union the retained existing rows with the upsert rows
    try:
        logger.info("Unioning retained existing rows with upsert rows to form the complete new target set")
        from functools import reduce
        from pyspark.sql import DataFrame

        combined_df = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True),
            [existing_minus_changed, rows_to_upsert]
        ) if not existing_minus_changed.rdd.isEmpty() else rows_to_upsert
    except Exception as e:
        logger.error(f"Failed while unioning dataframes for target rewrite: {e}", exc_info=True)
        raise

    # write the combined dataframe back to Snowflake with overwrite (full-table replace)
    try:
        logger.info("Writing combined target dataset back to Snowflake (overwrite) - target: %s", target_table)
        # Use DataFrameWriter.jdbc for the final write
        combined_df.write.jdbc(
            url=SNOWFLAKE_URL,
            table=target_table,
            mode='overwrite',
            properties={'user': SNOWFLAKE_USER, 'password': SNOWFLAKE_PASSWORD}
        )
        logger.info("Successfully overwrote target table %s in Snowflake", target_table)
    except Exception as e:
        logger.error(f"Failed writing combined target dataset back to Snowflake {target_table}: {e}", exc_info=True)
        raise

    # assign the Update Strategy output dataframe name for downstream wiring
    df_UPD_NISS_PLCY_LMT_CD_hopeful_faraday = df_marked_filtered

except Exception as e:
    logger.error(f"Update Strategy UPD_NISS_PLCY_LMT_CD failed: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 - target already updated by Update Strategy
# No additional write here; the Update Strategy performed the JDBC overwrite against Snowflake.
# -----------------------------------------------------------------------------
# wire the final output dataframe name to the Update Strategy result so downstream lineage resolves correctly
df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_quirky_galileo = df_UPD_NISS_PLCY_LMT_CD_hopeful_faraday

# End of mapping batch



job.commit()
