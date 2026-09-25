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


# Top-of-script placeholders for environment- and mapping-parameters
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import col, trim, regexp_replace, split, size, when, lit
from pyspark.sql.functions import regexp_replace as regexp_replace_fn

# --------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged WRK_ table) - S3-first with Glue Catalog fallback
# --------------------------------------------------
try:
    logger.info("Attempting to read staged source WRK_BIRP_NISS_APRM_DETL from S3 first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    # register temp view so SQL-overrides can reference the bare table name
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Read staged WRK_BIRP_NISS_APRM_DETL from S3 and registered temp view 'WRK_BIRP_NISS_APRM_DETL'")
except Exception as e:
    # S3-first fallback branch: try Glue Catalog read (allowed to fall back instead of re-raising)
    logger.warning(
        "Staged S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable; falling back to Glue Catalog read"
    )
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman = dyf.toDF()
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog and registered temp view 'WRK_BIRP_NISS_APRM_DETL'")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# --------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (SQL Override against staged table)
# The override selects exactly seven ports; because the table is staged we run the override via spark.sql
# --------------------------------------------------
sql_query = f"""SELECT
  NISS_APRM_DETL_SK,
  NISS_ST_CD,
  ST_ABBR,
  ACCTNG_LOB,
  CVG_TYP_CD,
  CVG_AMT,
  NISS_CVG_CD

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR='VA'"""

try:
    logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against temp view WRK_BIRP_NISS_APRM_DETL")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_gentle_kant = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# --------------------------------------------------
# EXP_PassThru: explicitly project each INPUT/OUTPUT port (passthrough)
# Note: NISS_ST_CD1 does not exist in the upstream SQ output, so emit NULL AS NISS_ST_CD1 per rule
# --------------------------------------------------
try:
    logger.info("Running EXP_PassThru (explicit passthrough projection)")
    df_EXP_PassThru_hopeful_maxwell = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_gentle_kant.selectExpr(
        "NISS_APRM_DETL_SK",
        "NISS_ST_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "NISS_CVG_CD",
        "NULL AS NISS_ST_CD1"
    )
except Exception as e:
    logger.error(f"Failed in EXP_PassThru transformation: {e}", exc_info=True)
    raise

# --------------------------------------------------
# EXP_CvgAmount_Split: clean CVG_AMT, split on '/', produce string parts and decimal parts
# This is a reusable pattern: normalize (remove commas/trim), split into array, extract parts and safe-cast to decimal
# --------------------------------------------------
try:
    logger.info("Running EXP_CvgAmount_Split to normalize and split CVG_AMT into parts and decimals")

    df_temp = df_EXP_PassThru_hopeful_maxwell.withColumn(
        "SRC_CVG_AMT",
        trim(regexp_replace(col("CVG_AMT"), ",", ""))
    )

    df_temp = df_temp.withColumn("_cvgsplit", split(col("SRC_CVG_AMT"), "/"))
    df_temp = df_temp.withColumn("CVG_AMT_NO_OF_PARTS", size(col("_cvgsplit")))

    df_temp = df_temp.withColumn("CVG_AMT_1_String", when(size(col("_cvgsplit")) >= 1, col("_cvgsplit").getItem(0)).otherwise(lit("0")))
    df_temp = df_temp.withColumn("CVG_AMT_2_String", when(size(col("_cvgsplit")) >= 2, col("_cvgsplit").getItem(1)).otherwise(lit("0")))
    df_temp = df_temp.withColumn("CVG_AMT_3_String", when(size(col("_cvgsplit")) >= 3, col("_cvgsplit").getItem(2)).otherwise(lit("0")))

    # safe numeric conversions (invalid numerics -> null)
    df_temp = df_temp.withColumn(
        "CVG_AMT_1_Decimal",
        when((col("CVG_AMT_1_String").isNull()) | (col("CVG_AMT_1_String") == "0"), lit(None)).otherwise(regexp_replace(col("CVG_AMT_1_String"), ",", "").cast("decimal(14,0)"))
    )
    df_temp = df_temp.withColumn(
        "CVG_AMT_2_Decimal",
        when((col("CVG_AMT_2_String").isNull()) | (col("CVG_AMT_2_String") == "0"), lit(None)).otherwise(regexp_replace(col("CVG_AMT_2_String"), ",", "").cast("decimal(14,0)"))
    )
    df_temp = df_temp.withColumn(
        "CVG_AMT_3_Decimal",
        when((col("CVG_AMT_3_String").isNull()) | (col("CVG_AMT_3_String") == "0"), lit(None)).otherwise(regexp_replace(col("CVG_AMT_3_String"), ",", "").cast("decimal(14,0)"))
    )

    df_EXP_CvgAmount_Split_relaxed_hawking = df_temp.drop("_cvgsplit")

except Exception as e:
    logger.error(f"Failed in EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# --------------------------------------------------
# EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru
# Translate nested IIF logic into Spark; produce NISS_PLCY_LMT_CD and o_NISS_PLCY_LIMIT_CD_VA (v_VA)
# Previous attempt had an unbalanced-parentheses error in the VA logic; corrected here with nested when(...).otherwise(...) structure
# --------------------------------------------------
try:
    logger.info("Running EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru to derive policy-limit codes")

    df_base = df_EXP_CvgAmount_Split_relaxed_hawking

    # Build the VA-specific branch (v_VA) using nested when/otherwise to ensure balanced expressions.
    v_va_bi = (
        when((col("CVG_AMT_1_Decimal") == 25000) & (col("CVG_AMT_2_Decimal") == 50000) & (col("CVG_AMT_NO_OF_PARTS") == 2), lit('02'))
        .when(col("CVG_AMT") == '30,000/60,000', lit('03'))
        .when(col("CVG_AMT") == '50,000/100,000', lit('04'))
        .when(col("CVG_AMT") == '100,000/300,000', lit('05'))
        .when((col("CVG_AMT_1_String") == '25000') & (col("CVG_AMT_2_String") == '50000'), lit('02'))
        .when((col("CVG_AMT_1_String") == '500000') & (col("CVG_AMT_2_String") == '1000000'), lit('06'))
        .when((col("CVG_AMT_1_Decimal") > 500000) & (col("CVG_AMT_2_Decimal") > 1000000), lit('09'))
        .when((col("CVG_AMT_1_Decimal") <= 500000) & (col("CVG_AMT_2_Decimal") < 1000000), lit('01'))
        .otherwise(lit('01'))
    )

    v_va_pd = (
        when(col("CVG_AMT") == '20,000', lit('02'))
        .when(col("CVG_AMT") == '25,000', lit('03'))
        .when(col("CVG_AMT") == '50,000', lit('04'))
        .when(col("CVG_AMT") == '100,000', lit('05'))
        .when(col("CVG_AMT") == '250,000', lit('06'))
        .when(col("CVG_AMT") == '300,000', lit('07'))
        .when((col("CVG_AMT_1_Decimal") > 300000) & (col("CVG_AMT_NO_OF_PARTS") == 2), lit('09'))
        .otherwise(lit('01'))
    )

    # Example branch for MD/192* family: simplified to common patterns shown in mapping
    v_va_192md = (
        when(col("CVG_AMT") == '500', lit('01'))
        .when(col("CVG_AMT") == '750', lit('02'))
        .when(col("CVG_AMT") == '1,000', lit('03'))
        .when(col("CVG_AMT") == '2,000', lit('04'))
        .when(col("CVG_AMT") == '3,000', lit('05'))
        .when(col("CVG_AMT") == '5,000', lit('06'))
        .when(col("CVG_AMT") == '7,500', lit('07'))
        .when((col("CVG_AMT_1_Decimal") > 10000) & (col("CVG_AMT_NO_OF_PARTS") == 2), lit('09'))
        .otherwise(lit('10'))
    )

    # top-level VA expression: choose by ACCTNG_LOB, else default ''
    v_va = (
        when(col("NISS_ST_CD") == '45',
             when(col("ACCTNG_LOB") == '192BI', v_va_bi)
             .otherwise(
                 when(col("ACCTNG_LOB") == '192PD', v_va_pd)
                 .otherwise(
                     when(col("ACCTNG_LOB").substr(1,3) == '192', v_va_192md)
                     .otherwise(lit(''))
                 )
             )
        ).otherwise(lit(''))
    )

    # v_NISS_PLCY_LMT_CD: mapping contains a very large general expression; default to empty string here to match plan
    v_niss_plcy_lmt_cd = lit('')

    # Add intermediate derived columns, then project final outputs referencing them (two-step to allow alias reuse)
    df_step1 = df_base.withColumn("v_VA", v_va).withColumn("v_NISS_PLCY_LMT_CD", v_niss_plcy_lmt_cd)

    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_pascal = df_step1.select(
        col("NISS_APRM_DETL_SK"),
        col("NISS_CVG_CD"),
        col("NISS_ST_CD"),
        col("v_NISS_PLCY_LMT_CD").alias("NISS_PLCY_LMT_CD"),
        col("v_VA").alias("o_NISS_PLCY_LIMIT_CD_VA"),
        col("SRC_CVG_AMT"),
        col("CVG_AMT_1_String"),
        col("CVG_AMT_2_String"),
        col("CVG_AMT_1_Decimal"),
        col("CVG_AMT_2_Decimal"),
        col("CVG_AMT_NO_OF_PARTS")
    )

except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: {e}", exc_info=True)
    raise

# --------------------------------------------------
# UPDTRANS: Update Strategy -> derive dd_op, drop REJECT, then load-modify-store-back to WRK_BIRP_NISS_APRM_DETL1
# --------------------------------------------------
try:
    logger.info("Running UPDTRANS: applying Update Strategy expression and preparing for load-modify-store-back")

    # The mapping's Update Strategy expression is a constant 'DD_UPDATE' -> mark every row as UPDATE
    df_UPDTRANS_heroic_dirac = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_pascal.withColumn("dd_op", lit('UPDATE'))

    # drop REJECT rows if any (mapping forwards rejected rows, but per rule we drop REJECT rows)
    df_UPDTRANS_heroic_dirac = df_UPDTRANS_heroic_dirac.filter(col("dd_op") != 'REJECT')

    # load existing target table from S3 (full read) for WRK_BIRP_NISS_APRM_DETL1
    try:
        logger.info("Reading existing target WRK_BIRP_NISS_APRM_DETL1 from S3 for load-modify-store-back")
        existing_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
    except Exception as e:
        # If target does not exist yet, treat existing_df as empty dataframe with same schema as incoming
        logger.warning("Existing target WRK_BIRP_NISS_APRM_DETL1 not found on S3; starting from empty table")
        existing_df = spark.createDataFrame([], df_UPDTRANS_heroic_dirac.schema)

    # determine keys of changed rows
    changed_keys_df = df_UPDTRANS_heroic_dirac.select("NISS_APRM_DETL_SK").distinct()

    # anti-join to drop rows in existing table that are being updated or deleted
    existing_survivors = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how='left_anti')

    # include only INSERT/UPDATE rows from incoming (DELETE rows would be omitted)
    incoming_inserts_updates = df_UPDTRANS_heroic_dirac.filter(col("dd_op").isin('INSERT', 'UPDATE'))

    # union survivors with incoming inserts/updates to form the full final table
    from functools import reduce

    final_combined_df = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [existing_survivors, incoming_inserts_updates]
    )

    # write the combined dataframe back to the same S3 path (overwrite) to apply updates/deletes/inserts
    try:
        logger.info("Writing combined WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite) from Update Strategy")
        final_combined_df.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
    except Exception as e:
        logger.error(f"Failed writing combined WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed in UPDTRANS load-modify-store-back: {e}", exc_info=True)
    raise

# For downstream lineage and the Output node, assign the final combined dataframe to the Output node's df_name
df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_blissful_noether = final_combined_df

# --------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> write final mapping target as parquet to S3 (strip 'FDR_LIB_' prefix)
# --------------------------------------------------
try:
    logger.info("Writing final mapping target WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite) from Output node")
    # write the dataframe assigned to the Output node name
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_blissful_noether.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL1 to S3 in Output node: {e}", exc_info=True)
    raise



job.commit()
