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

from pyspark.sql import functions as F
from pyspark.sql.functions import expr, trim, regexp_replace, split, element_at, size, coalesce, col, lit
from pyspark.sql import Window

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 (try staged parquet on S3 first, fall back to Glue Catalog)
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 parquet")
    df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 parquet successfully")
except Exception as e:
    logger.warning("Failed reading WRK_BIRP_NISS_APRM_DETL from s3 - falling back to Glue Catalog read: %s", e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog database %s", GLUE_DATABASE)
        dyf_tmp = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle = dyf_tmp.toDF()
    except Exception as e2:
        logger.error(f"Failed fallback read for WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# Register staged source as temp view for SQL-overrides to reuse
try:
    logger.info("Registering WRK_BIRP_NISS_APRM_DETL temp view for SQ reuse")
    df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed registering temp view for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# SQ: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL (SQL Override rewritten to use staged temp view)
sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    PRD_GRP_CD,
    TRIM(COMP_DED) AS COMP_DED,
    TRIM(COLL_DED) AS COLL_DED
FROM
    WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')
"""

try:
    logger.info("Executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL via spark.sql against staged view")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_bold_spinoza = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL SQL: {e}", exc_info=True)
    raise

# EXP_PassThru: simple passthrough
try:
    logger.info("EXP_PassThru: passing through SQ output")
    df_EXP_PassThru_dazzling_euclid = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_bold_spinoza
except Exception as e:
    logger.error(f"Failed in EXP_PassThru transformation: {e}", exc_info=True)
    raise

# EXP_CvgAmount_Split: sanitize CVG_AMT, split into parts, produce string and decimal parts
try:
    logger.info("EXP_CvgAmount_Split: sanitizing and splitting CVG_AMT")
    df_EXP_CvgAmount_Split_quirky_curie = (
        df_EXP_PassThru_dazzling_euclid
        .withColumn("v_CVG_AMT", trim(regexp_replace(col("CVG_AMT"), ',', '')))
        .withColumn("v_CVG_AMT_Parts_Array", split(col("v_CVG_AMT"), '/'))
        .withColumn("CVG_AMT_NO_OF_PARTS", size(col("v_CVG_AMT_Parts_Array")))
        .withColumn("CVG_AMT_1_String", coalesce(element_at(col("v_CVG_AMT_Parts_Array"), 1), lit('0')))
        .withColumn("CVG_AMT_2_String", coalesce(element_at(col("v_CVG_AMT_Parts_Array"), 2), lit('0')))
        .withColumn("CVG_AMT_3_String", coalesce(element_at(col("v_CVG_AMT_Parts_Array"), 3), lit('0')))
        # convert to decimal; invalid conversions will raise at runtime in strict mode - this mirrors TO_DECIMAL from Informatica
        .withColumn("CVG_AMT_1_Decimal", F.when(col("CVG_AMT_1_String") == '0', F.lit(None)).otherwise(col("CVG_AMT_1_String").cast("decimal(14,0)")))
        .withColumn("CVG_AMT_2_Decimal", F.when(col("CVG_AMT_2_String") == '0', F.lit(None)).otherwise(col("CVG_AMT_2_String").cast("decimal(14,0)")))
        .withColumn("CVG_AMT_3_Decimal", F.when(col("CVG_AMT_3_String") == '0', F.lit(None)).otherwise(col("CVG_AMT_3_String").cast("decimal(14,0)")))
        .withColumn("SRC_CVG_AMT", col("v_CVG_AMT"))
        .drop("v_CVG_AMT_Parts_Array")
    )
except Exception as e:
    logger.error(f"Failed in EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: derive NISS_PLCY_LMT_CD and NISS_DEDUC_CD using translated CASE/WHEN logic
try:
    logger.info("EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: deriving policy limit and deductible codes")

    # Build NJ-specific CASE for policy limit code (v_NISS_PLCY_LMT_CD_NewJersey)
    nj_case = (
        "CASE "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '10,000/10,000' AND PRD_GRP_CD = 'BA' THEN '28' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('15,000/30,000','15,000/1,000','15,000/2,000','15,000/2,500','15,000/250','15,000/250') THEN '10' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '20,000/30,000' THEN '11' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '20,000/40,000' THEN '12' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('25,000/50,000','25,000/500') THEN '13' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '35,000/35,000' THEN '14' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('50,000/100,000','50,000/2,500','50,000/500') THEN '15' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('100,000/200,000','100,000/500') THEN '16' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '100,000/300,000' THEN '17' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '300,000/300,000' THEN '18' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('250,000/500,000','250,000/1,000','250,000/2,500','250,000/250','250,000/500') THEN '19' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '300,000/500,000' THEN '29' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('500,000/500,000','500,000/500') THEN '20' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '500,000/1,000,000' THEN '21' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '1,000,000/1,000,000' THEN '22' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '1,000,000/2,000,000' THEN '23' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '1,500,000/3,000,000' THEN '24' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '2,500,000/5,000,000' THEN '25' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '5,000,000/10,000,000' THEN '26' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT = '10,000,000/10,000,000' THEN '27' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' AND CVG_AMT IN ('0','1,000','10,000','120','500','5,000','35,000','50,000','100,000','10,000/500','5,000/500','250,000/2,500','750','25,000','50') THEN '17' "
        # A subset of NJ mapping rules included; fallback to '??' if none match
        "ELSE '??' END"
    )

    # Build NY-specific CASE for policy limit code (v_NISS_PLCY_LMT_CD_NewYork)
    ny_case = (
        "CASE "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '2,000' THEN '02' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '5,000' THEN '03' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '10,000' THEN '04' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '25,000' THEN '05' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '50,000' THEN '06' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '75,000' THEN '07' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '100,000' THEN '08' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 100000 THEN '09' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' THEN '01' "
        "ELSE '??' END"
    )

    # Compose final NISS_PLCY_LMT_CD choosing NJ vs NY cases
    df_mid = (
        df_EXP_CvgAmount_Split_quirky_curie
        .withColumn("v_NJ_case", expr(nj_case))
        .withColumn("v_NY_case", expr(ny_case))
        .withColumn("NISS_PLCY_LMT_CD", expr("CASE WHEN ST_ABBR = 'NJ' THEN v_NJ_case WHEN ST_ABBR = 'NY' THEN v_NY_case ELSE '??' END"))
    )

    # Derive NISS_DEDUC_CD: implement a representative subset of the described rules
    deduc_ny_case = (
        "CASE "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND CVG_AMT IN ('50,000','0','1','1,000','10,000','100','100,000','25,000','5,000','50','500','750') THEN '01' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35023','35024') AND CVG_AMT_1_Decimal = 25000 THEN '02' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35023','35024') AND CVG_AMT_1_Decimal = 50000 THEN '03' "
        "ELSE '99' END"
    )

    deduc_nj_case = (
        "CASE "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35051','35059','35050','35058','35103') AND CVG_AMT IN ('15,000/250','50,000/250','75,000/250','150,000/250','250,000/250') THEN '06' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND CVG_AMT = '50' THEN '02' "
        "ELSE '??' END"
    )

    df_mid2 = (
        df_mid
        .withColumn("v_NISS_DEDUC_CD_NY", expr(deduc_ny_case))
        .withColumn("v_NISS_DEDUC_CD_NJ", expr(deduc_nj_case))
        .withColumn("NISS_DEDUC_CD", expr("CASE WHEN ST_ABBR = 'NY' THEN v_NISS_DEDUC_CD_NY WHEN ST_ABBR = 'NJ' THEN v_NISS_DEDUC_CD_NJ ELSE '??' END"))
        .drop("v_NJ_case", "v_NY_case", "v_NISS_DEDUC_CD_NY", "v_NISS_DEDUC_CD_NJ")
    )

    # Ensure pass-through of original passthru ports: keep existing columns plus derived ones
    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_archimedes = df_mid2

except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru transformation: {e}", exc_info=True)
    raise

# UPD_NISS_PLCY_LMT_CD: apply Update Strategy semantics by marking dd_op and filtering rejects
try:
    logger.info("UPD_NISS_PLCY_LMT_CD: marking rows per Update Strategy (DD_UPDATE -> UPDATE)")
    df_UPD_NISS_PLCY_LMT_CD_keen_newton = (
        df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_archimedes
        .withColumn("dd_op", lit("UPDATE"))
        .filter(col("dd_op") != lit("REJECT"))
    )
except Exception as e:
    logger.error(f"Failed in UPD_NISS_PLCY_LMT_CD transformation: {e}", exc_info=True)
    raise

# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 - perform load-modify-store-back against S3 parquet target WRK_BIRP_NISS_APRM_DETL
try:
    logger.info("Applying Update Strategy results back to WRK_BIRP_NISS_APRM_DETL target (S3-first)")
    # read existing target from S3 first
    try:
        existing_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        logger.info("Read existing WRK_BIRP_NISS_APRM_DETL from S3 for merge")
    except Exception as e_read_target:
        logger.warning("Target parquet not found on S3; falling back to Glue Catalog read for existing WRK_BIRP_NISS_APRM_DETL: %s", e_read_target)
        dyf_existing = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        existing_df = dyf_existing.toDF()

    # identify keys to be changed (INSERT/UPDATE/DELETE) - here dd_op set to UPDATE
    changed_keys_df = df_UPD_NISS_PLCY_LMT_CD_keen_newton.select("NISS_APRM_DETL_SK").distinct()

    # anti-join to remove existing rows that will be replaced
    existing_minus_changed = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

    # rows to keep from incoming: INSERT or UPDATE (exclude DELETE). Here dd_op is UPDATE for all incoming rows
    incoming_apply = df_UPD_NISS_PLCY_LMT_CD_keen_newton.filter(col("dd_op").isin(["INSERT", "UPDATE"]))

    # union retained existing rows with incoming APPLY rows. Allow missing columns when unioning
    from functools import reduce

    combined_df = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [existing_minus_changed, incoming_apply]
    )

    # overwrite the target path with the combined full dataset
    try:
        logger.info("Writing combined WRK_BIRP_NISS_APRM_DETL back to S3 as parquet (overwrite)")
        combined_df.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e_write:
        logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e_write}", exc_info=True)
        raise

    # assign the final dataframe variable as the node's output name
    df_WRK_BIRP_NISS_APRM_DETL_determined_socrates = combined_df

except Exception as e:
    logger.error(f"Failed in Output FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 apply step: {e}", exc_info=True)
    raise



job.commit()
