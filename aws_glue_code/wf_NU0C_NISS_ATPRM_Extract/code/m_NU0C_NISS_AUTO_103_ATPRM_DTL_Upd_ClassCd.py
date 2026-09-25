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

from pyspark.sql.functions import col, expr, when, lit, coalesce

# top-of-script placeholder constants
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
GLUE_TABLE_FOR_WRK = "WRK_BIRP_NISS_APRM_DETL"

# -----------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (S3-first read, fallback to Glue Catalog)
# upstream staged table read (WRK_ intermediate) - prefer staged parquet on S3
# -----------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 as parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/{GLUE_TABLE_FOR_WRK}/"
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3 staged path")
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read: %s", e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog (database=%s, table=%s)", GLUE_DATABASE, GLUE_TABLE_FOR_WRK)
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name=GLUE_TABLE_FOR_WRK)
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie = dyf.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from both S3 and Glue Catalog: {e2}", exc_info=True)
        raise

# Project only the ports that downstream nodes actually reference to avoid carrying excessive columns
# The SQ that follows trims many fields; select the commonly-used ports used by later steps
try:
    logger.info("Projecting upstream columns needed by downstream SQ/Expression from WRK_BIRP_NISS_APRM_DETL staged source")
    cols_needed = [
        'NISS_APRM_DETL_SK', 'ST_ABBR', 'ST_CD', 'RATNG_CMPY_CD', 'MLT_CAR_IND', 'RT_CLS',
        'FINAL_RDRVR_AGE', 'GENDR', 'MRTL_STAT', 'I_AUTO_USE_CD', 'MILES_TO_WRK',
        'GOOD_STDNT_IND', 'DRVR_TRNG_IND', 'SOI_TYP', 'ACCTNG_LOB', 'CVG_TYP_CD',
        'REC_EXCPN_IND', 'REC_EXCPN_RSN_DESC'
    ]
    # Keep only columns that actually exist in the dataframe to avoid AnalysisException
    existing_cols = [c for c in cols_needed if c in df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.columns]
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.select(*existing_cols)
except Exception as e:
    logger.error(f"Failed projecting columns from staged WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Source Qualifier (Application Source Qualifier) with SQL Override - staged case
# Register the upstream staged df as a temp view named WRK_BIRP_NISS_APRM_DETL and run the override via spark.sql
# -----------------------------------------------------------------
try:
    logger.info("Registering staged WRK_BIRP_NISS_APRM_DETL dataframe as temp view for SQL override")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT
  t1.NISS_APRM_DETL_SK,
  t1.ST_ABBR,
  t1.ST_CD,
  t1.RATNG_CMPY_CD,
  t1.MLT_CAR_IND,
  t1.RT_CLS,
  t1.FINAL_RDRVR_AGE,
  t1.GENDR,
  t1.MRTL_STAT,
  t1.AUTO_USE_CD,
  t1.MILES_TO_WRK,
  t1.GOOD_STDNT_IND,
  t1.DRVR_TRNG_IND,
  t1.SOI_TYP,
  t1.ACCTNG_LOB,
  t1.CVG_TYP_CD,
  CASE when t1.ST_ABBR='FL'   and t1.FINAL_RDRVR_AGE!='  ' then
    case
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD!='FRM' then '1620'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD='FRM' then '1623'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD!='FRM' then '1622'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD='FRM' then '1621'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','5M','2F')) AND t1.MRTL_STAT='S' AND t1.AUTO_USE_CD!='FRM' then '1630'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F')) AND t1.MRTL_STAT='S' AND t1.AUTO_USE_CD='FRM' then '1633'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F','5M')) AND (t1.MRTL_STAT IN ('S','') OR t1.MRTL_STAT IS NULL) AND t1.AUTO_USE_CD!='FRM' then '1632'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F','5M')) AND (t1.MRTL_STAT IN ('S','') OR t1.MRTL_STAT IS NULL) AND t1.AUTO_USE_CD='FRM' then '1631'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND t1.RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M','9') AND t1.AUTO_USE_CD!='FRM' then '1610'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND t1.RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND t1.AUTO_USE_CD='FRM' then '1613'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND t1.RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND t1.AUTO_USE_CD!='FRM' then '1612'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND t1.RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND t1.AUTO_USE_CD='FRM' then '1611'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N'  AND t1.GENDR='F' AND t1.AUTO_USE_CD!='FRM' then '1640'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N'  AND t1.GENDR='F' AND t1.AUTO_USE_CD='FRM' then '1643'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y'  AND t1.GENDR='F' AND t1.AUTO_USE_CD!='FRM' then '1642'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) < 25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y'  AND t1.GENDR='F' AND t1.AUTO_USE_CD='FRM' then '1641'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 24 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 30 AND t1.MLT_CAR_IND='N' AND (t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or t1.AUTO_USE_CD!='FRM') then '1650'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 24 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 30 AND t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('FRM') then '1653'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 24 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 30 AND t1.MLT_CAR_IND='Y' AND (t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or t1.AUTO_USE_CD!='FRM') then '1652'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 24 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 30 AND  t1.MLT_CAR_IND='Y' AND  t1.AUTO_USE_CD IN ('FRM') then '1651'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('FRM') then '1213'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1210'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1212'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('FRM') then '1211'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) < 10 then '1220'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) < 10 then '1222'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) >= 10 then '1230'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) >= 10 then '1232'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1400'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 29 AND CAST(t1.FINAL_RDRVR_AGE AS INT) < 65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1402'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD  IN ('PLS','STG','#') then '1510'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) < 10 then '1520'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) >= 10 then '1530'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('FRM')  then '1513'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1550'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD  IN ('PLS','STG','#') then '1512'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) < 10 then '1522'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT) >= 10 then '1532'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('FRM') then '1511'
      when CAST(t1.FINAL_RDRVR_AGE AS INT) > 64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1552'
    end
  else '' end as NISS_CLASS_CD_FL,
  t1.REC_EXCPN_IND,
  t1.REC_EXCPN_RSN_DESC
FROM (
  SELECT
  NISS_APRM_DETL_SK,
  LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
  LTRIM(RTRIM(ST_CD)) ST_CD,
  LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
  LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
  LTRIM(RTRIM(RT_CLS)) RT_CLS,
  LTRIM(RTRIM( FINAL_RDRVR_AGE)) FINAL_RDRVR_AGE,
  LTRIM(RTRIM(GENDR)) GENDR,
  LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
  LTRIM(RTRIM(I_AUTO_USE_CD)) AUTO_USE_CD,
  LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
  LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
  LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
  LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
  LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
  LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
  REC_EXCPN_IND,
  REC_EXCPN_RSN_DESC
  FROM WRK_BIRP_NISS_APRM_DETL
  WHERE ST_ABBR NOT IN ('NY','NJ') AND SOURCE_IND_DERIVED='FARMERS'
)"""

    logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_lucid_hilbert = spark.sql(sql_query)
    logger.info("Successfully executed SQL override and produced SQ output dataframe")
except Exception as e:
    logger.error(f"Failed executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Expression: EXP_To_Drv_Class_Cd1
# Project helpers (IAGE, IMILES_TO_WRK, AUTO_USE_CD) and derive final NISS_CLASS_CD and REC_EXCPN/REC_EXCP_DESC
# Note: the complex original logic is primarily encoded in the SQ for FL; here we ensure downstream ports exist and prefer the SQ-derived FL class when present.
# -----------------------------------------------------------------
try:
    logger.info("Transforming SQ output into EXP_To_Drv_Class_Cd1 outputs (derive IAGE/IMILES_TO_WRK/AUTO_USE_CD and final NISS_CLASS_CD)")
    df_temp = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_lucid_hilbert

    # compute integer age and miles, and normalized AUTO_USE_CD
    df_EXP_To_Drv_Class_Cd1_happy_planck = (
        df_temp
        .withColumn('IAGE', expr("CASE WHEN FINAL_RDRVR_AGE IS NULL OR TRIM(FINAL_RDRVR_AGE) = '' THEN NULL ELSE CAST(TRIM(FINAL_RDRVR_AGE) AS INT) END"))
        .withColumn('IMILES_TO_WRK', expr("CASE WHEN MILES_TO_WRK IS NULL OR TRIM(MILES_TO_WRK) = '' THEN NULL ELSE CAST(TRIM(MILES_TO_WRK) AS INT) END"))
        .withColumn('AUTO_USE_CD', coalesce(col('AUTO_USE_CD'), lit('')))
        # Prefer the SQ-derived FL-specific class when present; otherwise default to '??????' as the original expression intends
        .withColumn('NISS_CLASS_CD', when((col('NISS_CLASS_CD_FL').isNotNull()) & (col('NISS_CLASS_CD_FL') != ''), col('NISS_CLASS_CD_FL')).otherwise(lit('??????')))
        .withColumn('REC_EXCPN_IND', when(col('REC_EXCPN_IND') == 'Y', lit('Y')).otherwise(lit('')))
        .withColumn('REC_EXCP_DESC', expr("COALESCE(REC_EXCPN_RSN_DESC, '')"))
    )

    logger.info("EXP_To_Drv_Class_Cd1 transformation complete")
except Exception as e:
    logger.error(f"Failed transforming EXP_To_Drv_Class_Cd1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Update Strategy: UPD_NISS_CLASS_CD - derive dd_op, drop REJECT rows, apply load-modify-store-back to WRK target on S3
# -----------------------------------------------------------------
try:
    logger.info("Applying Update Strategy semantics for UPD_NISS_CLASS_CD: deriving dd_op and filtering REJECT rows")
    # The mapping's Update Strategy expression is 'DD_UPDATE' for all rows that reach this transformation
    df_with_dd = df_EXP_To_Drv_Class_Cd1_happy_planck.withColumn('dd_op', lit('UPDATE'))

    # Drop REJECT rows if any (none expected here, but follow rule)
    df_with_dd = df_with_dd.filter(col('dd_op') != 'REJECT')

    # Prepare changed rows (INSERT or UPDATE). According to mapping this node marks rows as UPDATE.
    changed_rows = df_with_dd.filter(col('dd_op').isin('INSERT', 'UPDATE'))

    # Read current full target from S3 (if missing, treat as empty)
    target_path = f"s3://{S3_OUTPUT_BUCKET}/{GLUE_TABLE_FOR_WRK}/"
    try:
        logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL from S3 for load-modify-store-back")
        existing_df = spark.read.parquet(target_path)
        logger.info("Successfully read existing target from S3")
    except Exception as read_e:
        logger.warning("Target path %s not found or not readable; treating existing target as empty for apply: %s", target_path, read_e)
        # Create empty dataframe with the same schema as changed_rows (drop dd_op first)
        changed_schema = changed_rows.drop('dd_op').schema
        existing_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), changed_schema)

    # Anti-join to remove any rows in existing target that are being updated/deleted
    try:
        logger.info("Anti-joining existing target to remove rows being updated/deleted")
        changed_keys = changed_rows.select('NISS_APRM_DETL_SK').distinct()
        existing_anti = existing_df.join(changed_keys, on=['NISS_APRM_DETL_SK'], how='left_anti')
    except Exception as e:
        logger.error(f"Failed anti-join during Update Strategy apply: {e}", exc_info=True)
        raise

    # Union the surviving existing rows with only the INSERT/UPDATE rows (drop dd_op column before writing)
    try:
        logger.info("Unioning surviving existing rows with changed INSERT/UPDATE rows to produce full new target")
        to_write_df = existing_anti.unionByName(changed_rows.drop('dd_op'), allowMissingColumns=True)
    except Exception as e:
        logger.error(f"Failed unioning rows during Update Strategy apply: {e}", exc_info=True)
        raise

    # Write the full combined dataframe back to the same target path in overwrite mode
    try:
        logger.info("Writing updated WRK_BIRP_NISS_APRM_DETL back to S3 as parquet (overwrite)")
        to_write_df.write.mode('overwrite').parquet(target_path)
        logger.info("Successfully wrote updated target to S3")
    except Exception as e:
        logger.error(f"Failed writing updated WRK_BIRP_NISS_APRM_DETL to S3 during Update Strategy apply: {e}", exc_info=True)
        raise

    # Assign result dataframe for downstream consumption
    df_UPD_NISS_CLASS_CD_admiring_noether = to_write_df
except Exception as e:
    logger.error(f"Failed Update Strategy UPD_NISS_CLASS_CD apply: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> write final intermediate WRK_ table as parquet to S3 (overwrite)
# -----------------------------------------------------------------
try:
    logger.info("Writing final WRK_BIRP_NISS_APRM_DETL target to S3 as parquet (overwrite) from Output node")
    # The target external name is FDR_LIB_WRK_BIRP_NISS_APRM_DETL -> real path WRK_BIRP_NISS_APRM_DETL
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_galileo = df_UPD_NISS_CLASS_CD_admiring_noether
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_galileo.write.mode('overwrite').parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Successfully wrote Output WRK_BIRP_NISS_APRM_DETL to S3")
except Exception as e:
    logger.error(f"Failed writing Output WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
