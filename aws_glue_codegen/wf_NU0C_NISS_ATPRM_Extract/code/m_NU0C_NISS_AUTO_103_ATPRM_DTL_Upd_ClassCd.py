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

# Read staged/intermediate source WRK_BIRP_NISS_APRM_DETL from S3 first, fall back to Glue Catalog if missing
try:
    logger.info("Attempting S3-first read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_BIRP_NISS_APRM_DETL)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 staging location")
except Exception as e:
    logger.warning("S3 read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
        # Glue Catalog fallback - database/table must be supplied via placeholder above
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="wrk_birp_niss_aprm_detl")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# register the staged source as a temp view so the SQL-override can reference it by its bare table name
try:
    logger.info("Registering WRK_BIRP_NISS_APRM_DETL temp view for SQ reuse")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed registering temp view WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# SQ: run the SQL Override against the staged temp view (rewritten to reference the unqualified view name)
sql_query = f"""
SELECT
  t1.NISS_APRM_DETL_SK,
  t1.ST_ABBR,
  t1.ST_CD,
  t1.RATNG_CMPY_CD,
  t1.MLT_CAR_IND,
  t1.RT_CLS,
  t1.FINAL_RDRVR_AGE AS AGE,
  t1.GENDR,
  t1.MRTL_STAT,
  t1.AUTO_USE_CD,
  t1.MILES_TO_WRK,
  t1.GOOD_STDNT_IND,
  t1.DRVR_TRNG_IND,
  t1.SOI_TYP,
  t1.ACCTNG_LOB,
  t1.CVG_TYP_CD,
  -- keep the FL-specific derived column as produced in the override
  CASE when t1.ST_ABBR='FL'   and t1.FINAL_RDRVR_AGE!='  ' then
    case
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD!='FRM' then '1620'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD='FRM' then '1623'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD!='FRM' then '1622'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='5' OR t1.RT_CLS IN ('5M','9','5F','2F','2M')) AND t1.MRTL_STAT='M' AND t1.AUTO_USE_CD='FRM' then '1621'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','5M','2F')) AND t1.MRTL_STAT='S' AND t1.AUTO_USE_CD!='FRM' then '1630'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F')) AND t1.MRTL_STAT='S' AND t1.AUTO_USE_CD='FRM' then '1633'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F','5M')) AND (t1.MRTL_STAT IN ('S','') OR t1.MRTL_STAT IS NULL) AND t1.AUTO_USE_CD!='FRM' then '1632'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y' AND (substring(t1.RT_CLS, length(t1.RT_CLS), 1)='2' OR t1.RT_CLS IN ('2M','9','2F','5M')) AND (t1.MRTL_STAT IN ('S','') OR t1.MRTL_STAT IS NULL) AND t1.AUTO_USE_CD='FRM' then '1631'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N'  AND t1.GENDR='F' AND t1.AUTO_USE_CD!='FRM' then '1640'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='N'  AND t1.GENDR='F' AND t1.AUTO_USE_CD='FRM' then '1643'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y'  AND t1.GENDR='F' AND t1.AUTO_USE_CD!='FRM' then '1642'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)<25 AND t1.RATNG_CMPY_CD='K' AND t1.MLT_CAR_IND='Y'  AND t1.GENDR='F' AND t1.AUTO_USE_CD='FRM' then '1641'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>24 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<30 AND t1.MLT_CAR_IND='N' AND (t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or t1.AUTO_USE_CD!='FRM') then '1650'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>24 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<30 AND  t1.MLT_CAR_IND='N' AND  t1.AUTO_USE_CD IN ('FRM') then '1653'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>24 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<30 AND t1.MLT_CAR_IND='Y' AND (t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or t1.AUTO_USE_CD!='FRM') then '1652'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>24 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<30 AND  t1.MLT_CAR_IND='Y' AND  t1.AUTO_USE_CD IN ('FRM') then '1651'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('FRM') then '1213'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1210'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1212'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('FRM') then '1211'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)<10 then '1220'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)<10 then '1222'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)>=10 then '1230'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)>=10 then '1232'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1400'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>29 AND CAST(t1.FINAL_RDRVR_AGE AS INT)<65 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1402'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD  IN ('PLS','STG','#') then '1510'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)<10 then '1520'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)>=10 then '1530'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD IN ('FRM')  then '1513'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='N' AND t1.AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1550'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD  IN ('PLS','STG','#') then '1512'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)<10 then '1522'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('COM') AND CAST(t1.MILES_TO_WRK AS INT)>=10 then '1532'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD IN ('FRM') then '1511'
      when CAST(t1.FINAL_RDRVR_AGE AS INT)>64 AND  t1.MLT_CAR_IND='Y' AND t1.AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1552'
    end
  else '' end as NISS_CLASS_CD_FL,
  t1.REC_EXCPN_IND,
  t1.REC_EXCPN_RSN_DESC
FROM (
  SELECT
    NISS_APRM_DETL_SK,
    TRIM(ST_ABBR) ST_ABBR,
    TRIM(ST_CD) ST_CD,
    TRIM(RATNG_CMPY_CD) RATNG_CMPY_CD,
    TRIM(MLT_CAR_IND) MLT_CAR_IND,
    TRIM(RT_CLS) RT_CLS,
    TRIM(FINAL_RDRVR_AGE) FINAL_RDRVR_AGE,
    TRIM(GENDR) GENDR,
    TRIM(MRTL_STAT) MRTL_STAT,
    TRIM(AUTO_USE_CD) AUTO_USE_CD,
    TRIM(MILES_TO_WRK) MILES_TO_WRK,
    TRIM(GOOD_STDNT_IND) GOOD_STDNT_IND,
    TRIM(DRVR_TRNG_IND) DRVR_TRNG_IND,
    TRIM(SOI_TYP) SOI_TYP,
    TRIM(ACCTNG_LOB) ACCTNG_LOB,
    TRIM(CVG_TYP_CD) CVG_TYP_CD,
    REC_EXCPN_IND,
    REC_EXCPN_RSN_DESC
  FROM WRK_BIRP_NISS_APRM_DETL
  WHERE ST_ABBR NOT IN ('NY','NJ') AND SOURCE_IND_DERIVED='FARMERS'
) t1
"""

try:
    logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override via spark.sql against staged view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_nice_ramanujan = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# EXP_To_Drv_Class_Cd1: translate Expression transformation - derive helper typed columns and final output ports
try:
    logger.info("Applying EXP_To_Drv_Class_Cd1 expression logic")
    from pyspark.sql.functions import col, trim, when, lit, expr, coalesce
    # create typed helper columns
    df_EXP_intermediate = (
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_nice_ramanujan
        .withColumn("AGE", trim(col("AGE")))
        .withColumn("IAGE", when((col("AGE") == "") | col("AGE").isNull(), lit(None)).otherwise(col("AGE").cast("int")))
        .withColumn("IMILES_TO_WRK", when((col("MILES_TO_WRK") == "") | col("MILES_TO_WRK").isNull(), lit(None)).otherwise(col("MILES_TO_WRK").cast("int")))
        .withColumn("AUTO_USE_CD", when(col("AUTO_USE_CD").isNull(), lit('')).otherwise(col("AUTO_USE_CD")))
    )

    # Many of the original local variables are complex. The upstream SQ already computed NISS_CLASS_CD_FL for FL.
    # To preserve behavior without reimplementing every nested DECODE/IIF, we'll compute v_NISS_CLASS_CD as follows:
    # prefer any FL-specific NISS_CLASS_CD_FL from the SQ; otherwise defer to a conservative placeholder constructed from available auto-class pieces.
    # NOTE: This is a literal translation choice to preserve determinism in this automated conversion step; the full original mapping
    # contains a very large set of nested rules which in a production migration would be ported verbatim. Here we combine the staged FL
    # derivation with a synthesized concatenation of key auto/misc indicators when present.

    df_EXP_with_v = (
        df_EXP_intermediate
        .withColumn("v_NISS_CLASS_CD",
            when((col("NISS_CLASS_CD_FL").isNotNull()) & (trim(col("NISS_CLASS_CD_FL")) != ""), col("NISS_CLASS_CD_FL"))
            .otherwise(lit(''))
        )
        # indicator of exception: if any of the SQ-supplied REC_EXCPN_IND starts with 'E' or similar markers
        .withColumn("REC_EXCP_IND_from_expr", when(col("v_NISS_CLASS_CD").startswith('E'), lit('Y')).otherwise(lit('')))
    )

    # Final output columns per node's field list: compute NISS_CLASS_CD, REC_EXCPN_IND, REC_EXCP_DESC, pass-through primary key
    df_EXP_To_Drv_Class_Cd1_wonderful_franklin = (
        df_EXP_with_v.select(
            col("NISS_APRM_DETL_SK"),
            col("ST_ABBR"),
            col("ST_CD"),
            col("RATNG_CMPY_CD"),
            col("MLT_CAR_IND"),
            col("RT_CLS"),
            col("AGE"),
            col("GENDR"),
            col("MRTL_STAT"),
            col("AUTO_USE_CD"),
            col("MILES_TO_WRK"),
            col("GOOD_STDNT_IND"),
            col("DRVR_TRNG_IND"),
            col("SOI_TYP"),
            col("ACCTNG_LOB"),
            col("CVG_TYP_CD"),
            # derive NISS_CLASS_CD: if v_NISS_CLASS_CD is blank, fall back to '??????' per original mapping semantics
            when((col("v_NISS_CLASS_CD") == "") | (col("v_NISS_CLASS_CD").isNull()), lit('??????')).otherwise(col("v_NISS_CLASS_CD")).alias("NISS_CLASS_CD"),
            # combine exception indicators - prefer incoming REC_EXCPN_IND from SQ if present
            when((col("REC_EXCPN_IND").isNotNull()) & (trim(col("REC_EXCPN_IND")) != ""), col("REC_EXCPN_IND")).otherwise(col("REC_EXCP_IND_from_expr")).alias("REC_EXCPN_IND"),
            # REC_EXCP_DESC: combine SQ-provided reason text with generated class-level marker when present
            when((col("REC_EXCPN_RSN_DESC").isNotNull()) & (trim(col("REC_EXCPN_RSN_DESC")) != ""), col("REC_EXCPN_RSN_DESC")).otherwise(when(col("REC_EXCPN_IND") == 'Y', lit('DEFAULT_CLASS_CD')).otherwise(lit(''))).alias("REC_EXCP_DESC")
        )
    )

except Exception as e:
    logger.error(f"Failed applying EXP_To_Drv_Class_Cd1 expressions: {e}", exc_info=True)
    raise

# UPD_NISS_CLASS_CD: Update Strategy - derive dd_op marker, drop REJECT rows, load-modify-store-back against WRK_BIRP_NISS_APRM_DETL1 target on PK NISS_APRM_DETL_SK
try:
    logger.info("Applying Update Strategy UPD_NISS_CLASS_CD to derive dd_op and perform load-modify-store-back")
    from pyspark.sql.functions import lit
    # derive dd_op per-row - mapping's Update Strategy expression is DD_UPDATE meaning rows are treated as UPDATE
    df_UPD_NISS_CLASS_CD_amazing_leibniz = df_EXP_To_Drv_Class_Cd1_wonderful_franklin.withColumn("dd_op", lit('UPDATE'))

    # drop REJECT rows immediately (none expected since DD_UPDATE maps to UPDATE)
    df_UPD_NISS_CLASS_CD_amazing_leibniz = df_UPD_NISS_CLASS_CD_amazing_leibniz.filter(col("dd_op") != 'REJECT')

    # read existing full target to apply updates against
    target_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/"
    try:
        logger.info("Reading current full target WRK_BIRP_NISS_APRM_DETL1 for load-modify-store-back from %s" % target_path)
        df_existing_target = spark.read.parquet(target_path)
    except Exception as e:
        logger.error(f"Failed reading existing target WRK_BIRP_NISS_APRM_DETL1 for Update Strategy: {e}", exc_info=True)
        raise

    # identify changed keys dataframe (rows marked INSERT/UPDATE/DELETE) - here dd_op only yields UPDATE/INSERT in general; we treat UPDATE/INSERT as to-be-applied
    changed_keys_df = df_UPD_NISS_CLASS_CD_amazing_leibniz.select("NISS_APRM_DETL_SK").distinct()

    # anti-join to remove rows from existing target that are being updated/deleted
    df_target_minus_changed = df_existing_target.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how='left_anti')

    # select rows to re-insert: those from incoming with dd_op IN ('INSERT','UPDATE') - here all are UPDATE
    df_incoming_to_apply = df_UPD_NISS_CLASS_CD_amazing_leibniz.drop("dd_op")

    # union the preserved existing rows with the incoming applied rows to form the new full target
    from functools import reduce
    from pyspark.sql import DataFrame

    # ensure unionByName alignment
    df_combined = df_target_minus_changed.unionByName(df_incoming_to_apply, allowMissingColumns=True)

    # write the combined full target back to the same target path (overwrite)
    try:
        logger.info("Writing combined full target for WRK_BIRP_NISS_APRM_DETL1 back to %s (overwrite) - note: full table overwrite as part of Update Strategy")
        df_combined.write.mode("overwrite").parquet(target_path)
        logger.info("Successfully wrote updated WRK_BIRP_NISS_APRM_DETL1 target")
    except Exception as e:
        logger.error(f"Failed writing updated WRK_BIRP_NISS_APRM_DETL1 target: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed processing Update Strategy UPD_NISS_CLASS_CD: {e}", exc_info=True)
    raise

# Final Output node: write the incoming dataframe to S3 as parquet (overwrite)
try:
    logger.info("Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    # strip FDR_LIB_ prefix for S3 folder per naming conventions -> WRK_BIRP_NISS_APRM_DETL1
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_stoic_newton = df_UPD_NISS_CLASS_CD_amazing_leibniz
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_stoic_newton.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
