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

from pyspark.sql.functions import expr, col, lit, round as spark_round, row_number
from pyspark.sql import Window
from functools import reduce

# Placeholder constants for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
PM_BAD_FILE_DIR = "REPLACE_WITH_PM_BAD_FILE_DIR"

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (instance: silly_franklin)
# Try to read staged WRK_ parquet from S3 first; on failure fall back to Glue Catalog read.
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting S3-first read for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin from WRK_BIRP_NISS_APRM_DETL path")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable; falling back to Glue Catalog read: {0}".format(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed fallback read for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (instance: humble_maxwell)
# Same S3-first behavior as the other source instance (separate df_name required).
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting S3-first read for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell from WRK_BIRP_NISS_APRM_DETL path")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable; falling back to Glue Catalog read: {0}".format(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell")
        dyf2 = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell = dyf2.toDF()
    except Exception as e2:
        logger.error(f"Failed fallback read for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Application Source Qualifier with SQL Override)
# Rewritten to query the staged temp view WRK_BIRP_NISS_APRM_DETL (staged case)
# SQL override projects: NISS_APRM_DETL_SK, ROUND((CVG_EXPS_VAL*12)) AS CVG_EXPS_VAL, PLCY_CNTRCT_NUM, UNIT_NUM, EFF_DT
# -----------------------------------------------------------------------------
try:
    # register the upstream staged dataframe as a temp view named exactly as the real table
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ROUND((CVG_EXPS_VAL * 12)) AS CVG_EXPS_VAL,
    PLCY_CNTRCT_NUM,
    UNIT_NUM,
    EFF_DT
FROM WRK_BIRP_NISS_APRM_DETL
WHERE CVG_TYP_IND = 'B'"""
    logger.info("Executing rewritten SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql()")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_optimistic_kant = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (Custom SQL with ranking, staged case)
# Register upstream staged df and run the provided CustomSQLQuery rewritten to reference the bare view
# -----------------------------------------------------------------------------
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT
    C.NISS_APRM_DETL_SK,
    C.CVG_EXPS_VAL,
    C.PLCY_CNTRCT_NUM,
    C.UNIT_NUM,
    C.EFF_DT
FROM (
    SELECT
        B.NISS_APRM_DETL_SK,
        B.CVG_EXPS_VAL,
        B.PLCY_CNTRCT_NUM,
        B.UNIT_NUM,
        B.EFF_DT,
        ROW_NUMBER() OVER (
            PARTITION BY B.PLCY_CNTRCT_NUM, B.UNIT_NUM, B.EFF_DT
            ORDER BY B.CVG_EXPS_VAL DESC
        ) AS ROW_RANK
    FROM WRK_BIRP_NISS_APRM_DETL B
    WHERE NOT EXISTS (
        SELECT 1 FROM WRK_BIRP_NISS_APRM_DETL A WHERE A.CVG_TYP_IND = 'B' AND A.PLCY_CNTRCT_NUM = B.PLCY_CNTRCT_NUM
    )
) C
WHERE C.ROW_RANK = 1"""
    logger.info("Executing rewritten Custom SQL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 via spark.sql()")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_euclid = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing Custom SQL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Exp_before_tgt (Expression - passthrough); project explicit ports from the SQ optimistic_kant
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting explicit ports in Exp_before_tgt (passthrough)")
    df_Exp_before_tgt_reverent_babbage = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_optimistic_kant.selectExpr(
        "NISS_APRM_DETL_SK",
        "CVG_EXPS_VAL",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT"
    )
except Exception as e:
    logger.error(f"Failed in Exp_before_tgt projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Exp_before_tgt1 (Expression): project ports from SQ1 and compute EXP_VAL_ROLLED = ROUND(CVG_EXPS_VAL * 12)
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting ports and computing EXP_VAL_ROLLED in Exp_before_tgt1")
    df_Exp_before_tgt1_awesome_shannon = (
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_euclid.selectExpr(
            "NISS_APRM_DETL_SK",
            "CVG_EXPS_VAL",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM",
            "EFF_DT"
        )
        .withColumn("EXP_VAL_ROLLED", expr("round(CVG_EXPS_VAL * 12)") )
    )
except Exception as e:
    logger.error(f"Failed in Exp_before_tgt1 computation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Upd_EXP_UPD (Update Strategy): mark all rows DD_UPDATE, drop REJECTs, load-modify-store-back to WRK_BIRP_NISS_APRM_DETL
# Primary key: NISS_APRM_DETL_SK
# -----------------------------------------------------------------------------
try:
    logger.info("Deriving dd_op and preparing changed rows for Upd_EXP_UPD")
    df_Upd_EXP_UPD_modest_ramanujan_pre = df_Exp_before_tgt_reverent_babbage.withColumn("dd_op", lit('UPDATE'))
    df_Upd_EXP_UPD_modest_ramanujan = df_Upd_EXP_UPD_modest_ramanujan_pre.filter(col("dd_op") != 'REJECT')

    # rows to insert/update (we treat DD_UPDATE as UPDATE here)
    df_changed = df_Upd_EXP_UPD_modest_ramanujan.filter(expr("dd_op IN ('INSERT','UPDATE')")).drop("dd_op")

    # load current target (S3 parquet preferred); if missing, treat as empty existing set
    try:
        logger.info("Loading current WRK_BIRP_NISS_APRM_DETL target from S3 for Upd_EXP_UPD apply")
        existing_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.warning("Target WRK_BIRP_NISS_APRM_DETL not present on S3 for Upd_EXP_UPD; will create target from incoming changed rows: {0}".format(e))
        # create empty DataFrame with same schema as incoming changed rows if none exist
        existing_df = spark.createDataFrame([], df_changed.schema)

    # anti-join to remove rows being updated/deleted based on primary key
    try:
        changed_keys_df = df_changed.select("NISS_APRM_DETL_SK").distinct()
        existing_surviving = existing_df.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")

        # union surviving existing rows with changed rows (INSERT/UPDATE)
        combined_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [existing_surviving, df_changed])

        # write back full combined dataset to the same target path (overwrite)
        logger.info("Writing merged WRK_BIRP_NISS_APRM_DETL target back to S3 for Upd_EXP_UPD (overwrite)")
        combined_df.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.error(f"Failed applying Upd_EXP_UPD load-modify-store-back: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Upd_EXP_UPD outer failure: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Upd_EXP_UPD_CVG_IND_NOT_B (Update Strategy): similar load-modify-store-back using df_Exp_before_tgt1_awesome_shannon
# -----------------------------------------------------------------------------
try:
    logger.info("Deriving dd_op and preparing changed rows for Upd_EXP_UPD_CVG_IND_NOT_B")
    df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo_pre = df_Exp_before_tgt1_awesome_shannon.withColumn("dd_op", lit('UPDATE'))
    df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo = df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo_pre.filter(col("dd_op") != 'REJECT')

    df_changed_2 = df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo.filter(expr("dd_op IN ('INSERT','UPDATE')")).drop("dd_op")

    try:
        logger.info("Loading current WRK_BIRP_NISS_APRM_DETL target from S3 for Upd_EXP_UPD_CVG_IND_NOT_B apply")
        existing_df_2 = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.warning("Target WRK_BIRP_NISS_APRM_DETL not present on S3 for Upd_EXP_UPD_CVG_IND_NOT_B; will create target from incoming changed rows: {0}".format(e))
        existing_df_2 = spark.createDataFrame([], df_changed_2.schema)

    try:
        changed_keys_df_2 = df_changed_2.select("NISS_APRM_DETL_SK").distinct()
        existing_surviving_2 = existing_df_2.join(changed_keys_df_2, on="NISS_APRM_DETL_SK", how="left_anti")

        combined_df_2 = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [existing_surviving_2, df_changed_2])

        logger.info("Writing merged WRK_BIRP_NISS_APRM_DETL target back to S3 for Upd_EXP_UPD_CVG_IND_NOT_B (overwrite)")
        combined_df_2.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.error(f"Failed applying Upd_EXP_UPD_CVG_IND_NOT_B load-modify-store-back: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Upd_EXP_UPD_CVG_IND_NOT_B outer failure: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (logical target node)
# The upstream Update Strategy already applied the update back to WRK_BIRP_NISS_APRM_DETL on S3.
# Do not perform an additional write here to avoid double-writing; assign the incoming DF to the output df_name
# -----------------------------------------------------------------------------
try:
    # Note: the actual update/write was performed by Upd_EXP_UPD_modest_ramanujan above (load-modify-store-back).
    df_WRK_BIRP_NISS_APRM_DETL_loving_gauss = df_Upd_EXP_UPD_modest_ramanujan
    logger.info("Mapped logical output FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to df_WRK_BIRP_NISS_APRM_DETL_loving_gauss (no extra write; Update Strategy applied updates)")
except Exception as e:
    logger.error(f"Failed assigning logical output df_WRK_BIRP_NISS_APRM_DETL_loving_gauss: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL3 (another logical target node)
# Same behavior as the other Output: Update Strategy upstream already applied changes.
# Also document any Post SQL as a comment for manual review.
# -----------------------------------------------------------------------------
try:
    df_WRK_BIRP_NISS_APRM_DETL_heroic_descartes = df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo
    logger.info("Mapped logical output FDR_LIB_WRK_BIRP_NISS_APRM_DETL3 to df_WRK_BIRP_NISS_APRM_DETL_heroic_descartes (no extra write; Update Strategy applied updates)")
    # Manual-review note: original mapping had Post SQL: UPDATE FDR.WRK_BIRP_NISS_APRM_DETL SET EXPS_VAL_ROLLED=0 WHERE (((ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13000', '13006' , '13007', '13008', '13009')) OR (TTL_WRITTN_PREM_AMT = 0)))
    # That post-SQL is documented here for reviewers; it is NOT executed by this generated script. If it must run, convert to an equivalent Spark DataFrame transformation and apply before the final write inside this job.
except Exception as e:
    logger.error(f"Failed assigning logical output df_WRK_BIRP_NISS_APRM_DETL_heroic_descartes: {e}", exc_info=True)
    raise



job.commit()
