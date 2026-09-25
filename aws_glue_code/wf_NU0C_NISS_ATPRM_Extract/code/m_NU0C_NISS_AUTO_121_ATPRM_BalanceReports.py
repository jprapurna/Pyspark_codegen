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


# Top-of-script placeholders for environment / mapping parameters
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"
OutputFile_PremRpt_Bal2 = "REPLACE_WITH_OutputFile_PremRpt_Bal2_VALUE"

from pyspark.sql.functions import expr, col, when, lit, coalesce

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_ staging source - S3-first)
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from workflow S3 staging location")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    ).selectExpr(
        "CLNDR_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "TTL_WRITTN_PREM_AMT",
        "REC_DROP_IND"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 staging location")
except Exception as e:
    logger.warning(f"Failed reading WRK_BIRP_NISS_APRM_DETL from S3 staging (will fall back to Glue Catalog): {e}")
    try:
        logger.info("Falling back to Glue catalog read for WRK_BIRP_NISS_APRM_DETL")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database="REPLACE_WITH_SOURCE_DB",
            table_name="WRK_BIRP_NISS_APRM_DETL"
        )
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg = dyf.toDF().selectExpr(
            "CLNDR_YR",
            "NISS_CMPNY_CD",
            "ST_NM",
            "TTL_WRITTN_PREM_AMT",
            "REC_DROP_IND"
        )
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog after S3 fallback: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL (WRK_ staging source - S3-first)
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_FINAL from workflow S3 staging location")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/"
    ).selectExpr(
        "CLNDR_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "TTL_WRITTN_PREM_AMT"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_FINAL from S3 staging location")
except Exception as e:
    logger.warning(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from S3 staging (will fall back to Glue Catalog): {e}")
    try:
        logger.info("Falling back to Glue catalog read for WRK_BIRP_NISS_APRM_FINAL")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database="REPLACE_WITH_SOURCE_DB",
            table_name="WRK_BIRP_NISS_APRM_FINAL"
        )
        df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell = dyf.toDF().selectExpr(
            "CLNDR_YR",
            "NISS_CMPNY_CD",
            "ST_NM",
            "TTL_WRITTN_PREM_AMT"
        )
        logger.info("Read WRK_BIRP_NISS_APRM_FINAL from Glue Catalog")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from Glue Catalog after S3 fallback: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (SQL Override - staged case)
# - register staged upstream dataframes as temp views and run rewritten override via spark.sql
# -----------------------------------------------------------------------------
try:
    # register inputs as temp views using the real table names the SQL override references
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")

    sql_query = f"""SELECT
COALESCE(DTL.CLNDR_YR, FNL.CLNDR_YR) CLNDR_YR,
COALESCE(DTL.NISS_CMPNY_CD, FNL.NISS_CMPNY_CD) NISS_CMPNY_CD,
COALESCE(DTL.ST_NM, FNL.ST_NM) ST_NM,
COALESCE(DTL_PREM_AMT,0) DTL_PREM_AMT,
COALESCE(DROP_PREM_AMT,0) DROP_PREM_AMT,
COALESCE(FNL_PREM_AMT,0) FNL_PREM_AMT

FROM
(SELECT
COALESCE(D_ALL.CLNDR_YR, D_DROP.CLNDR_YR) CLNDR_YR,
COALESCE(D_ALL.NISS_CMPNY_CD, D_DROP.NISS_CMPNY_CD) NISS_CMPNY_CD,
COALESCE(D_ALL.ST_NM, D_DROP.ST_NM) ST_NM,
COALESCE(DTL_PREM_AMT,0) DTL_PREM_AMT,
COALESCE(DROP_PREM_AMT,0) DROP_PREM_AMT

FROM
(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) DTL_PREM_AMT
FROM WRK_BIRP_NISS_APRM_DETL
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_ALL

FULL OUTER JOIN

(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) DROP_PREM_AMT
FROM WRK_BIRP_NISS_APRM_DETL
WHERE REC_DROP_IND='Y'
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_DROP

ON D_ALL.CLNDR_YR = D_DROP.CLNDR_YR
AND D_ALL.NISS_CMPNY_CD = D_DROP.NISS_CMPNY_CD
AND D_ALL.ST_NM = D_DROP.ST_NM
) DTL

FULL OUTER JOIN

(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) FNL_PREM_AMT
FROM WRK_BIRP_NISS_APRM_FINAL
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) FNL

ON DTL.CLNDR_YR = FNL.CLNDR_YR
AND DTL.NISS_CMPNY_CD = FNL.NISS_CMPNY_CD
AND DTL.ST_NM = FNL.ST_NM
"""

    logger.info("Executing rewritten SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged temp views")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper = spark.sql(sql_query)
    logger.info("Completed SQL override spark.sql for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_DeriveBalanaceIndicator
# - compute two-digit year, balance difference, and balance indicator; project explicit columns
# -----------------------------------------------------------------------------
try:
    logger.info("Transforming EXP_DeriveBalanaceIndicator: deriving o_CLNDR_YR, BAL_DIFF, BAL_IND")
    df_stage = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper

    # compute two-digit calendar year (SUBSTR(TRIM(CLNDR_YR),3,2))
    df_stage = df_stage.withColumn("o_CLNDR_YR", expr("SUBSTR(TRIM(CLNDR_YR),3,2)"))

    # compute BAL_DIFF = DET_PREM_AMT - DROP_PREM_AMT - FNL_PREM_AMT, coalescing nulls to 0 to avoid null propagation
    df_stage = df_stage.withColumn(
        "BAL_DIFF",
        expr("COALESCE(DTL_PREM_AMT,0) - COALESCE(DROP_PREM_AMT,0) - COALESCE(FNL_PREM_AMT,0)")
    )

    # derive BAL_IND from BAL_DIFF (Y if zero, else N)
    df_stage = df_stage.withColumn("BAL_IND", when(col("BAL_DIFF") == 0, lit('Y')).otherwise(lit('N')))

    # explicitly project and rename columns to match the downstream flat-file target's expected headings
    df_EXP_DeriveBalanaceIndicator_calm_heisenberg = df_stage.selectExpr(
        "o_CLNDR_YR AS Clndr_Year",
        "NISS_CMPNY_CD AS Company_Number",
        "ST_NM AS State_Name",
        "DTL_PREM_AMT AS Detail_Premium_Amount",
        "DROP_PREM_AMT AS Detail_Dropped_Premium_Amount",
        "FNL_PREM_AMT AS Final_Premium_Amount",
        "BAL_DIFF AS Premium_Amount_Difference",
        "BAL_IND AS Balance_Indicator"
    )
    logger.info("Completed EXP_DeriveBalanaceIndicator transformation")
except Exception as e:
    logger.error(f"Failed transforming EXP_DeriveBalanaceIndicator: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2 (flat file target)
# - assign expected df name, convert to pandas and write via pandas writer (filename from mapping parameter)
# -----------------------------------------------------------------------------
try:
    logger.info("Assigning Output dataframe variable for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2 and writing flat file target via pandas")

    # assign the mapping-expected output dataframe name so downstream/lineage expectations are met
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_stoic_mendel = df_EXP_DeriveBalanaceIndicator_calm_heisenberg

    # convert to pandas
    import pandas as _pd

    pandas_df = df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_stoic_mendel.toPandas()

    target_path = f"{PM_TARGET_FILE_DIR.rstrip('/')}/{OutputFile_PremRpt_Bal2}"
    # choose writer based on extension (default to csv)
    if OutputFile_PremRpt_Bal2.lower().endswith('.csv'):
        pandas_df.to_csv(target_path, index=False, mode='w')
    elif OutputFile_PremRpt_Bal2.lower().endswith('.xlsx'):
        pandas_df.to_excel(target_path, index=False)
    else:
        # fallback to csv if unknown extension
        pandas_df.to_csv(target_path, index=False, mode='w')

    logger.info(f"Successfully wrote flat file to {target_path}")
except Exception as e:
    logger.error(f"Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2: {e}", exc_info=True)
    raise



job.commit()
