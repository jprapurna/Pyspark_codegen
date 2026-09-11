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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

# Read staged intermediate WRK_BIRP_NISS_APRM_FINAL from S3 first, fall back to Glue Catalog if missing
try:
    logger.info("Attempting to read df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_nice_einstein from S3 path s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/")
    df_temp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/")
except Exception as e:
    logger.warning(f"S3 read for WRK_BIRP_NISS_APRM_FINAL failed or path not present, falling back to Glue Catalog: {e}")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_FINAL from Glue Data Catalog")
        df_temp = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_FINAL").toDF()
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from Glue Catalog: {e2}", exc_info=True)
        raise

# Project exactly the fields listed on the Source node
try:
    logger.info("Projecting exact columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_nice_einstein")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_nice_einstein = df_temp.selectExpr(
        "NISS_APRM_FINAL_SK",
        "CLNDR_YR",
        "CALL_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "ST_ABBR",
        "NISS_ST_CD",
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "NISS_TERR_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
        "GRGNG_ZIP_5",
        "NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD",
        "NISS_CMMCL_IND_CD",
        "NISS_EXCPN_CD",
        "NISS_FGVNS_CD",
        "NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD",
        "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
        "NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD",
        "NISS_LIAB_OR_NO_FAULT_CD",
        "NISS_ANNL_STMNT_LOB_CD",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE",
        "NISS_RSVD_CMPNY_USE",
        "NISS_MNFCTRS_MDL_YR",
        "CR_BY_MAPNG_ID",
        "DW_CR_TMSP",
        "UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP",
        "WRK_FLOW_RUN_ID"
    )
except Exception as e:
    logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: {e}", exc_info=True)
    raise

# Application Source Qualifier with SQL Override rewritten to run against the staged temp view
try:
    # register the upstream staged dataframe as the bare table name the override expects
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_nice_einstein.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")

    sql_query = f"""SELECT
    NISS_CMPNY_CD,
    CLNDR_YR,
    ST_ABBR,
    NISS_ST_CD,
    ACCDNT_YR,
    NISS_CVG_CD,
    NISS_CLASS_CD,
    NISS_SUBLOB_CD,
    NISS_TYP_LOSS_CD,
    NISS_ANNL_STMNT_LOB_CD,
    SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
    SUM(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
    NISS_PD_LOSS,
    NISS_PD_ALLOC_ADJUS_EXPNS,
    NISS_OUTSTNDG_LOSS,
    NISS_NO_PD_CLMS,
    NISS_NO_OUTSTND_CLMS,
    NISS_TERR_CD
FROM WRK_BIRP_NISS_APRM_FINAL
GROUP BY
    NISS_CMPNY_CD,
    CLNDR_YR,
    ST_ABBR,
    NISS_ST_CD,
    ACCDNT_YR,
    NISS_CVG_CD,
    NISS_CLASS_CD,
    NISS_SUBLOB_CD,
    NISS_TYP_LOSS_CD,
    NISS_ANNL_STMNT_LOB_CD,
    NISS_PD_LOSS,
    NISS_PD_ALLOC_ADJUS_EXPNS,
    NISS_OUTSTNDG_LOSS,
    NISS_NO_PD_CLMS,
    NISS_NO_OUTSTND_CLMS,
    NISS_TERR_CD
"""

    logger.info("Executing rewritten SQ SQL override against staged temp view for df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_loving_descartes")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_loving_descartes = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL SQL override against staged view: {e}", exc_info=True)
    raise

# EXP_Passthru: explicit projection of SQ outputs + Informatica PM... audit ports as NULLs
try:
    logger.info("Applying EXP_Passthru projection to create df_EXP_Passthru_laughing_galileo")
    df_EXP_Passthru_laughing_galileo = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_loving_descartes.selectExpr(
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "ST_ABBR",
        "NISS_ST_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "NISS_CLASS_CD",
        "NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD",
        "NISS_ANNL_STMNT_LOB_CD",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "NISS_TERR_CD",
        "NULL AS MAPPING_NAME",
        "NULL AS FOLDER_NAME",
        "NULL AS WORKFLOW_NAME"
    )
except Exception as e:
    logger.error(f"Failed in EXP_Passthru transformation: {e}", exc_info=True)
    raise

# EXP_Pass_Tgt: project target columns then generate surrogate key NISS_APRM_SUMRY_SK via row_number()
try:
    logger.info("Applying EXP_Pass_Tgt projection and generating surrogate key to create df_EXP_Pass_Tgt_pensive_plato")
    # project/pass-through all target columns except the surrogate key
    df_proj = df_EXP_Passthru_laughing_galileo.selectExpr(
        "NISS_CMPNY_CD",
        "CLNDR_YR",
        "ST_ABBR",
        "NISS_ST_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "NISS_CLASS_CD",
        "NISS_SUBLOB_CD",
        "NISS_TYP_LOSS_CD",
        "NISS_ANNL_STMNT_LOB_CD",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "NISS_PD_LOSS",
        "NISS_PD_ALLOC_ADJUS_EXPNS",
        "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS",
        "NISS_NO_OUTSTND_CLMS",
        "CR_BY_MAPNG_ID",
        "DW_CR_TMSP",
        "UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP",
        "WRK_FLOW_RUN_ID",
        "NISS_TERR_CD"
    )

    # NOTE: Sequence Generator semantics require a deterministic, gap-free row_number() over an ordering
    # This forces a single-partition shuffle; review if input volume is large.
    window = Window.orderBy(lit(1))
    df_EXP_Pass_Tgt_pensive_plato = df_proj.withColumn("NISS_APRM_SUMRY_SK", row_number().over(window))
    # Ensure the SK column appears in the schema in the expected position if downstream tooling expects it first
    # (We do not reorder here; downstream consumers should reference by name.)
except Exception as e:
    logger.error(f"Failed in EXP_Pass_Tgt (projection + SK generation): {e}", exc_info=True)
    raise

# Assign Output node's dataframe variable and write out to S3 as parquet (overwrite)
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_upbeat_franklin = df_EXP_Pass_Tgt_pensive_plato
except Exception as e:
    logger.error(f"Failed assigning output dataframe variable for FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY: {e}", exc_info=True)
    raise

# write intermediate WRK_ table as parquet to S3 (overwrite)
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_SUMRY to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_upbeat_franklin.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_SUMRY/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_SUMRY to S3: {e}", exc_info=True)
    raise


job.commit()
