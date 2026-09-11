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

from pyspark.sql.functions import col, lit

# placeholder constants for environment-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# ---------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# Attempt an S3-first read of the upstream intermediate parquet path for WRK_BIRP_NISS_APRM_DETL.
# If missing, fall back to a Glue Catalog read. Register the resulting DF for downstream use.
# ---------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_shannon = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 parquet successfully")
except Exception as e:
    logger.warning(
        "Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read"
    )
    try:
        # fallback to Glue Catalog/table read
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database %s", GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_shannon = dyf.toDF()
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# ---------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# This SQ has a SQL Override that reads from FDR.WRK_BIRP_NISS_APRM_DETL; reuse the already-staged
# upstream dataframe by registering it as a temp view named 'WRK_BIRP_NISS_APRM_DETL' and
# execute the override (rewritten to reference the bare view). Project the SQ output columns.
# ---------------------------------------------------------------------------
try:
    # register staged DF as bare table temp view for the SQL override
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_shannon.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT DISTINCT DETL.NISS_APRM_DETL_SK,DETL.CVG_ATTR_SK,Y.CVG_CD_ATTR_SK
FROM (
SELECT DISTINCT CVG_ATTR_SK,CVG_TYP_CD,CVG_TYP_IND,CVG_CD_ATTR,ROW_NUMBER() OVER ( ORDER BY CVG_CD_ATTR) AS CVG_CD_ATTR_SK
FROM (

SELECT DISTINCT CVG_ATTR_SK,CVG_TYP_CD,CVG_TYP_IND,
(CLNDR_YR || REG_PRD_YR || NISS_CMPNY_CD || NISS_ST_CD || GRGNG_ZIP_5 || NISS_TERR_CD || LINE_CD || ACCDNT_YR || NISS_CVG_CD || RTNG_ZNE_CD || TERM_ZNE_CD || NISS_CLASS_CD || NISS_ELIG_PNTS_CD || NISS_AGE_GRP_CD || NISS_CMMCL_IND_CD || NISS_EXCPN_CD || NISS_FGVNS_CD || NISS_PASSV_RESTRA_CD || NISS_DEFNS_DRVR_CRD_CD || NISS_ANTI_THFT_DVC_CD || NISS_DAY_TM_RUN_LAMPS_DISC_CD || NISS_PLCY_LMT_CD || NISS_DEDUC_CD || NISS_SSL_LIAB_CD || NISS_SUBLOB_CD || NISS_TYP_LOSS_CD || NISS_LIAB_OR_NO_FAULT_CD || NISS_ANNL_STMNT_LOB_CD || NISS_PD_LOSS || NISS_PD_ALLOC_ADJUS_EXPNS || NISS_OUTSTNDG_LOSS || NISS_NO_PD_CLMS || NISS_NO_OUTSTND_CLMS || RSVD_NISS_USE || NISS_RSVD_CMPNY_USE || NISS_MNFCTRS_MDL_YR || CVG_TYP_CD || CVG_TYP_IND) AS CVG_CD_ATTR
FROM (

SELECT DISTINCT
CVG_ATTR_SK,
CASE WHEN (LTRIM(RTRIM(CLNDR_YR)) IS NULL OR  LTRIM(RTRIM(CLNDR_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CLNDR_YR)) END AS CLNDR_YR,
CASE WHEN (LTRIM(RTRIM(REG_PRD_YR)) IS NULL  OR LTRIM(RTRIM(REG_PRD_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(REG_PRD_YR)) END AS REG_PRD_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CMPNY_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMPNY_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMPNY_CD)) END AS NISS_CMPNY_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ST_CD)) IS NULL OR LTRIM(RTRIM(NISS_ST_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ST_CD)) END AS NISS_ST_CD,
CASE WHEN (LTRIM(RTRIM(GRGNG_ZIP_5)) IS NULL OR LTRIM(RTRIM(GRGNG_ZIP_5)) ='') THEN ' ' ELSE LTRIM(RTRIM(GRGNG_ZIP_5)) END AS GRGNG_ZIP_5,
CASE WHEN (LTRIM(RTRIM(NISS_TERR_CD)) IS NULL OR LTRIM(RTRIM(NISS_TERR_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TERR_CD)) END AS NISS_TERR_CD,
CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD,
CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND,
CASE WHEN (LTRIM(RTRIM(LINE_CD)) IS NULL OR LTRIM(RTRIM(LINE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(LINE_CD)) END AS LINE_CD,
CASE WHEN (LTRIM(RTRIM(ACCDNT_YR)) IS NULL OR LTRIM(RTRIM(ACCDNT_YR)) ='') THEN ' ' ELSE LTRIM(RTRIM(ACCDNT_YR)) END AS ACCDNT_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CVG_CD)) IS NULL  OR LTRIM(RTRIM(NISS_CVG_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CVG_CD)) END AS NISS_CVG_CD,
CASE WHEN (LTRIM(RTRIM(RTNG_ZNE_CD)) IS NULL OR LTRIM(RTRIM(RTNG_ZNE_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(RTNG_ZNE_CD)) END AS RTNG_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(TERM_ZNE_CD)) IS NULL OR LTRIM(RTRIM(TERM_ZNE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(TERM_ZNE_CD)) END AS TERM_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CLASS_CD)) IS NULL OR LTRIM(RTRIM(NISS_CLASS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CLASS_CD)) END AS NISS_CLASS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) IS NULL OR LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) END AS NISS_ELIG_PNTS_CD,
TO_CHAR(CASE WHEN NISS_AGE_GRP_CD IS NULL THEN 0 ELSE NISS_AGE_GRP_CD END) AS NISS_AGE_GRP_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CMMCL_IND_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMMCL_IND_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMMCL_IND_CD)) END AS NISS_CMMCL_IND_CD,
CASE WHEN (LTRIM(RTRIM(NISS_EXCPN_CD)) IS NULL OR LTRIM(RTRIM(NISS_EXCPN_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_EXCPN_CD)) END AS NISS_EXCPN_CD,
CASE WHEN (LTRIM(RTRIM(NISS_FGVNS_CD)) IS NULL OR LTRIM(RTRIM(NISS_FGVNS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_FGVNS_CD)) END AS NISS_FGVNS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) IS NULL OR LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) END AS NISS_PASSV_RESTRA_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) END AS NISS_DEFNS_DRVR_CRD_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) END AS NISS_ANTI_THFT_DVC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) END AS NISS_DAY_TM_RUN_LAMPS_DISC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PLCY_LMT_CD)) IS NULL OR LTRIM(RTRIM(NISS_PLCY_LMT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PLCY_LMT_CD)) END AS NISS_PLCY_LMT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEDUC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEDUC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEDUC_CD)) END AS NISS_DEDUC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SSL_LIAB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SSL_LIAB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SSL_LIAB_CD)) END AS NISS_SSL_LIAB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SUBLOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SUBLOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SUBLOB_CD)) END AS NISS_SUBLOB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_TYP_LOSS_CD)) IS NULL OR LTRIM(RTRIM(NISS_TYP_LOSS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TYP_LOSS_CD)) END AS NISS_TYP_LOSS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) IS NULL OR LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) END AS NISS_LIAB_OR_NO_FAULT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) END AS NISS_ANNL_STMNT_LOB_CD,
CASE WHEN NISS_PD_LOSS IS NULL THEN 0 ELSE NISS_PD_LOSS END AS NISS_PD_LOSS,
CASE WHEN NISS_PD_ALLOC_ADJUS_EXPNS IS NULL THEN 0 ELSE NISS_PD_ALLOC_ADJUS_EXPNS END AS NISS_PD_ALLOC_ADJUS_EXPNS,
CASE WHEN NISS_OUTSTNDG_LOSS IS NULL THEN 0 ELSE NISS_OUTSTNDG_LOSS END AS NISS_OUTSTNDG_LOSS,
CASE WHEN NISS_NO_PD_CLMS IS NULL THEN 0 ELSE NISS_NO_PD_CLMS END AS NISS_NO_PD_CLMS,
CASE WHEN NISS_NO_OUTSTND_CLMS IS NULL THEN 0 ELSE NISS_NO_OUTSTND_CLMS END AS NISS_NO_OUTSTND_CLMS,
CASE WHEN (LTRIM(RTRIM(RSVD_NISS_USE)) IS NULL OR LTRIM(RTRIM(RSVD_NISS_USE)) ='') THEN 0 ELSE LTRIM(RTRIM(RSVD_NISS_USE)) END AS RSVD_NISS_USE,
CASE WHEN (LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) IS NULL OR LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) END AS NISS_RSVD_CMPNY_USE,
CASE WHEN NISS_MNFCTRS_MDL_YR IS NULL THEN 0 ELSE NISS_MNFCTRS_MDL_YR END AS NISS_MNFCTRS_MDL_YR

FROM WRK_BIRP_NISS_APRM_DETL

ORDER BY 
CLNDR_YR,
REG_PRD_YR,
NISS_CMPNY_CD,
NISS_ST_CD,
GRGNG_ZIP_5,
NISS_TERR_CD,
LINE_CD,
ACCDNT_YR,
NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
NISS_CLASS_CD,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR) A) X)Y,(SELECT NISS_APRM_DETL_SK,CVG_ATTR_SK,CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD,
CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND FROM WRK_BIRP_NISS_APRM_DETL) DETL
WHERE Y.CVG_ATTR_SK = DETL.CVG_ATTR_SK
AND LTRIM(RTRIM(Y.CVG_TYP_CD)) = LTRIM(RTRIM(DETL.CVG_TYP_CD))
AND LTRIM(RTRIM(Y.CVG_TYP_IND)) = LTRIM(RTRIM(DETL.CVG_TYP_IND))
ORDER BY DETL.NISS_APRM_DETL_SK,DETL.CVG_ATTR_SK
"""

    logger.info("Executing SQ override SQL against temp view WRK_BIRP_NISS_APRM_DETL")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_newton = spark.sql(sql_query)
    # Ensure SQ outputs exactly the three fields expected by downstream logic
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_newton = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_newton.selectExpr(
        "NISS_APRM_DETL_SK",
        "CVG_ATTR_SK",
        "CVG_CD_ATTR_SK"
    )
    logger.info("SQ override executed and projected expected columns")
except Exception as e:
    logger.error(f"Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# Expression: EXP_To_generate_CVG_CD_SK
# Project the explicit output ports (NISS_APRM_DETL_SK, CVG_CD_ATTR_SK)
# ---------------------------------------------------------------------------
try:
    logger.info("Applying EXP_To_generate_CVG_CD_SK projection")
    df_EXP_To_generate_CVG_CD_SK_sleepy_franklin = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_newton.selectExpr(
        "NISS_APRM_DETL_SK",
        "CVG_CD_ATTR_SK"
    )
except Exception as e:
    logger.error(f"Failed transforming EXP_To_generate_CVG_CD_SK: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# Update Strategy: Upd_CVG_CD_SK
# Derive dd_op = 'UPDATE' for every row, drop REJECT rows, then apply load-modify-store-back
# against the target WRK_BIRP_NISS_APRM_DETL1 on S3.
# ---------------------------------------------------------------------------
from pyspark.sql import DataFrame
try:
    logger.info("Deriving dd_op marker for Update Strategy (DD_UPDATE -> 'UPDATE')")
    df_Upd_CVG_CD_SK_kind_gauss_intermediate = df_EXP_To_generate_CVG_CD_SK_sleepy_franklin.withColumn(
        "dd_op", lit("UPDATE")
    )

    # drop REJECT rows if any
    df_Upd_CVG_CD_SK_kind_gauss_filtered = df_Upd_CVG_CD_SK_kind_gauss_intermediate.filter(col("dd_op") != "REJECT")

    # rows to apply (INSERT/UPDATE). In this mapping, all rows are UPDATE per configuration.
    df_to_apply = df_Upd_CVG_CD_SK_kind_gauss_filtered.filter(col("dd_op").isin(["INSERT", "UPDATE"]))

    # Build a full-row dataframe for the target schema by ensuring every target column exists.
    # List of output target columns is taken from the Output node's declared fields.
    target_columns = [
        "NISS_APRM_DETL_SK", "CLNDR_YR", "CALL_YR", "NAIC_CMPNY_CD", "NISS_CMPNY_CD", "ST_NM",
        "ST_CD", "NISS_ST_CD", "ST_ABBR", "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT",
        "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND", "UM_UMI_STACKING", "PIP_WVR_WL_IND", "PIP_MED_SEC_IND",
        "PIP_LOSS_INCOME_IND", "MI_PPO_IND", "PRD_GRP_CD", "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG",
        "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND", "NY_FULL_CVG_GLASS_COMP_IND", "GRGNG_ZIP_5",
        "NISS_TERR_CD", "RATNG_CMPY_CD", "MLT_CAR_IND", "RT_CLS", "AGE", "GENDR", "MRTL_STAT",
        "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND", "DRVR_TRNG_IND", "SOI_TYP", "PHY_DMG_IND",
        "NJ_RATD_PNTS", "VEH_MDL_YR", "NJ_EXCPTION_CD", "NJ_FGVN_PNTS", "PASSV_RESTRA_DISC",
        "SNR_DRVR_IND", "DEFNS_DRVR_DISC_IND", "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT",
        "ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL", "TTL_WRITTN_PREM_AMT", "LINE_CD",
        "ACCDNT_YR", "NISS_CVG_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "NISS_CLASS_CD", "NISS_ELIG_PNTS_CD",
        "NISS_AGE_GRP_CD", "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD",
        "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD", "NISS_TYP_LOSS_CD", "NISS_LIAB_OR_NO_FAULT_CD",
        "NISS_ANNL_STMNT_LOB_CD", "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_OUTSTNDG_LOSS",
        "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS", "RSVD_NISS_USE", "NISS_RSVD_CMPNY_USE", "NISS_MNFCTRS_MDL_YR",
        "CR_BY_MAPNG_ID", "DW_CR_TMSP", "UPD_BY_MAPNG_ID", "DW_UPD_TMSP", "WRK_FLOW_RUN_ID",
        "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "CVG_CNT_IND", "CVG_CNT",
        "CVG_CD_SK", "CVG_ATTR_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC", "CVG_ATTR_CHCKSUM", "COMP_DED", "COLL_DED", "PLCY_CNTRCT_NUM",
        "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH", "RDRVR_DT_OF_BRTH", "TERM_STRT_DT", "SRC_SYS_CD",
        "DERIVED_RDRVR_AGE", "FINAL_RDRVR_AGE", "PNI_AGE", "LOB", "PRINCIPAL_OPRT", "SOURCE_IND_DERIVED",
        # Note: many columns are present in the target; this list mirrors the Output node's declared fields
        "CVG_CD_ATTR_SK"  # include the CVG_CD_ATTR_SK produced upstream
    ]

    # Build expressions to ensure df_to_apply contains every target column (fill missing with NULL)
    exprs = []
    available_cols = set(df_to_apply.columns)
    for c in target_columns:
        if c in available_cols:
            exprs.append(col(c))
        else:
            exprs.append(lit(None).alias(c))

    df_to_apply_full = df_to_apply.select(*exprs)

    target_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/"

    # Read existing full target table; if missing, treat as empty (we will then write only the applied rows)
    try:
        logger.info("Reading existing target table from %s for load-modify-store-back", target_path)
        existing_df = spark.read.parquet(target_path)
        logger.info("Successfully read existing target table for WRK_BIRP_NISS_APRM_DETL1")
    except Exception as e:
        logger.warning("Existing target table WRK_BIRP_NISS_APRM_DETL1 not found on S3; treating as empty existing set")
        # create empty dataframe with same schema as df_to_apply_full
        existing_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_to_apply_full.schema)

    # Build keys-only DF of rows being changed
    changed_keys_df = df_to_apply_full.select("NISS_APRM_DETL_SK").distinct()

    # Anti-join to remove any existing rows that are being updated/deleted
    existing_remaining = existing_df.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")

    # Combine: surviving existing rows unioned with the incoming INSERT/UPDATE rows
    combined_df = existing_remaining.unionByName(df_to_apply_full, allowMissingColumns=True)

    # Write the full combined dataframe back to the same target path (overwrite)
    logger.info("Writing combined target table back to %s (overwrite) - this may be expensive for large targets", target_path)
    combined_df.write.mode("overwrite").parquet(target_path)
    logger.info("Update Strategy apply complete: WRK_BIRP_NISS_APRM_DETL1 updated on S3")

    # Assign the node's output dataframe variable to the applied result for downstream consumption
    df_Upd_CVG_CD_SK_kind_gauss = combined_df

except Exception as e:
    logger.error(f"Failed executing Update Strategy Upd_CVG_CD_SK: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1
# Write the incoming dataframe to S3 as parquet (overwrite)
# ---------------------------------------------------------------------------
try:
    # pass-through assignment
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_archimedes = df_Upd_CVG_CD_SK_kind_gauss

    logger.info("Writing WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_archimedes.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/"
    )
    logger.info("Write complete for WRK_BIRP_NISS_APRM_DETL1")
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
