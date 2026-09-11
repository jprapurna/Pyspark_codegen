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

from pyspark.sql.functions import expr, col, when, lit

# Placeholder constants for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"
OUTPUT_FILE_PREM_RPT_BAL2 = "REPLACE_WITH_OUTPUT_FILE_PREM_RPT_BAL2_VALUE"

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# Attempt S3-first read of WRK_BIRP_NISS_APRM_DETL, fall back to Glue Catalog on failure
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    # project exactly the fields listed on the node
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_stoic_socrates = df_tmp.select(
        "NISS_APRM_DETL_SK",
        "CLNDR_YR",
        "CALL_YR",
        "NAIC_CMPNY_CD",
        "NISS_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "NISS_ST_CD",
        "ST_ABBR",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "BI_LMT",
        "GA_ADDED_AT_FAULT_IND",
        "FA2_PLCY_IND",
        "UM_UMI_STACKING",
        "PIP_WVR_WL_IND",
        "PIP_MED_SEC_IND",
        "PIP_LOSS_INCOME_IND",
        "MI_PPO_IND",
        "PRD_GRP_CD",
        "NJ_HLTH_INSR_PRIM",
        "NJ_EXTR_PIP_PKG",
        "NJ_RESDNC_RLTNSHP_PIP_IND",
        "NY_SSL_IND",
        "NY_FULL_CVG_GLASS_COMP_IND",
        "GRGNG_ZIP_5",
        "NISS_TERR_CD",
        "RATNG_CMPY_CD",
        "MLT_CAR_IND",
        "RT_CLS",
        "AGE",
        "GENDR",
        "MRTL_STAT",
        "AUTO_USE_CD",
        "MILES_TO_WRK",
        "GOOD_STDNT_IND",
        "DRVR_TRNG_IND",
        "SOI_TYP",
        "PHY_DMG_IND",
        "NJ_RATD_PNTS",
        "VEH_MDL_YR",
        "NJ_EXCPTION_CD",
        "NJ_FGVN_PNTS",
        "PASSV_RESTRA_DISC",
        "SNR_DRVR_IND",
        "DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC",
        "DAY_TM_RUN_LIGHTS",
        "LMT_TORT",
        "ANNL_STMNT_LOB_CD",
        "CVG_TYP_IND",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
        "LINE_CD",
        "ACCDNT_YR",
        "NISS_CVG_CD",
        "RTNG_ZNE_CD",
        "TERM_ZNE_CD",
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
        "WRK_FLOW_RUN_ID",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "EXPS_VAL_ROLLED",
        "CVG_CNT_IND",
        "CVG_CNT",
        "CVG_CD_SK",
        "CVG_ATTR_SK",
        "REC_DROP_IND",
        "REC_DROP_RSN_DESC",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC",
        "CVG_ATTR_CHCKSUM",
        "COMP_DED",
        "COLL_DED",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT",
        "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "DERIVED_RDRVR_AGE",
        "FINAL_RDRVR_AGE",
        "PNI_AGE",
        "LOB",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED"
    )
except Exception as e:
    logger.warning("Staged S3 read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL" % GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_tmp = dyf.toDF()
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_stoic_socrates = df_tmp.select(
            "NISS_APRM_DETL_SK",
            "CLNDR_YR",
            "CALL_YR",
            "NAIC_CMPNY_CD",
            "NISS_CMPNY_CD",
            "ST_NM",
            "ST_CD",
            "NISS_ST_CD",
            "ST_ABBR",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "GA_ADDED_AT_FAULT_IND",
            "FA2_PLCY_IND",
            "UM_UMI_STACKING",
            "PIP_WVR_WL_IND",
            "PIP_MED_SEC_IND",
            "PIP_LOSS_INCOME_IND",
            "MI_PPO_IND",
            "PRD_GRP_CD",
            "NJ_HLTH_INSR_PRIM",
            "NJ_EXTR_PIP_PKG",
            "NJ_RESDNC_RLTNSHP_PIP_IND",
            "NY_SSL_IND",
            "NY_FULL_CVG_GLASS_COMP_IND",
            "GRGNG_ZIP_5",
            "NISS_TERR_CD",
            "RATNG_CMPY_CD",
            "MLT_CAR_IND",
            "RT_CLS",
            "AGE",
            "GENDR",
            "MRTL_STAT",
            "AUTO_USE_CD",
            "MILES_TO_WRK",
            "GOOD_STDNT_IND",
            "DRVR_TRNG_IND",
            "SOI_TYP",
            "PHY_DMG_IND",
            "NJ_RATD_PNTS",
            "VEH_MDL_YR",
            "NJ_EXCPTION_CD",
            "NJ_FGVN_PNTS",
            "PASSV_RESTRA_DISC",
            "SNR_DRVR_IND",
            "DEFNS_DRVR_DISC_IND",
            "ANTI_THFT_DISC",
            "DAY_TM_RUN_LIGHTS",
            "LMT_TORT",
            "ANNL_STMNT_LOB_CD",
            "CVG_TYP_IND",
            "CVG_EXPS_VAL",
            "TTL_WRITTN_PREM_AMT",
            "LINE_CD",
            "ACCDNT_YR",
            "NISS_CVG_CD",
            "RTNG_ZNE_CD",
            "TERM_ZNE_CD",
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
            "WRK_FLOW_RUN_ID",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "EXPS_VAL_ROLLED",
            "CVG_CNT_IND",
            "CVG_CNT",
            "CVG_CD_SK",
            "CVG_ATTR_SK",
            "REC_DROP_IND",
            "REC_DROP_RSN_DESC",
            "REC_EXCPN_IND",
            "REC_EXCPN_RSN_DESC",
            "CVG_ATTR_CHCKSUM",
            "COMP_DED",
            "COLL_DED",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM",
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "DERIVED_RDRVR_AGE",
            "FINAL_RDRVR_AGE",
            "PNI_AGE",
            "LOB",
            "PRINCIPAL_OPRT",
            "SOURCE_IND_DERIVED"
        )
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from both S3 and Glue Catalog: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_FINAL
# Attempt S3-first read of WRK_BIRP_NISS_APRM_FINAL, fall back to Glue Catalog on failure
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_FINAL from S3")
    df_tmp = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_FINAL/")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_epic_franklin = df_tmp.select(
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
    logger.warning("Staged S3 read for WRK_BIRP_NISS_APRM_FINAL failed, falling back to Glue Catalog: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_FINAL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_FINAL" % GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_FINAL")
        df_tmp = dyf.toDF()
        df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_epic_franklin = df_tmp.select(
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
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_FINAL from both S3 and Glue Catalog: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL
# The SQL Override reads workflow-staged tables; register the staged dataframes as temp views
# and run the rewritten override via spark.sql()
# -----------------------------------------------------------------------------
try:
    # register temp views for the staged inputs (use the real table names)
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_stoic_socrates.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_epic_franklin.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")

    sql_query = f"""SELECT
  COALESCE(DTL.CLNDR_YR, FNL.CLNDR_YR) AS CLNDR_YR,
  COALESCE(DTL.NISS_CMPNY_CD, FNL.NISS_CMPNY_CD) AS NISS_CMPNY_CD,
  COALESCE(DTL.ST_NM, FNL.ST_NM) AS ST_NM,
  COALESCE(DTL_PREM_AMT,0) AS DET_PREM_AMT,
  COALESCE(DROP_PREM_AMT,0) AS DROP_PREM_AMT,
  COALESCE(FNL_PREM_AMT,0) AS FNL_PREM_AMT

FROM
(
  SELECT
    COALESCE(D_ALL.CLNDR_YR, D_DROP.CLNDR_YR) AS CLNDR_YR,
    COALESCE(D_ALL.NISS_CMPNY_CD, D_DROP.NISS_CMPNY_CD) AS NISS_CMPNY_CD,
    COALESCE(D_ALL.ST_NM, D_DROP.ST_NM) AS ST_NM,
    COALESCE(DTL_PREM_AMT,0) AS DTL_PREM_AMT,
    COALESCE(DROP_PREM_AMT,0) AS DROP_PREM_AMT
  FROM
    (SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
      SUM(TTL_WRITTN_PREM_AMT) AS DTL_PREM_AMT
     FROM WRK_BIRP_NISS_APRM_DETL
     GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_ALL
  FULL OUTER JOIN
    (SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
      SUM(TTL_WRITTN_PREM_AMT) AS DROP_PREM_AMT
     FROM WRK_BIRP_NISS_APRM_DETL
     WHERE REC_DROP_IND='Y'
     GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_DROP
  ON D_ALL.CLNDR_YR = D_DROP.CLNDR_YR
  AND D_ALL.NISS_CMPNY_CD = D_DROP.NISS_CMPNY_CD
  AND D_ALL.ST_NM = D_DROP.ST_NM
) DTL

FULL OUTER JOIN
  (SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
    SUM(TTL_WRITTN_PREM_AMT) AS FNL_PREM_AMT
   FROM WRK_BIRP_NISS_APRM_FINAL
   GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) FNL

ON DTL.CLNDR_YR = FNL.CLNDR_YR
AND DTL.NISS_CMPNY_CD = FNL.NISS_CMPNY_CD
AND DTL.ST_NM = FNL.ST_NM
"""

    logger.info("Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_hopper = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_DeriveBalanaceIndicator
# Compute o_CLNDR_YR = substr(trim(CLNDR_YR),3,2), pass through NISS_CMPNY_CD, ST_NM,
# DET_PREM_AMT, DROP_PREM_AMT, FNL_PREM_AMT, compute BAL_DIFF then BAL_IND
# -----------------------------------------------------------------------------
try:
    logger.info("Transforming EXP_DeriveBalanaceIndicator: computing o_CLNDR_YR, BAL_DIFF, and BAL_IND")

    # explicit projection with derived o_CLNDR_YR; do not use '*' and list every column
    df_exp_proj = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_hopper.selectExpr(
        "CLNDR_YR",
        "NISS_CMPNY_CD",
        "ST_NM",
        "DET_PREM_AMT",
        "DROP_PREM_AMT",
        "FNL_PREM_AMT",
        "substr(trim(CLNDR_YR),3,2) AS o_CLNDR_YR"
    )

    # compute intermediate BAL_DIFF = DET_PREM_AMT - DROP_PREM_AMT - FNL_PREM_AMT
    df_with_bal = df_exp_proj.withColumn("BAL_DIFF", expr("DET_PREM_AMT - DROP_PREM_AMT - FNL_PREM_AMT"))

    # derive BAL_IND using the intermediate BAL_DIFF so we don't re-inline the arithmetic
    df_EXP_DeriveBalanaceIndicator_optimistic_curie = df_with_bal.withColumn(
        "BAL_IND",
        when(col("BAL_DIFF") == 0, lit('Y')).otherwise(lit('N'))
    )
except Exception as e:
    logger.error(f"Failed processing EXP_DeriveBalanaceIndicator: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2 (flat file target)
# Convert to pandas and write to the configured PM target directory using the
# configured filename. Choose writer by extension; default to CSV if unknown.
# -----------------------------------------------------------------------------
try:
    logger.info("Preparing flat-file output for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2")
    df_pd = df_EXP_DeriveBalanaceIndicator_optimistic_curie.toPandas()

    # Normalize the target directory and filename placeholders
    target_dir = PM_TARGET_FILE_DIR.rstrip('\\/')
    filename = OUTPUT_FILE_PREM_RPT_BAL2

    # Determine extension and choose writer
    if isinstance(filename, str) and ('.' in filename):
        ext = filename.split('.')[-1].lower()
    else:
        ext = 'csv'

    full_path = f"{target_dir}/{filename}"

    logger.info(f"Writing flat file to {full_path} (overwrite)")
    if ext in ('csv', 'txt', 'out'):
        df_pd.to_csv(full_path, index=False, mode='w')
    elif ext in ('xls', 'xlsx'):
        # to_excel will overwrite by default when mode='w' isn't accepted; use ExcelWriter for control
        with pd.ExcelWriter(full_path, engine='openpyxl', mode='w') as writer:
            df_pd.to_excel(writer, index=False)
    else:
        # default fallback to CSV
        df_pd.to_csv(full_path, index=False, mode='w')

    # assign the output dataframe variable name expected downstream (even though file is written)
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_silly_tesla = df_EXP_DeriveBalanaceIndicator_optimistic_curie
except Exception as e:
    logger.error(f"Failed writing flat file for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2: {e}", exc_info=True)
    raise


job.commit()
