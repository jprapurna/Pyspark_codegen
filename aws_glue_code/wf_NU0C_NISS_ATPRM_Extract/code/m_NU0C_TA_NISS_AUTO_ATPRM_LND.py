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

from pyspark.sql.functions import col, trim, when, lit, broadcast, row_number
from pyspark.sql.window import Window

# mapping / environment placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
SCHEMA_FPR = "REPLACE_WITH_SCHEMA_FPR"
FPR_TA_DA_CVG_LVL_STAT_TBL = "REPLACE_WITH_FPR_TA_DA_CVG_LVL_STAT_TBL"
GEO_ST_NM = "REPLACE_WITH_GEO_ST_NM"
RPT_YEAR = "REPLACE_WITH_RPT_YEAR"
LOOKUP_FILE_FF_NISS_STATE = "REPLACE_WITH_LookupFile_ff_NISS_STATE_PATH"
SOURCE_DATABASE = "REPLACE_WITH_SOURCE_DATABASE"

# ---------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND (WRK_ intermediate) - S3-first
# ---------------------------------------------------------------------
try:
    logger.info("Attempting S3-first read for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND")
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_TA_NISS_NU0C_APRM_LND/")
    )
    logger.info("Loaded Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from S3 parquet")
except Exception as e:
    logger.warning("S3 parquet for WRK_BIRP_TA_NISS_NU0C_APRM_LND not found or unreadable; falling back to Glue Catalog/source read: %s" % e)
    try:
        logger.info("Falling back to Glue Catalog read for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND")
        # NOTE: replace SOURCE_DATABASE and table name with real catalog values if available
        dynamic_df = glueContext.create_dynamic_frame.from_catalog(
            database=SOURCE_DATABASE,
            table_name="wrk_birp_ta_niss_nu0c_aprm_lnd"
        )
        df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = dynamic_df.toDF()
        logger.info("Loaded Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from Glue Catalog as fallback")
    except Exception as e2:
        logger.error(f"Failed fallback read for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND: {e2}", exc_info=True)
        raise

# ---------------------------------------------------------------------
# Application Source Qualifier with SQL override (non-staged default case)
# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND (bypassed — SQL Override reads the source directly)
# ---------------------------------------------------------------------
sql_query = f"""SELECT  REGSTR_CLS_MNTH,
 FISC_PER_YR,
NAIC_CMPNY_CD,
 NISS_CMPNY_CD,
 STATE_NM,
STATE_CD,
ACCT_LOB,
CVG_CD,
CVG_AMT,
BI_LMT,
GA_UMBI_PD_ADDED_IND,
FA2_PLCY_IND,
UM_UIM_STACKING_IND,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
PRD_GRP_CD,
NJ_HLTH_INSR_PRIM,
NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
RATNG_CMPY_CD,
MLT_CAR_IND,
RT_CLS,
RTD_DRVR_AGE,
RTD_DRVR_GNDR_CD,
RTD_DRVR_MRTL_STAT,
AUTO_USE_CD,
MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP_CD,
PHY_DMG_IND,
NJ_RTD_PNTS,
VEH_YR,
NJ_EXCPTION_CD,
NJ_FRGVN_PNTS,
PASSV_RESTRA_DISC_IND,
SNR_DRVR_IND,
DEFNSV_DRVR_DISC_IND,
ANTI_THFT_DISC_IND,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
CVG_TYP_IND,
CASE WHEN EXPSR_MNTH_CNT=0 THEN 12 ELSE EXPSR_MNTH_CNT END as EXPSR_MNTH_CNT,
WRITTN_PREM_AMT,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
COMP_DED,
COLL_DED,
PLCY_NUM,
UNIT_NUM,
EFF_DT,
NUM_OF_CARS_IN_HH,
RTD_DRVR_DOB,
PLCY_TRM_STRT_DT,
SRC_SYS_CD,
PNI_AGE,
MIS_LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED
FROM(
SELECT TO_CHAR(REGSTR_CLS_MNTH) AS REGSTR_CLS_MNTH,
'' as FISC_PER_YR,
NAIC_CMPNY_CD,
CASE WHEN NAIC_CMPNY_CD='21687' THEN '176'
WHEN NAIC_CMPNY_CD='44245' THEN '941' END AS NISS_CMPNY_CD,
TRIM(UPPER(STATE_NM)) AS STATE_NM,
FIG_STATE AS STATE_CD,
ACCT_LOB,
CVG_CD,
CVG_AMT,
BI_LMT,
GA_UMBI_PD_ADDED_IND,
'' as FA2_PLCY_IND,
UM_UIM_STACKING_IND,
NULL as PIP_WVR_WL_IND,
NULL as PIP_MED_SEC_IND,
NULL as PIP_LOSS_INCOME_IND,
NULL as MI_PPO_IND,
'' as PRD_GRP_CD,
'' as NJ_HLTH_INSR_PRIM,
'' as NJ_EXTR_PIP_PKG,
NULL as NJ_RESDNC_RLTNSHP_PIP_IND,
NULL as NY_SSL_IND,
NULL as NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
'' as RATNG_CMPY_CD,
MLT_CAR_IND,
'' as RT_CLS,
RTD_DRVR_AGE,
RTD_DRVR_GNDR_CD,
CASE WHEN RTD_DRVR_MRTL_STAT='single' THEN 'S'
WHEN RTD_DRVR_MRTL_STAT='married' THEN 'M' 
ELSE UPPER(RTD_DRVR_MRTL_STAT) END AS RTD_DRVR_MRTL_STAT,
AUTO_USE_CD,
'' as MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP_CD,
CASE WHEN PHY_DMG_IND='Y' THEN 1 
WHEN PHY_DMG_IND='N' THEN 0
ELSE NULL END AS PHY_DMG_IND,
'' as NJ_RTD_PNTS,
try_to_number(VEH_YR) as VEH_YR,
'' AS NJ_EXCPTION_CD,
'' AS NJ_FRGVN_PNTS,
PASSV_RESTRA_DISC_IND,
'' AS SNR_DRVR_IND,
CASE WHEN DEFNSV_DRVR_DISC_IND='Y' THEN 1
WHEN DEFNSV_DRVR_DISC_IND='N' THEN 0
ELSE NULL END AS DEFNSV_DRVR_DISC_IND,
ANTI_THFT_DISC_IND,
'' as DAY_TM_RUN_LIGHTS,
'' as LMT_TORT,
CVG_TYP_IND,
SUM(EXPSR_MNTH_CNT) AS EXPSR_MNTH_CNT,
SUM(WRITTN_PREM_AMT) as WRITTN_PREM_AMT,
NULL as NJ_NO_LWST_LMT_IND,
NULL as NJ_NMD_DRVR_EXCL_IND,
COMP_DED,
COLL_DED,
PLCY_NUM,
NULL as UNIT_NUM,
EFF_DT,
NULL AS NUM_OF_CARS_IN_HH,
RTD_DRVR_DOB,
PLCY_TRM_STRT_DT,
SRC_SYS_CD,
NULL AS PNI_AGE,
MIS_LOB,
'' as PRINCIPAL_OPRT,
'TOGGLE AUTO' AS SOURCE_IND_DERIVED
FROM {SCHEMA_FPR}.{FPR_TA_DA_CVG_LVL_STAT_TBL}
WHERE
TRIM(UPPER(STATE_NM)) NOT IN ({GEO_ST_NM})
AND 
SUBSTR(REGSTR_CLS_MNTH,1,4)={RPT_YEAR}
GROUP BY
REGSTR_CLS_MNTH,
NAIC_CMPNY_CD,
TRIM(UPPER(STATE_NM)),
FIG_STATE,
ACCT_LOB,
CVG_CD,
CVG_AMT,
BI_LMT,
GA_UMBI_PD_ADDED_IND,
PLCY_REWRIT_IND,
UM_UIM_STACKING_IND,
GRGNG_ZIP_5,
MLT_CAR_IND,
RTD_DRVR_AGE,
RTD_DRVR_GNDR_CD,
RTD_DRVR_MRTL_STAT,
AUTO_USE_CD,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP_CD,
PHY_DMG_IND,
VEH_YR,
PASSV_RESTRA_DISC_IND,
DEFNSV_DRVR_DISC_IND,
ANTI_THFT_DISC_IND,
CVG_TYP_IND,
COMP_DED,
COLL_DED,
PLCY_NUM,
EFF_DT,
RTD_DRVR_DOB,
PLCY_TRM_STRT_DT,
SRC_SYS_CD,
MIS_LOB) WHERE WRITTN_PREM_AMT!=0
"""

try:
    logger.info("Reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from Snowflake via JDBC override query")
    df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from Snowflake: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXP_PASS_THROUGH: pure passthrough - explicit column projection
# ---------------------------------------------------------------------
try:
    logger.info("Projecting EXP_PASS_THROUGH (explicit passthrough columns)")
    exp_cols = [
        'REG_PER_YR','FISC_PER_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD',
        'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','PLCY_IND',
        'UM_UMI_STACKING','PIP_WVR_WL_IND','PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND',
        'PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND','NY_SSL_IND',
        'NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR',
        'MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
        'NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT','CVG_TYP_IND','CVG_EXPS_VAL',
        'WRITTN_PREM_AMT','NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','COMP_DED','COLL_DED','PLCY_CNTRCT_NUM',
        'UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','PNI_AGE',
        'MIS_LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED','NISS_CMPNY_CD','REG_PER_YR'
    ]
    # selectExpr requires names; ensure every name appears exactly as declared
    df_EXP_PASS_THROUGH_reverent_dirac = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace.selectExpr(*exp_cols)
except Exception as e:
    logger.error(f"Failed in EXP_PASS_THROUGH projection: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXPTRANS: compute GRGNG_ZIP_5 and IFARMERS_STATE_CD (via local var v_FARMERS_STATE_CD)
# ---------------------------------------------------------------------
try:
    logger.info("Running EXPTRANS calculations (GRGNG_ZIP_5, IFARMERS_STATE_CD)")
    base = df_EXP_PASS_THROUGH_reverent_dirac
    # project all declared input/output ports first (explicit list)
    project_cols = [
        'REG_PER_YR','FISC_PER_YR','NAIC_CMPNY_CD','ST_NM','ST_CD','ACCTNG_LOB','CVG_TYP_CD','CVG_AMT',
        'BI_LMT','GA_ADDED_AT_FAULT_IND','PLCY_IND','UM_UMI_STACKING','PIP_WVR_WL_IND','PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND',
        'NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','i_GRGNG_ZIP','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR',
        'MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
        'NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT','CVG_TYP_IND','CVG_EXPS_VAL',
        'WRITTN_PREM_AMT','NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','COMP_DED','COLL_DED','PLCY_CNTRCT_NUM',
        'UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','PNI_AGE',
        'MIS_LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED','NISS_CMPNY_CD'
    ]
    df_proj = base.selectExpr(*project_cols)

    # compute GRGNG_ZIP_5 per DECODE logic: if i_GRGNG_ZIP is null or blank or trimmed='0' -> '00000' else trim(i_GRGNG_ZIP)
    df_with_zip = df_proj.withColumn(
        'GRGNG_ZIP_5',
        when(col('i_GRGNG_ZIP').isNull(), lit('00000'))
        .when(trim(col('i_GRGNG_ZIP')) == '', lit('00000'))
        .when(trim(col('i_GRGNG_ZIP')) == '0', lit('00000'))
        .otherwise(trim(col('i_GRGNG_ZIP')))
    )

    # local variable v_FARMERS_STATE_CD and IFARMERS_STATE_CD
    df_with_v = df_with_zip.withColumn('v_FARMERS_STATE_CD', when(col('ST_CD') == '#', lit('00')).otherwise(col('ST_CD')))
    df_EXPTRANS_magical_noether = df_with_v.withColumn('IFARMERS_STATE_CD', col('v_FARMERS_STATE_CD').cast('int'))
    # v_FARMERS_STATE_CD is local to this transformation — keep it in case downstream expects it, otherwise it can be dropped later
except Exception as e:
    logger.error(f"Failed in EXPTRANS transformations: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# LKP_ff_REF_NISS_STATE_CD: flat-file lookup, cached, deduplicated, broadcast join
# ---------------------------------------------------------------------
try:
    logger.info("Loading flat-file lookup for ff_REF_NISS_STATE_CD from path placeholder and performing broadcast join")
    lookup_df_raw = spark.read.option("header", "true").csv(LOOKUP_FILE_FF_NISS_STATE)
    # ensure deterministic first-row policy by dropping duplicates on FARMERS_STATE_NAME keeping first encountered row
    lookup_df = lookup_df_raw.dropDuplicates(['FARMERS_STATE_NAME']).select(
        trim(col('FARMERS_STATE_NAME')).alias('FARMERS_STATE_NAME'),
        trim(col('NISS_STATE_CODE')).alias('NISS_STATE_CODE')
    )

    # join the incoming EXPTRANS frame to the lookup, producing a dataframe that includes the lookup outputs
    df_joined_lookup = (
        df_EXPTRANS_magical_noether.alias('d')
        .join(broadcast(lookup_df.alias('lk')), col('d.i_ST_NM') == col('lk.FARMERS_STATE_NAME'), how='left')
        .select(
            col('d.i_ST_NM').alias('i_ST_NM'),
            col('lk.FARMERS_STATE_NAME').alias('FARMERS_STATE_NAME'),
            col('lk.NISS_STATE_CODE').alias('i_NISS_STATE_CODE')
        )
    )
    df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz = df_joined_lookup
except Exception as e:
    logger.error(f"Failed loading or joining lookup ff_REF_NISS_STATE_CD: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXPTRANS1: apply small derivations and passthroughs, join to lookup outputs where needed
# ---------------------------------------------------------------------
try:
    logger.info("Running EXPTRANS1: passthroughs and computed outputs (EXPS_VAL_ROLLED, ST_ABBR, GA_ADDED_AT_FAULT_IND, NISS_STATE_CODE)")
    d = df_EXPTRANS_magical_noether.alias('d')
    l = df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz.alias('l')
    # left join to bring in i_NISS_STATE_CODE
    joined = d.join(l, on=col('d.i_ST_NM') == col('l.i_ST_NM'), how='left')

    # compute fields
    df_exp1 = joined.withColumn('EXPS_VAL_ROLLED', lit(0))
    df_exp1 = df_exp1.withColumn(
        'ST_ABBR',
        when(col('i_ST_ABBR').isNull() | (trim(col('i_ST_ABBR')) == ''), lit('?')).otherwise(trim(col('i_ST_ABBR')))
    )
    df_exp1 = df_exp1.withColumn('GA_ADDED_AT_FAULT_IND', when(col('i_GA_ADDED_AT_FAULT_IND') == '1', lit('Y')).otherwise(lit('N')))
    # normalize i_NISS_STATE_CODE -> NISS_STATE_CODE with '?' fallback
    df_exp1 = df_exp1.withColumn('NISS_STATE_CODE', when(col('i_NISS_STATE_CODE').isNull() | (trim(col('i_NISS_STATE_CODE')) == ''), lit('?')).otherwise(col('i_NISS_STATE_CODE')))

    # Project explicit outputs (many passthroughs plus new computed columns). Keep core passthrough column list and add computed ones.
    project_cols_exp1 = [
        'REG_PER_YR','FISC_PER_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','ST_ABBR',
        'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','PLCY_IND',
        'UM_UMI_STACKING','PIP_WVR_WL_IND','PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND',
        'PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND','NY_SSL_IND',
        'NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR',
        'MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
        'NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT','CVG_TYP_IND','CVG_EXPS_VAL',
        'WRITTN_PREM_AMT','NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','COMP_DED','COLL_DED','PLCY_CNTRCT_NUM',
        'UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','PNI_AGE',
        'MIS_LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED','EXPS_VAL_ROLLED','NISS_STATE_CODE'
    ]
    # Some columns may not exist in joined frame; use selectExpr and COALESCE where appropriate. For simplicity use select of column expressions using col(...) and fallback to lit(None) for missing names.
    # Build column expressions programmatically
    exprs = []
    for c_name in project_cols_exp1:
        try:
            exprs.append(col(c_name))
        except Exception:
            exprs.append(lit(None).alias(c_name))
    # assemble dataframe
    df_EXPTRANS1_tender_babbage = df_exp1.select(*exprs)
except Exception as e:
    logger.error(f"Failed in EXPTRANS1: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXPTRANS2: derive NISS_TERR_CD, o_ANNUAL_STMT_LOB, and emit PM variables as NULLs
# ---------------------------------------------------------------------
try:
    logger.info("Running EXPTRANS2: compute NISS_TERR_CD, o_ANNUAL_STMT_LOB, and produce PM audit outputs as NULL")
    base2 = df_EXPTRANS1_tender_babbage
    # project passthrough columns first explicitly
    passthrough_cols = [
        'UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','PNI_AGE','MIS_LOB',
        'PRINCIPAL_OPRT','SOURCE_IND_DERIVED','i_NISS_STATE_CODE','NISS_STATE_CODE','i_NISS_TERR_CD','NISS_CMPNY_CD',
        'REG_PER_YR','FISC_PER_YR','NAIC_CMPNY_CD','ST_NM','ST_CD','ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT',
        'i_GA_ADDED_AT_FAULT_IND','GA_ADDED_AT_FAULT_IND','PLCY_IND','UM_UMI_STACKING','PIP_WVR_WL_IND','PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND',
        'NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','i_GRGNG_ZIP','GRGNG_ZIP_5','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS',
        'AGE','GENDR','MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
        'EXPS_VAL_ROLLED','NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT','CVG_TYP_IND','CVG_EXPS_VAL','WRITTN_PREM_AMT'
    ]
    df_proj2 = base2.select(*[c for c in passthrough_cols if c in base2.columns])

    # compute NISS_TERR_CD per DECODE-style logic
    df_proj2 = df_proj2.withColumn('NISS_TERR_CD', when(col('i_NISS_TERR_CD').isNull() | (trim(col('i_NISS_TERR_CD')) == ''), lit('?')).otherwise(trim(col('i_NISS_TERR_CD'))))
    # compute o_ANNUAL_STMT_LOB as substr(trim(ACCTNG_LOB),1,3)
    df_proj2 = df_proj2.withColumn('o_ANNUAL_STMT_LOB', when(col('ACCTNG_LOB').isNull(), lit(None)).otherwise(trim(col('ACCTNG_LOB')).substr(1,3)))

    # Emit Informatica PM vars as NULLs (they have no Glue equivalent)
    df_proj2 = df_proj2.withColumn('MAPPING_NAME', lit(None)).withColumn('FOLDER_NAME', lit(None)).withColumn('WORKFLOW_NAME', lit(None))

    df_EXPTRANS2_clever_schrodinger = df_proj2
except Exception as e:
    logger.error(f"Failed in EXPTRANS2: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# FDR_LIB_mplt_ABC_MAPPING_AUDIT: mapping-audit mapplet stub -> emit audit ports as NULLs
# ---------------------------------------------------------------------
try:
    logger.info("Producing mapplet audit outputs as NULLs (mapplet omitted because it only provides Informatica audit metadata)")
    # intentionally produce the exported audit ports as NULLs for downstream schema compatibility
    df_FDR_LIB_mplt_ABC_MAPPING_AUDIT_hopeful_hilbert = (
        df_EXPTRANS2_clever_schrodinger.select(
            lit(None).cast('long').alias('CR_BY_MAPNG_ID'),
            lit(None).cast('timestamp').alias('DW_CR_TMSP'),
            lit(None).cast('long').alias('UPD_BY_MAPNG_ID'),
            lit(None).cast('timestamp').alias('DW_UPD_TMSP'),
            lit(None).cast('long').alias('WRK_FLOW_RUN_ID')
        )
    )
    # Add a comment so reviewers see the intentional omission
    # NOTE: The mapping-audit mapplet is intentionally omitted and its outputs are emitted as NULLs.
except Exception as e:
    logger.error(f"Failed producing mapplet stub outputs: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# Final Output: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND1 -> write as parquet to S3 (overwrite)
# - project all target columns explicitly, generate surrogate key NISS_APRM_LND_SK via row_number() afterwards
# ---------------------------------------------------------------------
try:
    logger.info("Preparing final WRK_BIRP_TA_NISS_NU0C_APRM_LND dataframe for write to S3 (parquet)")
    src = df_EXPTRANS2_clever_schrodinger

    # build explicit projection list for target columns. Use upstream column when present, otherwise NULL.
    target_cols = [
        'REG_PER_YR','FISC_PER_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','NISS_STATE_CODE','ST_ABBR',
        'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','PLCY_IND','UM_UMI_STACKING',
        'PIP_WVR_WL_IND','PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM',
        'NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND','NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5',
        'NISS_TERR_CD','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR','MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK',
        'GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND','NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS',
        'PASSV_RESTRA_DISC','SNR_DRVR_IND','DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT',
        'ANNL_STMNT_LOB_CD','CVG_TYP_IND','CVG_EXPS_VAL','WRITTN_PREM_AMT','CR_BY_MAPNG_ID','DW_CR_TMSP','UPD_BY_MAPNG_ID',
        'DW_UPD_TMSP','WRK_FLOW_RUN_ID','NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','EXPS_VAL_ROLLED','COMP_DED','COLL_DED',
        'PLCY_CNTRCT_NUM','UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','PNI_AGE',
        'MIS_LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED'
    ]

    exprs = []
    for c in target_cols:
        if c in src.columns:
            exprs.append(col(c).alias(c))
        else:
            # for audit/mapplet outputs that we created as a separate dataframe, prefer those if available
            if c in df_FDR_LIB_mplt_ABC_MAPPING_AUDIT_hopeful_hilbert.columns:
                exprs.append(col(c).alias(c))
            else:
                exprs.append(lit(None).alias(c))

    # project everything except surrogate key first
    df_pre_sk = src.select(*[e for e in exprs if e._jc is not None or True])
    # NOTE: above we used aliasing expressions; ensure we actually build DataFrame with those aliases
    # Because we created exprs as Column objects, we can apply them directly
    df_pre_sk = src.select(*exprs)

    # now attach surrogate key NISS_APRM_LND_SK via row_number() over a global ordering
    window = Window.orderBy(lit(1))
    df_WRK_BIRP_TA_NISS_NU0C_APRM_LND_stoic_faraday = df_pre_sk.withColumn('NISS_APRM_LND_SK', row_number().over(window))

    # write as parquet to S3 (overwrite)
    logger.info("Writing WRK_BIRP_TA_NISS_NU0C_APRM_LND to S3 as parquet (overwrite)")
    df_WRK_BIRP_TA_NISS_NU0C_APRM_LND_stoic_faraday.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_TA_NISS_NU0C_APRM_LND/")
except Exception as e:
    logger.error(f"Failed preparing or writing WRK_BIRP_TA_NISS_NU0C_APRM_LND: {e}", exc_info=True)
    raise



job.commit()
