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

from pyspark.sql.functions import col, when, trim, lit, broadcast, row_number
from pyspark.sql import Window

# Top-of-script placeholder constants for mapping parameters, buckets and lookup files
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
REPLACE_WITH_GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
LOOKUP_FILE_ff_NISS_STATE = "REPLACE_WITH_LOOKUPFILE_ff_NISS_STATE_PATH"
GEO_ST_NM = "REPLACE_WITH_GEO_ST_NM_VALUE"
RPT_YEAR = "REPLACE_WITH_RPT_YEAR_VALUE"
SCHEMA_FPR = "REPLACE_WITH_SCHEMA_FPR_VALUE"
FPR_TA_DA_CVG_LVL_STAT_TBL = "REPLACE_WITH_FPR_TA_DA_CVG_LVL_STAT_TBL_VALUE"

# -------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND (try S3 parquet first, fall back to Glue Catalog)
# -------------------------------------------------------------------
try:
    logger.info("Attempting to read WRK_BIRP_TA_NISS_NU0C_APRM_LND from S3 parquet first")
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_TA_NISS_NU0C_APRM_LND/"
    )
    logger.info("Read WRK_BIRP_TA_NISS_NU0C_APRM_LND from S3 parquet successfully")
except Exception as e_s3:
    logger.warning(
        "Could not read WRK_BIRP_TA_NISS_NU0C_APRM_LND from s3; falling back to Glue Catalog read: %s" % str(e_s3)
    )
    try:
        logger.info("Reading WRK_BIRP_TA_NISS_NU0C_APRM_LND from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=REPLACE_WITH_GLUE_DATABASE,
            table_name="WRK_BIRP_TA_NISS_NU0C_APRM_LND",
        )
        df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = dyf.toDF()
        logger.info("Read WRK_BIRP_TA_NISS_NU0C_APRM_LND from Glue Catalog successfully")
    except Exception as e_cat:
        logger.error(
            f"Failed reading WRK_BIRP_TA_NISS_NU0C_APRM_LND from both S3 and Glue Catalog: {e_cat}",
            exc_info=True,
        )
        raise

# -------------------------------------------------------------------
# Application Source Qualifier: SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND
# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND (bypassed — SQL Override reads it directly)
# -------------------------------------------------------------------
sql_query = f"""SELECT
    REGSTR_CLS_MNTH,
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
    CASE WHEN EXPSR_MNTH_CNT=0 THEN 12 ELSE EXPSR_MNTH_CNT END AS EXPSR_MNTH_CNT,
    WRITTN_PREM_AMT AS WRITTN_PREM_AMT,
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
FROM (
    SELECT
        TO_CHAR(REGSTR_CLS_MNTH) AS REGSTR_CLS_MNTH,
        '' AS FISC_PER_YR,
        NAIC_CMPNY_CD,
        CASE WHEN NAIC_CMPNY_CD='21687' THEN '176' WHEN NAIC_CMPNY_CD='44245' THEN '941' END AS NISS_CMPNY_CD,
        TRIM(UPPER(STATE_NM)) AS STATE_NM,
        FIG_STATE AS STATE_CD,
        ACCT_LOB,
        CVG_CD,
        CVG_AMT,
        BI_LMT,
        GA_UMBI_PD_ADDED_IND,
        '' AS FA2_PLCY_IND,
        UM_UIM_STACKING_IND,
        NULL AS PIP_WVR_WL_IND,
        NULL AS PIP_MED_SEC_IND,
        NULL AS PIP_LOSS_INCOME_IND,
        NULL AS MI_PPO_IND,
        '' AS PRD_GRP_CD,
        '' AS NJ_HLTH_INSR_PRIM,
        '' AS NJ_EXTR_PIP_PKG,
        NULL AS NJ_RESDNC_RLTNSHP_PIP_IND,
        NULL AS NY_SSL_IND,
        NULL AS NY_FULL_CVG_GLASS_COMP_IND,
        GRGNG_ZIP_5,
        '' AS RATNG_CMPY_CD,
        MLT_CAR_IND,
        '' AS RT_CLS,
        RTD_DRVR_AGE,
        RTD_DRVR_GNDR_CD,
        CASE WHEN RTD_DRVR_MRTL_STAT='single' THEN 'S' WHEN RTD_DRVR_MRTL_STAT='married' THEN 'M' ELSE UPPER(RTD_DRVR_MRTL_STAT) END AS RTD_DRVR_MRTL_STAT,
        AUTO_USE_CD,
        '' AS MILES_TO_WRK,
        GOOD_STDNT_IND,
        DRVR_TRNG_IND,
        SOI_TYP_CD,
        CASE WHEN PHY_DMG_IND='Y' THEN 1 WHEN PHY_DMG_IND='N' THEN 0 ELSE NULL END AS PHY_DMG_IND,
        '' AS NJ_RTD_PNTS,
        TRY_TO_NUMBER(VEH_YR) AS VEH_YR,
        '' AS NJ_EXCPTION_CD,
        '' AS NJ_FRGVN_PNTS,
        PASSV_RESTRA_DISC_IND,
        '' AS SNR_DRVR_IND,
        CASE WHEN DEFNSV_DRVR_DISC_IND='Y' THEN 1 WHEN DEFNSV_DRVR_DISC_IND='N' THEN 0 ELSE NULL END AS DEFNSV_DRVR_DISC_IND,
        ANTI_THFT_DISC_IND,
        '' AS DAY_TM_RUN_LIGHTS,
        '' AS LMT_TORT,
        CVG_TYP_IND,
        SUM(EXPSR_MNTH_CNT) AS EXPSR_MNTH_CNT,
        SUM(WRITTN_PREM_AMT) AS WRITTN_PREM_AMT,
        NULL AS NJ_NO_LWST_LMT_IND,
        NULL AS NJ_NMD_DRVR_EXCL_IND,
        COMP_DED,
        COLL_DED,
        PLCY_NUM,
        NULL AS UNIT_NUM,
        EFF_DT,
        NULL AS NUM_OF_CARS_IN_HH,
        RTD_DRVR_DOB,
        PLCY_TRM_STRT_DT,
        SRC_SYS_CD,
        NULL AS PNI_AGE,
        MIS_LOB,
        '' AS PRINCIPAL_OPRT,
        'TOGGLE AUTO' AS SOURCE_IND_DERIVED
    FROM
        {SCHEMA_FPR}.{FPR_TA_DA_CVG_LVL_STAT_TBL}
    WHERE
        TRIM(UPPER(STATE_NM)) NOT IN ({GEO_ST_NM})
        AND SUBSTR(REGSTR_CLS_MNTH,1,4)={RPT_YEAR}
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
        PLcy_REWRIT_IND,
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
        MIS_LOB
) WHERE WRITTN_PREM_AMT!=0
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

# -------------------------------------------------------------------
# EXP_PASS_THROUGH: pure passthrough projection from SQ
# -------------------------------------------------------------------
try:
    logger.info("Projecting EXP_PASS_THROUGH from SQ output (explicit column list)")
    df_EXP_PASS_THROUGH_reverent_dirac = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace.select(
        "PNI_AGE",
        "MIS_LOB",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED",
        "NISS_CMPNY_CD",
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
        "CVG_TYP_IND",
        "CVG_EXPS_VAL",
        "WRITTN_PREM_AMT",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "COMP_DED",
        "COLL_DED",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT",
        "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "REG_PER_YR",
        "FISC_PER_YR",
        "NAIC_CMPNY_CD",
        "ST_NM",
        "ST_CD",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "BI_LMT",
        "GA_ADDED_AT_FAULT_IND",
        "PLCY_IND",
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
        "GRGNG_ZIP",
        "RATNG_CMPY_CD",
        "MLT_CAR_IND",
        "RT_CLS",
        "AGE",
        "GENDR",
        "MRTL_STAT",
        "AUTO_USE_CD",
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
        "CVG_TYP_IND",
        "CVG_EXPS_VAL",
        "WRITTN_PREM_AMT",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "EXPS_VAL_ROLLED",
        "COMP_DED",
        "COLL_DED",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT",
        "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "PNI_AGE",
        "MIS_LOB",
        "PRINCIPAL_OPRT"
    )
except Exception as e:
    logger.error(f"Failed during EXP_PASS_THROUGH projection: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------
# EXPTRANS: compute intermediate v_FARMERS_STATE_CD and GRGNG_ZIP_5, then IFARMERS_STATE_CD
# -------------------------------------------------------------------
try:
    logger.info("Computing EXPTRANS intermediate columns v_FARMERS_STATE_CD and GRGNG_ZIP_5")
    df_exp_tmp = (
        df_EXP_PASS_THROUGH_reverent_dirac
        .withColumn("v_FARMERS_STATE_CD", when(col("ST_CD") == "#", lit("00")).otherwise(col("ST_CD")))
        .withColumn(
            "GRGNG_ZIP_5",
            when(
                col("i_GRGNG_ZIP").isNull() | (trim(col("i_GRGNG_ZIP")) == "") | (trim(col("i_GRGNG_ZIP")) == "0"),
                lit("00000"),
            ).otherwise(trim(col("i_GRGNG_ZIP")))
        )
    )
    logger.info("Attaching IFARMERS_STATE_CD by casting v_FARMERS_STATE_CD to integer")
    df_EXPTRANS_magical_noether = (
        df_exp_tmp
        .withColumn("IFARMERS_STATE_CD", col("v_FARMERS_STATE_CD").cast("int"))
        .select(
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "PNI_AGE",
            "MIS_LOB",
            "PRINCIPAL_OPRT",
            "SOURCE_IND_DERIVED",
            "NISS_CMPNY_CD",
            "REG_PER_YR",
            "FISC_PER_YR",
            "NAIC_CMPNY_CD",
            "ST_NM",
            "ST_CD",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "GA_ADDED_AT_FAULT_IND",
            "PLCY_IND",
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
            "i_GRGNG_ZIP",
            "GRGNG_ZIP_5",
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
            "CVG_TYP_IND",
            "CVG_EXPS_VAL",
            "WRITTN_PREM_AMT",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "EXPS_VAL_ROLLED",
            "COMP_DED",
            "COLL_DED",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM",
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "PNI_AGE",
            "MIS_LOB",
            "PRINCIPAL_OPRT",
            "IFARMERS_STATE_CD"
        )
    )
except Exception as e:
    logger.error(f"Failed during EXPTRANS computations: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------
# LKP_ff_REF_NISS_STATE_CD: read small flat-file lookup and broadcast-join
# -------------------------------------------------------------------
try:
    logger.info("Reading lookup file for ff_NISS_STATE into lookup_df")
    lookup_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(LOOKUP_FILE_ff_NISS_STATE)
    )
    logger.info("Lookup file ff_NISS_STATE read successfully")
except Exception as e:
    logger.error(f"Failed reading lookup file {LOOKUP_FILE_ff_NISS_STATE}: {e}", exc_info=True)
    raise

try:
    logger.info("Performing broadcast left join of main data with ff_NISS_STATE lookup on FARMERS_STATE_NAME = ST_NM")
    df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz = (
        df_EXPTRANS_magical_noether.alias("main")
        .join(
            broadcast(lookup_df).alias("lkup"),
            col("main.ST_NM") == col("lkup.FARMERS_STATE_NAME"),
            how="left",
        )
        # preserve all main columns plus the lookup outputs
        .select(
            *[col(f"main.{c}") for c in df_EXPTRANS_magical_noether.columns],
            col("lkup.FARMERS_STATE_NAME"),
            col("lkup.NISS_STATE_CODE"),
        )
    )
except Exception as e:
    logger.error(f"Failed during lookup join for ff_NISS_STATE: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------
# EXPTRANS1: left-join main with lookup (already done), derive NISS_STATE_CODE, GA_ADDED_AT_FAULT_IND, EXPS_VAL_ROLLED
# -------------------------------------------------------------------
try:
    logger.info("Computing EXPTRANS1 derived fields and projecting final column list")
    df_joined = df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz

    # create i_NISS_STATE_CODE from lookup NISS_STATE_CODE (may be null)
    df_with_i_niss = df_joined.withColumn("i_NISS_STATE_CODE", col("NISS_STATE_CODE"))

    df_EXPTRANS1_tender_babbage = (
        df_with_i_niss
        .withColumn(
            "NISS_STATE_CODE",
            when(col("i_NISS_STATE_CODE").isNull() | (trim(col("i_NISS_STATE_CODE")) == ""), lit("?")).otherwise(col("i_NISS_STATE_CODE")),
        )
        .withColumn(
            "GA_ADDED_AT_FAULT_IND",
            when(col("i_GA_ADDED_AT_FAULT_IND") == "1", lit("Y")).otherwise(lit("N")),
        )
        .withColumn("EXPS_VAL_ROLLED", lit(0))
        .select(
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "PNI_AGE",
            "MIS_LOB",
            "PRINCIPAL_OPRT",
            "SOURCE_IND_DERIVED",
            "i_NISS_STATE_CODE",
            "NISS_STATE_CODE",
            "NISS_CMPNY_CD",
            "REG_PER_YR",
            "FISC_PER_YR",
            "NAIC_CMPNY_CD",
            "ST_NM",
            "ST_CD",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "i_GA_ADDED_AT_FAULT_IND",
            "GA_ADDED_AT_FAULT_IND",
            "PLCY_IND",
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
            "i_GRGNG_ZIP",
            "GRGNG_ZIP_5",
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
            "EXPS_VAL_ROLLED",
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
            "CVG_TYP_IND",
            "COMP_DED",
            "COLL_DED",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM"
        )
    )
except Exception as e:
    logger.error(f"Failed during EXPTRANS1 processing: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------
# EXPTRANS2: final projections and derived fields including o_ANNUAL_STMT_LOB and NISS_TERR_CD; map PM vars to NULL
# -------------------------------------------------------------------
try:
    logger.info("Computing EXPTRANS2 derived fields and final projection")
    df_temp = df_EXPTRANS1_tender_babbage

    df_EXPTRANS2_clever_schrodinger = (
        df_temp
        .withColumn("o_ANNUAL_STMT_LOB", trim(col("ACCTNG_LOB")).substr(1, 3))
        .withColumn(
            "NISS_TERR_CD",
            when(col("i_NISS_TERR_CD").isNull() | (trim(col("i_NISS_TERR_CD")) == ""), lit("?")).otherwise(trim(col("i_NISS_TERR_CD"))),
        )
        .withColumn("MAPPING_NAME", lit(None))
        .withColumn("FOLDER_NAME", lit(None))
        .withColumn("WORKFLOW_NAME", lit(None))
        .select(
            "UNIT_NUM",
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "PNI_AGE",
            "MIS_LOB",
            "MAPPING_NAME",
            "FOLDER_NAME",
            "WORKFLOW_NAME",
            "PRINCIPAL_OPRT",
            "SOURCE_IND_DERIVED",
            "i_NISS_STATE_CODE",
            "NISS_STATE_CODE",
            "i_NISS_TERR_CD",
            "NISS_TERR_CD",
            "NISS_CMPNY_CD",
            "REG_PER_YR",
            "FISC_PER_YR",
            "NAIC_CMPNY_CD",
            "ST_NM",
            "ST_CD",
            "ACCTNG_LOB",
            "o_ANNUAL_STMT_LOB",
            "CVG_TYP_CD",
            "CVG_AMT",
            "BI_LMT",
            "i_GA_ADDED_AT_FAULT_IND",
            "GA_ADDED_AT_FAULT_IND",
            "PLCY_IND",
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
            "i_GRGNG_ZIP",
            "GRGNG_ZIP_5",
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
            "EXPS_VAL_ROLLED",
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
            "CVG_TYP_IND",
            "CVG_EXPS_VAL",
            "WRITTN_PREM_AMT",
            "CR_BY_MAPNG_ID",
            "DW_CR_TMSP",
            "UPD_BY_MAPNG_ID",
            "DW_UPD_TMSP",
            "WRK_FLOW_RUN_ID",
            "NJ_NO_LWST_LMT_IND",
            "NJ_NMD_DRVR_EXCL_IND",
            "EXPS_VAL_ROLLED",
            "COMP_DED",
            "COLL_DED",
            "PLCY_CNTRCT_NUM",
            "UNIT_NUM",
            "EFF_DT",
            "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH",
            "TERM_STRT_DT",
            "SRC_SYS_CD",
            "PNI_AGE",
            "MIS_LOB",
            "PRINCIPAL_OPRT"
        )
    )
except Exception as e:
    logger.error(f"Failed during EXPTRANS2 processing: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------
# Output: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND1 -> write as parquet to S3 (overwrite)
# Generate surrogate key NISS_APRM_LND_SK via row_number() (gap-free sequence)
# -------------------------------------------------------------------
try:
    logger.info("Preparing final WRK_BIRP_TA_NISS_NU0C_APRM_LND1 output and generating surrogate key NISS_APRM_LND_SK")
    # Project all required columns first
    final_cols = [
        "REG_PER_YR",
        "FISC_PER_YR",
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
        "PLCY_IND",
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
        "GRGNG_ZIP",
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
        "WRITTN_PREM_AMT",
        "CR_BY_MAPNG_ID",
        "DW_CR_TMSP",
        "UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP",
        "WRK_FLOW_RUN_ID",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "EXPS_VAL_ROLLED",
        "COMP_DED",
        "COLL_DED",
        "PLCY_CNTRCT_NUM",
        "UNIT_NUM",
        "EFF_DT",
        "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH",
        "TERM_STRT_DT",
        "SRC_SYS_CD",
        "PNI_AGE",
        "MIS_LOB",
        "PRINCIPAL_OPRT",
        "SOURCE_IND_DERIVED"
    ]

    # Ensure columns that may not exist are referenced defensively: use df.columns membership check
    available_cols = df_EXPTRANS2_clever_schrodinger.columns
    proj_cols = [c for c in final_cols if c in available_cols]

    df_projected = df_EXPTRANS2_clever_schrodinger.select(*proj_cols)

    # Add surrogate key using row_number() over a single ordering to emulate SEQTRANS NEXTVAL (gap-free)
    # NOTE: This forces a single-partition shuffle and may be expensive on very large inputs - review if needed.
    window_spec = Window.orderBy(lit(1))
    df_with_sk = df_projected.withColumn("NISS_APRM_LND_SK", row_number().over(window_spec))

    # Reorder so surrogate key is first if required by consumers; here we place it at the front
    final_column_order = ["NISS_APRM_LND_SK"] + [c for c in proj_cols]
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND1_stoic_faraday = df_with_sk.select(*final_column_order)

    logger.info("Writing WRK_BIRP_TA_NISS_NU0C_APRM_LND1 to S3 as parquet (overwrite)")
    df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND1_stoic_faraday.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_TA_NISS_NU0C_APRM_LND1/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_TA_NISS_NU0C_APRM_LND1 to S3: {e}", exc_info=True)
    raise


job.commit()
