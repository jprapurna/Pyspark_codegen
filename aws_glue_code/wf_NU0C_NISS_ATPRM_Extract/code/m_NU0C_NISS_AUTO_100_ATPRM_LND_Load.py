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


# placeholder constants for mapping parameters and environment values
RPT_YEAR = "REPLACE_WITH_RPT_YEAR_VALUE"
GEO_ST_NM = "REPLACE_WITH_GEO_ST_NM_VALUE"
RUN_TYPE = "REPLACE_WITH_RUN_TYPE_VALUE"
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
LOOKUP_FILE_FF_NISS_STATE_PATH = "REPLACE_WITH_LOOKUP_FILE_FF_NISS_STATE_PATH"

# additional pyspark utilities used below
from pyspark.sql.functions import col, when, trim, lit, broadcast, row_number, expr, ltrim, rtrim
from pyspark.sql.window import Window

# ---------------------------------------------------------------------
# Application Source Qualifier: run captured SQL Override against Snowflake via JDBC
# Source: FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL, FDR_LIB_DIM_AG_SOI, FDR_LIB_DIM_AG_PLCY_ENH,
# FDR_LIB_DIM_AG_PLCY, FDR_LIB_DIM_AG_CVG, FDR_LIB_DIM_AG_RATED_GEO, FDR_LIB_DIM_AG_CVG_ENH,
# FDR_LIB_DIM_AG_FARMR_GEO_ST, FDR_LIB_DIM_AG_MINI_PLCY, FDR_LIB_DIM_DT, FDR_LIB_DIM_AG_MINI_CVG,
# FDR_LIB_DIM_AG_SOI_ENH, FDR_LIB_DIM_AG_MINI_SOI (bypassed — SQL Override below reads them directly)
sql_query = f"""
SELECT * FROM (
SELECT
    REGPER.CLNDR_YR REG_PER_YR,
    FISCPER.CLNDR_YR FISC_PER_YR,
    EPLCY.NAIC_CMPY_CD,
    LTRIM(RTRIM(GEOS.ST_NM)) AS ST_NM,
    LTRIM(RTRIM(GEOS.ST_CD)) AS ST_CD,
    LTRIM(RTRIM(COALESCE(CVG.ACCTNG_LOB,''))) AS ACCTNG_LOB,
    CVG.CVG_TYP_CD,
    MCVG.CVG_AMT,
    MPLCY.BI_LMT,
    MPLCY.FA2_PLCY_IND,
    ECVG.NJ_RTD_PNTS,
    ECVG.NJ_FRGVN_PNTS,
    ECVG.CVG_TYP_IND,
    SUM(COALESCE(FACT.TTL_WRITTN_PREM_AMT,0)) TTL_WRITTN_PREM_AMT,
    SUM(COALESCE(FACT.CVG_EXPS_VAL,0)) CVG_EXPS_VAL,
    HH.NUM_OF_CARS_IN_HH,
    ESOI.RDRVR_DT_OF_BRTH,
    PLCY.TERM_STRT_DT,
    LTRIM(RTRIM(PLCY.PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
    PLCY.SRC_SYS_CD,
    PLCY.PNI_AGE,
    FACT.UNIT_NUM,
    PLUG.PRIOR_COLS_PLACEHOLDER
FROM AGDM.FACT_AG_WRITTN_PREM_CVG_LVL FACT
JOIN AGDM.DIM_AG_PLCY PLCY
  ON FACT.PLCY_SK=PLCY.PLCY_SK
JOIN AGDM.DIM_AG_MINI_PLCY MPLCY
  ON FACT.MINI_PLCY_SK = MPLCY.MINI_PLCY_SK
JOIN AGDM.DIM_AG_PLCY_ENH EPLCY
  ON FACT.PLCY_ENH_SK = EPLCY.PLCY_ENH_SK
JOIN AGDM.DIM_AG_SOI SOI
  ON FACT.SOI_SK = SOI.SOI_SK
JOIN AGDM.DIM_AG_MINI_SOI MSOI
  ON FACT.MINI_SOI_SK = MSOI.MINI_SOI_SK
JOIN AGDM.DIM_AG_SOI_ENH ESOI
  ON FACT.SOI_ENH_SK = ESOI.SOI_ENH_SK
JOIN AGDM.DIM_AG_CVG CVG
  ON FACT.CVG_SK = CVG.CVG_SK
JOIN AGDM.DIM_AG_MINI_CVG MCVG
  ON FACT.MINI_CVG_SK = MCVG.MINI_CVG_SK
JOIN AGDM.DIM_AG_CVG_ENH ECVG
  ON FACT.ON_OFF_PREM_SK = ECVG.ON_OFF_PREM_SK
JOIN AGDM.DIM_AG_FARMR_GEO_DISTR GEOD
  ON FACT.FARMR_GEO_DISTR_SK = GEOD.FARMR_GEO_DISTR_SK
JOIN AGDM.DIM_AG_FARMR_GEO_ST GEOS
  ON GEOD.FARMR_GEO_ST_SK = GEOS.FARMR_GEO_ST_SK
JOIN AGDM.DIM_AG_RATED_GEO RGEO
  ON FACT.RATED_GEO_SK = RGEO.RATED_GEO_SK
JOIN AGDM.DIM_DT REGPER
  ON FACT.REGSTR_PER_SK=REGPER.DT_SK
JOIN AGDM.DIM_DT FISCPER
  ON FACT.FISC_PER_SK =FISCPER.DT_SK
JOIN AGDM.DIM_AG_HH HH
  ON HH.HH_SK=FACT.HH_SK
WHERE 1 = 1
AND TRIM(GEOS.ST_NM) NOT IN ({GEO_ST_NM})
AND (case when {RUN_TYPE} ='Register' then REGPER.CLNDR_YR else fiscper.clndr_yr end ) = {RPT_YEAR}
AND AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_CD NOT IN ('WO')
AND (TTL_WRITTN_PREM_AMT !=0 AND CVG_EXPS_VAL!=0)
GROUP BY REGPER.CLNDR_YR, FISCPER.CLNDR_YR, PLCY.PLCY_CNTRCT_NUM, MPLCY.BI_LMT, MPLCY.FA2_PLCY_IND, MPLCY.RATNG_CMPY_CD, EPLCY.NAIC_CMPY_CD, EPLCY.PHY_DMG_IND, EPLCY.NY_SSL_IND, EPLCY.GA_UMBI_PD_ADDED_IND,
SOI.DRVR_TRNG_IND, SOI.SOI_TYP, SOI.VEH_MDL_YR, SOI.SNR_DRVR_IND, SOI.UNIT_NUM, ESOI.UM_UIM_STACKING, ESOI.PIP_WVR_WL_IND, ESOI.PIP_MED_SEC_IND, ESOI.PIP_LOSS_INCOME_IND, ESOI.MI_PPO_IND, ESOI.PRD_GRP_CD, ESOI.NJ_HLTH_INSR_PRIM, ESOI.NJ_EXTR_PIP_PKG, ESOI.NJ_RESDNC_RLTNSHP_PIP_IND, ESOI.NY_FULL_CVG_GLASS_COMP_IND, ESOI.NJ_NO_LWST_LMT_IND, ESOI.NJ_NMD_DRVR_EXCL_IND, ESOI.RT_CLS, ESOI.AGE, ESOI.GENDR, ESOI.MRTL_STAT, ESOI.AUTO_USE_CD, ESOI.MILES_TO_WRK, ESOI.NJ_EXCPTION_CD, ESOI.PASSV_RESTRA_DISC, ESOI.DEFNS_DRVR_DISC_IND, ESOI.ANTI_THFT_DISC, ESOI.DAY_TM_RUN_LIGHTS, ESOI.LMT_TORT, ESOI.COMP_DED, MSOI.MLT_CAR_IND, MSOI.GOOD_STDNT_IND, MSOI.COLL_DED,
CVG.ACCTNG_LOB, CVG.CVG_TYP_CD, ECVG.EFF_DT, ECVG.NJ_RTD_PNTS, ECVG.NJ_FRGVN_PNTS, ECVG.CVG_TYP_IND, MCVG.CVG_AMT,
GEOS.ST_NM, GEOS.ST_CD, RGEO.GRGNG_ZIP_5, HH.NUM_OF_CARS_IN_HH, ESOI.RDRVR_DT_OF_BRTH, PLCY.TERM_STRT_DT, PLCY.SRC_SYS_CD, PLCY.PNI_AGE
) ORDER BY ST_CD, ST_NM
"""

try:
    logger.info("Reading SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Snowflake via JDBC override query")
    df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Snowflake: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# Materialize Source node df_names so every expected df variable exists in the script.
# In the DEFAULT (non-staged) SQL-override case these Source nodes are logically bypassed
# (the SQ's JDBC override reads the real warehouse tables). We still create simple
# aliases to the SQ result so downstream nodes referencing these df_names resolve.
try:
    logger.info("Assigning Source node df_name aliases to the SQ dataframe for lineage compatibility")
    df_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_friendly_newton = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_DIM_AG_SOI_loving_pascal = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_PLCY_keen_leibniz = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_CVG_blissful_darwin = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_CVG_ENH_modest_bohr = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_DT_adoring_feynman = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
    df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
except Exception as e:
    logger.error(f"Failed creating source df aliases: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXP_Defaults: pure passthrough - reuse SQ result directly
try:
    logger.info("EXP_Defaults: passthrough assignment from SQ result")
    df_EXP_Defaults_jovial_feynman = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan
except Exception as e:
    logger.error(f"EXP_Defaults failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXP_Passthru: compute local v_FARMERS_STATE_CD and normalize GRGNG_ZIP_5; then derive IFARMERS_STATE_CD
try:
    logger.info("EXP_Passthru: computing v_FARMERS_STATE_CD and normalizing GRGNG_ZIP_5")
    df_tmp = (
        df_EXP_Defaults_jovial_feynman
        .withColumn(
            "v_FARMERS_STATE_CD",
            when(col("ST_CD") == "#", lit("00")).otherwise(col("ST_CD"))
        )
        .withColumn(
            "GRGNG_ZIP_5",
            when(col("GRGNG_ZIP_5").isNull() | (trim(col("GRGNG_ZIP_5")) == "") | (trim(col("GRGNG_ZIP_5")) == "0"), lit("00000")).otherwise(trim(col("GRGNG_ZIP_5")))
        )
    )
    df_EXP_Passthru_nifty_schrodinger = df_tmp.withColumn("IFARMERS_STATE_CD", col("v_FARMERS_STATE_CD").cast("int"))
except Exception as e:
    logger.error(f"EXP_Passthru failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# LKP_ff_REF_NISS_STATE_CD: read small flat-file lookup and expose it as a dataframe
try:
    logger.info("Reading LKP_ff_REF_NISS_STATE_CD flat file lookup from provided path")
    df_LKP_ff_REF_NISS_STATE_CD_elated_turing = (
        spark.read.option("header", "true").csv(LOOKUP_FILE_FF_NISS_STATE_PATH)
    )
except Exception as e:
    logger.error(f"Failed reading lookup file for LKP_ff_REF_NISS_STATE_CD: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXP_To_Derive_Vals: join passthru df with lookup and derive trimmed/mapped company code, NISS state code, ANNL_STMNT_LOB_CD, EXPS_VAL_ROLLED, and null mapping/system vars
try:
    logger.info("EXP_To_Derive_Vals: joining with lookup and deriving values")
    df_joined = df_EXP_Passthru_nifty_schrodinger.join(
        broadcast(df_LKP_ff_REF_NISS_STATE_CD_elated_turing),
        df_EXP_Passthru_nifty_schrodinger.ST_NM == df_LKP_ff_REF_NISS_STATE_CD_elated_turing.FARMERS_STATE_NAME,
        how="left"
    )

    case_expr = (
        "CASE "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '10315' THEN '172' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '10317' THEN '174' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '10318' THEN '173' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '10806' THEN '171' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '10873' THEN '170' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21598' THEN '177' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21601' THEN '079' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21628' THEN '073' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21636' THEN '178' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21644' THEN '077' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21652' THEN '070' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21660' THEN '175' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21679' THEN '072' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21687' THEN '176' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21695' THEN '179' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '21709' THEN '071' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '24392' THEN '169' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '28673' THEN '180' "
        "WHEN LTRIM(RTRIM(NAIC_CMPY_CD)) = '36889' THEN '074' "
        "ELSE '' END"
    )

    df_EXP_To_Derive_Vals_kind_noether = (
        df_joined
        .withColumn("v_NAIC_CMPY_CD", trim(col("NAIC_CMPY_CD")))
        .withColumn("o_NAIC_CMPY_CD", trim(col("NAIC_CMPY_CD")))
        .withColumn("o_NISS_CMPNY_CD", expr(case_expr))
        .withColumn("NISS_STATE_CODE", when(col("NISS_STATE_CODE").isNull() | (trim(col("NISS_STATE_CODE")) == ""), lit("?")).otherwise(trim(col("NISS_STATE_CODE"))))
        .withColumn("o_ANNL_STMNT_LOB_CD", expr("SUBSTR(ltrim(rtrim(ACCTNG_LOB)),1,3)"))
        .withColumn("EXPS_VAL_ROLLED", lit(0))
        # Informatica PM* system variables -> no equivalent in Glue; produce explicit NULLs per rule
        .withColumn("MAPPING_NAME", lit(None).cast("string"))
        .withColumn("FOLDER_NAME", lit(None).cast("string"))
        .withColumn("WORKFLOW_NAME", lit(None).cast("string"))
    )
except Exception as e:
    logger.error(f"EXP_To_Derive_Vals failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# FDR_LIB_mplt_ABC_MAPPING_AUDIT: mapping-audit mapplet placeholder (suppressed)
# Per project policy mapplet audit outputs are non-business housekeeping and are suppressed.
try:
    logger.info("Instantiating mapping-audit placeholder dataframe (suppressed housekeeping columns)")
    df_mplt_ABC_MAPPING_AUDIT_stoic_babbage = (
        df_EXP_To_Derive_Vals_kind_noether.select(
            lit(None).cast("integer").alias("CR_BY_MAPNG_ID"),
            lit(None).cast("timestamp").alias("DW_CR_TMSP"),
            lit(None).cast("integer").alias("UPD_BY_MAPNG_ID"),
            lit(None).cast("timestamp").alias("DW_UPD_TMSP"),
            lit(None).cast("integer").alias("WRK_FLOW_RUN_ID")
        )
    )
except Exception as e:
    logger.error(f"Mapplet placeholder creation failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# EXP_Passtotgt: project required columns, derive/normalize NISS_TERR_CD, then attach surrogate key NISS_ATPRM_LND_SK
try:
    logger.info("EXP_Passtotgt: projecting to target columns and generating surrogate key NISS_ATPRM_LND_SK")
    projected_cols = [
        "REG_PER_YR", "FISC_PER_YR", "NAIC_CMPY_CD", "o_NISS_CMPNY_CD as NISS_CMPNY_CD",
        "ST_NM", "ST_CD", "NISS_STATE_CODE as NISS_ST_CD", "STATE_ABBR as ST_ABBR",
        "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT", "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND",
        "UM_UIM_STACKING", "PIP_WVR_WL_IND", "PIP_MED_SEC_IND", "PIP_LOSS_INCOME_IND", "MI_PPO_IND",
        "PRD_GRP_CD", "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG", "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND",
        "NY_FULL_CVG_GLASS_COMP_IND", "GRGNG_ZIP_5", "NISS_TERR_CD", "RATNG_CMPY_CD", "MLT_CAR_IND",
        "RT_CLS", "AGE", "GENDR", "MRTL_STAT", "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND",
        "DRVR_TRNG_IND", "SOI_TYP", "PHY_DMG_IND", "NJ_RATD_PNTS as NJ_RATD_PNTS", "VEH_MDL_YR",
        "NJ_EXCPTION_CD as NJ_EXCPTION_CD", "NJ_FGVN_PNTS as NJ_FGVN_PNTS", "PASSV_RESTRA_DISC",
        "SNR_DRVR_IND", "DEFNS_DRVR_DISC_IND", "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT",
        "o_ANNL_STMNT_LOB_CD as ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL", "TTL_WRITTN_PREM_AMT",
        # omit CR_BY_MAPNG_ID/DW_CR_TMSP/UPD_BY_MAPNG_ID/DW_UPD_TMSP/WRK_FLOW_RUN_ID here per audit suppression policy
        "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "COMP_DED", "COLL_DED",
        "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH", "RDRVR_DT_OF_BRTH", "TERM_STRT_DT",
        "SRC_SYS_CD", "PNI_AGE", "ANNL_STMNT_LOB_CD as LOB"
    ]

    df_pre_sk = df_EXP_To_Derive_Vals_kind_noether.withColumn(
        "NISS_TERR_CD",
        when(col("i_NISS_TERR_CD").isNull() | (trim(col("i_NISS_TERR_CD")) == ""), lit("?")).otherwise(trim(col("i_NISS_TERR_CD")))
    )

    df_projected = df_pre_sk.selectExpr(*projected_cols)

    window_spec = Window.orderBy(lit(1))
    df_EXP_Passtotgt_zen_fermat = df_projected.withColumn("NISS_ATPRM_LND_SK", row_number().over(window_spec))
except Exception as e:
    logger.error(f"EXP_Passtotgt failed: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------
# Final Output: write target as parquet to S3 (overwrite)
try:
    logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_LND_laughing_faraday = df_EXP_Passtotgt_zen_fermat
    df_FDR_LIB_WRK_BIRP_NISS_APRM_LND_laughing_faraday.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_LND/"
    )
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3: {e}", exc_info=True)
    raise



job.commit()
