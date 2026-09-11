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


# placeholder constants for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
LOOKUP_FILE_FF_NISS_STATE = "REPLACE_WITH_LOOKUP_FILE_FF_NISS_STATE_PATH"
GEO_ST_NM = "REPLACE_WITH_GEO_ST_NM_VALUE"
RUN_TYPE = "REPLACE_WITH_RUN_TYPE_VALUE"
RPT_YEAR = "REPLACE_WITH_RPT_YEAR_VALUE"

# Additional imports used by transformations below
from pyspark.sql.functions import col, when, trim, ltrim, rtrim, lit, row_number, substring
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

# -------------------------------------------------------------------------
# Read source tables from Glue Catalog (project only the ports listed in
# each node's plan). Each read is wrapped in try/except and will re-raise on
# failure so the job fails visibly if a required source is missing.
# -------------------------------------------------------------------------
try:
    logger.info("Reading FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL (FACT_AG_WRITTN_PREM_CVG_LVL) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="FACT_AG_WRITTN_PREM_CVG_LVL")
    df_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_friendly_newton = dyf.toDF().select(
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT"
    )
except Exception as e:
    logger.error(f"Failed reading FACT_AG_WRITTN_PREM_CVG_LVL from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_SOI (DIM_AG_SOI) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_SOI")
    df_FDR_LIB_DIM_AG_SOI_loving_pascal = dyf.toDF().select(
        "DRVR_TRNG_IND",
        "SOI_TYP",
        "VEH_MDL_YR",
        "SNR_DRVR_IND",
        "UNIT_NUM"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_SOI from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_PLCY_ENH (DIM_AG_PLCY_ENH) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_PLCY_ENH")
    df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether = dyf.toDF().select(
        "NAIC_CMPY_CD",
        "PHY_DMG_IND",
        "NY_SSL_IND",
        "GA_UMBI_PD_ADDED_IND"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_PLCY_ENH from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_PLCY (DIM_AG_PLCY) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_PLCY")
    df_FDR_LIB_DIM_AG_PLCY_keen_leibniz = dyf.toDF().select(
        "PNI_AGE",
        "TERM_STRT_DT",
        "PLCY_CNTRCT_NUM"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_PLCY from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_CVG (DIM_AG_CVG) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_CVG")
    df_FDR_LIB_DIM_AG_CVG_blissful_darwin = dyf.toDF().select(
        "ACCTNG_LOB",
        "CVG_TYP_CD"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_CVG from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_RATED_GEO (DIM_AG_RATED_GEO) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_RATED_GEO")
    df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford = dyf.toDF().select(
        "GRGNG_ZIP_5"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_RATED_GEO from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_CVG_ENH (DIM_AG_CVG_ENH) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_CVG_ENH")
    df_FDR_LIB_DIM_AG_CVG_ENH_modest_bohr = dyf.toDF().select(
        "NJ_RTD_PNTS",
        "CVG_TYP_IND",
        "NJ_FRGVN_PNTS",
        "EFF_DT"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_CVG_ENH from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_FARMR_GEO_ST (DIM_AG_FARMR_GEO_ST) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_FARMR_GEO_ST")
    df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume = dyf.toDF().select(
        "ST_CD",
        "ST_NM"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_FARMR_GEO_ST from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_PLCY (DIM_AG_MINI_PLCY) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_PLCY")
    df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck = dyf.toDF().select(
        "FA2_PLCY_IND",
        "BI_LMT",
        "RATNG_CMPY_CD"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_MINI_PLCY from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_DT (DIM_DT) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_DT")
    df_FDR_LIB_DIM_DT_adoring_feynman = dyf.toDF().select(
        "CLNDR_YR"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_DT from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_CVG (DIM_AG_MINI_CVG) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_CVG")
    df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr = dyf.toDF().select(
        "CVG_AMT"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_MINI_CVG from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_SOI_ENH (DIM_AG_SOI_ENH) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_SOI_ENH")
    df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking = dyf.toDF().select(
        "RDRVR_DT_OF_BRTH",
        "PIP_MED_SEC_IND",
        "PIP_LOSS_INCOME_IND",
        "MI_PPO_IND",
        "UM_UIM_STACKING",
        "PIP_WVR_WL_IND",
        "COMP_DED",
        "PASSV_RESTRA_DISC",
        "RT_CLS",
        "AGE",
        "GENDR",
        "MRTL_STAT",
        "NJ_EXCPTION_CD",
        "DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC",
        "NJ_RESDNC_RLTNSHP_PIP_IND",
        "AUTO_USE_CD",
        "PRD_GRP_CD",
        "NJ_HLTH_INSR_PRIM",
        "NJ_EXTR_PIP_PKG",
        "MILES_TO_WRK",
        "NY_FULL_CVG_GLASS_COMP_IND",
        "LMT_TORT",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "DAY_TM_RUN_LIGHTS",
        "NJ_FRGVN_PNTS"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_SOI_ENH from Glue Data Catalog: {e}", exc_info=True)
    raise

try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_SOI (DIM_AG_MINI_SOI) from Glue Data Catalog")
    dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_SOI")
    df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume = dyf.toDF().select(
        "MINI_SOI_SK",
        "COLL_DED",
        "GOOD_STDNT_IND",
        "MLT_CAR_IND"
    )
except Exception as e:
    logger.error(f"Failed reading DIM_AG_MINI_SOI from Glue Data Catalog: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Application Source Qualifier: SQL override against Snowflake (default case)
# Bypasses the upstream Source nodes (lineage comment added for traceability)
# -------------------------------------------------------------------------
# Source: (bypassed) FACT_AG_WRITTN_PREM_CVG_LVL, DIM_AG_PLCY, DIM_AG_MINI_PLCY, DIM_AG_PLCY_ENH, DIM_AG_SOI,
# DIM_AG_MINI_SOI, DIM_AG_SOI_ENH, DIM_AG_CVG, DIM_AG_MINI_CVG, DIM_AG_CVG_ENH, DIM_AG_FARMR_GEO_DISTR,
# DIM_AG_FARMR_GEO_ST, DIM_AG_RATED_GEO, DIM_DT, DIM_AG_HH
sql_query = f"""SELECT * FROM (SELECT
    REG_PER_YR,
    FISC_PER_YR,
    NAIC_CMPY_CD,
    LTRIM(RTRIM(ST_NM)) AS ST_NM,
    LTRIM(RTRIM(ST_CD)) AS ST_CD,
    LTRIM(RTRIM(ACCTNG_LOB)) AS ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    BI_LMT,
    GA_UMBI_PD_ADDED_IND,
    FA2_PLCY_IND,
    UM_UIM_STACKING,
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
    NJ_NO_LWST_LMT_IND,
    NJ_NMD_DRVR_EXCL_IND,
    GRGNG_ZIP_5,
    RATNG_CMPY_CD,
    MLT_CAR_IND,
    RT_CLS,
    AGE,
    GENDR,
    MRTL_STAT,
    AUTO_USE_CD,
    MILES_TO_WRK,
    GOOD_STDNT_IND,
    DRVR_TRNG_IND,
    SOI_TYP,
    PHY_DMG_IND,
    NJ_RTD_PNTS,
    VEH_MDL_YR,
    NJ_EXCPTION_CD,
    NJ_FRGVN_PNTS,
    PASSV_RESTRA_DISC,
    SNR_DRVR_IND,
    DEFNS_DRVR_DISC_IND,
    ANTI_THFT_DISC,
    DAY_TM_RUN_LIGHTS,
    LMT_TORT,
    CVG_TYP_IND,
    CVG_EXPS_VAL,
    TTL_WRITTN_PREM_AMT,
    COMP_DED,
    COLL_DED,
    LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
    UNIT_NUM,
    EFF_DT,
    NUM_OF_CARS_IN_HH,
    RDRVR_DT_OF_BRTH,
    TERM_STRT_DT,
    COALESCE(SRC_SYS_CD,'') as SRC_SYS_CD,
    PNI_AGE
FROM (

SELECT
REGPER.CLNDR_YR REG_PER_YR,
FISCPER.CLNDR_YR FISC_PER_YR,
--Policy attributes
PLCY.PLCY_CNTRCT_NUM,
MPLCY.BI_LMT,
MPLCY.FA2_PLCY_IND,
MPLCY.RATNG_CMPY_CD,
EPLCY.NAIC_CMPY_CD,
EPLCY.PHY_DMG_IND,
EPLCY.NY_SSL_IND,
EPLCY.GA_UMBI_PD_ADDED_IND,
--Soi attributes
SOI.DRVR_TRNG_IND,
SOI.SOI_TYP,
SOI.VEH_MDL_YR,
SOI.SNR_DRVR_IND,
SOI.UNIT_NUM,
ESOI.UM_UIM_STACKING,
ESOI.PIP_WVR_WL_IND,
ESOI.PIP_MED_SEC_IND,
ESOI.PIP_LOSS_INCOME_IND,
ESOI.MI_PPO_IND,
ESOI.PRD_GRP_CD,
ESOI.NJ_HLTH_INSR_PRIM,
ESOI.NJ_EXTR_PIP_PKG,
ESOI.NJ_RESDNC_RLTNSHP_PIP_IND,
ESOI.NY_FULL_CVG_GLASS_COMP_IND,
ESOI.NJ_NO_LWST_LMT_IND,
ESOI.NJ_NMD_DRVR_EXCL_IND,
ESOI.RT_CLS,
ESOI.AGE,
ESOI.GENDR,
ESOI.MRTL_STAT,
ESOI.AUTO_USE_CD,
ESOI.MILES_TO_WRK,
ESOI.NJ_EXCPTION_CD,
ESOI.PASSV_RESTRA_DISC,
ESOI.DEFNS_DRVR_DISC_IND,
ESOI.ANTI_THFT_DISC,
ESOI.DAY_TM_RUN_LIGHTS,
ESOI.LMT_TORT,
ESOI.COMP_DED,
MSOI.MLT_CAR_IND,
MSOI.GOOD_STDNT_IND,
MSOI.COLL_DED,
--Coverage attributes
CVG.ACCTNG_LOB,
CVG.CVG_TYP_CD,
ECVG.EFF_DT,
ECVG.NJ_RTD_PNTS,
ECVG.NJ_FRGVN_PNTS,
ECVG.CVG_TYP_IND,
MCVG.CVG_AMT,
--Geo attributes
GEOS.ST_NM,
GEOS.ST_CD,
RGEO.GRGNG_ZIP_5,
--Fact attributes
SUM(COALESCE(FACT.TTL_WRITTN_PREM_AMT,0)) TTL_WRITTN_PREM_AMT,
SUM(COALESCE(FACT.CVG_EXPS_VAL,0)) CVG_EXPS_VAL,
HH.NUM_OF_CARS_IN_HH,
ESOI.RDRVR_DT_OF_BRTH,
PLCY.TERM_STRT_DT,
PLCY.SRC_SYS_CD,
PLCY.PNI_AGE

FROM AGDM.FACT_AG_WRITTN_PREM_CVG_LVL FACT

--Policy tables
JOIN AGDM.DIM_AG_PLCY PLCY
ON FACT.PLCY_SK=PLCY.PLCY_SK

JOIN AGDM.DIM_AG_MINI_PLCY MPLCY
ON FACT.MINI_PLCY_SK = MPLCY.MINI_PLCY_SK

JOIN AGDM.DIM_AG_PLCY_ENH EPLCY
ON FACT.PLCY_ENH_SK = EPLCY.PLCY_ENH_SK

JOIN AGDM.DIM_AG_TRANS_TYP_PLCY 
ON ( FACT.TRANS_TYP_PLCY_SK=AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_SK )

--SOI tables
JOIN AGDM.DIM_AG_SOI SOI
ON FACT.SOI_SK = SOI.SOI_SK

JOIN AGDM.DIM_AG_MINI_SOI MSOI
ON FACT.MINI_SOI_SK = MSOI.MINI_SOI_SK

JOIN AGDM.DIM_AG_SOI_ENH ESOI
ON FACT.SOI_ENH_SK = ESOI.SOI_ENH_SK

--CVG tables
JOIN AGDM.DIM_AG_CVG CVG
ON FACT.CVG_SK = CVG.CVG_SK

JOIN AGDM.DIM_AG_MINI_CVG MCVG
ON FACT.MINI_CVG_SK = MCVG.MINI_CVG_SK

JOIN AGDM.DIM_AG_CVG_ENH ECVG
ON FACT.ON_OFF_PREM_SK = ECVG.ON_OFF_PREM_SK

--Others
JOIN AGDM.DIM_AG_FARMR_GEO_DISTR GEOD
ON FACT.FARMR_GEO_DISTR_SK = GEOD.FARMR_GEO_DISTR_SK

JOIN AGDM.DIM_AG_FARMR_GEO_ST GEOS
ON GEOD.FARMR_GEO_ST_SK = GEOS.FARMR_GEO_ST_SK

JOIN AGDM.DIM_AG_RATED_GEO RGEO
ON FACT.RATED_GEO_SK = RGEO.RATED_GEO_SK

JOIN AGDM.DIM_DT  REGPER
ON FACT.REGSTR_PER_SK=REGPER.DT_SK

JOIN AGDM.DIM_DT  FISCPER
ON FACT.FISC_PER_SK =FISCPER.DT_SK

JOIN AGDM.DIM_AG_HH HH
ON HH.HH_SK=FACT.HH_SK

WHERE 1 = 1
AND TRIM(GEOS.ST_NM) NOT IN ({GEO_ST_NM})
AND (case when {RUN_TYPE} ='Register' then REGPER.CLNDR_YR else fiscper.clndr_yr end ) = {RPT_YEAR}
AND AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_CD NOT IN ('WO') and (TTL_WRITTN_PREM_AMT !=0 AND CVG_EXPS_VAL!=0)

GROUP BY 
REGPER.CLNDR_YR,
FISCPER.CLNDR_YR ,
--Policy attributes
PLCY.PLCY_CNTRCT_NUM,
MPLCY.BI_LMT,
MPLCY.FA2_PLCY_IND,
MPLCY.RATNG_CMPY_CD,
EPLCY.NAIC_CMPY_CD,
EPLCY.PHY_DMG_IND,
EPLCY.NY_SSL_IND,
EPLCY.GA_UMBI_PD_ADDED_IND,
--Soi attributes
SOI.DRVR_TRNG_IND,
SOI.SOI_TYP,
SOI.VEH_MDL_YR,
SOI.SNR_DRVR_IND,
SOI.UNIT_NUM,
ESOI.UM_UIM_STACKING,
ESOI.PIP_WVR_WL_IND,
ESOI.PIP_MED_SEC_IND,
ESOI.PIP_LOSS_INCOME_IND,
ESOI.MI_PPO_IND,
ESOI.PRD_GRP_CD,
ESOI.NJ_HLTH_INSR_PRIM,
ESOI.NJ_EXTR_PIP_PKG,
ESOI.NJ_RESDNC_RLTNSHP_PIP_IND,
ESOI.NY_FULL_CVG_GLASS_COMP_IND,
ESOI.NJ_NO_LWST_LMT_IND,
ESOI.NJ_NMD_DRVR_EXCL_IND,
ESOI.RT_CLS,
ESOI.AGE,
ESOI.GENDR,
ESOI.MRTL_STAT,
ESOI.AUTO_USE_CD,
ESOI.MILES_TO_WRK,
ESOI.NJ_EXCPTION_CD,
ESOI.PASSV_RESTRA_DISC,
ESOI.DEFNS_DRVR_DISC_IND,
ESOI.ANTI_THFT_DISC,
ESOI.DAY_TM_RUN_LIGHTS,
ESOI.LMT_TORT,
ESOI.COMP_DED,
MSOI.MLT_CAR_IND,
MSOI.GOOD_STDNT_IND,
MSOI.COLL_DED,
--Coverage attributes
CVG.ACCTNG_LOB,
CVG.CVG_TYP_CD,
ECVG.EFF_DT,
ECVG.NJ_RTD_PNTS,
ECVG.NJ_FRGVN_PNTS,
ECVG.CVG_TYP_IND,
MCVG.CVG_AMT,
--Geo attributes
GEOS.ST_NM,
GEOS.ST_CD,
RGEO.GRGNG_ZIP_5,
HH.NUM_OF_CARS_IN_HH,
ESOI.RDRVR_DT_OF_BRTH,
PLCY.TERM_STRT_DT,
PLCY.SRC_SYS_CD,
PLCY.PNI_AGE
)
ORDER BY ST_CD, ST_NM) --where (TTL_WRITTN_PREM_AMT !=0 AND CVG_EXPS_VAL!=0)
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

# -------------------------------------------------------------------------
# EXP_Defaults: explicit projection of every listed port from the SQ dataframe
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_Defaults projection")
    df_EXP_Defaults_jovial_feynman = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan.selectExpr(
        "MRTL_STAT",
        "AUTO_USE_CD",
        "MILES_TO_WRK",
        "GOOD_STDNT_IND",
        "DRVR_TRNG_IND",
        "SOI_TYP",
        "PHY_DMG_IND",
        "NJ_RTD_PNTS",
        "VEH_MDL_YR",
        "NJ_EXCPTION_CD",
        "NJ_FRGVN_PNTS",
        "PASSV_RESTRA_DISC",
        "SNR_DRVR_IND",
        "DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC",
        "DAY_TM_RUN_LIGHTS",
        "LMT_TORT",
        "CVG_TYP_IND",
        "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT",
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
        "REG_PER_YR",
        "FISC_PER_YR",
        "NAIC_CMPY_CD",
        "ST_NM",
        "ST_CD",
        "ACCTNG_LOB",
        "CVG_TYP_CD",
        "CVG_AMT",
        "BI_LMT",
        "GA_UMBI_PD_ADDED_IND",
        "FA2_PLCY_IND",
        "UM_UIM_STACKING",
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
        "i_GRGNG_ZIP_5 AS i_GRGNG_ZIP_5",
        "RATNG_CMPY_CD",
        "MLT_CAR_IND",
        "RT_CLS",
        "AGE",
        "GENDR"
    )
except Exception as e:
    logger.error(f"Failed applying EXP_Defaults projection: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# EXP_Passthru: derive GRGNG_ZIP_5 and v_FARMERS_STATE_CD/IFARMERS_STATE_CD
# Use two-step projection to allow reuse of intermediate alias (v_FARMERS_STATE_CD)
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_Passthru transformations (GRGNG_ZIP_5, v_FARMERS_STATE_CD, IFARMERS_STATE_CD)")
    # create i_GRGNG_ZIP_5 from whatever the upstream provided (we selected it as i_GRGNG_ZIP_5)
    df_tmp = df_EXP_Defaults_jovial_feynman.withColumn("i_GRGNG_ZIP_5", col("i_GRGNG_ZIP_5"))

    # compute GRGNG_ZIP_5 per DECODE semantics: null/blank/'0' => '00000' else trimmed value
    df_tmp = df_tmp.withColumn(
        "GRGNG_ZIP_5",
        when(col("i_GRGNG_ZIP_5").isNull() | (trim(col("i_GRGNG_ZIP_5")) == "") | (trim(col("i_GRGNG_ZIP_5")) == "0"), lit("00000")).otherwise(trim(col("i_GRGNG_ZIP_5")))
    )

    # compute local variable v_FARMERS_STATE_CD and expose it as column for later reuse
    df_tmp = df_tmp.withColumn("v_FARMERS_STATE_CD", when(col("ST_CD") == '#', lit('00')).otherwise(col("ST_CD")))

    # second step: create IFARMERS_STATE_CD as integer cast of v_FARMERS_STATE_CD
    df_EXP_Passthru_nifty_schrodinger = df_tmp.withColumn("IFARMERS_STATE_CD", col("v_FARMERS_STATE_CD").cast(T.IntegerType()))

except Exception as e:
    logger.error(f"Failed applying EXP_Passthru transformations: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# LKP_ff_REF_NISS_STATE_CD: read flat-file lookup source and keep as dataframe
# -------------------------------------------------------------------------
try:
    logger.info("Reading lookup flat file for ff_REF_NISS_STATE_CD from provided path")
    # assume CSV; mapping specified a flat-file lookup. Header assumed true.
    df_LKP_ff_REF_NISS_STATE_CD_elated_turing = (
        spark.read.option("header", "true").csv(LOOKUP_FILE_FF_NISS_STATE)
        .selectExpr("FARMERS_STATE_NAME", "NISS_STATE_CODE")
    )
except Exception as e:
    logger.error(f"Failed reading lookup file {LOOKUP_FILE_FF_NISS_STATE}: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# EXP_To_Derive_Vals: derive many fields, join the lookup (broadcast) for
# NISS_STATE_CODE, compute o_NAIC_CMPY_CD and o_NISS_CMPNY_CD (DECODE chain),
# set EXPS_VAL_ROLLED = 0, and set mapping/audit fields to NULL (PM vars)
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_To_Derive_Vals: derive EXPS_VAL_ROLLED, o_NAIC_CMPY_CD, o_NISS_CMPNY_CD and join lookup")
    df_in = df_EXP_Passthru_nifty_schrodinger

    # Left-broadcast-join to lookup by ST_NM -> FARMERS_STATE_NAME
    # join condition: trimmed ST_NM == trimmed FARMERS_STATE_NAME
    df_joined = df_in.join(broadcast(df_LKP_ff_REF_NISS_STATE_CD_elated_turing), trim(col("ST_NM")) == trim(col("FARMERS_STATE_NAME")), how="left")

    # compute v_NAIC_CMPY_CD trimmed
    df_joined = df_joined.withColumn("v_NAIC_CMPY_CD", trim(col("NAIC_CMPY_CD")))

    # compute o_NAIC_CMPY_CD as trimmed NAIC
    df_joined = df_joined.withColumn("o_NAIC_CMPY_CD", col("v_NAIC_CMPY_CD"))

    # compute o_NISS_CMPNY_CD via chained when/otherwise mapping from v_NAIC_CMPY_CD
    df_joined = df_joined.withColumn(
        "o_NISS_CMPNY_CD",
        when(col("v_NAIC_CMPY_CD") == '10315', lit('172'))
        .when(col("v_NAIC_CMPY_CD") == '10317', lit('174'))
        .when(col("v_NAIC_CMPY_CD") == '10318', lit('173'))
        .when(col("v_NAIC_CMPY_CD") == '10806', lit('171'))
        .when(col("v_NAIC_CMPY_CD") == '10873', lit('170'))
        .when(col("v_NAIC_CMPY_CD") == '21598', lit('177'))
        .when(col("v_NAIC_CMPY_CD") == '21601', lit('079'))
        .when(col("v_NAIC_CMPY_CD") == '21628', lit('073'))
        .when(col("v_NAIC_CMPY_CD") == '21636', lit('178'))
        .when(col("v_NAIC_CMPY_CD") == '21644', lit('077'))
        .when(col("v_NAIC_CMPY_CD") == '21652', lit('070'))
        .when(col("v_NAIC_CMPY_CD") == '21660', lit('175'))
        .when(col("v_NAIC_CMPY_CD") == '21679', lit('072'))
        .when(col("v_NAIC_CMPY_CD") == '21687', lit('176'))
        .when(col("v_NAIC_CMPY_CD") == '21695', lit('179'))
        .when(col("v_NAIC_CMPY_CD") == '21709', lit('071'))
        .when(col("v_NAIC_CMPY_CD") == '24392', lit('169'))
        .when(col("v_NAIC_CMPY_CD") == '28673', lit('180'))
        .when(col("v_NAIC_CMPY_CD") == '36889', lit('074'))
        .otherwise(lit(''))
    )

    # GA_UMBI_PD_ADDED_IND: mapping IIF(i_GA_UMBI_PD_ADDED_IND=1,'Y','N')
    # upstream column may be GA_UMBI_PD_ADDED_IND; use that if present
    source_col = None
    if 'i_GA_UMBI_PD_ADDED_IND' in df_joined.columns:
        source_col = 'i_GA_UMBI_PD_ADDED_IND'
    elif 'GA_UMBI_PD_ADDED_IND' in df_joined.columns:
        source_col = 'GA_UMBI_PD_ADDED_IND'

    if source_col is not None:
        df_joined = df_joined.withColumn('GA_UMBI_PD_ADDED_IND', when(col(source_col) == lit(1), lit('Y')).otherwise(lit('N')))
    else:
        # if missing, set to NULL so downstream reviewers see it's not available
        df_joined = df_joined.withColumn('GA_UMBI_PD_ADDED_IND', lit(None))

    # EXPS_VAL_ROLLED = 0
    df_joined = df_joined.withColumn('EXPS_VAL_ROLLED', lit(0))

    # o_ANNL_STMNT_LOB_CD = SUBSTR(ltrim(rtrim(ACCTNG_LOB)),1,3)
    df_joined = df_joined.withColumn('ANNL_STMNT_LOB_CD', substring(trim(col('ACCTNG_LOB')), 1, 3))

    # mapping/audit system variables become NULL per rules
    df_joined = df_joined.withColumn('MAPPING_NAME', lit(None)).withColumn('FOLDER_NAME', lit(None)).withColumn('WORKFLOW_NAME', lit(None))

    # NISS_STATE_CODE comes from lookup column NISS_STATE_CODE (may be null if no match)
    df_joined = df_joined.withColumn('NISS_STATE_CODE', col('NISS_STATE_CODE'))

    df_EXP_To_Derive_Vals_kind_noether = df_joined

except Exception as e:
    logger.error(f"Failed applying EXP_To_Derive_Vals logic: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# EXP_Passtotgt: final projection and surrogate key generation (row_number)
# Note: we must not reference the surrogate column in the projection that
# precedes withColumn adding it.
# -------------------------------------------------------------------------
try:
    logger.info("Applying EXP_Passtotgt: final projection and NISS_APRM_LND_SK generation")

    # Project all required target columns explicitly. Any missing lookup/audit
    # inputs omitted earlier remain NULL because we set them to NULL above.
    select_cols = [
        'WRK_FLOW_RUN_ID',
        'NJ_NO_LWST_LMT_IND',
        'NJ_NMD_DRVR_EXCL_IND',
        'EXPS_VAL_ROLLED',
        'COMP_DED',
        'COLL_DED',
        'PLCY_CNTRCT_NUM',
        'UNIT_NUM',
        'EFF_DT',
        'NUM_OF_CARS_IN_HH',
        'RDRVR_DT_OF_BRTH',
        'TERM_STRT_DT',
        'SRC_SYS_CD',
        'PNI_AGE',
        'REG_PER_YR',
        'FISC_PER_YR',
        'NAIC_CMPY_CD',
        'o_NISS_CMPNY_CD AS NISS_CMPNY_CD',
        'ST_NM',
        'ST_CD',
        'NISS_STATE_CODE as NISS_ST_CD',
        'STATE_ABBR as ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'BI_LMT',
        'GA_UMBI_PD_ADDED_IND AS GA_ADDED_AT_FAULT_IND',
        'FA2_PLCY_IND',
        'UM_UIM_STACKING',
        'PIP_WVR_WL_IND',
        'PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND',
        'MI_PPO_IND',
        'PRD_GRP_CD',
        'NJ_HLTH_INSR_PRIM',
        'NJ_EXTR_PIP_PKG',
        'NJ_RESDNC_RLTNSHP_PIP_IND',
        'NY_SSL_IND',
        'NY_FULL_CVG_GLASS_COMP_IND',
        'GRGNG_ZIP_5',
        'NISS_TERR_CD',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'RT_CLS',
        'AGE',
        'GENDR',
        'MRTL_STAT',
        'AUTO_USE_CD',
        'MILES_TO_WRK',
        'GOOD_STDNT_IND',
        'DRVR_TRNG_IND',
        'SOI_TYP',
        'PHY_DMG_IND',
        'NJ_RTD_PNTS AS NJ_RATD_PNTS',
        'VEH_MDL_YR',
        'NJ_EXCPTION_CD',
        'NJ_FRGVN_PNTS AS NJ_FGVN_PNTS',
        'PASSV_RESTRA_DISC',
        'SNR_DRVR_IND',
        'DEFNS_DRVR_DISC_IND',
        'ANTI_THFT_DISC',
        'DAY_TM_RUN_LIGHTS',
        'LMT_TORT',
        'ANNL_STMNT_LOB_CD',
        'CVG_TYP_IND',
        'CVG_EXPS_VAL',
        'TTL_WRITTN_PREM_AMT',
        'CR_BY_MAPNG_ID',
        'DW_CR_TMSP',
        'UPD_BY_MAPNG_ID',
        'DW_UPD_TMSP'
    ]

    # selectExpr expects strings; ensure all fields exist or will produce nulls
    df_proj = df_EXP_To_Derive_Vals_kind_noether.selectExpr(*select_cols)

    # Now attach the gap-free surrogate key using row_number() over a single
    # ordering (this forces a single-partition shuffle; review if input is large)
    window_spec = Window.orderBy(lit(1))
    df_EXP_Passtotgt_zen_fermat = df_proj.withColumn('NISS_APRM_LND_SK', row_number().over(window_spec))

except Exception as e:
    logger.error(f"Failed applying EXP_Passtotgt logic: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# Final Output: write intermediate WRK_ table to S3 as Parquet (overwrite)
# Target: FDR_LIB_WRK_BIRP_NISS_APRM_LND -> write to s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_LND/
# -------------------------------------------------------------------------
try:
    logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3 as parquet (overwrite)")
    df_EXP_Passtotgt_zen_fermat.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_LND/"
    )
    # expose the written dataframe variable name expected downstream
    df_FDR_LIB_WRK_BIRP_NISS_APRM_LND_laughing_faraday = df_EXP_Passtotgt_zen_fermat
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3: {e}", exc_info=True)
    raise


job.commit()
