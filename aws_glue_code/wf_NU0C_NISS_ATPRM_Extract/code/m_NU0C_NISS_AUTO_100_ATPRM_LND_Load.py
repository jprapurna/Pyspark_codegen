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

# Placeholder for Glue Data Catalog database name
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# Read source DIM_AG_SOI from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_SOI (DIM_AG_SOI) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_SOI = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_SOI")
    df_FDR_LIB_DIM_AG_SOI_loving_pascal = (
        dyf_FDR_LIB_DIM_AG_SOI.toDF()
        .selectExpr(
            "DRVR_TRNG_IND",
            "SOI_TYP",
            "VEH_MDL_YR",
            "SNR_DRVR_IND",
            "UNIT_NUM",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_SOI from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_PLCY_ENH from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_PLCY_ENH (DIM_AG_PLCY_ENH) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_PLCY_ENH = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_PLCY_ENH")
    df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether = (
        dyf_FDR_LIB_DIM_AG_PLCY_ENH.toDF()
        .selectExpr(
            "NAIC_CMPY_CD",
            "PHY_DMG_IND",
            "NY_SSL_IND",
            "GA_UMBI_PD_ADDED_IND",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_PLCY_ENH from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_PLCY from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_PLCY (DIM_AG_PLCY) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_PLCY = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_PLCY")
    df_FDR_LIB_DIM_AG_PLCY_keen_leibniz = (
        dyf_FDR_LIB_DIM_AG_PLCY.toDF()
        .selectExpr(
            "PNI_AGE",
            "TERM_STRT_DT",
            "PLCY_CNTRCT_NUM",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_PLCY from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_CVG from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_CVG (DIM_AG_CVG) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_CVG = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_CVG")
    df_FDR_LIB_DIM_AG_CVG_blissful_darwin = (
        dyf_FDR_LIB_DIM_AG_CVG.toDF()
        .selectExpr(
            "ACCTNG_LOB",
            "CVG_TYP_CD",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_CVG from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_RATED_GEO from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_RATED_GEO (DIM_AG_RATED_GEO) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_RATED_GEO = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_RATED_GEO")
    df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford = (
        dyf_FDR_LIB_DIM_AG_RATED_GEO.toDF()
        .selectExpr(
            "GRGNG_ZIP_5",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_RATED_GEO from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_FARMR_GEO_ST from Glue Data Catalog and project required columns
# Note: metadata listed ST_CD twice; select it once along with ST_NM as required
try:
    logger.info("Reading FDR_LIB_DIM_AG_FARMR_GEO_ST (DIM_AG_FARMR_GEO_ST) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_FARMR_GEO_ST = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_FARMR_GEO_ST")
    df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume = (
        dyf_FDR_LIB_DIM_AG_FARMR_GEO_ST.toDF()
        .selectExpr(
            "ST_CD",
            "ST_NM",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_FARMR_GEO_ST from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_MINI_PLCY from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_PLCY (DIM_AG_MINI_PLCY) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_MINI_PLCY = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_PLCY")
    df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck = (
        dyf_FDR_LIB_DIM_AG_MINI_PLCY.toDF()
        .selectExpr(
            "FA2_PLCY_IND",
            "BI_LMT",
            "RATNG_CMPY_CD",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_MINI_PLCY from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_DT from Glue Data Catalog and project required columns
# Deduplicate CLNDR_YR if metadata lists it more than once
try:
    logger.info("Reading FDR_LIB_DIM_DT (DIM_DT) from Glue Catalog")
    dyf_FDR_LIB_DIM_DT = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_DT")
    df_FDR_LIB_DIM_DT_adoring_feynman = (
        dyf_FDR_LIB_DIM_DT.toDF()
        .selectExpr(
            "CLNDR_YR",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_DT from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_MINI_CVG from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_CVG (DIM_AG_MINI_CVG) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_MINI_CVG = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_CVG")
    df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr = (
        dyf_FDR_LIB_DIM_AG_MINI_CVG.toDF()
        .selectExpr(
            "CVG_AMT",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_MINI_CVG from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_SOI_ENH from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_SOI_ENH (DIM_AG_SOI_ENH) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_SOI_ENH = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_SOI_ENH")
    df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking = (
        dyf_FDR_LIB_DIM_AG_SOI_ENH.toDF()
        .selectExpr(
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
            "DAY_TM_RUN_LIGHTS",
            "NJ_NMD_DRVR_EXCL_IND",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_SOI_ENH from Glue Catalog: {e}", exc_info=True)
    raise

# Read source DIM_AG_MINI_SOI from Glue Data Catalog and project required columns
try:
    logger.info("Reading FDR_LIB_DIM_AG_MINI_SOI (DIM_AG_MINI_SOI) from Glue Catalog")
    dyf_FDR_LIB_DIM_AG_MINI_SOI = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="DIM_AG_MINI_SOI")
    df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume = (
        dyf_FDR_LIB_DIM_AG_MINI_SOI.toDF()
        .selectExpr(
            "MINI_SOI_SK",
            "COLL_DED",
            "GOOD_STDNT_IND",
            "MLT_CAR_IND",
        )
    )
except Exception as e:
    logger.error(f"Failed reading FDR_LIB_DIM_AG_MINI_SOI from Glue Catalog: {e}", exc_info=True)
    raise


job.commit()
