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
CATALOG_DATABASE = "REPLACE_WITH_GLUE_DATABASE"
CATALOG_TABLE = "WRK_BIRP_NISS_APRM_DETL"

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL: attempt to read staged intermediate parquet first, fall back to Glue catalog if missing
try:
    logger.info("Attempting to read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 parquet at s3://{}/WRK_BIRP_NISS_APRM_DETL/".format(S3_OUTPUT_BUCKET))
    df_raw_FDR_LIB_WRK_BIRP_NISS_APRM_DETL = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
except Exception as e:
    logger.warning(f"S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read: {e}", exc_info=True)
    try:
        logger.info("Reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue catalog %s.%s" % (CATALOG_DATABASE, CATALOG_TABLE))
        dyf = glueContext.create_dynamic_frame_from_catalog(database=CATALOG_DATABASE, table_name=CATALOG_TABLE)
        df_raw_FDR_LIB_WRK_BIRP_NISS_APRM_DETL = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue catalog: {e2}", exc_info=True)
        raise

# Project exactly the fields listed on the Source node
try:
    logger.info("Projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL into df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_archimedes")
    _cols = [
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
        "SOURCE_IND_DERIVED",
    ]

    # select the listed columns explicitly
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_archimedes = df_raw_FDR_LIB_WRK_BIRP_NISS_APRM_DETL.select(*_cols)
    logger.info("Successfully prepared df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_archimedes with %d columns" % len(_cols))
except Exception as e:
    logger.error(f"Failed projecting columns for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
