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

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# placeholder environment/run-time constants
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# --- Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL2 (WRK_ staged, S3-first with fallback) ---
try:
    logger.info("Reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL2 from S3 parquet (staged) - trying S3 first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    # project exactly the ports listed on this Source node
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.select(
        "NISS_APRM_DETL_SK", "CLNDR_YR", "CALL_YR", "NAIC_CMPNY_CD", "NISS_CMPNY_CD", "ST_NM", "ST_CD", "NISS_ST_CD",
        "ST_ABBR", "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT", "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND",
        "UM_UMI_STACKING", "PIP_WVR_WL_IND", "PIP_MED_SEC_IND", "PIP_LOSS_INCOME_IND", "MI_PPO_IND", "PRD_GRP_CD",
        "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG", "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND", "NY_FULL_CVG_GLASS_COMP_IND",
        "GRGNG_ZIP_5", "NISS_TERR_CD", "RATNG_CMPY_CD", "MLT_CAR_IND", "RT_CLS", "AGE", "GENDR", "MRTL_STAT",
        "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND", "DRVR_TRNG_IND", "SOI_TYP", "PHY_DMG_IND", "NJ_RATD_PNTS",
        "VEH_MDL_YR", "NJ_EXCPTION_CD", "NJ_FGVN_PNTS", "PASSV_RESTRA_DISC", "SNR_DRVR_IND", "DEFNS_DRVR_DISC_IND",
        "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT", "ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL",
        "TTL_WRITTN_PREM_AMT", "LINE_CD", "ACCDNT_YR", "NISS_CVG_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "NISS_CLASS_CD",
        "NISS_ELIG_PNTS_CD", "NISS_AGE_GRP_CD", "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD",
        "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD",
        "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD", "NISS_TYP_LOSS_CD", "NISS_LIAB_OR_NO_FAULT_CD", "NISS_ANNL_STMNT_LOB_CD",
        "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_OUTSTNDG_LOSS", "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS",
        "RSVD_NISS_USE", "NISS_RSVD_CMPNY_USE", "NISS_MNFCTRS_MDL_YR", "CR_BY_MAPNG_ID", "DW_CR_TMSP", "UPD_BY_MAPNG_ID",
        "DW_UPD_TMSP", "WRK_FLOW_RUN_ID", "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "CVG_CNT_IND",
        "CVG_CNT", "CVG_CD_SK", "CVG_ATTR_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC",
        "CVG_ATTR_CHCKSUM", "COMP_DED", "COLL_DED", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH",
        "RDRVR_DT_OF_BRTH", "TERM_STRT_DT", "SRC_SYS_CD", "DERIVED_RDRVR_AGE", "FINAL_RDRVR_AGE", "PNI_AGE", "LOB",
        "PRINCIPAL_OPRT", "SOURCE_IND_DERIVED"
    )
except Exception as e:
    logger.warning("S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2 failed; falling back to Glue Catalog read and logging the fallback")
    try:
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla = dyf.toDF().select(
            "NISS_APRM_DETL_SK", "CLNDR_YR", "CALL_YR", "NAIC_CMPNY_CD", "NISS_CMPNY_CD", "ST_NM", "ST_CD", "NISS_ST_CD",
            "ST_ABBR", "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT", "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND",
            "UM_UMI_STACKING", "PIP_WVR_WL_IND", "PIP_MED_SEC_IND", "PIP_LOSS_INCOME_IND", "MI_PPO_IND", "PRD_GRP_CD",
            "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG", "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND", "NY_FULL_CVG_GLASS_COMP_IND",
            "GRGNG_ZIP_5", "NISS_TERR_CD", "RATNG_CMPY_CD", "MLT_CAR_IND", "RT_CLS", "AGE", "GENDR", "MRTL_STAT",
            "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND", "DRVR_TRNG_IND", "SOI_TYP", "PHY_DMG_IND", "NJ_RATD_PNTS",
            "VEH_MDL_YR", "NJ_EXCPTION_CD", "NJ_FGVN_PNTS", "PASSV_RESTRA_DISC", "SNR_DRVR_IND", "DEFNS_DRVR_DISC_IND",
            "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT", "ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL",
            "TTL_WRITTN_PREM_AMT", "LINE_CD", "ACCDNT_YR", "NISS_CVG_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "NISS_CLASS_CD",
            "NISS_ELIG_PNTS_CD", "NISS_AGE_GRP_CD", "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD",
            "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD",
            "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD", "NISS_TYP_LOSS_CD", "NISS_LIAB_OR_NO_FAULT_CD", "NISS_ANNL_STMNT_LOB_CD",
            "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_OUTSTNDG_LOSS", "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS",
            "RSVD_NISS_USE", "NISS_RSVD_CMPNY_USE", "NISS_MNFCTRS_MDL_YR", "CR_BY_MAPNG_ID", "DW_CR_TMSP", "UPD_BY_MAPNG_ID",
            "DW_UPD_TMSP", "WRK_FLOW_RUN_ID", "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "CVG_CNT_IND",
            "CVG_CNT", "CVG_CD_SK", "CVG_ATTR_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC",
            "CVG_ATTR_CHCKSUM", "COMP_DED", "COLL_DED", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH",
            "RDRVR_DT_OF_BRTH", "TERM_STRT_DT", "SRC_SYS_CD", "DERIVED_RDRVR_AGE", "FINAL_RDRVR_AGE", "PNI_AGE", "LOB",
            "PRINCIPAL_OPRT", "SOURCE_IND_DERIVED"
        )
    except Exception as e:
        logger.error(f"Failed fallback catalog read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2: {e}", exc_info=True)
        raise

# --- Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (Shortcut staged source; S3-first with catalog fallback) ---
try:
    logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 from S3 parquet (staged) - trying S3 first")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_2/"
    )
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin.select(
        "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5",
        "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCTNG_LOB",
        "NUM_OF_CARS_IN_HH"
    )
except Exception as e:
    logger.warning("S3 parquet read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 failed; falling back to Glue Catalog read and logging the fallback")
    try:
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL_2")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin = dyf.toDF().select(
            "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5",
            "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
            "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
            "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCTNG_LOB",
            "NUM_OF_CARS_IN_HH"
        )
    except Exception as e:
        logger.error(f"Failed fallback catalog read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2: {e}", exc_info=True)
        raise

# --- Application Source Qualifier: SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (staged override -> use temp view) ---
try:
    # register the staged upstream dataframe as a temp view named after its real table
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT DET.NISS_APRM_DETL_SK
FROM WRK_BIRP_NISS_APRM_DETL DET
WHERE DET.CVG_ATTR_SK IN (
    SELECT DETL.CVG_ATTR_SK
    FROM WRK_BIRP_NISS_APRM_DETL DETL
    WHERE DETL.REC_EXCPN_IND='Y'
    GROUP BY DETL.CVG_ATTR_SK
    HAVING SUM(DETL.TTL_WRITTN_PREM_AMT) = 0 AND SUM(DETL.EXPS_VAL_ROLLED) = 0
)
"""
    logger.info("Running SQL override for SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops against staged temp view via spark.sql()")
    df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_loving_faraday = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing staged SQL override for SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: {e}", exc_info=True)
    raise

# --- Application Source Qualifier: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (JDBC override reading external Snowflake table) ---
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (bypassed — SQL override reads it directly)
sql_query = f"""SELECT DETL.NISS_APRM_DETL_SK,
DETL.NISS_CMPNY_CD,
DETL.NISS_ST_CD,
DETL.NISS_CVG_CD,
DETL.NISS_TERR_CD,
DETL.GRGNG_ZIP_5,
DETL.NISS_CLASS_CD,
DETL.NISS_EXCPN_CD,
DETL.NISS_FGVNS_CD,
DETL.NISS_PASSV_RESTRA_CD,
DETL.NISS_DEFNS_DRVR_CRD_CD,
DETL.NISS_ANTI_THFT_DVC_CD,
DETL.NISS_DAY_TM_RUN_LAMPS_DISC_CD,
DETL.NISS_PLCY_LMT_CD,
DETL.NISS_DEDUC_CD,
DETL.NISS_SSL_LIAB_CD,
DETL.NISS_SUBLOB_CD,
DETL.NISS_LIAB_OR_NO_FAULT_CD,
DETL.REC_EXCPN_IND,
DETL.REC_EXCPN_RSN_DESC,
DETL.AGE,
DETL.ACCTNG_LOB,
DETL.NUM_OF_CARS_IN_HH

FROM FDR.WRK_BIRP_NISS_APRM_DETL DETL"""
try:
    logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 from Snowflake via JDBC override query")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_jovial_einstein = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 from Snowflake: {e}", exc_info=True)
    raise

# --- EXP_PassThrough_Src1: passthrough key + two constant derived outputs ---
try:
    logger.info("Applying EXP_PassThrough_Src1 transformation (passthrough + constants)")
    df_EXP_PassThrough_Src1_dazzling_curie = (
        df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_loving_faraday.selectExpr(
            "NISS_APRM_DETL_SK",
            "'Y' AS REC_DROP_IND",
            "'997' AS REC_DROP_RSN_DESC"
        )
    )
except Exception as e:
    logger.error(f"Failed EXP_PassThrough_Src1: {e}", exc_info=True)
    raise

# --- EXP_PassThrough_Src: pure passthrough of listed ports ---
try:
    logger.info("Applying EXP_PassThrough_Src passthrough projection")
    df_EXP_PassThrough_Src_mystifying_hopper = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_jovial_einstein.select(
        "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5",
        "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCNTG_LOB",
        "NUM_OF_CARS_IN_HH"
    )
except Exception as e:
    logger.error(f"Failed EXP_PassThrough_Src projection: {e}", exc_info=True)
    raise

# --- EXP_Gen_Exceptions: compute many local fragments, concatenate into REC_EXCPN_RSN_DESC and set REC_EXCPN_IND ---
try:
    logger.info("Applying EXP_Gen_Exceptions complex validation expressions")
    df = df_EXP_PassThrough_Src_mystifying_hopper

    # helper trims
    trimcol = lambda c: F.trim(F.col(c))

    df = (
        df.withColumn("NISS_CMPNY_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_CMPNY_CD"), 1, 1) == "?", F.lit("NISS_CMPNY_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_ST_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_ST_CD"), 1, 1) == "?", F.lit("NISS_ST_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_CVG_CD_RecExcpnRsn",
                      F.when(F.substring(trimcol("NISS_CVG_CD"), 1, 1) == "?", F.lit("NISS_CVG_CD;") )
                       .when((F.col("REC_EXCPN_IND") == 'Y') & (F.substring(trimcol("NISS_CVG_CD"),1,1) == 'E'), F.lit("Dflt NISS_CVG_CD;"))
                       .otherwise(F.lit("") )
          )
          .withColumn("NISS_TERR_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_TERR_CD"),1,1) == "?", F.lit("NISS_TERR_CD;")).otherwise(F.lit("")))
          .withColumn("GRGNG_ZIP_5_RecExcpnRsn", F.when((F.substring(trimcol("GRGNG_ZIP_5"),1,1) == "?") | (F.col("GRGNG_ZIP_5") == '00000'), F.lit("GRGNG_ZIP;")).otherwise(F.lit("")))
          .withColumn("NISS_CLASS_CD_RecExcpnRsn",
                      F.when((F.col("REC_EXCPN_IND") == 'Y') & (F.instr(F.col("REC_EXCPN_RSN_DESC"), 'DEFAULT_CLASS_CD') != 0), F.lit("DEFAULT_CLASS_CD;") )
                       .when(F.substring(trimcol("NISS_CLASS_CD"),1,1) == "?", F.lit("106:DEFAULT_CLASS_CD;") )
                       .when(F.substring(trimcol("NISS_CLASS_CD"),1,1) == "E", F.lit("118:DEFAULT_CLASS_CD;") )
                       .otherwise(F.lit(""))
          )
          .withColumn("NISS_DFLT_ZIP_RecExcpnRsn", F.when((F.col("REC_EXCPN_IND") == 'Y') & (F.instr(F.col("REC_EXCPN_RSN_DESC"), 'assigned default zip code') != 0), F.lit('assigned default zip code;')).otherwise(F.lit('')))
          .withColumn("NISS_EXCPN_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_EXCPN_CD"),1,1) == "?", F.lit("NISS_EXCPN_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_FGVNS_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_FGVNS_CD"),1,1) == "?", F.lit("NISS_FGVNS_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_PASSV_RESTRA_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_PASSV_RESTRA_CD"),1,1) == "?", F.lit("NISS_PASSV_RESTRA_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_DEFNS_DRVR_CRD_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_DEFNS_DRVR_CRD_CD"),1,1) == "?", F.lit("DEFNS_DRVR_CRD_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_ANTI_THFT_DVC_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_ANTI_THFT_DVC_CD"),1,1) == "?", F.lit("ANTI_THFT_DVC_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_DAY_TM_RUN_LAMPS_DISC_CD"),1,1) == "?", F.lit("DAY_TM_RUN_LAMPS_DISC_CD;")).otherwise(F.lit("")))
          .withColumn("NISS_PLCY_LMT_CD_RecExcpnRsn",
                      F.when(F.substring(trimcol("NISS_PLCY_LMT_CD"),1,1) == "?", F.lit("PLCY_LMT_CD;") )
                       .when(F.col("NISS_PLCY_LMT_CD") == '??', F.lit('INVALID PLCY_LMT_CD;'))
                       .when(F.substring(trimcol("NISS_PLCY_LMT_CD"),1,1) == 'E', F.lit('DFLT_PLCY_LMT_CD;'))
                       .otherwise(F.lit(''))
          )
          .withColumn("NISS_DEDUC_CD_RecExcpnRsn",
                      F.when(F.substring(trimcol("NISS_DEDUC_CD"),1,1) == '?', F.lit('DEDUC_CD;'))
                       .when(F.substring(trimcol("NISS_DEDUC_CD"),1,1) == 'E', F.lit('DFLT_DEDUC_CD;'))
                       .otherwise(F.lit(''))
          )
          .withColumn("NISS_SSL_LIAB_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_SSL_LIAB_CD"),1,1) == '?', F.lit('SSL_LIAB_CD;')).otherwise(F.lit('')))
          .withColumn("NISS_SUBLOB_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_SUBLOB_CD"),1,1) == '?', F.lit('SUBLOB_CD;')).otherwise(F.lit('')))
          .withColumn("NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn", F.when(F.substring(trimcol("NISS_LIAB_OR_NO_FAULT_CD"),1,1) == '?', F.lit('IAB_OR_NO_FAULT_CD;')).otherwise(F.lit('')))
          .withColumn("NUM_OF_CARS_IN_HH_RecExcpnRsn", F.when((F.col('NISS_ST_CD') == '09') & (F.col('NUM_OF_CARS_IN_HH').isNull()), F.lit('BLANK VEHICLE COUNT;')).otherwise(F.lit('')))
          .withColumn("ACCNTG_LOB_RecExcpnRsn", F.when((F.col('ACCNTG_LOB') == '') | (F.col('ACCNTG_LOB').isNull()) | (F.col('ACCNTG_LOB') == ' ') | (F.length(F.trim(F.col('ACCNTG_LOB'))) == 0), F.lit('Blank ACCNTG_LOB;')).otherwise(F.lit('')))
          .withColumn("AGE_RecExcpnRsn", F.when((F.col('AGE').cast('int') > 120) | (F.col('AGE').cast('int') < 14) | (F.trim(F.col('AGE')) == ''), F.lit('Invalid Rated Driver Age;')).otherwise(F.lit('')))
    )

    # concatenation of fragments
    df = df.withColumn(
        "v_REC_EXCPN_RSN_DESC",
        F.concat(
            F.col('NISS_DFLT_ZIP_RecExcpnRsn'), F.col('NISS_CMPNY_CD_RecExcpnRsn'), F.col('NISS_ST_CD_RecExcpnRsn'),
            F.col('NISS_CVG_CD_RecExcpnRsn'), F.col('NISS_TERR_CD_RecExcpnRsn'), F.col('GRGNG_ZIP_5_RecExcpnRsn'),
            F.col('NISS_CLASS_CD_RecExcpnRsn'), F.col('NISS_EXCPN_CD_RecExcpnRsn'), F.col('NISS_FGVNS_CD_RecExcpnRsn'),
            F.col('NISS_PASSV_RESTRA_CD_RecExcpnRsn'), F.col('NISS_DEFNS_DRVR_CRD_CD_RecExcpnRsn'), F.col('NISS_ANTI_THFT_DVC_CD_RecExcpnRsn'),
            F.col('NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn'), F.col('NISS_PLCY_LMT_CD_RecExcpnRsn'), F.col('NISS_DEDUC_CD_RecExcpnRsn'),
            F.col('NISS_SSL_LIAB_CD_RecExcpnRsn'), F.col('NISS_SUBLOB_CD_RecExcpnRsn'), F.col('NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn'),
            F.col('AGE_RecExcpnRsn'), F.col('NUM_OF_CARS_IN_HH_RecExcpnRsn'), F.col('ACCNTG_LOB_RecExcpnRsn')
        )
    )

    # final outputs
    df = df.withColumn("REC_EXCPN_RSN_DESC", F.col('v_REC_EXCPN_RSN_DESC'))
    df = df.withColumn(
        "REC_EXCPN_IND",
        F.when((F.trim(F.col('i_REC_EXCPN_IND').cast('string')) == '') & (F.col('v_REC_EXCPN_RSN_DESC') == ''), F.lit('')).otherwise(F.lit('Y'))
    )

    # keep only the declared INPUT/INPUT-OUTPUT/OUTPUT ports for downstream
    df_EXP_Gen_Exceptions_romantic_maxwell = df.select(
        "i_REC_EXCPN_IND", "i_REC_EXCPN_RSN_DESC", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD",
        "GRGNG_ZIP_5", "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
        "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
        "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "NUM_OF_CARS_IN_HH", "ACCNTG_LOB", "SRC_SYS_CD", "AGE",
        "REC_EXCPN_RSN_DESC", "REC_EXCPN_IND"
    )
except Exception as e:
    logger.error(f"Failed EXP_Gen_Exceptions: {e}", exc_info=True)
    raise

# --- EXP_Gen_RecDrops: compute drop fragments, concatenate into REC_DROP_RSN_DESC and set REC_DROP_IND ---
try:
    logger.info("Applying EXP_Gen_RecDrops validation expressions")
    df = df_EXP_PassThrough_Src_mystifying_hopper

    df = (
        df.withColumn("NISS_CLASS_CD_RecDropRsn", F.when(F.substring(F.trim(F.col('NISS_CLASS_CD')),1,1) == '?', F.lit('INVALID CLASS_CD;')).otherwise(F.lit('')))
          .withColumn("NISS_CVG_CD_RecDropRsn", F.when((F.lower(F.col('NISS_CVG_CD')) == 'na') | (F.lower(F.col('NISS_CVG_CD')) == 'n/a') | (F.substring(F.trim(F.col('NISS_CVG_CD')),1,1) == '?'), F.lit('999;')).otherwise(F.lit('')))
          .withColumn("NISS_PLCY_LMT_CD_RecDropRsn", F.when((F.col('NISS_PLCY_LMT_CD') == 'na') | (F.col('NISS_PLCY_LMT_CD') == '??'), F.lit('998;')).otherwise(F.lit('')))
          .withColumn("NISS_DEDUC_CD_RecDropRsn", F.when(F.col('NISS_DEDUC_CD') == '??', F.lit('INVALID DEDUC CD;')).otherwise(F.lit('')))
          .withColumn("ACCNTG_LOB_RecDropRsn", F.when((F.col('ACCNTG_LOB') == '') | (F.col('ACCNTG_LOB').isNull()) | (F.col('ACCNTG_LOB') == ' ') | (F.length(F.trim(F.col('ACCNTG_LOB'))) == 0), F.lit('Blank ACCNTG_LOB;')).otherwise(F.lit('')))
          .withColumn("NO_OF_CARS_HH_RecDropRsn", F.when((F.col('NISS_ST_CD') == '09') & (F.col('NUM_OF_CARS_IN_HH').isNull()), F.lit('Blank Vehicle Count;')).otherwise(F.lit('')))
          .withColumn("NISS_ST_CD_RecDropRsn", F.when(F.col('NISS_ST_CD') == '??', F.lit('INVALID ST_CD;')).otherwise(F.lit('')))
    )

    df = df.withColumn("v_REC_DROP_RSN_DESC",
                         F.concat(
                             F.col('NISS_CVG_CD_RecDropRsn'), F.col('NISS_PLCY_LMT_CD_RecDropRsn'), F.col('NISS_ST_CD_RecDropRsn'),
                             F.col('NISS_CLASS_CD_RecDropRsn'), F.col('NISS_DEDUC_CD_RecDropRsn'), F.col('ACCNTG_LOB_RecDropRsn'), F.col('NO_OF_CARS_HH_RecDropRsn')
                         ))

    df = df.withColumn("REC_DROP_RSN_DESC", F.col('v_REC_DROP_RSN_DESC'))
    df = df.withColumn("REC_DROP_IND", F.when(F.col('v_REC_DROP_RSN_DESC') == '', F.lit('')).otherwise(F.lit('Y')))

    df_EXP_Gen_RecDrops_jolly_faraday = df.select(
        "NISS_CVG_CD", "NISS_ST_CD", "NISS_CLASS_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "ACCNTG_LOB",
        "NUM_OF_CARS_IN_HH", "REC_DROP_RSN_DESC", "REC_DROP_IND"
    )
except Exception as e:
    logger.error(f"Failed EXP_Gen_RecDrops: {e}", exc_info=True)
    raise

# --- EXP_Dummy: union the three upstream pipelines and project final passthrough fields ---
try:
    logger.info("Applying EXP_Dummy union of upstream pipelines")
    from functools import reduce
    dfs_to_union = [df_EXP_Gen_RecDrops_jolly_faraday, df_EXP_Gen_Exceptions_romantic_maxwell, df_EXP_PassThrough_Src_mystifying_hopper]
    df_union = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_to_union)
    # project explicit passthrough/output ports
    df_EXP_Dummy_tender_hilbert = df_union.selectExpr(
        "NISS_APRM_DETL_SK",
        "REC_DROP_IND",
        "REC_DROP_RSN_DESC",
        "REC_EXCPN_IND",
        "REC_EXCPN_RSN_DESC"
    )
except Exception as e:
    logger.error(f"Failed EXP_Dummy union: {e}", exc_info=True)
    raise

# --- FIL_OnlyExceptions_or_Drops: apply filter REC_DROP_IND='Y' OR REC_EXCPN_IND='Y' ---
try:
    logger.info("Applying FIL_OnlyExceptions_or_Drops filter")
    df_FIL_OnlyExceptions_or_Drops_affectionate_turing = df_EXP_Dummy_tender_hilbert.filter(F.expr("REC_DROP_IND='Y' OR REC_EXCPN_IND='Y'"))
except Exception as e:
    logger.error(f"Failed FIL_OnlyExceptions_or_Drops filter: {e}", exc_info=True)
    raise

# --- Upd_RecDrop_Ind: apply Update Strategy (derive dd_op, drop REJECT, load-modify-store-back to Snowflake target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL) ---
try:
    logger.info("Applying Upd_RecDrop_Ind Update Strategy: deriving dd_op and applying load-modify-store-back to Snowflake target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
    # derive dd_op (rule says DD_UPDATE => mark as UPDATE)
    df_Upd_RecDrop_Ind_sleepy_leibniz = df_EXP_PassThrough_Src1_dazzling_curie.withColumn("dd_op", F.lit('UPDATE'))
    # drop rejects
    df_Upd_RecDrop_Ind_sleepy_leibniz = df_Upd_RecDrop_Ind_sleepy_leibniz.filter(F.col('dd_op') != 'REJECT')

    # load current full target from Snowflake
    try:
        logger.info("Reading existing target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Snowflake for load-modify-store-back")
        existing_df = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            .load()
        )
    except Exception as e:
        logger.error(f"Failed reading existing target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Snowflake: {e}", exc_info=True)
        raise

    # derive changed keys
    changed_keys_df = df_Upd_RecDrop_Ind_sleepy_leibniz.select("NISS_APRM_DETL_SK").distinct()
    # anti-join to remove rows being updated/deleted
    remaining_df = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how='left_anti')
    # take rows marked INSERT/UPDATE to union back (here UPDATE only)
    rows_to_apply = df_Upd_RecDrop_Ind_sleepy_leibniz.filter(F.col('dd_op').isin('INSERT', 'UPDATE')).drop('dd_op')
    # compose final full table
    combined_df = remaining_df.unionByName(rows_to_apply, allowMissingColumns=True)

    # write back full table to Snowflake (overwrite)
    try:
        logger.info("Writing combined full target back to Snowflake table FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL (overwrite)")
        combined_df.write.jdbc(
            SNOWFLAKE_URL,
            'FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL',
            mode='overwrite',
            properties={'user': SNOWFLAKE_USER, 'password': SNOWFLAKE_PASSWORD}
        )
    except Exception as e:
        logger.error(f"Failed writing combined target back to Snowflake: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed Upd_RecDrop_Ind update strategy apply: {e}", exc_info=True)
    raise

# --- Upd_CVG_CD_SK: apply Update Strategy on filtered exceptions/drops and write back to Snowflake target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL ---
try:
    logger.info("Applying Upd_CVG_CD_SK Update Strategy: deriving dd_op and applying load-modify-store-back to Snowflake target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
    df_Upd_CVG_CD_SK_gentle_maxwell = df_FIL_OnlyExceptions_or_Drops_affectionate_turing.withColumn('dd_op', F.lit('UPDATE'))
    df_Upd_CVG_CD_SK_gentle_maxwell = df_Upd_CVG_CD_SK_gentle_maxwell.filter(F.col('dd_op') != 'REJECT')

    # load current full target from Snowflake
    try:
        logger.info("Reading existing target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Snowflake for Upd_CVG_CD_SK load-modify-store-back")
        existing_df2 = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            .load()
        )
    except Exception as e:
        logger.error(f"Failed reading existing target for Upd_CVG_CD_SK: {e}", exc_info=True)
        raise

    changed_keys_df2 = df_Upd_CVG_CD_SK_gentle_maxwell.select("NISS_APRM_DETL_SK").distinct()
    remaining_df2 = existing_df2.join(changed_keys_df2, on=["NISS_APRM_DETL_SK"], how='left_anti')
    rows_to_apply2 = df_Upd_CVG_CD_SK_gentle_maxwell.filter(F.col('dd_op').isin('INSERT', 'UPDATE')).drop('dd_op')
    combined_df2 = remaining_df2.unionByName(rows_to_apply2, allowMissingColumns=True)

    try:
        logger.info("Writing combined full target for Upd_CVG_CD_SK back to Snowflake table FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL (overwrite)")
        combined_df2.write.jdbc(
            SNOWFLAKE_URL,
            'FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL',
            mode='overwrite',
            properties={'user': SNOWFLAKE_USER, 'password': SNOWFLAKE_PASSWORD}
        )
    except Exception as e:
        logger.error(f"Failed writing combined target for Upd_CVG_CD_SK back to Snowflake: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed Upd_CVG_CD_SK update strategy apply: {e}", exc_info=True)
    raise

# --- Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops ---
# The transactional apply for this target was executed by the upstream Update Strategy (Upd_RecDrop_Ind). No additional write here.
try:
    logger.info("Assigning Upd_RecDrop_Ind result to Output node variable for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (no additional write)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_sleepy_turing = df_Upd_RecDrop_Ind_sleepy_leibniz
except Exception as e:
    logger.error(f"Failed assigning Output df for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: {e}", exc_info=True)
    raise

# --- Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (final) ---
# The transactional apply for this target was executed by the upstream Update Strategy (Upd_CVG_CD_SK). No additional write here.
try:
    logger.info("Assigning Upd_CVG_CD_SK result to Output node variable for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (no additional write)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_eager_archimedes = df_Upd_CVG_CD_SK_gentle_maxwell
except Exception as e:
    logger.error(f"Failed assigning Output df for FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise



job.commit()
