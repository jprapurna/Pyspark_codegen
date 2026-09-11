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
CATALOG_DATABASE = "REPLACE_WITH_CATALOG_DATABASE"

from pyspark.sql.functions import expr, when, lit, size, split, trim, regexp_replace, col, row_number
from pyspark.sql import Window

# -----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_BIRP_NISS_APRM_DETL) - try S3-first
# -----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_calm_turing = (
        spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
        .select(
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
        )
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 successfully")
except Exception as e:
    logger.warning("Failed to read WRK_BIRP_NISS_APRM_DETL from S3; falling back to catalog read: %s" % e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback")
        # Fallback - requires the catalog database/table to be provided via placeholder constant
        dyn = glueContext.create_dynamic_frame.from_catalog(database=CATALOG_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_calm_turing = dyn.toDF().select(
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
        )
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog successfully")
    except Exception as e2:
        logger.error(f"Failed fallback catalog read for WRK_BIRP_NISS_APRM_DETL: {e2}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (SQL Override) - staged case
# -----------------------------------------------------------------------------
try:
    # register staged upstream DF as temp view for SQL override to reference
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_calm_turing.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_NM,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    BI_LMT,
    MI_PPO_IND,
    LMT_TORT,
    TERM_STRT_DT
FROM
    WRK_BIRP_NISS_APRM_DETL
WHERE
    ST_ABBR NOT IN ('NY','NJ')
"""
    logger.info("Running SQL Override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql against temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_clever_gauss = spark.sql(sql_query)
    logger.info("Completed SQL Override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_BILimit_Split: split BI_LMT into parts, produce decimals and counts
# -----------------------------------------------------------------------------
try:
    logger.info("Computing EXP_BILimit_Split (clean BI_LMT, parts, numeric casts)")
    df_EXP_BILimit_Split_trusting_euclid = (
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_clever_gauss
        .withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ',', ''))
        .withColumn("_bi_parts", split(col("v_BI_LMT"), '/'))
        .withColumn("BI_LMT_NO_OF_PARTS", size(col("_bi_parts")))
        .withColumn("BI_LMT_1_Decimal",
                    when(size(col("_bi_parts")) >= 1,
                         regexp_replace(col("_bi_parts").getItem(0), '\\s+', '')
                         .cast("decimal(38,2)")
                         ).otherwise(lit(0).cast("decimal(38,2)"))
                   )
        .withColumn("BI_LMT_2_Decimal",
                    when(size(col("_bi_parts")) >= 2,
                         regexp_replace(col("_bi_parts").getItem(1), '\\s+', '')
                         .cast("decimal(38,2)")
                         ).otherwise(lit(0).cast("decimal(38,2)"))
                   )
        .withColumn("BI_LMT_3_Decimal",
                    when(size(col("_bi_parts")) >= 3,
                         regexp_replace(col("_bi_parts").getItem(2), '\\s+', '')
                         .cast("decimal(38,2)")
                         ).otherwise(lit(0).cast("decimal(38,2)"))
                   )
        .withColumn("SRC_BI_LMT", col("v_BI_LMT"))
        .drop("_bi_parts")
    )
    logger.info("Completed EXP_BILimit_Split")
except Exception as e:
    logger.error(f"Failed computing EXP_BILimit_Split: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_Derive_NISS_SUBLOB_CD_And_PassThru: derive MI flag, intermediate vv_, v_ and final NISS_SUBLOB_CD
# -----------------------------------------------------------------------------
try:
    logger.info("Starting EXP_Derive_NISS_SUBLOB_CD_And_PassThru - joining BI limit parts with SQ output")
    # join the BI-limit-derived dataframe to the SQ on the natural key NISS_APRM_DETL_SK
    df_joined = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_clever_gauss.alias("sq").join(
        df_EXP_BILimit_Split_trusting_euclid.select(
            "NISS_APRM_DETL_SK", "BI_LMT_1_Decimal", "BI_LMT_NO_OF_PARTS", "SRC_BI_LMT"
        ).alias("bl"), on=["NISS_APRM_DETL_SK"], how="left"
    )

    # compute v_MI_PPO_IND: DECODE(1, MI_PPO_IND = 1,'Y', MI_PPO_IND =0,'N','')
    df_stage1 = (
        df_joined
        .withColumn("v_MI_PPO_IND", when(col("MI_PPO_IND") == 1, lit('Y')).when(col("MI_PPO_IND") == 0, lit('N')).otherwise(lit('')))
    )

    # compute vv_NISS_SUBLOB_CD per the long DECODE mapping - translated to when/otherwise chain in order
    # NOTE: This follows the explicit conditions present in the source_logic in the same precedence order.
    def in_acctng_prefix(dfcol, prefixes):
        # helper expression: SUBSTR(ACCTNG_LOB,1,3) IN (...) - emulate with substr and isin
        return dfcol.substr(1, 3)

    df_stage2 = (
        df_stage1
        .withColumn("vv_NISS_SUBLOB_CD",
            when(col("ST_ABBR") == 'NH', lit('8'))
            # PA specific rules
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("ST_ABBR") == 'PA') & (col("LMT_TORT") != '1') & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('4'))
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("LMT_TORT") != '1') & (col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('4'))
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("LMT_TORT") != '1') & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('5'))
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("LMT_TORT") == '1') & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('6'))
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("LMT_TORT") == '1') & (col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('6'))
            .when((col("ST_ABBR") == 'PA') & (in_acctng_prefix(col("ACCTNG_LOB"), None) == col("ACCTNG_LOB")) & (col("LMT_TORT") == '1') & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('7'))
            # KY rules
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (col("CVG_TYP_CD") == '35028') & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0), lit('3'))
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (col("CVG_TYP_CD") == '35028') & ((col("BI_LMT") == '0') | (col("BI_LMT_NO_OF_PARTS") > 1)), lit('4'))
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (col("LMT_TORT") != '#' ) & (~col("LMT_TORT").isin('', ' ')) & (~col("LMT_TORT").isNull()) & (col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('3'))
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (col("LMT_TORT") != '#' ) & (~col("LMT_TORT").isin('', ' ')) & (~col("LMT_TORT").isNull()) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('4'))
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & ((col("LMT_TORT") == '#') | (col("LMT_TORT").isin('', ' ')) | (col("LMT_TORT").isNull())) & (col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('2'))
            .when((col("ST_ABBR") == 'KY') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & ((col("LMT_TORT") == '#') | (col("LMT_TORT").isin('', ' ')) | (col("LMT_TORT").isNull())) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('1'))
            # FL GWPC rule
            .when((col("ST_ABBR") == 'FL') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('2'))
            .when((col("ST_ABBR") == 'FL') & (col("ACCTNG_LOB").substr(1,3).isin('191','192')) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('1'))
            # 9 no fault states rules
            .when(col("ST_ABBR").isin('CT','KS','MD','MI','MN','ND','OR','UT','WA') & col("ACCTNG_LOB").substr(1,3).isin('191','192') & (col("v_MI_PPO_IND") == 'Y') & col("CVG_TYP_CD").isin('13000','13006','13007','13008'), lit('6'))
            .when(col("ST_ABBR").isin('CT','KS','MD','MI','MN','ND','OR','UT','WA') & col("ACCTNG_LOB").substr(1,3).isin('191','192') & (col("v_MI_PPO_IND") == 'Y') & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('5'))
            .when(col("ST_ABBR").isin('CT','KS','MD','MI','MN','ND','OR','UT','WA') & col("ACCTNG_LOB").substr(1,3).isin('191','192') & (col("v_MI_PPO_IND") != 'Y') & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0) & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('2'))
            .when(col("ST_ABBR").isin('CT','KS','MD','MI','MN','ND','OR','UT','WA') & col("ACCTNG_LOB").substr(1,3).isin('191','192') & (col("v_MI_PPO_IND") != 'Y') & col("CVG_TYP_CD").isin('13000','13006','13007','13008'), lit('2'))
            .when(col("ST_ABBR").isin('CT','KS','MD','MI','MN','ND','OR','UT','WA') & col("ACCTNG_LOB").substr(1,3).isin('191','192') & (col("v_MI_PPO_IND") != 'Y') & (~col("CVG_TYP_CD").isin('13000','13006','13007','13008')), lit('1'))
            # 11 states + FL rule - set to '0'
            .when(col("ST_ABBR").isin('KY','PA','CT','KS','MD','MI','MN','ND','OR','UT','WA','FL') & (col("ACCTNG_LOB").substr(1,3) == '211'), lit('0'))
            # Exceptions list mapping to '?'
            .when(col("ST_ABBR").isin('NH','KY','PA','CT','KS','MD','MI','MN','ND','OR','UT','WA'), lit('?'))
            .when(col("ST_NM").isin('DISTRICT OF COLUMBIA','DELAWARE','HAWAII'), lit('?'))
            # default for other states
            .otherwise(lit('0'))
        )
    )

    # compute v_NISS_SUBLOB_CD which applies special MI + effective-date logic
    df_stage3 = (
        df_stage2
        .withColumn("v_NISS_SUBLOB_CD",
            when(
                (col("ST_ABBR") == 'MI') & (
                    (trim(col("ACCTNG_LOB")).substr(1,3) == '191') | (trim(col("ACCTNG_LOB")) == '191')
                ) & (col("EFF_DT") >= expr("TO_DATE('07/01/2020','MM/dd/yyyy')")),
                # nested DECODE logic for MI true branch
                when(col("CVG_TYP_CD").isin('35000','35082') & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0), lit('2'))
                .when(col("CVG_TYP_CD").isin('35000','35082') & ((col("BI_LMT") == '0') | (col("BI_LMT_NO_OF_PARTS") > 1)), lit('1'))
                .when((col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0) & (~col("CVG_TYP_CD").isin('35000','35082')), lit('8'))
                .when((~col("CVG_TYP_CD").isin('35000','35082')) & ((col("BI_LMT") == '0') | (col("BI_LMT_NO_OF_PARTS") > 1)), lit('7'))
                .when(col("CVG_TYP_CD").isin('',' ') & ((col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0)), lit('?'))
                .when(col("CVG_TYP_CD").isin('',' ') & ((col("BI_LMT") == '0') | (col("BI_LMT_NO_OF_PARTS") > 1)), lit('?'))
                .otherwise(col("vv_NISS_SUBLOB_CD"))
            ).otherwise(col("vv_NISS_SUBLOB_CD"))
        )
    )

    # final NISS_SUBLOB_CD = IIF(v_NISS_SUBLOB_CD='','?',v_NISS_SUBLOB_CD)
    df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_fancy_feynman = (
        df_stage3
        .withColumn("NISS_SUBLOB_CD",
                    when((col("v_NISS_SUBLOB_CD") == '') | (col("v_NISS_SUBLOB_CD").isNull()), lit('?')).otherwise(col("v_NISS_SUBLOB_CD"))
                   )
        # preserve passthroughs explicitly
        .select(
            "NISS_APRM_DETL_SK",
            "ST_NM",
            "ST_ABBR",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "BI_LMT",
            "MI_PPO_IND",
            "LMT_TORT",
            "BI_LMT_1_Decimal",
            "BI_LMT_NO_OF_PARTS",
            "EFF_DT",
            "v_MI_PPO_IND",
            "vv_NISS_SUBLOB_CD",
            "v_NISS_SUBLOB_CD",
            "NISS_SUBLOB_CD"
        )
    )
    logger.info("Completed EXP_Derive_NISS_SUBLOB_CD_And_PassThru")
except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_SUBLOB_CD_And_PassThru: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# UPD_NISS_SUBLOB_CD (Update Strategy): mark rows and apply load-modify-store-back
# -----------------------------------------------------------------------------
try:
    logger.info("Starting Update Strategy UPD_NISS_SUBLOB_CD: derive dd_op and apply changes to target")
    # derive dd_op - source_logic says 'DD_UPDATE' so mark as UPDATE
    df_UPD_pre = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_fancy_feynman.withColumn("dd_op", lit('UPDATE'))

    # drop REJECT rows if any (none expected but follow rule)
    df_UPD_surviving = df_UPD_pre.filter(col("dd_op") != 'REJECT')

    # target path and primary key
    target_path = f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/"
    pk = "NISS_APRM_DETL_SK"

    # load existing target
    try:
        logger.info(f"Reading existing target table from {target_path} for load-modify-store-back")
        existing_target_df = spark.read.parquet(target_path)
    except Exception as e:
        logger.warning(f"Existing target read failed (may not exist): {e}. Treating as empty existing set.")
        # create an empty dataframe with the same schema as surviving rows to allow union later
        existing_target_df = spark.createDataFrame([], df_UPD_surviving.schema)

    # build keys of changed rows
    changed_keys_df = df_UPD_surviving.select(pk).distinct()

    # anti-join to remove rows being updated/deleted from existing set
    existing_anti_df = existing_target_df.join(changed_keys_df, on=pk, how='left_anti')

    # keep only INSERT/UPDATE rows to union back (here dd_op == 'UPDATE')
    to_apply_df = df_UPD_surviving.filter(col("dd_op").isin('INSERT','UPDATE'))

    # union the remaining existing rows with the applied rows
    combined_df = existing_anti_df.unionByName(to_apply_df.drop("dd_op"), allowMissingColumns=True)

    # write the combined dataframe back to the same target path (overwrite)
    try:
        logger.info(f"Writing combined target dataframe back to {target_path} (overwrite)")
        combined_df.write.mode("overwrite").parquet(target_path)
        logger.info("Update Strategy applied and target overwritten successfully")
    except Exception as e:
        logger.error(f"Failed writing combined target dataframe to {target_path}: {e}", exc_info=True)
        raise

    # assign the node output dataframe to the combined result so downstream Output node can write it again
    df_UPD_NISS_SUBLOB_CD_fancy_nash = combined_df
except Exception as e:
    logger.error(f"Failed in Update Strategy UPD_NISS_SUBLOB_CD: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 - final parquet write (overwrite)
# -----------------------------------------------------------------------------
try:
    logger.info("Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_bold_dirac = df_UPD_NISS_SUBLOB_CD_fancy_nash
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_bold_dirac.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/"
    )
    logger.info("Final target write completed")
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
