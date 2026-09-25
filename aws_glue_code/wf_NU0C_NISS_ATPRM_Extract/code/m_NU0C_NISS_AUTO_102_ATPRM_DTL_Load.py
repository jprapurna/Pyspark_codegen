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

from pyspark.sql.functions import col, lit, trim, length, substring, when, row_number, broadcast
from pyspark.sql.window import Window

# mapping / environment placeholders (replace prior to running)
RUN_TYPE = "REPLACE_WITH_RUN_TYPE_VALUE"
CLNDR_YR = "REPLACE_WITH_CLNDR_YR_VALUE"
GEO_ST_NM_BCKP = "REPLACE_WITH_GEO_ST_NM_BCKP_VALUE"
RPT_YEAR = "REPLACE_WITH_RPT_YEAR_VALUE"
BACKENDFIX_DT = "REPLACE_WITH_BACKENDFIX_DT_VALUE"
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# -----------------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL (bypassed - SQL Override reads from Snowflake)
# NOTE: This Source is bypassed by the Application Source Qualifier's SQL Override below; the ASQ reads directly from Snowflake.
# For plan/lineage purposes we will alias the ASQ result into the Source df_name so downstream nodes that reference it resolve correctly.
# -----------------------------------------------------------------------------

sql_query = f"""SELECT
    SQ.*,
    THEFT.ANTI_THFT_CTGY_CD
FROM (
    SELECT
        CASE WHEN {"'" + RUN_TYPE + "'"}='Register' THEN REG_PER_YR ELSE FISC_PER_YR END CLNDR_YR,
        NAIC_CMPNY_CD,
        NISS_CMPNY_CD,
        LTRIM(RTRIM(ST_NM)) ST_NM,
        LTRIM(RTRIM(ST_CD)) ST_CD,
        NISS_ST_CD,
        LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
        LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
        LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
        LTRIM(RTRIM(CVG_AMT)) CVG_AMT,
        LTRIM(RTRIM(BI_LMT)) BI_LMT,
        GA_ADDED_AT_FAULT_IND,
        FA2_PLCY_IND,
        UM_UMI_STACKING,
        PIP_WVR_WL_IND,
        PIP_MED_SEC_IND,
        PIP_LOSS_INCOME_IND,
        MI_PPO_IND,
        LTRIM(RTRIM(PRD_GRP_CD)) PRD_GRP_CD,
        LTRIM(RTRIM(NJ_HLTH_INSR_PRIM)) NJ_HLTH_INSR_PRIM,
        LTRIM(RTRIM(NJ_EXTR_PIP_PKG)) NJ_EXTR_PIP_PKG,
        NJ_RESDNC_RLTNSHP_PIP_IND,
        NY_SSL_IND,
        NY_FULL_CVG_GLASS_COMP_IND,
        LTRIM(RTRIM(GRGNG_ZIP_5)) GRGNG_ZIP_5,
        NISS_TERR_CD,
        LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
        LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
        LTRIM(RTRIM(RT_CLS)) RT_CLS,
        LTRIM(RTRIM(AGE)) AGE,
        LTRIM(RTRIM(GENDR)) GENDR,
        LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
        LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
        LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
        LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
        LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
        CASE WHEN NISS_ST_CD='29' AND LTRIM(RTRIM(ACCTNG_LOB)) IN ('2110T','2110F') THEN '01' ELSE LTRIM(RTRIM(SOI_TYP)) END SOI_TYP,
        PHY_DMG_IND,
        NJ_RATD_PNTS,
        VEH_MDL_YR,
        NJ_EXCPTION_CD,
        NJ_FGVN_PNTS,
        PASSV_RESTRA_DISC,
        SNR_DRVR_IND,
        DEFNS_DRVR_DISC_IND,
        ANTI_THFT_DISC,
        DAY_TM_RUN_LIGHTS,
        LMT_TORT,
        ANNL_STMNT_LOB_CD,
        CVG_TYP_IND,
        CVG_EXPS_VAL,
        ROUND(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
        NJ_NO_LWST_LMT_IND,
        NJ_NMD_DRVR_EXCL_IND,
        COALESCE(LTRIM(RTRIM(COMP_DED)),'') AS COMP_DED,
        COALESCE(LTRIM(RTRIM(COLL_DED)),'') AS COLL_DED,
        LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
        UNIT_NUM,
        EFF_DT,
        NUM_OF_CARS_IN_HH,
        COALESCE(RDRVR_DT_OF_BRTH,'0') AS RDRVR_DT_OF_BRTH,
        TERM_STRT_DT,
        COALESCE(LTRIM(RTRIM(SRC_SYS_CD)),'') AS SRC_SYS_CD,
        PNI_AGE,
        '' as LOB,
        '' as PRINCIPAL_OPRT,
        'FARMERS' AS SOURCE_IND_DERIVED
    FROM FDR.WRK_BIRP_NISS_APRM_LND

    UNION ALL

    SELECT
        SUBSTR(REG_PER_YR,1,4) AS CLNDR_YR,
        NAIC_CMPNY_CD,
        NISS_CMPNY_CD,
        LTRIM(RTRIM(ST_NM)) ST_NM,
        LTRIM(RTRIM(ST_CD)) ST_CD,
        NISS_ST_CD,
        LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
        LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
        LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
        LTRIM(RTRIM(COALESCE(CVG_AMT,''))) CVG_AMT,
        LTRIM(RTRIM(BI_LMT)) BI_LMT,
        GA_ADDED_AT_FAULT_IND,
        PLCY_IND,
        UM_UMI_STACKING,
        PIP_WVR_WL_IND,
        PIP_MED_SEC_IND,
        PIP_LOSS_INCOME_IND,
        MI_PPO_IND,
        LTRIM(RTRIM(PRD_GRP_CD)) PRD_GRP_CD,
        LTRIM(RTRIM(NJ_HLTH_INSR_PRIM)) NJ_HLTH_INSR_PRIM,
        LTRIM(RTRIM(NJ_EXTR_PIP_PKG)) NJ_EXTR_PIP_PKG,
        NJ_RESDNC_RLTNSHP_PIP_IND,
        NY_SSL_IND,
        NY_FULL_CVG_GLASS_COMP_IND,
        LTRIM(RTRIM(GRGNG_ZIP)) GRGNG_ZIP,
        NISS_TERR_CD,
        LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
        LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
        LTRIM(RTRIM(RT_CLS)) RT_CLS,
        LTRIM(RTRIM(AGE)) AGE,
        LTRIM(RTRIM(GENDR)) GENDR,
        LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
        LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
        LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
        LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
        LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
        LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
        PHY_DMG_IND,
        NJ_RATD_PNTS,
        VEH_MDL_YR,
        NJ_EXCPTION_CD,
        NJ_FGVN_PNTS,
        PASSV_RESTRA_DISC,
        SNR_DRVR_IND,
        DEFNS_DRVR_DISC_IND,
        ANTI_THFT_DISC,
        DAY_TM_RUN_LIGHTS,
        LMT_TORT,
        ANNL_STMNT_LOB_CD,
        CVG_TYP_IND,
        CVG_EXPS_VAL,
        ROUND(WRITTN_PREM_AMT) WRITTN_PREM_AMT,
        NJ_NO_LWST_LMT_IND,
        NJ_NMD_DRVR_EXCL_IND,
        COALESCE(LTRIM(RTRIM(COMP_DED)),'') AS COMP_DED,
        COALESCE(LTRIM(RTRIM(COLL_DED)),'') AS COLL_DED,
        LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
        UNIT_NUM,
        EFF_DT,
        NUM_OF_CARS_IN_HH,
        COALESCE(RDRVR_DT_OF_BRTH,'0') AS RDRVR_DT_OF_BRTH,
        TERM_STRT_DT,
        COALESCE(LTRIM(RTRIM(SRC_SYS_CD)),'') AS SRC_SYS_CD,
        PNI_AGE,
        MIS_LOB,
        PRINCIPAL_OPRT,
        SOURCE_IND_DERIVED
    FROM BIRP.WRK_BIRP_TA_NISS_NU0C_APRM_LND
) SQ
LEFT OUTER JOIN (
    SELECT DISTINCT A.ANTI_THFT_CTGY_CD AS ANTI_THFT_CTGY_CD,
                    A.PLCY_CNTRCT_NUM AS PLCY_CNTRCT_NUM,
                    A.UNIT_NUM AS UNIT_NUM,
                    A.TERM_STRT_DT AS TERM_STRT_DT
    FROM (
        SELECT
            PLCY.PLCY_CNTRCT_NUM,
            PLCY.TERM_STRT_DT,
            SOI.UNIT_NUM,
            ASOI.ANTI_THFT_CTGY_CD,
            ROW_NUMBER() OVER (PARTITION BY PLCY.PLCY_CNTRCT_NUM, PLCY.TERM_STRT_DT, SOI.UNIT_NUM ORDER BY ASOI.ANTI_THFT_CTGY_CD DESC) AS CNT1
        FROM AGDM.FACT_AG_WRITTN_PREM_CVG_LVL FACT
        JOIN AGDM.DIM_AG_PLCY PLCY ON FACT.PLCY_SK = PLCY.PLCY_SK
        JOIN AGDM.DIM_AG_TRANS_TYP_PLCY ON (FACT.TRANS_TYP_PLCY_SK = AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_SK)
        JOIN AGDM.DIM_AG_SOI SOI ON FACT.SOI_SK = SOI.SOI_SK
        JOIN AGDM.DIM_AG_FARMR_GEO_DISTR GEOD ON FACT.FARMR_GEO_DISTR_SK = GEOD.FARMR_GEO_DISTR_SK
        JOIN AGDM.DIM_AG_FARMR_GEO_ST GEOS ON GEOD.FARMR_GEO_ST_SK = GEOS.FARMR_GEO_ST_SK
        JOIN AGDM.DIM_AG_RATED_GEO RGEO ON FACT.RATED_GEO_SK = RGEO.RATED_GEO_SK
        JOIN AGDM.DIM_DT REGPER ON FACT.REGSTR_PER_SK = REGPER.DT_SK
        JOIN AGDM.DIM_DT FISCPER ON FACT.FISC_PER_SK = FISCPER.DT_SK
        LEFT OUTER JOIN FDR.FDR_AUTO_SOI ASOI ON PLCY.PLCY_ID_SK = ASOI.PLCY_ID_SK AND SOI.UNIT_NUM = ASOI.UNIT_NUM AND PLCY.TERM_STRT_DT = ASOI.EFF_DT
        WHERE 1 = 1 AND ASOI.ANTI_THFT_CTGY_CD IS NOT NULL
          AND TRIM(GEOS.ST_NM) IN ({GEO_ST_NM_BCKP})
          AND fiscper.clndr_yr = {RPT_YEAR}
          AND AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_CD NOT IN ('WO')
    ) A
    WHERE A.CNT1 = 1
) THEFT
ON TRIM(THEFT.PLCY_CNTRCT_NUM) = TRIM(SQ.PLCY_CNTRCT_NUM)
  AND THEFT.UNIT_NUM = SQ.UNIT_NUM
  AND SQ.TERM_STRT_DT = THEFT.TERM_STRT_DT
WHERE {BACKENDFIX_DT} = {CLNDR_YR}
  AND LTRIM(RTRIM(NISS_CMPNY_CD)) NOT IN ('180')
"""

try:
    logger.info("Reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from Snowflake via JDBC override query")
    df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from Snowflake: {e}", exc_info=True)
    raise

# Alias the ASQ result to the Source df_name so downstream nodes referencing the Source variable resolve correctly
try:
    df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday
    logger.info("Aliased ASQ result to df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin for lineage compatibility")
except Exception as e:
    logger.error(f"Failed creating alias for Source df df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXPTRANS: pure passthrough, reuse the ASQ dataframe directly
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXPTRANS passthrough projection")
    df_EXPTRANS_blissful_pasteur = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday
except Exception as e:
    logger.error(f"Failed EXPTRANS passthrough projection: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Lookup: LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob (instance 1)
# - load reference via JDBC (SQL override), de-duplicate to emulate 'Use First Value', then broadcast left-join
# -----------------------------------------------------------------------------
lookup_sql_1 = f"""SELECT
    REF.NISS_TERR_CD as NISS_TERR_CD,
    REF.NISS_ST_CD as NISS_ST_CD,
    REF.ST_ABBRV as ST_ABBRV,
    REF.ZIP_CD as ZIP_CD
FROM BIRP.RBI_REF_AUTO_TERR REF
WHERE REF.END_EFF_DT='2999-12-31'
  AND REF.PP_COMMRCL_CD IN ('BOTH','PP')
ORDER BY NISS_ST_CD, ST_ABBRV, ZIP_CD
"""

try:
    logger.info("Reading lookup table BIRP.RBI_REF_AUTO_TERR for LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob (instance 1) from Snowflake via JDBC")
    df_lookup_raw_1 = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", lookup_sql_1)
        .load()
    )
    # emulate 'Use First Value' by dropping duplicates on the lookup key tuple (keep first encountered)
    df_lookup_1 = df_lookup_raw_1.dropDuplicates(["NISS_ST_CD", "ST_ABBRV", "ZIP_CD"])
except Exception as e:
    logger.error(f"Failed reading lookup BIRP.RBI_REF_AUTO_TERR (instance 1): {e}", exc_info=True)
    raise

try:
    logger.info("Joining EXPTRANS with lookup (instance 1) via broadcast left join")
    # join keys: NISS_ST_CD = i_NISS_ST_CD, ST_ABBRV = i_ST_ABBRV, ZIP_CD = i_ZIP_CD
    # because both frames have the same column names, qualify left frame by aliasing before join if necessary; for brevity we perform a left join and let Spark disambiguate
    df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_amazing_hilbert = (
        df_EXPTRANS_blissful_pasteur.join(
            broadcast(df_lookup_1),
            on=["NISS_ST_CD", "ST_ABBRV", "ZIP_CD"],
            how="left"
        )
    )
except Exception as e:
    logger.error(f"Failed joining EXPTRANS with lookup (instance 1): {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru: perform focused derived calculations and constants, propagate many passthrough columns
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru complex expression logic (partial derivations + passthroughs)")
    df = df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_amazing_hilbert

    # Derived constants and literal outputs
    df = df.withColumn("NISS_PD_LOSS", lit(0))
    df = df.withColumn("NISS_PD_ALLOC_ADJUS_EXPNS", lit(0))
    df = df.withColumn("NISS_OUTSTNDG_LOSS", lit(0))
    df = df.withColumn("NISS_NO_PD_CLMS", lit(0))
    df = df.withColumn("NISS_NO_OUTSTND_CLMS", lit(0))
    df = df.withColumn("RSVD_NISS_USE", lit(' '))
    df = df.withColumn("NISS_RSVD_CMPNY_USE", lit(''))

    # mapping-audit system vars ($PM...) have no Glue equivalent -> NULLs
    df = df.withColumn("MAPPING_NAME", lit(None))
    df = df.withColumn("FOLDER_NAME", lit(None))
    df = df.withColumn("WORKFLOW_NAME", lit(None))

    # EXPS_VAL_ROLLED: IIF(SOURCE_IND_DERIVED='TOGGLE AUTO', CVG_EXPS_VAL, 0.00)
    df = df.withColumn("EXPS_VAL_ROLLED", when(col("SOURCE_IND_DERIVED") == 'TOGGLE AUTO', col("CVG_EXPS_VAL")).otherwise(lit(0.00)))

    # NISS_MNFCTRS_MDL_YR: substring of VEH_MDL_YR trimmed
    df = df.withColumn("v_STR_VEH_MDL_YR", trim(col("VEH_MDL_YR").cast("string")))
    df = df.withColumn("v_LEN_OF_VEH_MDL_YR", length(col("v_STR_VEH_MDL_YR")))
    df = df.withColumn(
        "NISS_MNFCTRS_MDL_YR",
        substring(col("v_STR_VEH_MDL_YR"), when(col("v_LEN_OF_VEH_MDL_YR") - 1 < 1, 1).otherwise(col("v_LEN_OF_VEH_MDL_YR") - 1), 2)
    )

    # o_ANTI_THFT_DISC: conditional mapping; prefer lookup-derived category where applicable
    df = df.withColumn("v_ANTI_THFT_CTGY_CD", when(col("ANTI_THFT_CTGY_CD").isNotNull(), col("ANTI_THFT_CTGY_CD").cast("string")).otherwise(col("ANTI_THFT_DISC")))
    df = df.withColumn("o_ANTI_THFT_DISC", when(col("ST_ABBR") == 'NJ', col("v_ANTI_THFT_CTGY_CD")).otherwise(col("ANTI_THFT_DISC")))

    df_EXP_PassThru_elegant_descartes = df
except Exception as e:
    logger.error(f"Failed EXP_PassThru transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Lookup: LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1 (instance 2)
# same pattern as the earlier instance
# -----------------------------------------------------------------------------
try:
    logger.info("Reading lookup table BIRP.RBI_REF_AUTO_TERR for LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1 (instance 2) from Snowflake via JDBC")
    df_lookup_raw_2 = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", lookup_sql_1)
        .load()
    )
    df_lookup_2 = df_lookup_raw_2.dropDuplicates(["NISS_ST_CD", "ST_ABBRV", "ZIP_CD"])
except Exception as e:
    logger.error(f"Failed reading lookup BIRP.RBI_REF_AUTO_TERR (instance 2): {e}", exc_info=True)
    raise

try:
    logger.info("Joining EXP_PassThru output with lookup (instance 2) via broadcast left join")
    df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_careful_einstein = (
        df_EXP_PassThru_elegant_descartes.join(
            broadcast(df_lookup_2),
            on=["NISS_ST_CD", "ST_ABBRV", "ZIP_CD"],
            how="left"
        )
    )
except Exception as e:
    logger.error(f"Failed joining EXP_PassThru with lookup (instance 2): {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Mapplet: FDR_LIB_mplt_ABC_MAPPING_AUDIT
# - Mapping-audit mapplet -> do not implement internal lookups; produce audit columns as NULLs per migration guidance
# -----------------------------------------------------------------------------
try:
    logger.info("Applying mapping-audit mapplet placeholder (producing audit columns as NULLs)")
    df_mplt_ABC_MAPPING_AUDIT_modest_kepler = (
        df_EXP_PassThru_elegant_descartes
        .withColumn("CR_BY_MAPNG_ID", lit(None).cast("long"))
        .withColumn("DW_CR_TMSP", lit(None).cast("timestamp"))
        .withColumn("UPD_BY_MAPNG_ID", lit(None).cast("long"))
        .withColumn("DW_UPD_TMSP", lit(None).cast("timestamp"))
        .withColumn("WRK_FLOW_RUN_ID", lit(None).cast("long"))
    )
except Exception as e:
    logger.error(f"Failed applying mapping-audit mapplet placeholder: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru_Tgt: final pass-through + surrogate-key generation
# - project required columns (we preserve current columns) then attach surrogate key NISS_APRM_DETL_SK via row_number() over Window.orderBy(lit(1))
# -----------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PassThru_Tgt final projection and generating surrogate key NISS_APRM_DETL_SK")

    df_base = df_mplt_ABC_MAPPING_AUDIT_modest_kepler

    # Keep existing columns; attach surrogate key via row_number() (gap-free sequence). Forces a single-partition ordering - review for large inputs.
    window_spec = Window.orderBy(lit(1))
    df_EXP_PassThru_Tgt_busy_nash = df_base.withColumn("NISS_APRM_DETL_SK", row_number().over(window_spec))

except Exception as e:
    logger.error(f"Failed EXP_PassThru_Tgt transformation (projection + SK generation): {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: write FDR_LIB_WRK_BIRP_NISS_APRM_DETL as parquet to S3 (overwrite)
# Target real name (strip 'FDR_LIB_' prefix): WRK_BIRP_NISS_APRM_DETL
# -----------------------------------------------------------------------------
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_galileo = df_EXP_PassThru_Tgt_busy_nash
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_galileo.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
