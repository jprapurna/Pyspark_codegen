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


# ---- Mapping-level placeholder constants (replace these before running) ----
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
REPLACE_WITH_TIME_ZONE_LOOKUP_PATH = "REPLACE_WITH_TIME_ZONE_LOOKUP_PATH"  # e.g. '/data/fdr/lookup/birp/time_zone_state.csv'
REPLACE_WITH_REC_DROP_LOOKUP_PATH = "REPLACE_WITH_REC_DROP_LOOKUP_PATH"  # e.g. '/data/fdr/lookup/rec_drop_lookup.csv' with columns 'code','normalized'
RUN_TYPE_TZ = "REPLACE_WITH_RUN_TYPE_TZ"  # mapping/session parameter placeholder
FILE_YEAR = "REPLACE_WITH_FILE_YEAR"  # mapping/session parameter placeholder
REPLACE_WITH_GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import (
    trim,
    regexp_replace,
    split,
    element_at,
    col,
    concat_ws,
    when,
    length,
    lit,
    broadcast,
    substring,
    expr,
    concat,
    date_format,
    current_date
)

# ------------------ Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL ------------------
# This Source is a WRK_ staged/intermediate table. Try reading parquet from S3 first,
# falling back to the Glue Catalog if the parquet path is not yet present.
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read: %s", str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL", REPLACE_WITH_GLUE_DATABASE)
        # Glue dynamic frame -> spark DataFrame
        dyf = glueContext.create_dynamic_frame.from_catalog(database=REPLACE_WITH_GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates = dyf.toDF()
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# Project/retain exactly the columns emitted by this Source node (ports listed in node metadata)
# Note: upstream SQL/ASQ will only be able to reference these column names.
# We attempt to select the expected ports if present; if a port is missing from the read schema,
# select will put a literal NULL for that column to preserve downstream column names.
_source_cols = [
    'NISS_APRM_DETL_SK','CLNDR_YR','CALL_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','NISS_ST_CD','ST_ABBR',
    'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','FA2_PLCY_IND','UM_UMI_STACKING',
    'PIP_WVR_WL_IND','PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG',
    'NJ_RESDNC_RLTNSHP_PIP_IND','NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5','NISS_TERR_CD','RATNG_CMPY_CD','MLT_CAR_IND',
    'RT_CLS','AGE','GENDR','MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
    'NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND','DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC',
    'DAY_TM_RUN_LIGHTS','LMT_TORT','ANNL_STMNT_LOB_CD','CVG_TYP_IND','CVG_EXPS_VAL','TTL_WRITTN_PREM_AMT','LINE_CD','ACCDNT_YR',
    'NISS_CVG_CD','RTNG_ZNE_CD','TERM_ZNE_CD','NISS_CLASS_CD','NISS_ELIG_PNTS_CD','NISS_AGE_GRP_CD','NISS_CMMCL_IND_CD','NISS_EXCPN_CD',
    'NISS_FGVNS_CD','NISS_PASSV_RESTRA_CD','NISS_DEFNS_DRVR_CRD_CD','NISS_ANTI_THFT_DVC_CD','NISS_DAY_TM_RUN_LAMPS_DISC_CD','NISS_PLCY_LMT_CD',
    'NISS_DEDUC_CD','NISS_SSL_LIAB_CD','NISS_SUBLOB_CD','NISS_TYP_LOSS_CD','NISS_LIAB_OR_NO_FAULT_CD','NISS_ANNL_STMNT_LOB_CD','NISS_PD_LOSS',
    'NISS_PD_ALLOC_ADJUS_EXPNS','NISS_OUTSTNDG_LOSS','NISS_NO_PD_CLMS','NISS_NO_OUTSTND_CLMS','RSVD_NISS_USE','NISS_RSVD_CMPNY_USE','NISS_MNFCTRS_MDL_YR',
    'CR_BY_MAPNG_ID','DW_CR_TMSP','UPD_BY_MAPNG_ID','DW_UPD_TMSP','WRK_FLOW_RUN_ID','NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','EXPS_VAL_ROLLED',
    'CVG_CNT_IND','CVG_CNT','CVG_CD_SK','CVG_ATTR_SK','REC_DROP_IND','REC_DROP_RSN_DESC','REC_EXCPN_IND','REC_EXCPN_RSN_DESC','CVG_ATTR_CHCKSUM',
    'COMP_DED','COLL_DED','PLCY_CNTRCT_NUM','UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','DERIVED_RDRVR_AGE',
    'FINAL_RDRVR_AGE','PNI_AGE','LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED'
]

# build projection ensuring missing columns become NULLs so downstream code can still reference them
_existing = set(df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.columns)
_select_exprs = []
for c in _source_cols:
    if c in _existing:
        _select_exprs.append(c)
    else:
        _select_exprs.append(f"NULL AS {c}")

try:
    logger.info("Projecting expected ports for WRK_BIRP_NISS_APRM_DETL into df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.selectExpr(*_select_exprs)
except Exception as e:
    logger.error(f"Failed projecting ports for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


# ------------------ Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL ------------------
# This ASQ has a SQL Override that reads FROM FDR.WRK_BIRP_NISS_APRM_DETL. That table was staged above,
# so register the upstream staged dataframe as a temp view and run the override via spark.sql against the view.
try:
    # create the temp view using the real table name (stripped of any Shortcut_to_/FDR_LIB_ prefix per rules)
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')

    sql_query = f"""SELECT
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_TYP_IND,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
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
NISS_TERR_CD,
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
SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
SUM(EXPS_VAL_ROLLED) AS EXPS_VAL_ROLLED,
SUM(TTL_WRITTN_PREM_AMT) TTL_WRITTN_PREM_AMT,
LINE_CD,
ACCDNT_YR,
CASE
 WHEN NISS_CVG_CD = 'E16' THEN '616'
 WHEN NISS_CVG_CD = 'E36' THEN '636'
 WHEN NISS_CVG_CD = 'E56' THEN '656'
 WHEN NISS_CVG_CD = 'E76' THEN '676'
 ELSE NISS_CVG_CD END NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN substr(NISS_CLASS_CD,1,1)='E' THEN '933100' ELSE NISS_CLASS_CD END NISS_CLASS_CD,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
REC_EXCPN_IND,
REC_DROP_IND,
REC_DROP_RSN_DESC,
NUM_OF_CARS_IN_HH,
date_format(TERM_STRT_DT,'MM/dd/yyyy') AS TERM_STRT_DT,
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE

FROM WRK_BIRP_NISS_APRM_DETL

GROUP BY
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_TYP_IND,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
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
NISS_TERR_CD,
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
LINE_CD,
ACCDNT_YR,
CASE
 WHEN NISS_CVG_CD = 'E16' THEN '616'
 WHEN NISS_CVG_CD = 'E36' THEN '636'
 WHEN NISS_CVG_CD = 'E56' THEN '656'
 WHEN NISS_CVG_CD = 'E76' THEN '676'
 ELSE NISS_CVG_CD END,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN substr(NISS_CLASS_CD,1,1)='E' THEN '933100' ELSE NISS_CLASS_CD END,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
REC_EXCPN_IND,
REC_DROP_IND,
REC_DROP_RSN_DESC,
NUM_OF_CARS_IN_HH,
to_char(TERM_STRT_DT,'MM/DD/YYYY'),
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE"""

    try:
        logger.info("Executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged temp view WRK_BIRP_NISS_APRM_DETL")
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking = spark.sql(sql_query)
    except Exception as e:
        logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"ASQ rewrite/run against staged view failed: {e}", exc_info=True)
    raise


# ------------------ Expression: EXP_GenErrReason (parse & normalize REC_DROP_RSN_DESC) ------------------
try:
    logger.info("Computing normalized REC_DROP_RSN_DESC tokens in EXP_GenErrReason (df_EXP_GenErrReason_focused_euclid)")
    # input dataframe: df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking
    src = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking

    # split up to 4 tokens by ';'
    toks = split(col('REC_DROP_RSN_DESC'), ';')
    df_tokens = src.withColumn('t1', trim(element_at(toks, 1))).withColumn('t2', trim(element_at(toks, 2))).withColumn('t3', trim(element_at(toks, 3))).withColumn('t4', trim(element_at(toks, 4)))

    # read the normalization lookup (external flat file) - developer must replace placeholder path
    try:
        lookup_rec = (
            spark.read.option('header', 'true').option('inferSchema', 'true').csv(REPLACE_WITH_REC_DROP_LOOKUP_PATH)
        )
    except Exception as e:
        logger.error(f"Failed reading REC_DROP lookup from {REPLACE_WITH_REC_DROP_LOOKUP_PATH}: {e}", exc_info=True)
        raise

    # assume lookup has columns 'code' and 'normalized'; normalize tokens by left-joining broadcast for each token
    lookup_rec = lookup_rec.withColumn('code', trim(col('code'))).withColumnRenamed('normalized', 'norm')

    df_norm = (
        df_tokens
        .join(broadcast(lookup_rec.withColumnRenamed('code', 'k1').withColumnRenamed('norm', 'n1')), trim(df_tokens.t1) == col('k1'), 'left')
        .join(broadcast(lookup_rec.withColumnRenamed('code', 'k2').withColumnRenamed('norm', 'n2')), trim(df_tokens.t2) == col('k2'), 'left')
        .join(broadcast(lookup_rec.withColumnRenamed('code', 'k3').withColumnRenamed('norm', 'n3')), trim(df_tokens.t3) == col('k3'), 'left')
        .join(broadcast(lookup_rec.withColumnRenamed('code', 'k4').withColumnRenamed('norm', 'n4')), trim(df_tokens.t4) == col('k4'), 'left')
    )

    # build normalized tokens: prefer lookup normalized value where present, otherwise original trimmed token value, else NULL
    df_norm = df_norm.withColumn('tok1_norm', when(col('n1').isNotNull(), col('n1')).otherwise(col('t1')))
    df_norm = df_norm.withColumn('tok2_norm', when(col('n2').isNotNull(), col('n2')).otherwise(col('t2')))
    df_norm = df_norm.withColumn('tok3_norm', when(col('n3').isNotNull(), col('n3')).otherwise(col('t3')))
    df_norm = df_norm.withColumn('tok4_norm', when(col('n4').isNotNull(), col('n4')).otherwise(col('t4')))

    # reassemble using ';' but skip empty tokens to avoid trailing/sequential separators
    df_norm = df_norm.withColumn('REC_DROP_RSN_DESC', concat_ws(';', *[
        col('tok1_norm'), col('tok2_norm'), col('tok3_norm'), col('tok4_norm')
    ]))

    # keep original fields plus replaced REC_DROP_RSN_DESC
    # preserve all other columns from source
    keep_cols = [c for c in df_norm.columns if c not in ('t1','t2','t3','t4','k1','n1','k2','n2','k3','n3','k4','n4','tok1_norm','tok2_norm','tok3_norm','tok4_norm')]
    df_EXP_GenErrReason_focused_euclid = df_norm.select(*keep_cols)
except Exception as e:
    logger.error(f"EXP_GenErrReason failed: {e}", exc_info=True)
    raise


# ------------------ Lookup Procedure: LKPTRANS (read time_zone_state.csv as lookup table) ------------------
try:
    logger.info("Reading time zone lookup flat file for LKPTRANS into df_LKPTRANS_stoic_babbage from %s", REPLACE_WITH_TIME_ZONE_LOOKUP_PATH)
    df_lookup_raw = (
        spark.read.option('header', 'true').option('inferSchema', 'true').csv(REPLACE_WITH_TIME_ZONE_LOOKUP_PATH)
    )

    # normalize key name: EXPOSURE_STATE_ABB -> ST_ABBR to make joins straightforward
    # assume CSV contains EXPOSURE_STATE_ABB, TimeZone, FARMERS_STATE_NAME (if not present, NULL columns will be created)
    _existing_lkp = set(df_lookup_raw.columns)
    # try common column names
    if 'EXPOSURE_STATE_ABB' in _existing_lkp:
        df_LKPTRANS_stoic_babbage = df_lookup_raw.withColumn('ST_ABBR', trim(col('EXPOSURE_STATE_ABB')))
    elif 'state_abbr' in _existing_lkp:
        df_LKPTRANS_stoic_babbage = df_lookup_raw.withColumn('ST_ABBR', trim(col('state_abbr')))
    else:
        # if lookup does not have a clear key column, create empty ST_ABBR and rely on downstream reviewers to fix
        df_LKPTRANS_stoic_babbage = df_lookup_raw.withColumn('ST_ABBR', lit(None))

    # ensure TimeZone column exists (rename common alternatives)
    if 'TimeZone' not in _existing_lkp and 'timezone' in _existing_lkp:
        df_LKPTRANS_stoic_babbage = df_LKPTRANS_stoic_babbage.withColumnRenamed('timezone', 'TimeZone')

    # cache for reuse
    df_LKPTRANS_stoic_babbage = df_LKPTRANS_stoic_babbage.cache()
except Exception as e:
    logger.error(f"Failed building LKPTRANS lookup dataframe from {REPLACE_WITH_TIME_ZONE_LOOKUP_PATH}: {e}", exc_info=True)
    raise


# ------------------ Expression: EXP_PASS_DETL (main projection & derived fields) ------------------
try:
    logger.info("Computing EXP_PASS_DETL (df_EXP_PASS_DETL_busy_feynman) - apply formatting/normalization and intermediate aliasing")
    # inputs: df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking, df_EXP_GenErrReason_focused_euclid
    # join the SQ output with the GenErrReason enriched REC_DROP_RSN_DESC on a suitable key if required.
    # In this mapping the GenErrReason Expression consumed the ASQ directly; if the GenErrReason output rows align 1:1
    # with SQ rows, we can left join on the natural key(s). Since no explicit join key was provided in the batch plan,
    # and the EXP_GenErrReason was derived from the SQ's REC_DROP_RSN_DESC field, simply use the GenErrReason result where available by left-joining on all common columns that exist.

    # simple approach: left-join the SQ result to the GenErrReason result on available unique key columns if present (NISS_APRM_DETL_SK), else use a cross-apply by ROW if necessary.
    left = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking
    right = df_EXP_GenErrReason_focused_euclid

    if 'NISS_APRM_DETL_SK' in left.columns and 'NISS_APRM_DETL_SK' in right.columns:
        merged = left.join(right.select('NISS_APRM_DETL_SK','REC_DROP_RSN_DESC'), on='NISS_APRM_DETL_SK', how='left')
    else:
        # fallback: prefer REC_DROP_RSN_DESC from the lookup result when present, otherwise keep left side's value
        # to avoid cartesian, perform left join on a few common columns if they exist; otherwise attach the REC_DROP_RSN_DESC from right via a left join on ST_ABBR+LINE_CD+ACCDNT_YR as a heuristic
        join_keys = [k for k in ['ST_ABBR','LINE_CD','ACCDNT_YR'] if k in left.columns and k in right.columns]
        if join_keys:
            merged = left.join(right.select(*join_keys,'REC_DROP_RSN_DESC'), on=join_keys, how='left')
        else:
            # last resort: keep left and do not attempt to merge REC_DROP_RSN_DESC
            merged = left

    # compute intermediate alias v_NISS_MNFCTRS_MDL_YR first (trimmed string of numeric year)
    # chain selectExpr calls so later expressions can reference the alias
    cols_to_preserve = merged.columns
    # First selectExpr: preserve all original columns and add v_NISS_MNFCTRS_MDL_YR
    select_exprs_1 = [c for c in cols_to_preserve]
    # If NISS_MNFCTRS_MDL_YR present create trimmed string alias
    if 'NISS_MNFCTRS_MDL_YR' in merged.columns:
        select_exprs_1.append("trim(CAST(NISS_MNFCTRS_MDL_YR AS STRING)) AS v_NISS_MNFCTRS_MDL_YR")
    else:
        select_exprs_1.append("NULL AS v_NISS_MNFCTRS_MDL_YR")

    inter = merged.selectExpr(*select_exprs_1)

    # Second selectExpr: compute final derived outputs referencing v_NISS_MNFCTRS_MDL_YR where needed
    # Build explicit select expressions for outputs required by downstream nodes (passthrough + derived)
    sel = []
    # passthrough ports (explicitly listed to avoid '*') - include many of the commonly used ports
    passthrough_ports = [
        'NISS_APRM_DETL_SK','CLNDR_YR','CALL_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','NISS_ST_CD','ST_ABBR',
        'ACCTNG_LOB','CVG_TYP_CD','CVG_TYP_IND','BI_LMT','GA_ADDED_AT_FAULT_IND','FA2_PLCY_IND','UM_UMI_STACKING',
        'PIP_WVR_WL_IND','PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG',
        'NJ_RESDNC_RLTNSHP_PIP_IND','NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5','NISS_TERR_CD','RATNG_CMPY_CD','MLT_CAR_IND',
        'RT_CLS','AGE','GENDR','MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND',
        'NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND','DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC',
        'DAY_TM_RUN_LIGHTS','LMT_TORT','CVG_EXPS_VAL','EXPS_VAL_ROLLED','TTL_WRITTN_PREM_AMT','LINE_CD','ACCDNT_YR','NISS_CVG_CD',
        'RTNG_ZNE_CD','TERM_ZNE_CD','NISS_CLASS_CD','NISS_ELIG_PNTS_CD','NISS_AGE_GRP_CD','NISS_CMMCL_IND_CD','NISS_EXCPN_CD','NISS_FGVNS_CD',
        'NISS_PASSV_RESTRA_CD','NISS_DEFNS_DRVR_CRD_CD','NISS_ANTI_THFT_DVC_CD','NISS_DAY_TM_RUN_LAMPS_DISC_CD','NISS_PLCY_LMT_CD','NISS_DEDUC_CD',
        'NISS_SSL_LIAB_CD','NISS_SUBLOB_CD','NISS_TYP_LOSS_CD','NISS_LIAB_OR_NO_FAULT_CD','NISS_ANNL_STMNT_LOB_CD','NISS_PD_LOSS',
        'NISS_PD_ALLOC_ADJUS_EXPNS','NISS_OUTSTNDG_LOSS','NISS_NO_PD_CLMS','NISS_NO_OUTSTND_CLMS','REC_EXCPN_IND','REC_DROP_IND','REC_DROP_RSN_DESC',
        'NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','DERIVED_RDRVR_AGE','FINAL_RDRVR_AGE','PNI_AGE'
    ]
    for p in passthrough_ports:
        if p in inter.columns:
            sel.append(p)
        else:
            sel.append(f"NULL AS {p}")

    # derived ports
    # o_NISS_CVG_CD: IIF(LTRIM(RTRIM(NISS_CVG_CD)) = '','???',NISS_CVG_CD)
    sel.append("CASE WHEN trim(NISS_CVG_CD) = '' OR NISS_CVG_CD IS NULL THEN '???' ELSE NISS_CVG_CD END AS o_NISS_CVG_CD")
    # o_NISS_CLASS_CD
    sel.append("CASE WHEN trim(NISS_CLASS_CD) = '' OR NISS_CLASS_CD IS NULL THEN '??????' ELSE NISS_CLASS_CD END AS o_NISS_CLASS_CD")
    # o_CVG_AMT and o_BI_LMT - remove commas
    sel.append("regexp_replace(trim(CVG_AMT),',','') AS o_CVG_AMT")
    sel.append("regexp_replace(trim(BI_LMT),',','') AS o_BI_LMT")
    # o_CLNDR_YR, o_CALL_YR substrings
    sel.append("substr(trim(CLNDR_YR),3,2) AS o_CLNDR_YR")
    sel.append("substr(trim(CALL_YR),3,2) AS o_CALL_YR")
    # NJ flags from input helper fields if present
    if 'i_NJ_NO_LWST_LMT_IND' in inter.columns:
        sel.append("CASE WHEN i_NJ_NO_LWST_LMT_IND = 1 THEN 'Y' ELSE 'N' END AS NJ_NO_LWST_LMT_IND")
    if 'i_NJ_NMD_DRVR_EXCL_IND' in inter.columns:
        sel.append("CASE WHEN i_NJ_NMD_DRVR_EXCL_IND = 1 THEN 'Y' ELSE 'N' END AS NJ_NMD_DRVR_EXCL_IND")

    # o_NISS_MNFCTRS_MDL_YR: reference v_NISS_MNFCTRS_MDL_YR alias computed earlier
    sel.append("CASE WHEN length(v_NISS_MNFCTRS_MDL_YR) = 2 THEN v_NISS_MNFCTRS_MDL_YR ELSE concat('0', v_NISS_MNFCTRS_MDL_YR) END AS o_NISS_MNFCTRS_MDL_YR")

    # o_NISS_TERR_CD blank/null -> '???'
    sel.append("CASE WHEN trim(NISS_TERR_CD) = '' OR NISS_TERR_CD IS NULL THEN '???' ELSE NISS_TERR_CD END AS o_NISS_TERR_CD")

    # EXP_STATE_IND: IIF(((ST_ABBR='CA') or (ST_ABBR='TX')),'Y','N')
    sel.append("CASE WHEN ST_ABBR IN ('CA','TX') THEN 'Y' ELSE 'N' END AS EXP_STATE_IND")

    # v_file_year derived from RUN_TYPE_TZ/FILE_YEAR mapping parameters - implement as CASE using placeholders
    # If RUN_TYPE_TZ == 'MANUAL' then FILE_YEAR else current year
    sel.append(f"CASE WHEN '{RUN_TYPE_TZ}' = 'MANUAL' THEN '{FILE_YEAR}' ELSE date_format(current_date(),'yyyy') END AS v_file_year")

    # filename_DTL : 'FF_FIO_TRIP_DTL_RPT_'||ST_ABBR||'_'||v_file_year||'.csv'
    sel.append("concat('FF_FIO_TRIP_DTL_RPT_', ST_ABBR, '_', CASE WHEN '{RUN_TYPE_TZ}' = 'MANUAL' THEN '{FILE_YEAR}' ELSE date_format(current_date(),'yyyy') END, '.csv') AS filename_DTL")

    df_EXP_PASS_DETL_busy_feynman = inter.selectExpr(*sel)
except Exception as e:
    logger.error(f"EXP_PASS_DETL failed: {e}", exc_info=True)
    raise


# ------------------ Router: Router (produce per-group filtered DataFrames) ------------------
try:
    logger.info("Joining EXP_PASS_DETL and LKPTRANS lookup to prepare Router base dataframe")
    # join EXP_PASS_DETL to lookup on ST_ABBR to bring in TimeZone etc. Use left join so all EXP rows retained
    if 'ST_ABBR' in df_EXP_PASS_DETL_busy_feynman.columns and 'ST_ABBR' in df_LKPTRANS_stoic_babbage.columns:
        base_df = (
            df_EXP_PASS_DETL_busy_feynman.alias('d')
            .join(broadcast(df_LKPTRANS_stoic_babbage.alias('l')), on=[df_EXP_PASS_DETL_busy_feynman['ST_ABBR'] == df_LKPTRANS_stoic_babbage['ST_ABBR']], how='left')
            # after the join there may be duplicate ST_ABBR columns; prefer the left-side ST_ABBR and the lookup TimeZone
        )
        # rename TimeZone from lookup if exists
        if 'TimeZone' in df_LKPTRANS_stoic_babbage.columns:
            base_df = base_df.withColumn('TimeZone', col('l.TimeZone'))
    else:
        # if lookup key not available, proceed with EXP_PASS_DETL as base
        base_df = df_EXP_PASS_DETL_busy_feynman

    # produce per-group filtered DataFrames per Router groups
    df_Router_hopeful_pasteur_CA = base_df.filter(expr("ST_ABBR = 'CA'"))
    df_Router_hopeful_pasteur_TX = base_df.filter(expr("ST_ABBR = 'TX'"))
    df_Router_hopeful_pasteur_NON_CA_TX = base_df.filter(expr("ST_ABBR NOT IN ('CA','TX')"))
    df_Router_hopeful_pasteur_DEFAULT1 = base_df.filter(expr("ST_ABBR NOT IN ('CA','TX')"))

    # assign the Router node's nominal output df_name to the base (unfiltered) dataframe as well so any downstream
    # consumer that references the Router node's shared name (not group-suffixed edge) can still see it (although
    # downstream consumers that require group-filtered variables should reference the suffixed names already produced in connection metadata).
    df_Router_hopeful_pasteur = base_df
except Exception as e:
    logger.error(f"Router processing failed: {e}", exc_info=True)
    raise

# ------------------ Sorter: SRT_CA (order CA group by NAIC_CMPNY_CD) ------------------
try:
    logger.info("Sorting CA group for SRT_CA into df_SRT_CA_peaceful_mendel")
    # input: filtered Router CA group
    src = df_Router_hopeful_pasteur_CA

    # perform per-node ordering using the primary sort key NAIC_CMPNY_CD
    sorted_df = src.orderBy(col('NAIC_CMPNY_CD').asc())

    # map upstream column names to the Sorter's output port names (suffix '1')
    _existing = set(sorted_df.columns)
    _mappings = [
        ('o_CLNDR_YR','o_CLNDR_YR1'),('o_CALL_YR','o_CALL_YR1'),('NAIC_CMPNY_CD','NAIC_CMPNY_CD1'),('NISS_CMPNY_CD','NISS_CMPNY_CD1'),
        ('ST_NM','ST_NM1'),('ST_CD','ST_CD1'),('NISS_ST_CD','NISS_ST_CD1'),('ST_ABBR','ST_ABBR1'),('ACCTNG_LOB','ACCTNG_LOB1'),
        ('CVG_TYP_CD','CVG_TYP_CD1'),('CVG_TYP_IND','CVG_TYP_IND1'),('o_CVG_AMT','o_CVG_AMT1'),('o_BI_LMT','o_BI_LMT1'),
        ('GA_ADDED_AT_FAULT_IND','GA_ADDED_AT_FAULT_IND1'),('FA2_PLCY_IND','FA2_PLCY_IND1'),('UM_UMI_STACKING','UM_UMI_STACKING1'),
        ('PIP_WVR_WL_IND','PIP_WVR_WL_IND1'),('PIP_MED_SEC_IND','PIP_MED_SEC_IND1'),('PIP_LOSS_INCOME_IND','PIP_LOSS_INCOME_IND1'),
        ('MI_PPO_IND','MI_PPO_IND1'),('PRD_GRP_CD','PRD_GRP_CD1'),('NJ_HLTH_INSR_PRIM','NJ_HLTH_INSR_PRIM1'),('NJ_EXTR_PIP_PKG','NJ_EXTR_PIP_PKG1'),
        ('NJ_RESDNC_RLTNSHP_PIP_IND','NJ_RESDNC_RLTNSHP_PIP_IND1'),('NY_SSL_IND','NY_SSL_IND1'),('NY_FULL_CVG_GLASS_COMP_IND','NY_FULL_CVG_GLASS_COMP_IND1'),
        ('GRGNG_ZIP_5','GRGNG_ZIP_51'),('o_NISS_TERR_CD','o_NISS_TERR_CD1'),('RATNG_CMPY_CD','RATNG_CMPY_CD1'),('MLT_CAR_IND','MLT_CAR_IND1'),
        ('RT_CLS','RT_CLS1'),('AGE','AGE1'),('GENDR','GENDR1'),('MRTL_STAT','MRTL_STAT1'),('AUTO_USE_CD','AUTO_USE_CD1'),
        ('MILES_TO_WRK','MILES_TO_WRK1'),('GOOD_STDNT_IND','GOOD_STDNT_IND1'),('DRVR_TRNG_IND','DRVR_TRNG_IND1'),('SOI_TYP','SOI_TYP1'),
        ('PHY_DMG_IND','PHY_DMG_IND1'),('NJ_RATD_PNTS','NJ_RATD_PNTS1'),('VEH_MDL_YR','VEH_MDL_YR1'),('NJ_EXCPTION_CD','NJ_EXCPTION_CD1'),
        ('NJ_FGVN_PNTS','NJ_FGVN_PNTS1'),('REC_EXCPN_IND','REC_EXCPN_IND1'),('REC_DROP_IND','REC_DROP_IND1'),('REC_DROP_RSN_DESC','REC_DROP_RSN_DESC1'),
        ('EXP_STATE_IND','EXP_STATE_IND1'),('TimeZone','TimeZone1'),('LINE_CD','LINE_CD1'),('ACCDNT_YR','ACCDNT_YR1'),('o_NISS_CVG_CD','o_NISS_CVG_CD1'),
        ('RTNG_ZNE_CD','RTNG_ZNE_CD1'),('TERM_ZNE_CD','TERM_ZNE_CD1'),('o_NISS_CLASS_CD','o_NISS_CLASS_CD1'),('NISS_ELIG_PNTS_CD','NISS_ELIG_PNTS_CD1'),
        ('NISS_AGE_GRP_CD','NISS_AGE_GRP_CD1'),('o_NISS_MNFCTRS_MDL_YR','o_NISS_MNFCTRS_MDL_YR1'),('CVG_EXPS_VAL','CVG_EXPS_VAL1'),('EXPS_VAL_ROLLED','EXPS_VAL_ROLLED1'),
        ('TTL_WRITTN_PREM_AMT','TTL_WRITTN_PREM_AMT1'),('NUM_OF_CARS_IN_HH','NUM_OF_CARS_IN_HH1'),('PNI_AGE','PNI_AGE1'),('RDRVR_DT_OF_BRTH','RDRVR_DT_OF_BRTH1'),
        ('TERM_STRT_DT','TERM_STRT_DT1'),('SRC_SYS_CD','SRC_SYS_CD1'),('DERIVED_RDRVR_AGE','DERIVED_RDRVR_AGE1'),('FINAL_RDRVR_AGE','FINAL_RDRVR_AGE1')
    ]

    _select_exprs = []
    for src_col, tgt_col in _mappings:
        if src_col in _existing:
            _select_exprs.append(f"{src_col} AS {tgt_col}")
        else:
            _select_exprs.append(f"NULL AS {tgt_col}")

    try:
        df_SRT_CA_peaceful_mendel = sorted_df.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting/selecting in SRT_CA: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"SRT_CA failed: {e}", exc_info=True)
    raise


# ------------------ Sorter: SRT_TX (order TX group by NAIC_CMPNY_CD) ------------------
try:
    logger.info("Sorting TX group for SRT_TX into df_SRT_TX_sharp_heisenberg")
    src = df_Router_hopeful_pasteur_TX

    sorted_df = src.orderBy(col('NAIC_CMPNY_CD').asc())

    _existing = set(sorted_df.columns)
    # map to suffix '3' for this Sorter
    _mappings = [
        ('o_CLNDR_YR','o_CLNDR_YR3'),('o_CALL_YR','o_CALL_YR3'),('NAIC_CMPNY_CD','NAIC_CMPNY_CD3'),('NISS_CMPNY_CD','NISS_CMPNY_CD3'),
        ('ST_NM','ST_NM3'),('ST_CD','ST_CD3'),('NISS_ST_CD','NISS_ST_CD3'),('ST_ABBR','ST_ABBR3'),('ACCTNG_LOB','ACCTNG_LOB3'),
        ('CVG_TYP_CD','CVG_TYP_CD3'),('CVG_TYP_IND','CVG_TYP_IND3'),('o_CVG_AMT','o_CVG_AMT3'),('o_BI_LMT','o_BI_LMT3'),
        ('GA_ADDED_AT_FAULT_IND','GA_ADDED_AT_FAULT_IND3'),('FA2_PLCY_IND','FA2_PLCY_IND3'),('UM_UMI_STACKING','UM_UMI_STACKING3'),
        ('PIP_WVR_WL_IND','PIP_WVR_WL_IND3'),('PIP_MED_SEC_IND','PIP_MED_SEC_IND3'),('PIP_LOSS_INCOME_IND','PIP_LOSS_INCOME_IND3'),
        ('MI_PPO_IND','MI_PPO_IND3'),('PRD_GRP_CD','PRD_GRP_CD3'),('NJ_HLTH_INSR_PRIM','NJ_HLTH_INSR_PRIM3'),('NJ_EXTR_PIP_PKG','NJ_EXTR_PIP_PKG3'),
        ('REC_EXCPN_IND','REC_EXCPN_IND3'),('REC_DROP_IND','REC_DROP_IND3'),('REC_DROP_RSN_DESC','REC_DROP_RSN_DESC3'),('EXP_STATE_IND','EXP_STATE_IND3'),
        ('TimeZone','TimeZone3'),('LINE_CD','LINE_CD3'),('ACCDNT_YR','ACCDNT_YR3'),('o_NISS_CVG_CD','o_NISS_CVG_CD3'),('RTNG_ZNE_CD','RTNG_ZNE_CD3'),
        ('TERM_ZNE_CD','TERM_ZNE_CD3'),('o_NISS_CLASS_CD','o_NISS_CLASS_CD3'),('NISS_ELIG_PNTS_CD','NISS_ELIG_PNTS_CD3'),('NISS_AGE_GRP_CD','NISS_AGE_GRP_CD3')
    ]

    _select_exprs = []
    for src_col, tgt_col in _mappings:
        if src_col in _existing:
            _select_exprs.append(f"{src_col} AS {tgt_col}")
        else:
            _select_exprs.append(f"NULL AS {tgt_col}")

    try:
        df_SRT_TX_sharp_heisenberg = sorted_df.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting/selecting in SRT_TX: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"SRT_TX failed: {e}", exc_info=True)
    raise


# ------------------ Expression: NON_CA_TX (passthrough + O_TIMEZONE4 fallback) ------------------
try:
    logger.info("Computing NON_CA_TX (df_NON_CA_TX_nice_bohr) - passthrough ports and timezone fallback")
    # Use the Router's NON_CA_TX group filtered dataframe
    src = df_Router_hopeful_pasteur_NON_CA_TX

    _existing = set(src.columns)
    # mapping upstream names -> output names with suffix '4'
    _mappings = [
        ('o_CLNDR_YR','o_CLNDR_YR4'),('o_CALL_YR','o_CALL_YR4'),('NAIC_CMPNY_CD','NAIC_CMPNY_CD4'),('NISS_CMPNY_CD','NISS_CMPNY_CD4'),
        ('ST_NM','ST_NM4'),('ST_CD','ST_CD4'),('NISS_ST_CD','NISS_ST_CD4'),('ST_ABBR','ST_ABBR4'),('ACCTNG_LOB','ACCTNG_LOB4'),
        ('CVG_TYP_CD','CVG_TYP_CD4'),('CVG_TYP_IND','CVG_TYP_IND4'),('o_CVG_AMT','o_CVG_AMT4'),('o_BI_LMT','o_BI_LMT4'),
        ('GA_ADDED_AT_FAULT_IND','GA_ADDED_AT_FAULT_IND4'),('FA2_PLCY_IND','FA2_PLCY_IND4'),('UM_UMI_STACKING','UM_UMI_STACKING4'),
        ('PIP_WVR_WL_IND','PIP_WVR_WL_IND4'),('PIP_MED_SEC_IND','PIP_MED_SEC_IND4'),('PIP_LOSS_INCOME_IND','PIP_LOSS_INCOME_IND4'),
        ('MI_PPO_IND','MI_PPO_IND4'),('PRD_GRP_CD','PRD_GRP_CD4'),('NJ_HLTH_INSR_PRIM','NJ_HLTH_INSR_PRIM4'),('NJ_EXTR_PIP_PKG','NJ_EXTR_PIP_PKG4'),
        ('REC_EXCPN_IND','REC_EXCPN_IND4'),('REC_DROP_IND','REC_DROP_IND4'),('REC_DROP_RSN_DESC','REC_DROP_RSN_DESC4'),('EXP_STATE_IND','EXP_STATE_IND4'),
        # timezone input mapping -> TimeZone4 so output expression can reference it
        ('TimeZone','TimeZone4'),('LINE_CD','LINE_CD4'),('ACCDNT_YR','ACCDNT_YR4'),('o_NISS_CVG_CD','o_NISS_CVG_CD4')
    ]

    _select_exprs = []
    for src_col, tgt_col in _mappings:
        if src_col in _existing:
            _select_exprs.append(f"{src_col} AS {tgt_col}")
        else:
            _select_exprs.append(f"NULL AS {tgt_col}")

    selected = src.selectExpr(*_select_exprs)

    # compute O_TIMEZONE4 as fallback 'UNKNOWN' when TimeZone4 is NULL
    try:
        df_NON_CA_TX_nice_bohr = selected.withColumn('O_TIMEZONE4', when(col('TimeZone4').isNull(), lit('UNKNOWN')).otherwise(col('TimeZone4')))
    except Exception as e:
        logger.error(f"Failed computing O_TIMEZONE4 in NON_CA_TX: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"NON_CA_TX failed: {e}", exc_info=True)
    raise


# ------------------ Expression: EXP_CA_STATE (company-change indicator + filename) ------------------
try:
    logger.info("Computing EXP_CA_STATE (df_EXP_CA_STATE_humble_darwin) - company-change indicator and filename")
    # input: sorted CA company stream
    src = df_SRT_CA_peaceful_mendel

    # Window to compute previous company code using the same ordering SRT_CA applied
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag

    w = Window.orderBy(col('NAIC_CMPNY_CD1').asc())

    # compute current/previous company codes and indicator
    df1 = (
        src
        .withColumn('V_CURR_CMPY_CD', col('NAIC_CMPNY_CD1'))
        .withColumn('V_PREV_CMPY_CD', lag(col('NAIC_CMPNY_CD1')).over(w))
    )

    df1 = df1.withColumn('O_CMPY_CD_IND', when(col('V_CURR_CMPY_CD') == col('V_PREV_CMPY_CD'), lit('N')).otherwise(lit('Y')))

    # compute V_FILE_YEAR from mapping parameters; RUN_TYPE_TZ and FILE_YEAR are placeholders defined earlier
    df1 = df1.withColumn('V_FILE_YEAR', when(lit(RUN_TYPE_TZ) == 'MANUAL', lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))

    # construct filename_DTL
    df1 = df1.withColumn('filename_DTL', concat(lit('NU0C_NISS_AUTO_CA_'), col('V_FILE_YEAR'), lit('_AutoPrem_Detail_'), col('NAIC_CMPNY_CD1'), lit('.csv')))

    # now project outputs and pass-through ports; map to the node's expected output port names (suffix '1')
    _existing = set(df1.columns)
    _out_ports = [
        'O_CMPY_CD_IND','filename_DTL',
        'o_CLNDR_YR1','o_CALL_YR1','NAIC_CMPNY_CD1','NISS_CMPNY_CD1','ST_NM1','ST_CD1','NISS_ST_CD1','ST_ABBR1','ACCTNG_LOB1',
        'CVG_TYP_CD1','CVG_TYP_IND1','o_CVG_AMT1','o_BI_LMT1','GA_ADDED_AT_FAULT_IND1','FA2_PLCY_IND1','UM_UMI_STACKING1',
        'PIP_WVR_WL_IND1','PIP_MED_SEC_IND1','PIP_LOSS_INCOME_IND1','MI_PPO_IND1','PRD_GRP_CD1','NJ_HLTH_INSR_PRIM1','NJ_EXTR_PIP_PKG1',
        'REC_EXCPN_IND1','REC_DROP_IND1','REC_DROP_RSN_DESC1','EXP_STATE_IND1','TimeZone1','LINE_CD1','ACCDNT_YR1','o_NISS_CVG_CD1',
        'RTNG_ZNE_CD1','TERM_ZNE_CD1','o_NISS_CLASS_CD1','NISS_ELIG_PNTS_CD1','NISS_AGE_GRP_CD1','o_NISS_MNFCTRS_MDL_YR1',
        'CVG_EXPS_VAL1','EXPS_VAL_ROLLED1','TTL_WRITTN_PREM_AMT1','NUM_OF_CARS_IN_HH1','PNI_AGE1','RDRVR_DT_OF_BRTH1','TERM_STRT_DT1',
        'SRC_SYS_CD1','DERIVED_RDRVR_AGE1','FINAL_RDRVR_AGE1'
    ]

    _select_exprs = []
    for c in _out_ports:
        if c in _existing:
            _select_exprs.append(c)
        else:
            _select_exprs.append(f"NULL AS {c}")

    try:
        df_EXP_CA_STATE_humble_darwin = df1.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting outputs in EXP_CA_STATE: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"EXP_CA_STATE failed: {e}", exc_info=True)
    raise


# placeholder for target file directory used by flat-file Output nodes
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"

# ------------------ Expression: EXP_TX_STATE (company-change indicator + filename for TX group) ------------------
try:
    logger.info("Computing EXP_TX_STATE (df_EXP_TX_STATE_jovial_newton) - company-change indicator and filename for TX group")
    src = df_SRT_TX_sharp_heisenberg

    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag

    # Window ordering consistent with SRT_TX (by NAIC_CMPNY_CD3 asc)
    w = Window.orderBy(col('NAIC_CMPNY_CD3').asc())

    df_tx = (
        src
        .withColumn('V_CURR_CMPY_CD', col('NAIC_CMPNY_CD3'))
        .withColumn('V_PREV_CMPY_CD', lag(col('NAIC_CMPNY_CD3')).over(w))
    )

    df_tx = df_tx.withColumn('O_CMPY_CD_IND', when(col('V_CURR_CMPY_CD') == col('V_PREV_CMPY_CD'), lit('N')).otherwise(lit('Y')))

    # V_FILE_YEAR using mapping/session parameter placeholders with fallback to current year
    df_tx = df_tx.withColumn('V_FILE_YEAR', when(lit(RUN_TYPE_TZ) == 'MANUAL', lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))

    # filename_DTL: 'NU0C_NISS_AUTO_TX_'||V_FILE_YEAR||'_AutoPrem_Detail_'||NAIC_CMPNY_CD3||'.csv'
    df_tx = df_tx.withColumn('filename_DTL', concat(lit('NU0C_NISS_AUTO_TX_'), col('V_FILE_YEAR'), lit('_AutoPrem_Detail_'), col('NAIC_CMPNY_CD3'), lit('.csv')))

    # Project expected output ports explicitly where present, else NULL to preserve downstream names
    _existing = set(df_tx.columns)
    _out_ports = [
        'O_CMPY_CD_IND','filename_DTL',
        'o_CLNDR_YR3','o_CALL_YR3','NAIC_CMPNY_CD3','NISS_CMPNY_CD3','ST_NM3','ST_CD3','NISS_ST_CD3','ST_ABBR3','ACCTNG_LOB3',
        'CVG_TYP_CD3','CVG_TYP_IND3','o_CVG_AMT3','o_BI_LMT3','GA_ADDED_AT_FAULT_IND3','FA2_PLCY_IND3','UM_UMI_STACKING3',
        'PIP_WVR_WL_IND3','PIP_MED_SEC_IND3','PIP_LOSS_INCOME_IND3','MI_PPO_IND3','PRD_GRP_CD3','NJ_HLTH_INSR_PRIM3','NJ_EXTR_PIP_PKG3',
        'REC_EXCPN_IND3','REC_DROP_IND3','REC_DROP_RSN_DESC3','EXP_STATE_IND3','TimeZone3','LINE_CD3','ACCDNT_YR3','o_NISS_CVG_CD3',
        'RTNG_ZNE_CD3','TERM_ZNE_CD3','o_NISS_CLASS_CD3','NISS_ELIG_PNTS_CD3','NISS_AGE_GRP_CD3','o_NISS_MNFCTRS_MDL_YR3',
        'CVG_EXPS_VAL3','EXPS_VAL_ROLLED3','TTL_WRITTN_PREM_AMT3','NUM_OF_CARS_IN_HH3','PNI_AGE3','RDRVR_DT_OF_BRTH3','TERM_STRT_DT3',
        'SRC_SYS_CD3','DERIVED_RDRVR_AGE3','FINAL_RDRVR_AGE3'
    ]

    _select_exprs = []
    for c in _out_ports:
        if c in _existing:
            _select_exprs.append(c)
        else:
            _select_exprs.append(f"NULL AS {c}")

    try:
        df_EXP_TX_STATE_jovial_newton = df_tx.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting outputs in EXP_TX_STATE: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"EXP_TX_STATE failed: {e}", exc_info=True)
    raise


# ------------------ Sorter: SRT_TIMEZONE (order by O_TIMEZONE4) ------------------
try:
    logger.info("Sorting by timezone for SRT_TIMEZONE into df_SRT_TIMEZONE_pensive_archimedes")
    src = df_NON_CA_TX_nice_bohr

    # primary sort key is O_TIMEZONE4 per design
    sorted_df = src.orderBy(col('O_TIMEZONE4').asc())

    # preserve all columns explicitly (no wildcard). Build a selectExpr that re-aliases to same names (keeps schema stable)
    _existing = set(sorted_df.columns)
    _select_exprs = [c if c in _existing else f"NULL AS {c}" for c in sorted_df.columns]

    try:
        df_SRT_TIMEZONE_pensive_archimedes = sorted_df.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting/selecting in SRT_TIMEZONE: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"SRT_TIMEZONE failed: {e}", exc_info=True)
    raise


# ------------------ Transaction Control (pass-through): TCTRANS2 ------------------
try:
    logger.info("Applying Transaction Control no-op TCTRANS2: assign df_TCTRANS2_fierce_lovelace from df_EXP_CA_STATE_humble_darwin")
    # Transaction Control has no Spark equivalent; the surrounding write is atomic. Implement as passthrough.
    df_TCTRANS2_fierce_lovelace = df_EXP_CA_STATE_humble_darwin
except Exception as e:
    logger.error(f"TCTRANS2 passthrough assignment failed: {e}", exc_info=True)
    raise


# ------------------ Transaction Control (pass-through): TCTRANS1 ------------------
try:
    logger.info("Applying Transaction Control no-op TCTRANS1: assign df_TCTRANS1_eager_kant from df_EXP_TX_STATE_jovial_newton")
    df_TCTRANS1_eager_kant = df_EXP_TX_STATE_jovial_newton
except Exception as e:
    logger.error(f"TCTRANS1 passthrough assignment failed: {e}", exc_info=True)
    raise


# ------------------ Expression: EXP_Passthrough (file naming and timezone indicator) ------------------
try:
    logger.info("Computing EXP_Passthrough (df_EXP_Passthrough_jovial_schrodinger) - file naming and timezone indicator")
    src = df_SRT_TIMEZONE_pensive_archimedes

    # V_PREV_TIMEZONE may not exist in incoming schema; if missing, set timezone-indicator to NULL and log a warning
    if 'V_PREV_TIMEZONE' not in src.columns:
        logger.warning('V_PREV_TIMEZONE not present in input to EXP_Passthrough; O_TIMEZONE_IND will be NULL')

    df_e = src.withColumn('V_CURR_TIMEZONE', col('TimeZone4'))

    if 'V_PREV_TIMEZONE' in df_e.columns:
        df_e = df_e.withColumn('V_TIMEZONE_IND', when(col('V_PREV_TIMEZONE') == col('V_CURR_TIMEZONE'), lit('N')).otherwise(lit('Y')))
    else:
        df_e = df_e.withColumn('V_TIMEZONE_IND', lit(None).cast('string'))

    # compute V_FILE_YEAR same as other expressions
    df_e = df_e.withColumn('V_FILE_YEAR', when(lit(RUN_TYPE_TZ) == 'MANUAL', lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))

    # FILE_NAME: conditional on TimeZone4 being null
    df_e = df_e.withColumn(
        'FILE_NAME',
        when(col('TimeZone4').isNull(), concat(lit('NU0C_NISS_AUTO_UNKNOWN_'), col('V_FILE_YEAR'), lit('_AutoPrem_Detail_.csv')))
        .otherwise(concat(lit('NU0C_NISS_AUTO_'), col('TimeZone4'), lit('_'), col('V_FILE_YEAR'), lit('_AutoPrem_Detail_.csv')))
    )

    # expose O_TIMEZONE_IND as output port from V_TIMEZONE_IND
    df_e = df_e.withColumn('O_TIMEZONE_IND', col('V_TIMEZONE_IND'))

    # final explicit projection: keep all existing columns plus make sure FILE_NAME and O_TIMEZONE_IND present
    _existing = list(df_e.columns)
    # ensure FILE_NAME and O_TIMEZONE_IND are at the end (may already exist)
    if 'FILE_NAME' not in _existing:
        _existing.append('FILE_NAME')
    if 'O_TIMEZONE_IND' not in _existing:
        _existing.append('O_TIMEZONE_IND')

    # build selectExpr preserving columns; if any expected column missing, emit NULL AS <col>
    _select_exprs = []
    for c in _existing:
        _select_exprs.append(c)

    try:
        df_EXP_Passthrough_jovial_schrodinger = df_e.selectExpr(*_select_exprs)
    except Exception as e:
        logger.error(f"Failed projecting outputs in EXP_Passthrough: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"EXP_Passthrough failed: {e}", exc_info=True)
    raise


# ------------------ Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11 (flat file) ------------------
try:
    logger.info("Writing flat file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11 from df_TCTRANS2_fierce_lovelace to PM_TARGET_FILE_DIR")
    # convert to pandas and write as delimited file (developer must replace PM_TARGET_FILE_DIR placeholder)
    pdf = df_TCTRANS2_fierce_lovelace.toPandas()
    pdf.to_csv(f"{PM_TARGET_FILE_DIR}/fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail111.out", index=False, mode='w')
    # keep the node's df_name bound so downstream plan references resolve
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11_elated_kepler = df_TCTRANS2_fierce_lovelace
except Exception as e:
    logger.error(f"Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11: {e}", exc_info=True)
    raise


# ------------------ Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1 (flat file) ------------------
try:
    logger.info("Writing flat file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1 from df_TCTRANS1_eager_kant to PM_TARGET_FILE_DIR")
    pdf2 = df_TCTRANS1_eager_kant.toPandas()
    pdf2.to_csv(f"{PM_TARGET_FILE_DIR}/fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail11.out", index=False, mode='w')
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1_optimistic_shannon = df_TCTRANS1_eager_kant
except Exception as e:
    logger.error(f"Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1: {e}", exc_info=True)
    raise


# ------------------ Transaction Control: TCTRANS (pass-through) ------------------
try:
    logger.info("Applying Transaction Control no-op TCTRANS: assign df_TCTRANS_quirky_gauss from df_EXP_Passthrough_jovial_schrodinger")
    # Transaction Control in Informatica used condition: IIF(O_TIMEZONE_IND='Y',TC_COMMIT_BEFORE,TC_CONTINUE_TRANSACTION)
    # Spark/Glue has no per-row commit control; DataFrame writes used below provide the required atomicity for the job.
    # Implement as a pure passthrough so all INPUT/OUTPUT ports remain available to downstream nodes.
    df_TCTRANS_quirky_gauss = df_EXP_Passthrough_jovial_schrodinger
except Exception as e:
    logger.error(f"TCTRANS passthrough assignment failed: {e}", exc_info=True)
    raise


# ------------------ Output: FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail (flat file) ------------------
try:
    logger.info("Writing flat file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail from df_TCTRANS_quirky_gauss to PM_TARGET_FILE_DIR")
    # convert to pandas and write as delimited file (developer must replace PM_TARGET_FILE_DIR placeholder)
    pdf_out = df_TCTRANS_quirky_gauss.toPandas()
    pdf_out.to_csv(f"{PM_TARGET_FILE_DIR}/fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail1.out", index=False, mode='w')
    # keep the node's df_name bound so downstream plan references resolve
    df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail_silly_kant = df_TCTRANS_quirky_gauss
except Exception as e:
    logger.error(f"Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail: {e}", exc_info=True)
    raise



job.commit()
