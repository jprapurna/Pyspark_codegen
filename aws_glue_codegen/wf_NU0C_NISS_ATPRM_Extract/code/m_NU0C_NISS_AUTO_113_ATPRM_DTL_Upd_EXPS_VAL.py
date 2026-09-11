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

# Placeholder constants for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
REPLACE_WITH_GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import expr, col, lit

# ----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged WRK_ table read with S3-first fallback)
# ----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 as parquet")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat_raw = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3")
except Exception as e:
    logger.warning(
        "Could not read WRK_BIRP_NISS_APRM_DETL from S3; falling back to Glue Catalog read: %s" % e
    )
    try:
        # Fallback to Glue Catalog read - project will be applied below
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database/table placeholder")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=REPLACE_WITH_GLUE_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL",
        )
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat_raw = dyf.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
    except Exception as e2:
        logger.error(
            f"Failed falling back to Glue Catalog for WRK_BIRP_NISS_APRM_DETL: {e2}",
            exc_info=True,
        )
        raise

# Project exactly the fields listed on the Source node
try:
    logger.info("Projecting exact columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat")
    src_cols = [
        'NISS_APRM_DETL_SK', 'CLNDR_YR', 'CALL_YR', 'NAIC_CMPNY_CD', 'NISS_CMPNY_CD',
        'ST_NM', 'ST_CD', 'NISS_ST_CD', 'ST_ABBR', 'ACCTNG_LOB', 'CVG_TYP_CD', 'CVG_AMT',
        'BI_LMT', 'GA_ADDED_AT_FAULT_IND', 'FA2_PLCY_IND', 'UM_UMI_STACKING', 'PIP_WVR_WL_IND',
        'PIP_MED_SEC_IND', 'PIP_LOSS_INCOME_IND', 'MI_PPO_IND', 'PRD_GRP_CD', 'NJ_HLTH_INSR_PRIM',
        'NJ_EXTR_PIP_PKG', 'NJ_RESDNC_RLTNSHP_PIP_IND', 'NY_SSL_IND', 'NY_FULL_CVG_GLASS_COMP_IND',
        'GRGNG_ZIP_5', 'NISS_TERR_CD', 'RATNG_CMPY_CD', 'MLT_CAR_IND', 'RT_CLS', 'AGE', 'GENDR',
        'MRTL_STAT', 'AUTO_USE_CD', 'MILES_TO_WRK', 'GOOD_STDNT_IND', 'DRVR_TRNG_IND', 'SOI_TYP',
        'PHY_DMG_IND', 'NJ_RATD_PNTS', 'VEH_MDL_YR', 'NJ_EXCPTION_CD', 'NJ_FGVN_PNTS', 'PASSV_RESTRA_DISC',
        'SNR_DRVR_IND', 'DEFNS_DRVR_DISC_IND', 'ANTI_THFT_DISC', 'DAY_TM_RUN_LIGHTS', 'LMT_TORT',
        'ANNL_STMNT_LOB_CD', 'CVG_TYP_IND', 'CVG_EXPS_VAL', 'TTL_WRITTN_PREM_AMT', 'LINE_CD',
        'ACCDNT_YR', 'NISS_CVG_CD', 'RTNG_ZNE_CD', 'TERM_ZNE_CD', 'NISS_CLASS_CD', 'NISS_ELIG_PNTS_CD',
        'NISS_AGE_GRP_CD', 'NISS_CMMCL_IND_CD', 'NISS_EXCPN_CD', 'NISS_FGVNS_CD', 'NISS_PASSV_RESTRA_CD',
        'NISS_DEFNS_DRVR_CRD_CD', 'NISS_ANTI_THFT_DVC_CD', 'NISS_DAY_TM_RUN_LAMPS_DISC_CD', 'NISS_PLCY_LMT_CD',
        'NISS_DEDUC_CD', 'NISS_SSL_LIAB_CD', 'NISS_SUBLOB_CD', 'NISS_TYP_LOSS_CD', 'NISS_LIAB_OR_NO_FAULT_CD',
        'NISS_ANNL_STMNT_LOB_CD', 'NISS_PD_LOSS', 'NISS_PD_ALLOC_ADJUS_EXPNS', 'NISS_OUTSTNDG_LOSS',
        'NISS_NO_PD_CLMS', 'NISS_NO_OUTSTND_CLMS', 'RSVD_NISS_USE', 'NISS_RSVD_CMPNY_USE', 'NISS_MNFCTRS_MDL_YR',
        'CR_BY_MAPNG_ID', 'DW_CR_TMSP', 'UPD_BY_MAPNG_ID', 'DW_UPD_TMSP', 'WRK_FLOW_RUN_ID',
        'NJ_NO_LWST_LMT_IND', 'NJ_NMD_DRVR_EXCL_IND', 'EXPS_VAL_ROLLED', 'CVG_CNT_IND', 'CVG_CNT',
        'CVG_CD_SK', 'CVG_ATTR_SK', 'REC_DROP_IND', 'REC_DROP_RSN_DESC', 'REC_EXCPN_IND',
        'REC_EXCPN_RSN_DESC', 'CVG_ATTR_CHCKSUM', 'COMP_DED', 'COLL_DED', 'PLCY_CNTRCT_NUM',
        'UNIT_NUM', 'EFF_DT', 'NUM_OF_CARS_IN_HH', 'RDRVR_DT_OF_BRTH', 'TERM_STRT_DT', 'SRC_SYS_CD',
        'DERIVED_RDRVR_AGE', 'FINAL_RDRVR_AGE', 'PNI_AGE', 'LOB', 'PRINCIPAL_OPRT', 'SOURCE_IND_DERIVED'
    ]
    # Ensure only available columns are selected to avoid AnalysisException
    available_cols = [c for c in src_cols if c in df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat_raw.columns]
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat_raw.select(*available_cols)
    logger.info("Projected columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat")
except Exception as e:
    logger.error(f"Failed projecting columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL2 (another staged consumer of the same WRK_ table)
# ----------------------------------------------------------------------------
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 as parquet for second consumer")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck_raw = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3 for second consumer")
except Exception as e:
    logger.warning(
        "Could not read WRK_BIRP_NISS_APRM_DETL from S3 for second consumer; falling back to Glue Catalog read: %s" % e
    )
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog for second consumer")
        dyf2 = glueContext.create_dynamic_frame_from_catalog(
            database=REPLACE_WITH_GLUE_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL",
        )
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck_raw = dyf2.toDF()
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Catalog for second consumer")
    except Exception as e2:
        logger.error(
            f"Failed falling back to Glue Catalog for WRK_BIRP_NISS_APRM_DETL (second consumer): {e2}",
            exc_info=True,
        )
        raise

# Project exactly the fields listed on the second Source node (same field set)
try:
    logger.info("Projecting exact columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck")
    available_cols2 = [c for c in src_cols if c in df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck_raw.columns]
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck_raw.select(*available_cols2)
    logger.info("Projected columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck")
except Exception as e:
    logger.error(f"Failed projecting columns for df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (SQL Override against staged WRK_BIRP_NISS_APRM_DETL)
# Rule: staged case -> register upstream df as temp view and run spark.sql on rewritten query
# ----------------------------------------------------------------------------
try:
    logger.info("Registering temp view WRK_BIRP_NISS_APRM_DETL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_kind_fermat.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ROUND((CVG_EXPS_VAL*12)) AS CVG_EXPS_VAL,
    PLCY_CNTRCT_NUM,
    UNIT_NUM,
    EFF_DT

FROM WRK_BIRP_NISS_APRM_DETL

WHERE CVG_TYP_IND = 'B'
"""

    logger.info("Executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_daring_aristotle = spark.sql(sql_query)
    logger.info("Completed spark.sql for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (SQL Override with row_number/window over staged WRK table)
# ----------------------------------------------------------------------------
try:
    logger.info("Registering temp view WRK_BIRP_NISS_APRM_DETL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_quirky_planck.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

    sql_query = f"""SELECT
C.NISS_APRM_DETL_SK,
C.CVG_EXPS_VAL,
C.PLCY_CNTRCT_NUM,
C.UNIT_NUM,
C.EFF_DT
from 
(SELECT 
B.NISS_APRM_DETL_SK,
B.CVG_EXPS_VAL,
B.PLCY_CNTRCT_NUM,
B.UNIT_NUM,
B.EFF_DT,
ROW_NUMBER() OVER  ( PARTITION BY 
B.PLCY_CNTRCT_NUM,
B.UNIT_NUM,
B.EFF_DT
ORDER BY 
B.CVG_EXPS_VAL DESC
) AS ROW_RANK

FROM WRK_BIRP_NISS_APRM_DETL B  where not exists
(SELECT 1 FROM WRK_BIRP_NISS_APRM_DETL A WHERE A.CVG_TYP_IND ='B' and A.PLCY_CNTRCT_NUM=B.PLCY_CNTRCT_NUM)  ) C

WHERE C.ROW_RANK=1
"""

    logger.info("Executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 via spark.sql")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_cool_spinoza = spark.sql(sql_query)
    logger.info("Completed spark.sql for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
except Exception as e:
    logger.error(f"Failed executing staged SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Exp_before_tgt: project five explicit columns in the listed order
# Input: df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_daring_aristotle
# ----------------------------------------------------------------------------
try:
    logger.info("Projecting EFF_DT, NISS_APRM_DETL_SK, CVG_EXPS_VAL, PLCY_CNTRCT_NUM, UNIT_NUM in Exp_before_tgt")
    df_Exp_before_tgt_jolly_aristotle = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_daring_aristotle.selectExpr(
        'EFF_DT',
        'NISS_APRM_DETL_SK',
        'CVG_EXPS_VAL',
        'PLCY_CNTRCT_NUM',
        'UNIT_NUM'
    )
    logger.info("Completed projection for Exp_before_tgt")
except Exception as e:
    logger.error(f"Failed Exp_before_tgt projection: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Exp_before_tgt1: compute EXP_VAL_ROLLED = ROUND((CVG_EXPS_VAL * 12)) while passing through other ports
# Input: df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_cool_spinoza
# ----------------------------------------------------------------------------
try:
    logger.info("Projecting base columns for Exp_before_tgt1")
    df_tmp_exp1 = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_cool_spinoza.selectExpr(
        'NISS_APRM_DETL_SK', 'CVG_EXPS_VAL', 'PLCY_CNTRCT_NUM', 'UNIT_NUM', 'EFF_DT'
    )
    logger.info("Computing EXP_VAL_ROLLED in Exp_before_tgt1")
    # create the derived alias explicitly
    df_Exp_before_tgt1_peaceful_franklin = df_tmp_exp1.selectExpr(
        'NISS_APRM_DETL_SK',
        'CVG_EXPS_VAL',
        'PLCY_CNTRCT_NUM',
        'UNIT_NUM',
        'EFF_DT',
        'ROUND((CVG_EXPS_VAL * 12)) AS EXP_VAL_ROLLED'
    )
    logger.info("Completed Exp_before_tgt1 transformation")
except Exception as e:
    logger.error(f"Failed Exp_before_tgt1 transformation: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Upd_EXP_UPD: Update Strategy (DD_UPDATE) applied to the target WRK_BIRP_NISS_APRM_DETL1
# Input: df_Exp_before_tgt_jolly_aristotle
# Primary key assumed: NISS_APRM_DETL_SK
# Pattern: derive dd_op, drop REJECT, load existing target, anti-join changed keys, union updated rows, overwrite target
# ----------------------------------------------------------------------------
try:
    logger.info("Deriving dd_op marker for Upd_EXP_UPD and filtering REJECT rows")
    df_Upd_EXP_UPD_peaceful_lovelace_pre = df_Exp_before_tgt_jolly_aristotle.withColumn('dd_op', lit('UPDATE'))
    df_Upd_EXP_UPD_peaceful_lovelace = df_Upd_EXP_UPD_peaceful_lovelace_pre.filter(col('dd_op') != 'REJECT')
    logger.info("Prepared rows for Upd_EXP_UPD (DD_UPDATE)")

    target_path_1 = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/"
    try:
        logger.info(f"Reading existing target for WRK_BIRP_NISS_APRM_DETL1 from {target_path_1}")
        existing_df_1 = spark.read.parquet(target_path_1)
        logger.info("Successfully read existing target WRK_BIRP_NISS_APRM_DETL1")
    except Exception as read_e:
        logger.warning(
            f"Existing target WRK_BIRP_NISS_APRM_DETL1 not found or unreadable ({read_e}); proceeding with empty existing dataframe"
        )
        # create empty dataframe with same schema as incoming so union works
        existing_df_1 = spark.createDataFrame([], df_Upd_EXP_UPD_peaceful_lovelace.schema)

    # compute changed keys (primary key rows being updated)
    changed_keys_1 = df_Upd_EXP_UPD_peaceful_lovelace.select('NISS_APRM_DETL_SK').distinct()

    # anti-join to remove rows being updated from existing
    existing_surviving_1 = existing_df_1.join(changed_keys_1, on='NISS_APRM_DETL_SK', how='left_anti')

    # union surviving existing rows with update rows (do not include deleted rows)
    combined_1 = existing_surviving_1.unionByName(df_Upd_EXP_UPD_peaceful_lovelace.drop('dd_op'), allowMissingColumns=True)

    # write back over the same target path (overwrite)
    try:
        logger.info("Writing combined dataframe back to WRK_BIRP_NISS_APRM_DETL1 as parquet (overwrite)")
        combined_1.write.mode('overwrite').parquet(target_path_1)
        logger.info("Successfully wrote WRK_BIRP_NISS_APRM_DETL1 to S3")
    except Exception as write_e:
        logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL1 to S3: {write_e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Failed Update Strategy Upd_EXP_UPD: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Upd_EXP_UPD_CVG_IND_NOT_B: Update Strategy (DD_UPDATE) applied to WRK_BIRP_NISS_APRM_DETL3
# Input: df_Exp_before_tgt1_peaceful_franklin (contains EXP_VAL_ROLLED)
# ----------------------------------------------------------------------------
try:
    logger.info("Deriving dd_op marker for Upd_EXP_UPD_CVG_IND_NOT_B and filtering REJECT rows")
    df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla_pre = df_Exp_before_tgt1_peaceful_franklin.withColumn('dd_op', lit('UPDATE'))
    df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla = df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla_pre.filter(col('dd_op') != 'REJECT')
    logger.info("Prepared rows for Upd_EXP_UPD_CVG_IND_NOT_B (DD_UPDATE)")

    target_path_3 = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL3/"
    try:
        logger.info(f"Reading existing target for WRK_BIRP_NISS_APRM_DETL3 from {target_path_3}")
        existing_df_3 = spark.read.parquet(target_path_3)
        logger.info("Successfully read existing target WRK_BIRP_NISS_APRM_DETL3")
    except Exception as read_e:
        logger.warning(
            f"Existing target WRK_BIRP_NISS_APRM_DETL3 not found or unreadable ({read_e}); proceeding with empty existing dataframe"
        )
        existing_df_3 = spark.createDataFrame([], df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla.schema)

    changed_keys_3 = df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla.select('NISS_APRM_DETL_SK').distinct()
    existing_surviving_3 = existing_df_3.join(changed_keys_3, on='NISS_APRM_DETL_SK', how='left_anti')

    combined_3 = existing_surviving_3.unionByName(df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla.drop('dd_op'), allowMissingColumns=True)

    try:
        logger.info("Writing combined dataframe back to WRK_BIRP_NISS_APRM_DETL3 as parquet (overwrite)")
        combined_3.write.mode('overwrite').parquet(target_path_3)
        logger.info("Successfully wrote WRK_BIRP_NISS_APRM_DETL3 to S3")
    except Exception as write_e:
        logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL3 to S3: {write_e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Failed Update Strategy Upd_EXP_UPD_CVG_IND_NOT_B: {e}", exc_info=True)
    raise

# ----------------------------------------------------------------------------
# Output nodes: map stream variables to final df_names so downstream code (or the job end) can reference them
# (These assignments make the mapping's output df_names available for any further orchestration.)
# ----------------------------------------------------------------------------
# The Update Strategy nodes already performed the writes. Expose the final dataframe variables as mapping outputs.

# df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_gifted_gauss is the output reference for the target WRK_BIRP_NISS_APRM_DETL1
df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_gifted_gauss = df_Upd_EXP_UPD_peaceful_lovelace

# df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL3_busy_bohr is the output reference for the target WRK_BIRP_NISS_APRM_DETL3
df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL3_busy_bohr = df_Upd_EXP_UPD_CVG_IND_NOT_B_hopeful_tesla


job.commit()
