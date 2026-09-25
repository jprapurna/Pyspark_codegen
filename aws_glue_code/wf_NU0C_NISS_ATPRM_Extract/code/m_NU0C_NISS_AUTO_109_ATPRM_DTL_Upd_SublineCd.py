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


# placeholder constants
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
SOURCE_GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import col, expr, split, size, trim, regexp_replace, lit, when, broadcast, row_number
from pyspark.sql.window import Window

# --- Source: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged S3 preferred read) ---
try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read")
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback")
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=SOURCE_GLUE_DATABASE,
            table_name="WRK_BIRP_NISS_APRM_DETL",
        )
        # project exactly the ports listed on the Source node
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = dyf.toDF().selectExpr(
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
            "SOURCE_IND_DERIVED"
        )
    except Exception as e:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {e}", exc_info=True)
        raise

# --- Application Source Qualifier: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL ---
# The upstream table is staged; register the staged DF as a temp view and run the SQL override against it.
try:
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
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

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR NOT IN ('NY','NJ')
"""
    try:
        logger.info("Running SQL Override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged temp view")
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid = spark.sql(sql_query).select(
            "NISS_APRM_DETL_SK",
            "ST_NM",
            "ST_ABBR",
            "ACCTNG_LOB",
            "CVG_TYP_CD",
            "BI_LMT",
            "MI_PPO_IND",
            "LMT_TORT",
            "TERM_STRT_DT",
        )
    except Exception as e:
        logger.error(f"Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
        raise
except Exception as e:
    logger.error(f"Failed preparing staged temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# --- Expression: EXP_BILimit_Split ---
try:
    logger.info("Computing BI_LMT parts and numeric conversions in EXP_BILimit_Split")
    df_EXP_BILimit_Split_zen_bohr = (
        df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid
        .withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ',', ''))
        .withColumn("v_BI_LMT_Parts_Array", split(col("v_BI_LMT"), '/'))
        .withColumn("BI_LMT_NO_OF_PARTS", size(col("v_BI_LMT_Parts_Array")))
        .withColumn("v_Limit_FIELD1", when(col("BI_LMT_NO_OF_PARTS") >= 1, col("v_BI_LMT_Parts_Array").getItem(0)).otherwise(lit('0')))
        .withColumn("v_Limit_FIELD2", when(col("BI_LMT_NO_OF_PARTS") >= 2, col("v_BI_LMT_Parts_Array").getItem(1)).otherwise(lit('0')))
        .withColumn("v_Limit_FIELD3", when(col("BI_LMT_NO_OF_PARTS") >= 3, col("v_BI_LMT_Parts_Array").getItem(2)).otherwise(lit('0')))
        .withColumn("BI_LMT_1_Decimal", col("v_Limit_FIELD1").cast("decimal(14,0)"))
        .withColumn("BI_LMT_2_Decimal", col("v_Limit_FIELD2").cast("decimal(14,0)"))
        .withColumn("BI_LMT_3_Decimal", col("v_Limit_FIELD3").cast("decimal(14,0)"))
        .withColumn("SRC_BI_LMT", col("v_BI_LMT"))
        .select(
            "NISS_APRM_DETL_SK",
            "BI_LMT",
            "v_BI_LMT",
            "BI_LMT_NO_OF_PARTS",
            "v_Limit_FIELD1",
            "v_Limit_FIELD2",
            "v_Limit_FIELD3",
            "BI_LMT_1_Decimal",
            "BI_LMT_2_Decimal",
            "BI_LMT_3_Decimal",
            "SRC_BI_LMT",
        )
    )
except Exception as e:
    logger.error(f"Failed in EXP_BILimit_Split transformation: {e}", exc_info=True)
    raise

# --- Expression: EXP_Derive_NISS_SUBLOB_CD_And_PassThru ---
try:
    logger.info("Joining SQ and BI limit split results for EXP_Derive_NISS_SUBLOB_CD_And_PassThru")
    df_join_for_exp = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid.join(
        df_EXP_BILimit_Split_zen_bohr,
        on="NISS_APRM_DETL_SK",
        how="left",
    )

    # compute vv_NISS_SUBLOB_CD via a CASE expression translated from the DECODE logic
    logger.info("Computing intermediate vv_NISS_SUBLOB_CD")
    vv_case = expr(
        """
        CASE
          WHEN ST_ABBR = 'NH' THEN '8'

          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT != '1'
            AND (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '4'
          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT != '1' AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '4'
          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT != '1' AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '5'
          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT = '1' AND (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '6'
          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT = '1' AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '6'
          WHEN ST_ABBR = 'PA' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND LMT_TORT = '1' AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '7'

          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND CVG_TYP_CD = '35028' AND (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) THEN '3'
          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND CVG_TYP_CD = '35028' AND (BI_LMT = '0' OR BI_LMT_NO_OF_PARTS > 1) THEN '4'
          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND (LMT_TORT != '#' AND NOT (LMT_TORT IS NULL OR trim(LMT_TORT) = '')) AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '3'
          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND (LMT_TORT != '#' AND NOT (LMT_TORT IS NULL OR trim(LMT_TORT) = '')) AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '4'
          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND (LMT_TORT = '#' OR trim(LMT_TORT) = '' OR LMT_TORT IS NULL) AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '2'
          WHEN ST_ABBR = 'KY' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND (LMT_TORT = '#' OR trim(LMT_TORT) = '' OR LMT_TORT IS NULL) AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '1'

          WHEN ST_ABBR = 'FL' AND substr(ACCTNG_LOB,1,3) IN ('191','192') AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '2'
          WHEN substr(ACCTNG_LOB,1,3) IN ('191','192') AND ST_ABBR IN ('CT','KS','MD','MI','MN','ND','OR','UT','WA') AND MI_PPO_IND = 1 AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '6'
          WHEN substr(ACCTNG_LOB,1,3) IN ('191','192') AND ST_ABBR IN ('CT','KS','MD','MI','MN','ND','OR','UT','WA') AND MI_PPO_IND = 1 AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '5'
          WHEN substr(ACCTNG_LOB,1,3) IN ('191','192') AND ST_ABBR IN ('CT','KS','MD','MI','MN','ND','OR','UT','WA') AND MI_PPO_IND != 1 AND (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '2'
          WHEN substr(ACCTNG_LOB,1,3) IN ('191','192') AND ST_ABBR IN ('CT','KS','MD','MI','MN','ND','OR','UT','WA') AND MI_PPO_IND != 1 AND NOT (CVG_TYP_CD IN ('13000','13006','13007','13008')) THEN '1'

          WHEN ST_ABBR IN ('KY','PA','CT','KS','MD','MI','MN','ND','OR','UT','WA','FL') AND substr(ACCTNG_LOB,1,3) = '211' THEN '0'
          WHEN ST_NM IN ('DISTRICT OF COLUMBIA','DELAWARE','HAWAII') THEN '?'
          WHEN NOT (ST_ABBR IN ('NY','NJ','NC','NH','KY','PA','CT','KS','MD','MI','MN','ND','OR','UT','WA','FL')) AND NOT (ST_NM IN ('DISTRICT OF COLUMBIA','DELAWARE','HAWAII')) THEN '0'
          ELSE '?'
        END
        """
    )

    df_with_vv = df_join_for_exp.withColumn("vv_NISS_SUBLOB_CD", vv_case)

    # v_NISS_SUBLOB_CD: MI-specific override based on EFF_DT >= 07/01/2020
    logger.info("Applying MI-specific adjustments to vv_NISS_SUBLOB_CD to produce v_NISS_SUBLOB_CD")
    mi_case = expr(
        """
        CASE
          WHEN ST_ABBR = 'MI' AND (substr(trim(ACCTNG_LOB),1,3) = '191' OR ACCTNG_LOB = '191') AND EFF_DT >= to_date('07/01/2020','MM/dd/yyyy') THEN
            CASE
              WHEN CVG_TYP_CD IN ('35000','35082') AND (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) THEN '2'
              WHEN CVG_TYP_CD IN ('35000','35082') AND (BI_LMT = '0' OR BI_LMT_NO_OF_PARTS > 1) THEN '1'
              WHEN (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) AND NOT (CVG_TYP_CD IN ('35000','35082')) THEN '8'
              WHEN NOT (CVG_TYP_CD IN ('35000','35082')) AND (BI_LMT = '0' OR BI_LMT_NO_OF_PARTS > 1) THEN '7'
              WHEN CVG_TYP_CD IN ('',' ') AND (BI_LMT_NO_OF_PARTS = 1 AND BI_LMT_1_Decimal > 0) THEN '?'
              WHEN CVG_TYP_CD IN ('',' ') AND (BI_LMT = '0' OR BI_LMT_NO_OF_PARTS > 1) THEN '?'
              ELSE vv_NISS_SUBLOB_CD
            END
          ELSE vv_NISS_SUBLOB_CD
        END
        """
    )

    df_with_v = df_with_vv.withColumn("v_NISS_SUBLOB_CD", mi_case)

    # final NISS_SUBLOB_CD: default '?' if empty
    logger.info("Deriving final NISS_SUBLOB_CD and projecting passthrough columns")
    df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_gifted_babbage = (
        df_with_v
        .withColumn("NISS_SUBLOB_CD", expr("CASE WHEN v_NISS_SUBLOB_CD IS NULL OR trim(v_NISS_SUBLOB_CD) = '' THEN '?' ELSE v_NISS_SUBLOB_CD END"))
        .select(
            "NISS_APRM_DETL_SK",
            "EFF_DT",
            "NISS_SUBLOB_CD",
        )
    )
except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_SUBLOB_CD_And_PassThru: {e}", exc_info=True)
    raise

# --- Update Strategy: UPD_NISS_SUBLOB_CD (derive dd_op marker, apply changes back to WRK target) ---
try:
    logger.info("Applying Update Strategy logic to mark dd_op and filter rejects")
    # Original mapping marks rows as DD_UPDATE => translate to 'UPDATE' marker for all incoming rows
    df_with_dd = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_gifted_babbage.withColumn("dd_op", lit("UPDATE"))
    # drop REJECT rows if any (none expected here)
    df_with_dd_filtered = df_with_dd.filter(col("dd_op") != 'REJECT')

    # load current target full table (S3-first, then Glue Catalog fallback)
    try:
        logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL from S3 for load-modify-store-back")
        df_existing_target = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.warning("Target WRK_BIRP_NISS_APRM_DETL not found on S3; falling back to Glue Catalog read for existing target")
        try:
            dyf_target = glueContext.create_dynamic_frame.from_catalog(database=SOURCE_GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
            df_existing_target = dyf_target.toDF()
        except Exception as ee:
            logger.error(f"Failed reading existing target WRK_BIRP_NISS_APRM_DETL from Glue Catalog fallback: {ee}", exc_info=True)
            raise

    # prepare the incoming rows to be applied (INSERT/UPDATE only)
    rows_to_apply = df_with_dd_filtered.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

    # keys of changed rows
    changed_keys_df = rows_to_apply.select("NISS_APRM_DETL_SK").dropDuplicates()

    # anti-join to remove any existing rows that will be updated/deleted
    df_surviving_existing = df_existing_target.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")

    # union surviving existing rows with new/updated rows
    full_rebuilt_df = df_surviving_existing.unionByName(rows_to_apply, allowMissingColumns=True)

    # write the full rebuilt table back to the same S3 location (overwrite)
    try:
        logger.info("Writing rebuilt WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) from Update Strategy apply")
        full_rebuilt_df.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.error(f"Failed writing rebuilt WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
        raise

    # expose the post-apply dataframe under the node's df_name so downstream Output node can use it
    df_UPD_NISS_SUBLOB_CD_trusting_kepler = full_rebuilt_df

except Exception as e:
    logger.error(f"Failed in Update Strategy UPD_NISS_SUBLOB_CD: {e}", exc_info=True)
    raise

# --- Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (write final target as parquet to S3) ---
try:
    logger.info("Writing final WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) - mapping target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
    df_UPD_NISS_SUBLOB_CD_trusting_kepler.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    # assign the output df name for any downstream references
    df_WRK_BIRP_NISS_APRM_DETL_bold_noether = df_UPD_NISS_SUBLOB_CD_trusting_kepler
except Exception as e:
    logger.error(f"Failed writing mapping target WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
