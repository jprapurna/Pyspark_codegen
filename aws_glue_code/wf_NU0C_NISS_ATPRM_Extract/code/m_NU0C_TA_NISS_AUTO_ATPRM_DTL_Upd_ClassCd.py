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


# top-of-script mapping/run placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

from pyspark.sql.functions import expr, col, lit, when, trim, substring, concat_ws, coalesce, broadcast, row_number
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: Application Source Qualifier with SQL Override
# Attempt to reuse an earlier-workflow-staged parquet for WRK_BIRP_NISS_APRM_DETL first;
# if missing, fall back to executing the override via JDBC against Snowflake.
# ---------------------------------------------------------------------------
sql_query = f"""SELECT
 NISS_APRM_DETL_SK,
 LTRIM(RTRIM(ST_CD)) ST_CD,
 LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
 LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
LTRIM(RTRIM(CVG_TYP_CD)),
 LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
 LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
 LTRIM(RTRIM( FINAL_RDRVR_AGE)) FINAL_RDRVR_AGE,
 LTRIM(RTRIM(GENDR)) GENDR,
 LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
 LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
 LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
 NISS_CLASS_CD,
 REC_DROP_IND,
 REC_DROP_RSN_DESC,
 REC_EXCPN_IND,
 REC_EXCPN_RSN_DESC,
 LTRIM(RTRIM(PRINCIPAL_OPRT)) PRINCIPAL_OPRT,
 SOURCE_IND_DERIVED

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR NOT IN ('NY','NJ') AND
SOURCE_IND_DERIVED='TOGGLE AUTO'"""

try:
    logger.info("Attempting to read staged parquet for WRK_BIRP_NISS_APRM_DETL from S3 before falling back to Snowflake")
    df_staged_WRK_BIRP_NISS_APRM_DETL = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    # register temp view named after the physical table so the override can select from it
    df_staged_WRK_BIRP_NISS_APRM_DETL.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Staged parquet found and registered as temp view WRK_BIRP_NISS_APRM_DETL; executing overridden SQL via spark.sql")
    try:
        df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck = spark.sql(sql_query)
    except Exception as e:
        logger.error(f"Failed executing rewritten override SQL against staged temp view: {e}", exc_info=True)
        raise
except Exception as s3_exc:
    # allowed fallback: parquet not present -> run the override via JDBC against Snowflake
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not available; falling back to executing SQL override via Snowflake JDBC")
    try:
        logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Snowflake via JDBC override query")
        df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("query", sql_query)
            .load()
        )
    except Exception as e:
        logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Snowflake: {e}", exc_info=True)
        raise

# ---------------------------------------------------------------------------
# EXP_PASS_THROUGH: explicit projection of every listed port (no '*')
# ---------------------------------------------------------------------------
try:
    logger.info("Applying EXP_PASS_THROUGH projection")
    df_EXP_PASS_THROUGH_amazing_ramanujan = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'FINAL_RDRVR_AGE AS AGE',
        'GENDR',
        'MRTL_STAT',
        'AUTO_USE_CD',
        'SOI_TYP',
        'NISS_CLASS_CD',
        'REC_DROP_IND',
        'REC_DROP_RSN_DESC',
        'REC_EXCPN_IND',
        'REC_EXCPN_RSN_DESC',
        'PRINCIPAL_OPRT',
        'SOURCE_IND_DERIVED',
        'CVG_TYP_CD'
    )
except Exception as e:
    logger.error(f"Failed applying EXP_PASS_THROUGH projection: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# EXP_DERV_CLASS_CD: multi-step expression computing intermediates and final outputs
# - create typed/normalized columns (IAGE, AUTO_USE_CD)
# - compute intermediate class-code candidates using CASE/WHEN expressions
# - compute final NISS_CLASS_CD, REC_EXCPN_IND, REC_EXCP_DESC
# ---------------------------------------------------------------------------
try:
    logger.info("Starting EXP_DERV_CLASS_CD transformations (intermediates + final outputs)")

    df1 = df_EXP_PASS_THROUGH_amazing_ramanujan.withColumn('IAGE', expr("CASE WHEN AGE IS NULL OR TRIM(AGE) = '' THEN NULL ELSE CAST(TRIM(AGE) AS INT) END"))
    df1 = df1.withColumn('AUTO_USE_CD_N', when(col('i_AUTO_USE_CD').isNull(), lit('')).otherwise(col('i_AUTO_USE_CD')))

    # v_CLASS_CD_Indemnity: translate DECODE -> CASE WHEN ... THEN ... ELSE '??????'
    df1 = df1.withColumn(
        'v_CLASS_CD_Indemnity',
        expr(
            "CASE \
                WHEN (ST_ABBR NOT IN ('NY','NJ','NC','AR','SD','PA') AND ACCTNG_LOB = '1923D') THEN '941400' \
                WHEN (ST_ABBR IN ('AR') AND ACCTNG_LOB = '191') THEN '946400' \
                WHEN (ST_ABBR IN ('SD') AND ACCTNG_LOB = '1923D') THEN '946400' \
                WHEN (ST_ABBR IN ('PA') AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35043','35041','30005','34008','30016')) THEN '999700' \
                ELSE '??????' END"
        )
    )

    # v_CLASS_CODE_TAuto: large nested conditions -> implement as chained CASE WHEN with precedence
    df1 = df1.withColumn(
        'v_CLASS_CODE_TAuto',
        expr(
            "CASE \n                WHEN ST_ABBR NOT IN ('NY','NJ','NC','MI','MT','PA','FL','NH','LA') THEN \n                    CASE \n                        WHEN MLT_CAR_IND='N' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD_N='PLS' THEN '1200'\n                        WHEN MLT_CAR_IND='Y' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD_N='PLS' THEN '1202'\n                        WHEN MLT_CAR_IND='N' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD_N='TPF' THEN '1400'\n                        WHEN MLT_CAR_IND='Y' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD_N='TPF' THEN '1402'\n                        WHEN MLT_CAR_IND='N' AND IAGE < 25 AND GENDR='M' AND MRTL_STAT='M' THEN '1620'\n                        WHEN MLT_CAR_IND='Y' AND IAGE < 25 AND GENDR='M' AND MRTL_STAT='M' THEN '1622'\n                        WHEN MLT_CAR_IND='N' AND IAGE < 25 AND GENDR='M' THEN '1600'\n                        WHEN MLT_CAR_IND='Y' AND IAGE < 25 AND GENDR='M' THEN '1602'\n                        WHEN MLT_CAR_IND='N' AND IAGE < 25 AND GENDR='F' THEN '1640'\n                        WHEN MLT_CAR_IND='Y' AND IAGE < 25 AND GENDR='F' THEN '1642'\n                        WHEN MLT_CAR_IND='N' AND IAGE < 25 AND (GENDR IN ('U','') OR GENDR IS NULL OR GENDR='X') THEN '1600'\n                        WHEN MLT_CAR_IND='Y' AND IAGE < 25 AND (GENDR IN ('U','') OR GENDR IS NULL OR GENDR='X') THEN '1602'\n                        WHEN IAGE >= 65 THEN '1700'\n                        ELSE '' \n                    END\n                ELSE '' END"
        )
    )

    # normalized final TAUTO code: if empty -> '????'
    df1 = df1.withColumn('v_CLASS_CODE_TAuto_Final', expr("CASE WHEN v_CLASS_CODE_TAuto != '' THEN v_CLASS_CODE_TAuto ELSE '????' END"))

    # v_CLASS_CODE_Secondary (DECODE -> CASE)
    df1 = df1.withColumn('v_CLASS_CODE_Secondary', expr("CASE WHEN ST_ABBR='PA' AND MLT_CAR_IND='N' THEN '10' WHEN ST_ABBR='PA' AND MLT_CAR_IND='Y' THEN '20' WHEN ST_ABBR NOT IN ('NY','NJ','NC','PA') THEN '00' ELSE '??' END"))

    # v_CLASS_CD_Misc_1 .. _6: implement key branches observed in source logic
    df1 = df1.withColumn('v_CLASS_CD_Misc_1', expr(
        "CASE \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='07' THEN '950900' \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='02' THEN '939200' \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='09' THEN '952900' \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='03' THEN '949200' \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='04' THEN '934000' \n            WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP='08' THEN '941300' \n            WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP='07' THEN '950800' \n            WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP='02' THEN '958800' \n            WHEN ST_ABBR IN ('LA') AND IAGE > 64 AND SOI_TYP='09' THEN '952800' \n            WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP='03' THEN '959800' \n            WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP='04' THEN '954800' \n            WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP='08' THEN '951800' \n            WHEN ST_ABBR = 'LA' AND SOI_TYP NOT IN ('02','03','04','07','08','09') THEN 'E1212' \n            ELSE '' END"
    ))

    df1 = df1.withColumn('v_CLASS_CD_Misc_2', expr(
        "CASE \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='07' THEN '950700' \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='02' THEN '958700' \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='09' THEN '952700' \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='03' THEN '959700' \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='04' THEN '954700' \n            WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP='08' THEN '951700' \n            WHEN ST_ABBR='PA' AND SOI_TYP NOT IN ('01','05','30','02','03','04','07','08','09') THEN 'E88712' \n            ELSE '' END"
    ))

    df1 = df1.withColumn('v_CLASS_CD_Misc_3', expr(
        "CASE \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='07' AND GENDR='M' THEN '950700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='07' AND GENDR='F' THEN '950900' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='02' AND GENDR='M' THEN '958700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='02' AND GENDR='F' THEN '939200' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='09' AND GENDR='M' THEN '952700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='09' AND GENDR='F' THEN '952900' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='03' AND GENDR='M' THEN '959700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='03' AND GENDR='F' THEN '949200' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='04' AND GENDR='M' THEN '954700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='04' AND GENDR='F' THEN '934000' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='08' AND GENDR='M' THEN '951700' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP='08' AND GENDR='F' THEN '941300' \n            ELSE '' END"
    ))

    df1 = df1.withColumn('v_CLASS_CD_Misc_4', expr(
        "CASE WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 THEN \n            CASE WHEN SOI_TYP='07' THEN '950900' WHEN SOI_TYP='02' THEN '939200' WHEN SOI_TYP='09' THEN '952900' WHEN SOI_TYP='03' THEN '949200' WHEN SOI_TYP='04' THEN '934000' WHEN SOI_TYP='08' THEN '941300' ELSE '' END \n        ELSE '' END"
    ))

    df1 = df1.withColumn('v_CLASS_CD_Misc_5', expr(
        "CASE \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='05' THEN '933200' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='30' THEN '933100' \n            WHEN ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD = '40030' THEN '958000' \n            WHEN ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD = '40031' THEN '949000' \n            WHEN ST_ABBR NOT IN ('NY','NC','NJ','PA') AND SOI_TYP NOT IN ('01','05','30') THEN 'E1202' \n            ELSE '' END"
    ))

    df1 = df1.withColumn('v_CLASS_CD_Misc_6_FL', expr(
        "CASE WHEN ST_ABBR='FL' THEN \n            CASE \n                WHEN SOI_TYP='07' AND IAGE < 25 AND GENDR='M' THEN '950700' \n                WHEN SOI_TYP='07' AND IAGE < 25 AND GENDR='F' THEN '950900' \n                WHEN SOI_TYP='07' AND IAGE >= 25 AND IAGE < 65 THEN '950900' \n                WHEN SOI_TYP='07' AND IAGE > 64 THEN '950800' \n                WHEN SOI_TYP='02' AND IAGE < 25 AND GENDR='M' THEN '958700' \n                WHEN SOI_TYP='02' AND IAGE < 25 AND GENDR='F' THEN '939200' \n                WHEN SOI_TYP='02' AND IAGE >= 25 AND IAGE < 65 THEN '939200' \n                WHEN SOI_TYP='02' AND IAGE > 64 THEN '958800' \n                WHEN SOI_TYP='09' AND IAGE < 25 AND GENDR='M' THEN '952700' \n                WHEN SOI_TYP='09' AND IAGE < 25 AND GENDR='F' THEN '952900' \n                WHEN SOI_TYP='09' AND IAGE >= 25 AND IAGE < 65 THEN '952900' \n                WHEN SOI_TYP='09' AND IAGE > 64 THEN '952800' \n                WHEN SOI_TYP='03' AND IAGE < 25 AND GENDR='M' THEN '959700' \n                WHEN SOI_TYP='03' AND IAGE < 25 AND GENDR='F' THEN '949200' \n                WHEN SOI_TYP='03' AND IAGE >= 25 AND IAGE < 65 THEN '949200' \n                WHEN SOI_TYP='03' AND IAGE > 64 THEN '959800' \n                WHEN SOI_TYP='04' AND IAGE < 25 AND GENDR='M' THEN '954700' \n                WHEN SOI_TYP='04' AND IAGE < 25 AND GENDR='F' THEN '934000' \n                WHEN SOI_TYP='04' AND IAGE >= 25 AND IAGE < 65 THEN '934000' \n                WHEN SOI_TYP='04' AND IAGE > 64 THEN '954800' \n                WHEN SOI_TYP='08' AND IAGE < 25 AND GENDR='M' THEN '951700' \n                WHEN SOI_TYP='08' AND IAGE < 25 AND GENDR='F' THEN '941300' \n                WHEN SOI_TYP='08' AND IAGE >= 25 AND IAGE < 65 THEN '941300' \n                WHEN SOI_TYP='08' AND IAGE > 64 THEN '951800' \n                WHEN ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='05' THEN '933200' \n                WHEN ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='30' THEN '933100' \n                ELSE '' END \n        ELSE '' END"
    ))

    # v_CLASS_CD_Misc_Final: pick first non-empty or '??????'
    df1 = df1.withColumn('v_CLASS_CD_Misc_Final', expr(
        "CASE WHEN v_CLASS_CD_Misc_1 != '' THEN v_CLASS_CD_Misc_1 WHEN v_CLASS_CD_Misc_2 != '' THEN v_CLASS_CD_Misc_2 WHEN v_CLASS_CD_Misc_3 != '' THEN v_CLASS_CD_Misc_3 WHEN v_CLASS_CD_Misc_4 != '' THEN v_CLASS_CD_Misc_4 WHEN v_CLASS_CD_Misc_5 != '' THEN v_CLASS_CD_Misc_5 WHEN v_CLASS_CD_Misc_6_FL != '' THEN v_CLASS_CD_Misc_6_FL ELSE '??????' END"
    ))

    # v_NISS_CLASS_CD: choose indemnity if not '??????', else TAUTO_FINAL+SECONDARY when SOI_TYP='01', else misc final
    df1 = df1.withColumn('v_NISS_CLASS_CD', expr(
        "CASE WHEN v_CLASS_CD_Indemnity != '??????' THEN v_CLASS_CD_Indemnity WHEN SOI_TYP = '01' THEN v_CLASS_CODE_TAuto_Final || v_CLASS_CODE_Secondary WHEN SOI_TYP != '01' THEN v_CLASS_CD_Misc_Final ELSE '??????' END"
    ))

    # final NISS_CLASS_CD per original IIF validation
    df1 = df1.withColumn('NISS_CLASS_CD', expr("CASE WHEN substr(ltrim(rtrim(v_NISS_CLASS_CD)),1,4) = '????' OR substr(v_NISS_CLASS_CD,5,2) = '??' THEN '??????' ELSE v_NISS_CLASS_CD END"))

    # REC_EXCP_IND and REC_EXCP_DESC_class
    df1 = df1.withColumn('REC_EXCP_IND_local', expr("CASE WHEN substr(ltrim(rtrim(v_CLASS_CODE_TAuto_Final)),1,1)='E' OR substr(ltrim(rtrim(v_CLASS_CD_Misc_Final)),1,1)='E' THEN 'Y' ELSE '' END"))
    df1 = df1.withColumn('REC_EXCP_DESC_CLASS', when(col('REC_EXCP_IND_local') == 'Y', lit('DEFAULT_CLASS_CD')).otherwise(lit('')))

    # REC_EXCPN_IND final: combine incoming i_REC_EXCPN_IND with local REC_EXCP_IND_local
    df1 = df1.withColumn('REC_EXCPN_IND', when((col('i_REC_EXCPN_IND') == 'Y') | (col('REC_EXCP_IND_local') == 'Y'), lit('Y')).otherwise(lit('')))

    # REC_EXCP_DESC concatenation
    df1 = df1.withColumn('REC_EXCP_DESC', concat_ws('', col('REC_EXCPN_RSN_DESC_ZIP'), col('REC_EXCP_DESC_CLASS')))

    # Ensure final projection contains exactly the INPUT/OUTPUT and OUTPUT ports expected downstream
    df_EXP_DERV_CLASS_CD_gentle_curie = df1.select(
        'NISS_APRM_DETL_SK',
        'ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'AGE',
        'IAGE',
        'GENDR',
        'MRTL_STAT',
        'i_AUTO_USE_CD',
        'SOI_TYP',
        'NISS_CLASS_CD',
        'i_REC_EXCPN_IND',
        'REC_EXCPN_IND',
        'REC_EXCPN_RSN_DESC_ZIP',
        'REC_EXCP_DESC'
    )

except Exception as e:
    logger.error(f"Failed EXP_DERV_CLASS_CD transformations: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# UPD_NISS_CLASS_CD: Update Strategy - load-modify-store-back against Snowflake target
# Primary key: NISS_APRM_DETL_SK
# Steps: read existing target, derive dd_op per row, filter out NOOP/REJECT, anti-join existing rows being changed, union INSERT/UPDATE rows, overwrite target via JDBC, and expose combined_df to downstream
# ---------------------------------------------------------------------------
try:
    logger.info("Starting Update Strategy: reading current target FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake via JDBC")
    existing_df = (
        spark.read.format('jdbc')
        .option('url', SNOWFLAKE_URL)
        .option('user', SNOWFLAKE_USER)
        .option('password', SNOWFLAKE_PASSWORD)
        .option('dbtable', 'FDR.WRK_BIRP_NISS_APRM_DETL')
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading existing target FDR.WRK_BIRP_NISS_APRM_DETL from Snowflake: {e}", exc_info=True)
    raise

try:
    logger.info("Determining row-level DD_* operations by comparing incoming rows to existing target")
    # left join incoming to existing to determine existence and compare key business columns
    inc = df_EXP_DERV_CLASS_CD_gentle_curie.alias('inc')
    ex = existing_df.alias('ex')
    joined = inc.join(ex, on=['NISS_APRM_DETL_SK'], how='left')

    # dd_op logic: INSERT if existing key null, UPDATE if exists and any tracked column differs, else NOOP
    cmp_expr = (
        (coalesce(col('ex.NISS_CLASS_CD'), lit('')) != coalesce(col('inc.NISS_CLASS_CD'), lit(''))) |
        (coalesce(col('ex.REC_EXCPN_IND'), lit('')) != coalesce(col('inc.REC_EXCPN_IND'), lit(''))) |
        (coalesce(col('ex.REC_EXCPN_RSN_DESC'), lit('')) != coalesce(col('inc.REC_EXCP_DESC'), lit('')))
    )

    with_op = joined.withColumn(
        'dd_op',
        when(col('ex.NISS_APRM_DETL_SK').isNull(), lit('INSERT'))
        .when(cmp_expr, lit('UPDATE'))
        .otherwise(lit('NOOP'))
    )

    # Drop REJECT rows if any (none defined) and drop NOOP rows as they do not need applying
    to_apply = with_op.filter(col('dd_op').isin('INSERT','UPDATE'))

    # Build changed keys df for anti-join
    changed_keys_df = to_apply.select(col('NISS_APRM_DETL_SK')).distinct()

    # existing rows excluding those that will be updated/deleted
    existing_anti = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')

    # prepare apply rows in the target schema: map incoming ports to target columns where names match
    # Note: source has REC_EXCP_DESC while target column storing description was REC_EXCPN_RSN_DESC in existing_df metadata; align by naming to preserve semantics
    to_apply_df = to_apply.select(
        col('inc.NISS_APRM_DETL_SK').alias('NISS_APRM_DETL_SK'),
        col('inc.NISS_CLASS_CD').alias('NISS_CLASS_CD'),
        col('inc.REC_EXCPN_IND').alias('REC_EXCPN_IND'),
        col('inc.REC_EXCP_DESC').alias('REC_EXCPN_RSN_DESC')
    )

    # combined snapshot: existing rows that are not changing + updated/inserted rows
    from functools import reduce
    combined_df = existing_anti.unionByName(to_apply_df, allowMissingColumns=True)

    # write combined snapshot back to Snowflake target as full overwrite
    try:
        logger.info("Writing combined snapshot back to Snowflake table FDR.WRK_BIRP_NISS_APRM_DETL (overwrite)")
        combined_df.write.format('jdbc').option('url', SNOWFLAKE_URL).option('user', SNOWFLAKE_USER).option('password', SNOWFLAKE_PASSWORD).option('dbtable', 'FDR.WRK_BIRP_NISS_APRM_DETL').mode('overwrite').save()
    except Exception as e:
        logger.error(f"Failed writing combined snapshot back to Snowflake target: {e}", exc_info=True)
        raise

    # expose the combined snapshot as the Update Strategy node's output df for downstream Output write
    df_UPD_NISS_CLASS_CD_blissful_ramanujan = combined_df

except Exception as e:
    logger.error(f"Failed Update Strategy load-modify-store-back processing: {e}", exc_info=True)
    raise

# ---------------------------------------------------------------------------
# FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Output - write the WRK_ table as parquet to S3 (overwrite)
# ---------------------------------------------------------------------------
# assign output df name expected by downstream consumers
df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_planck = df_UPD_NISS_CLASS_CD_blissful_ramanujan

try:
    logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_planck.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
