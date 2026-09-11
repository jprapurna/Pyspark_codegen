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
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import col, lit, broadcast, row_number, trim, substring, concat, coalesce
from pyspark.sql import Window

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 (attempt S3-first, fall back to Glue Catalog)
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL parquet from S3 for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_wonderful_heisenberg = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not found on S3, falling back to Glue Catalog read: %s" % e)
    try:
        logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Glue Data Catalog database %s, table WRK_BIRP_NISS_APRM_DETL" % GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_wonderful_heisenberg = dyf.toDF()
    except Exception as ex:
        logger.error(f"Failed fallback read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Glue Catalog: {ex}", exc_info=True)
        raise

# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: SQL Override against staged table - register temp view and run override via spark.sql
try:
    # register the upstream staged dataframe as a temp view named exactly as the real table
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_wonderful_heisenberg.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

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

    logger.info("Running SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 against temp view WRK_BIRP_NISS_APRM_DETL")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_daring_fermat = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed running SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# EXP_PASS_THROUGH: explicit passthrough projection of listed ports
try:
    logger.info("Projecting explicit passthrough columns in EXP_PASS_THROUGH")
    df_EXP_PASS_THROUGH_elated_newton = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_daring_fermat.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'AGE as AGE',
        'GENDR',
        'MRTL_STAT',
        'AUTO_USE_CD as i_AUTO_USE_CD',
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
    logger.error(f"Failed in EXP_PASS_THROUGH projection: {e}", exc_info=True)
    raise

# EXP_DERV_CLASS_CD: compute many intermediate variables and final NISS_CLASS_CD, REC_EXCPN_IND, REC_EXCP_DESC
try:
    logger.info("Computing derived class codes and exception flags in EXP_DERV_CLASS_CD (intermediate computation)")

    # First stage: compute intermediate variables v_* using selectExpr so aliases are materialized
    df_intermediate = df_EXP_PASS_THROUGH_elated_newton.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'AGE',
        "CAST(AGE AS INT) AS IAGE",
        'GENDR',
        'MRTL_STAT',
        'i_AUTO_USE_CD',
        "COALESCE(i_AUTO_USE_CD,'') AS AUTO_USE_CD",
        'SOI_TYP',
        'REC_EXCPN_IND as i_REC_EXCPN_IND',
        'REC_EXCPN_RSN_DESC as REC_EXCPN_RSN_DESC_ZIP',
        'REC_EXCPN_RSN_DESC',
        'REC_DROP_IND',
        'REC_DROP_RSN_DESC',
        'PRINCIPAL_OPRT',
        'SOURCE_IND_DERIVED',
        # v_CLASS_CD_Indemnity translated from DECODE pattern
        "CASE WHEN (ST_ABBR NOT IN ('NY','NJ','NC','AR','SD','PA') AND ACCTNG_LOB='1923D') THEN '941400' WHEN (ST_ABBR IN ('AR') AND ACCTNG_LOB='191') THEN '946400' WHEN (ST_ABBR IN ('SD') AND ACCTNG_LOB='1923D') THEN '946400' WHEN (ST_ABBR IN ('PA') AND ACCTNG_LOB='191' AND CVG_TYP_CD IN ('35043','35041','30005','34008','30016')) THEN '999700' ELSE '??????' END AS v_CLASS_CD_Indemnity",
        # v_CLASS_CODE_TAuto: large nested IIFs converted to CASE/WHEN
        ("CASE WHEN ST_ABBR NOT IN ('NY','NJ','NC','MI','MT','PA','FL','NH','LA') THEN "
         "CASE WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) >=25 AND CAST(AGE AS INT) < 65 AND AUTO_USE_CD='PLS' THEN '1200' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) >=25 AND CAST(AGE AS INT) < 65 AND AUTO_USE_CD='PLS' THEN '1202' "
         "WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) >=25 AND CAST(AGE AS INT) < 65 AND AUTO_USE_CD='TPF' THEN '1400' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) >=25 AND CAST(AGE AS INT) < 65 AND AUTO_USE_CD='TPF' THEN '1402' "
         "WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) < 25 AND GENDR = 'M' AND MRTL_STAT ='M' THEN '1620' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) < 25 AND GENDR = 'M' AND MRTL_STAT ='M' THEN '1622' "
         "WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) < 25 AND GENDR = 'M' THEN '1600' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) < 25 AND GENDR = 'M' THEN '1602' "
         "WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) < 25 AND GENDR = 'F' THEN '1640' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) < 25 AND GENDR = 'F' THEN '1642' "
         "WHEN MLT_CAR_IND='N' AND CAST(AGE AS INT) < 25 AND (GENDR = 'U' OR GENDR = '' OR GENDR = 'X' OR GENDR IS NULL) THEN '1600' "
         "WHEN MLT_CAR_IND='Y' AND CAST(AGE AS INT) < 25 AND (GENDR = 'U' OR GENDR = '' OR GENDR = 'X' OR GENDR IS NULL) THEN '1602' "
         "WHEN CAST(AGE AS INT) >= 65 THEN '1700' ELSE '' END "
         "ELSE '' END AS v_CLASS_CODE_TAuto"),
        # v_CLASS_CODE_TAuto_Final
        "CASE WHEN v_CLASS_CODE_TAuto != '' THEN v_CLASS_CODE_TAuto ELSE '????' END AS v_CLASS_CODE_TAuto_Final",
        # v_CLASS_CODE_Secondary
        "CASE WHEN ST_ABBR='PA' AND MLT_CAR_IND='N' THEN '10' WHEN ST_ABBR='PA' AND MLT_CAR_IND='Y' THEN '20' WHEN ST_ABBR NOT IN ('NY','NJ','NC','PA') THEN '00' ELSE '??' END AS v_CLASS_CODE_Secondary",
        # v_CLASS_CD_Misc_1 (LA specific)
        ("CASE WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '07' THEN '950900' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '02' THEN '939200' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '09' THEN '952900' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '03' THEN '949200' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '04' THEN '934000' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) >= 25 AND CAST(AGE AS INT) < 65 AND SOI_TYP = '08' THEN '941300' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '07' THEN '950800' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '02' THEN '958800' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '09' THEN '952800' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '03' THEN '959800' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '04' THEN '954800' "
         "WHEN ST_ABBR = 'LA' AND CAST(AGE AS INT) > 64 AND SOI_TYP = '08' THEN '951800' ELSE '' END AS v_CLASS_CD_Misc_1"),
        # v_CLASS_CD_Misc_2 (MI,MT,PA - youth cases)
        ("CASE WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '07' THEN '950700' "
         "WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '02' THEN '958700' "
         "WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '09' THEN '952700' "
         "WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '03' THEN '959700' "
         "WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '04' THEN '954700' "
         "WHEN ST_ABBR IN ('MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '08' THEN '951700' ELSE '' END AS v_CLASS_CD_Misc_2"),
        # v_CLASS_CD_Misc_3 and v_CLASS_CD_Misc_4 approximations for other states
        "CASE WHEN NOT ST_ABBR IN ('NY','NC','NJ','MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '07' AND GENDR='M' THEN '950700' WHEN NOT ST_ABBR IN ('NY','NC','NJ','MI','MT','PA') AND CAST(AGE AS INT) < 25 AND SOI_TYP = '07' AND GENDR='F' THEN '950900' ELSE '' END AS v_CLASS_CD_Misc_3",
        "CASE WHEN NOT ST_ABBR IN ('NY','NC','NJ','LA') AND CAST(AGE AS INT) >= 25 AND SOI_TYP = '07' THEN '950900' WHEN NOT ST_ABBR IN ('NY','NC','NJ','LA') AND CAST(AGE AS INT) >= 25 AND SOI_TYP = '02' THEN '939200' ELSE '' END AS v_CLASS_CD_Misc_4",
        # v_CLASS_CD_Misc_5 (other combos, CVG_TYP_CD-driven)
        ("CASE WHEN (ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='05') THEN '933200' "
         "WHEN (ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP='30') THEN '933100' "
         "WHEN (ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD='40030') THEN '958000' "
         "WHEN (ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD='40031') THEN '949000' "
         "WHEN (ST_ABBR NOT IN ('NY','NC','NJ','PA') AND NOT SOI_TYP IN ('01','05','30')) THEN 'E1202' ELSE '' END AS v_CLASS_CD_Misc_5"),
        # v_CLASS_CD_Misc_6_FL (FL special)
        ("CASE WHEN ST_ABBR='FL' THEN (CASE WHEN SOI_TYP='07' AND CAST(AGE AS INT) < 25 AND GENDR='M' THEN '950700' WHEN SOI_TYP='07' AND CAST(AGE AS INT) < 25 AND GENDR='F' THEN '950900' WHEN SOI_TYP='07' AND CAST(AGE AS INT) >=25 AND CAST(AGE AS INT) <65 THEN '950900' WHEN SOI_TYP='07' AND CAST(AGE AS INT) > 64 THEN '950800' ELSE '' END) ELSE '' END AS v_CLASS_CD_Misc_6_FL"),
        # combine misc into final
        "CASE WHEN v_CLASS_CD_Misc_1 != '' THEN v_CLASS_CD_Misc_1 WHEN v_CLASS_CD_Misc_2 != '' THEN v_CLASS_CD_Misc_2 WHEN v_CLASS_CD_Misc_3 != '' THEN v_CLASS_CD_Misc_3 WHEN v_CLASS_CD_Misc_4 != '' THEN v_CLASS_CD_Misc_4 WHEN v_CLASS_CD_Misc_5 != '' THEN v_CLASS_CD_Misc_5 WHEN v_CLASS_CD_Misc_6_FL != '' THEN v_CLASS_CD_Misc_6_FL ELSE '??????' END AS v_CLASS_CD_Misc_Final"
    )

    # Second stage: compute v_NISS_CLASS_CD, REC_EXCP_IND, REC_EXCP_DESC_CLASS, final NISS_CLASS_CD and REC_EXCPN_IND/REC_EXCP_DESC
    df_with_final = df_intermediate.selectExpr(
        '*',
        "CASE WHEN v_CLASS_CD_Indemnity != '??????' THEN v_CLASS_CD_Indemnity WHEN SOI_TYP = '01' THEN CONCAT(v_CLASS_CODE_TAuto_Final, v_CLASS_CODE_Secondary) WHEN SOI_TYP != '01' THEN v_CLASS_CD_Misc_Final ELSE '??????' END AS v_NISS_CLASS_CD",
        "CASE WHEN SUBSTRING(TRIM(v_CLASS_CODE_TAuto_Final),1,1) = 'E' OR SUBSTRING(TRIM(v_CLASS_CD_Misc_Final),1,1) = 'E' THEN 'Y' ELSE '' END AS REC_EXCP_IND_from_calc",
        "CASE WHEN SUBSTRING(TRIM(v_CLASS_CODE_TAuto_Final),1,1) = 'E' OR SUBSTRING(TRIM(v_CLASS_CD_Misc_Final),1,1) = 'E' THEN 'DEFAULT_CLASS_CD' ELSE '' END AS REC_EXCP_DESC_CLASS",
        # final NISS_CLASS_CD with the IIF/substring checks
        "CASE WHEN SUBSTRING(TRIM(v_NISS_CLASS_CD),1,4) = '????' OR SUBSTRING(v_NISS_CLASS_CD,5,6) = '??' THEN '??????' ELSE v_NISS_CLASS_CD END AS NISS_CLASS_CD_computed",
        # REC_EXCPN_IND final combining existing indicator and computed
        "CASE WHEN i_REC_EXCPN_IND = 'Y' OR (SUBSTRING(TRIM(v_CLASS_CODE_TAuto_Final),1,1) = 'E' OR SUBSTRING(TRIM(v_CLASS_CD_Misc_Final),1,1) = 'E') THEN 'Y' ELSE '' END AS REC_EXCPN_IND_final",
        # REC_EXCP_DESC concatenation
        "CONCAT(COALESCE(REC_EXCPN_RSN_DESC_ZIP,''), COALESCE(REC_EXCP_DESC_CLASS,'')) AS REC_EXCP_DESC_COMPUTED"
    )

    # Final projection: produce the output ports expected by downstream
    df_EXP_DERV_CLASS_CD_mighty_spinoza = df_with_final.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_CD',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'RATNG_CMPY_CD',
        'MLT_CAR_IND',
        'AGE',
        'GENDR',
        'MRTL_STAT',
        'i_AUTO_USE_CD',
        'AUTO_USE_CD',
        'SOI_TYP',
        'NISS_CLASS_CD_computed AS NISS_CLASS_CD',
        "CASE WHEN i_REC_EXCPN_IND = 'Y' OR REC_EXCP_IND_from_calc = 'Y' THEN 'Y' ELSE '' END AS REC_EXCPN_IND",
        'REC_EXCP_DESC_COMPUTED AS REC_EXCP_DESC',
        'REC_EXCPN_RSN_DESC_ZIP',
        'REC_EXCPN_RSN_DESC'
    )

except Exception as e:
    logger.error(f"Failed computing derived class codes in EXP_DERV_CLASS_CD: {e}", exc_info=True)
    raise

# UPD_NISS_CLASS_CD: Update Strategy - derive dd_op, drop REJECT, load existing target, anti-join out changed keys, union INSERT/UPDATE rows, write back to same S3 path
try:
    logger.info("Applying Update Strategy semantics in UPD_NISS_CLASS_CD: derive dd_op, apply load-modify-store-back to target WRK_BIRP_NISS_APRM_DETL")
    # derive dd_op - mapping indicates DD_UPDATE for ports, so mark rows as UPDATE
    df_with_dd = df_EXP_DERV_CLASS_CD_mighty_spinoza.withColumn('dd_op', lit('UPDATE'))

    # drop REJECT rows if any
    df_surviving = df_with_dd.filter(col('dd_op') != 'REJECT')

    # load existing target from S3 (full table)
    try:
        logger.info("Reading existing target WRK_BIRP_NISS_APRM_DETL from s3 for update-apply")
        existing_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.error(f"Failed reading existing target WRK_BIRP_NISS_APRM_DETL from S3: {e}", exc_info=True)
        raise

    # build changed keys DF
    changed_keys_df = df_surviving.select('NISS_APRM_DETL_SK').dropDuplicates()

    # anti-join existing rows to remove those being updated/deleted
    existing_minus_changed = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')

    # rows to re-insert: only INSERT or UPDATE (exclude DELETE). Here dd_op is 'UPDATE', so keep all
    rows_to_apply = df_surviving.filter(col('dd_op').isin('INSERT', 'UPDATE'))

    # union back together - allow missing columns to avoid strict schema mismatch
    from functools import reduce
    dfs_to_union = [existing_minus_changed, rows_to_apply.drop('dd_op')]
    combined_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_to_union)

    # assign output dataframe name expected by downstream
    df_UPD_NISS_CLASS_CD_amazing_ramanujan = combined_df

    # write the full combined table back to the same S3 path (overwrite)
    try:
        logger.info("Writing combined WRK_BIRP_NISS_APRM_DETL to s3 (overwrite) as part of Update Strategy apply")
        df_UPD_NISS_CLASS_CD_amazing_ramanujan.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    except Exception as e:
        logger.error(f"Failed writing applied updates for WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed in UPD_NISS_CLASS_CD Update Strategy apply: {e}", exc_info=True)
    raise

# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL -> write final parquet to S3 (strip FDR_LIB_ prefix for path)
try:
    # assign this node's df_name to the Update Strategy result so downstream references resolve
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_upbeat_hawking = df_UPD_NISS_CLASS_CD_amazing_ramanujan

    # write intermediate WRK_ table as parquet to S3 (overwrite)
    try:
        logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) from Output node FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_upbeat_hawking.write.mode('overwrite').parquet(
            f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
        )
    except Exception as e:
        logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed in Output node FDR_LIB_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise


job.commit()
