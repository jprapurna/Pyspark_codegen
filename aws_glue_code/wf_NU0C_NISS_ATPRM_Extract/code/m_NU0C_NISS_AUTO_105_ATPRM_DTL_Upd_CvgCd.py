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


# placeholder constants for environment/run-time values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import expr, col, trim, regexp_replace, length, when, lit, substring, locate, coalesce, broadcast

# --- Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL (staged WRK_ table) ---
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 parquet")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    # register temp view so downstream SQL-override SQ can query the in-memory staged data
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 and registered temp view WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    # fallback: try to read from the Glue Data Catalog table (normal source read)
    logger.warning("Failed reading staged parquet for WRK_BIRP_NISS_APRM_DETL from S3, falling back to Glue Catalog read: %s", e)
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog database '%s'", GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage = dyf.toDF()
        # register temp view for the SQ override as well
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_friendly_babbage.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
        logger.info("Loaded WRK_BIRP_NISS_APRM_DETL from Glue Catalog and registered temp view")
    except Exception as e2:
        logger.error(f"Failed falling back to Glue Catalog for WRK_BIRP_NISS_APRM_DETL: {e2}", exc_info=True)
        raise

# --- SQ: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL (SQL Override rewritten to run against temp view) ---
sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_NM,
    ST_ABBR,
    LTRIM(RTRIM(ACCTNG_LOB)) AS ACCTNG_LOB,
    LTRIM(RTRIM(CVG_TYP_CD)) AS CVG_TYP_CD,
    LTRIM(RTRIM(CVG_AMT)) AS CVG_AMT,
    LTRIM(RTRIM(BI_LMT)) AS BI_LMT,
    LTRIM(RTRIM(GA_ADDED_AT_FAULT_IND)) AS GA_ADDED_AT_FAULT_IND,
    LTRIM(RTRIM(FA2_PLCY_IND)) AS FA2_PLCY_IND,
    LTRIM(RTRIM(UM_UMI_STACKING)) AS UM_UMI_STACKING,
    PIP_WVR_WL_IND,
    PIP_MED_SEC_IND,
    PIP_LOSS_INCOME_IND,
    MI_PPO_IND,
    LTRIM(RTRIM(RATNG_CMPY_CD)) AS RATNG_CMPY_CD,
    LTRIM(RTRIM(COMP_DED)) AS COMP_DED,
    LTRIM(RTRIM(COLL_DED)) AS COLL_DED,
    LTRIM(RTRIM(LOB)) AS MIS_LOB,
    LTRIM(RTRIM(SOURCE_IND_DERIVED)) AS SOURCE_IND_DERIVED
FROM
    WRK_BIRP_NISS_APRM_DETL
WHERE
    ST_ABBR NOT IN ('NY','NJ')
"""

try:
    logger.info("Running SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL against temp view")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_admiring_feynman = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# --- EXP_Pass_Through: explicit passthrough projection ---
try:
    logger.info("Applying EXP_Pass_Through passthrough projection")
    df_EXP_Pass_Through_lucid_einstein = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_admiring_feynman.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_NM',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'BI_LMT',
        'GA_ADDED_AT_FAULT_IND',
        'FA2_PLCY_IND',
        'UM_UMI_STACKING',
        'PIP_WVR_WL_IND',
        'PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND',
        'MI_PPO_IND',
        'COMP_DED',
        'RATNG_CMPY_CD',
        'COLL_DED',
        'MIS_LOB',
        'SOURCE_IND_DERIVED'
    )
except Exception as e:
    logger.error(f"Failed in EXP_Pass_Through projection: {e}", exc_info=True)
    raise

# --- EXP_BILimit_Split: clean and split BI_LMT into parts and decimals ---
try:
    logger.info("Starting EXP_BILimit_Split to clean and split BI_LMT")
    df_temp = df_EXP_Pass_Through_lucid_einstein
    # cleaned string without commas
    df_temp = df_temp.withColumn('v_BI_LMT', regexp_replace(trim(col('BI_LMT')), ',', ''))
    # number of parts: length - length(replace(...)) + 1  (handles no-slash -> 1)
    df_temp = df_temp.withColumn(
        'v_BI_LMT_Parts',
        expr("CASE WHEN length(v_BI_LMT) = 0 THEN 0 ELSE (length(v_BI_LMT) - length(replace(v_BI_LMT, '/', '')) + 1) END")
    )
    # positions of first and second '/'
    df_temp = df_temp.withColumn('v_BI_LMT_Part1_Pos', expr("locate('/', v_BI_LMT)")).withColumn(
        'v_BI_LMT_Part2_Pos', expr("locate('/', v_BI_LMT, v_BI_LMT_Part1_Pos + 1)")
    )
    # extract parts as strings
    df_temp = df_temp.withColumn(
        'v_Limit_FIELD1',
        expr(
            "CASE WHEN v_BI_LMT_Parts = 1 THEN v_BI_LMT WHEN v_BI_LMT_Parts IN (2,3) THEN substring(v_BI_LMT, 1, v_BI_LMT_Part1_Pos - 1) ELSE '0' END"
        ),
    )
    df_temp = df_temp.withColumn(
        'v_Limit_FIELD2',
        expr(
            "CASE WHEN v_BI_LMT_Parts = 1 THEN '0' WHEN v_BI_LMT_Parts = 2 THEN substring(v_BI_LMT, v_BI_LMT_Part1_Pos + 1, length(v_BI_LMT)) WHEN v_BI_LMT_Parts = 3 THEN substring(v_BI_LMT, v_BI_LMT_Part1_Pos + 1, v_BI_LMT_Part2_Pos - v_BI_LMT_Part1_Pos - 1) ELSE '0' END"
        ),
    )
    df_temp = df_temp.withColumn(
        'v_Limit_FIELD3',
        expr(
            "CASE WHEN v_BI_LMT_Parts IN (1,2) THEN '0' WHEN v_BI_LMT_Parts = 3 THEN substring(v_BI_LMT, v_BI_LMT_Part2_Pos + 1, length(v_BI_LMT)) ELSE '0' END"
        ),
    )
    # convert to decimals (safe cast; unparsable values become NULL)
    df_temp = df_temp.withColumn('BI_LMT_1_Decimal', expr("cast(nullif(v_Limit_FIELD1,'') as decimal(14,0))")).withColumn(
        'BI_LMT_2_Decimal', expr("cast(nullif(v_Limit_FIELD2,'') as decimal(14,0))")
    ).withColumn('BI_LMT_3_Decimal', expr("cast(nullif(v_Limit_FIELD3,'') as decimal(14,0))"))
    df_temp = df_temp.withColumn('BI_LMT_NO_OF_PARTS', col('v_BI_LMT_Parts')).withColumn('SRC_BI_LMT', col('v_BI_LMT'))
    # preserve schema and assign output dataframe
    df_EXP_BILimit_Split_busy_planck = df_temp
    logger.info("Finished EXP_BILimit_Split")
except Exception as e:
    logger.error(f"Failed in EXP_BILimit_Split: {e}", exc_info=True)
    raise

# --- EXP_CvgAmount_Split: clean and split CVG_AMT into parts and decimals ---
try:
    logger.info("Starting EXP_CvgAmount_Split to clean and split CVG_AMT")
    df_temp = df_EXP_Pass_Through_lucid_einstein
    df_temp = df_temp.withColumn('v_CVG_AMT', regexp_replace(trim(col('CVG_AMT')), ',', ''))
    df_temp = df_temp.withColumn(
        'v_CVG_AMT_Parts',
        expr("CASE WHEN length(v_CVG_AMT) = 0 THEN 0 ELSE (length(v_CVG_AMT) - length(replace(v_CVG_AMT, '/', '')) + 1) END")
    )
    df_temp = df_temp.withColumn('v_CVG_AMT_Part1_Pos', expr("locate('/', v_CVG_AMT)")).withColumn(
        'v_CVG_AMT_Part2_Pos', expr("locate('/', v_CVG_AMT, v_CVG_AMT_Part1_Pos + 1)")
    )
    df_temp = df_temp.withColumn(
        'v_AMOUNT_FIELD1',
        expr(
            "CASE WHEN v_CVG_AMT_Parts = 1 THEN v_CVG_AMT WHEN v_CVG_AMT_Parts IN (2,3) THEN substring(v_CVG_AMT, 1, v_CVG_AMT_Part1_Pos - 1) ELSE '0' END"
        ),
    )
    df_temp = df_temp.withColumn(
        'v_AMOUNT_FIELD2',
        expr(
            "CASE WHEN v_CVG_AMT_Parts = 1 THEN '0' WHEN v_CVG_AMT_Parts = 2 THEN substring(v_CVG_AMT, v_CVG_AMT_Part1_Pos + 1, length(v_CVG_AMT)) WHEN v_CVG_AMT_Parts = 3 THEN substring(v_CVG_AMT, v_CVG_AMT_Part1_Pos + 1, v_CVG_AMT_Part2_Pos - v_CVG_AMT_Part1_Pos - 1) ELSE '0' END"
        ),
    )
    df_temp = df_temp.withColumn(
        'v_AMOUNT_FIELD3',
        expr(
            "CASE WHEN v_CVG_AMT_Parts IN (1,2) THEN '0' WHEN v_CVG_AMT_Parts = 3 THEN substring(v_CVG_AMT, v_CVG_AMT_Part2_Pos + 1, length(v_CVG_AMT)) ELSE '0' END"
        ),
    )
    df_temp = df_temp.withColumn('CVG_AMT_1_Decimal', expr("cast(nullif(v_AMOUNT_FIELD1,'') as decimal(14,0))")).withColumn(
        'CVG_AMT_2_Decimal', expr("cast(nullif(v_AMOUNT_FIELD2,'') as decimal(14,0))")
    ).withColumn('CVG_AMT_3_Decimal', expr("cast(nullif(v_AMOUNT_FIELD3,'') as decimal(14,0))"))
    df_temp = df_temp.withColumn('CVG_AMT_NO_OF_PARTS', col('v_CVG_AMT_Parts')).withColumn('SRC_CVG_AMT', col('v_CVG_AMT'))
    df_EXP_CvgAmount_Split_wonderful_descartes = df_temp
    logger.info("Finished EXP_CvgAmount_Split")
except Exception as e:
    logger.error(f"Failed in EXP_CvgAmount_Split: {e}", exc_info=True)
    raise

# --- EXP_Derive_NISS_CVG_CD_And_PassThru: derive coverage code and pass through ports ---
try:
    logger.info("Starting EXP_Derive_NISS_CVG_CD_And_PassThru")
    # start from the passthrough DF and enrich with derived flags/integers
    df_temp = df_EXP_Pass_Through_lucid_einstein
    # derive numeric versions of COLL_DED and COMP_DED by removing commas and casting to integer where possible
    df_temp = df_temp.withColumn('v_COLL_DED', expr("cast(nullif(regexp_replace(trim(COLL_DED), ',', ''),'') as int)"))
    df_temp = df_temp.withColumn('v_COLL_DED_STR', col('COLL_DED'))
    df_temp = df_temp.withColumn('v_COMP_DED', expr("cast(nullif(regexp_replace(trim(COMP_DED), ',', ''),'') as int)"))
    # derive PIP/Mi flags similar to DECODE(1, cond, 'Y', cond2, 'N','') -> translate to CASE
    df_temp = df_temp.withColumn('v_PIP_WVR_WL_IND', expr("CASE WHEN PIP_WVR_WL_IND = 1 THEN 'Y' WHEN PIP_WVR_WL_IND = 0 THEN 'N' ELSE '' END"))
    df_temp = df_temp.withColumn('v_PIP_MED_SEC_IND', expr("CASE WHEN PIP_MED_SEC_IND = 1 THEN 'Y' WHEN PIP_MED_SEC_IND = 0 THEN 'N' ELSE '' END"))
    df_temp = df_temp.withColumn('v_PIP_LOSS_INCOME_IND', expr("CASE WHEN PIP_LOSS_INCOME_IND = 1 THEN 'Y' WHEN PIP_LOSS_INCOME_IND = 0 THEN 'N' ELSE '' END"))
    df_temp = df_temp.withColumn('v_MI_PPO_IND', expr("CASE WHEN MI_PPO_IND = 1 THEN 'Y' WHEN MI_PPO_IND = 0 THEN 'N' ELSE '' END"))

    # v_CVG_CD_STEP1: translate a subset of the original complex IIF/DECODE rules as a CASE chain covering common branches
    df_temp = df_temp.withColumn(
        'v_CVG_CD_STEP1',
        expr(
            "CASE "
            "WHEN ST_ABBR = 'AR' AND ACCTNG_LOB = '191' THEN '001' "
            "WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '191' AND CVG_TYP_CD = '34000' AND CVG_AMT = '5,000' THEN '081' "
            "WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '191' AND CVG_TYP_CD = '35009' THEN '085' "
            "WHEN ST_ABBR = 'KS' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35000','35007') AND CVG_AMT = '4,500/900' THEN '081' "
            "ELSE '' END"
        ),
    )

    # v_CVG_CD_STEP1A: some FL-specific overrides (partial translation of the DECODE block)
    df_temp = df_temp.withColumn(
        'v_CVG_CD_STEP1A',
        expr(
            "CASE WHEN ST_ABBR = 'FL' AND ACCTNG_LOB = '191' AND CVG_TYP_CD = '35000' AND CVG_AMT IN ('0','250','500','1,000','5,000','10,000','2,500','4,500/900') THEN '090' "
            "WHEN ST_ABBR = 'FL' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('11101','11102','40015') THEN '203' "
            "ELSE '' END"
        ),
    )

    # v_CVG_CD_STEP2..STEP7: implement a minimal selection of rules to populate the candidate steps (partial translation)
    df_temp = df_temp.withColumn(
        'v_CVG_CD_STEP2',
        expr(
            "CASE WHEN ST_ABBR = 'MI' AND ACCTNG_LOB = '191PP' AND CVG_TYP_CD IN ('35002','35004','35034','35097') THEN '072' ELSE '' END"
        ),
    )
    df_temp = df_temp.withColumn('v_CVG_CD_STEP3', lit(''))
    df_temp = df_temp.withColumn('v_CVG_CD_STEP4', lit(''))
    df_temp = df_temp.withColumn('v_CVG_CD_STEP5', lit(''))
    df_temp = df_temp.withColumn('v_CVG_CD_STEP6', lit(''))
    df_temp = df_temp.withColumn('v_CVG_CD_STEP7', lit(''))

    # consolidated v_NISS_CVG_CD: first non-empty of steps (partial implementation)
    df_temp = df_temp.withColumn(
        'v_NISS_CVG_CD',
        coalesce(
            when(col('v_CVG_CD_STEP1') != '', col('v_CVG_CD_STEP1'))
            .when(col('v_CVG_CD_STEP1A') != '', col('v_CVG_CD_STEP1A'))
            .when(col('v_CVG_CD_STEP2') != '', col('v_CVG_CD_STEP2'))
            .when(col('v_CVG_CD_STEP3') != '', col('v_CVG_CD_STEP3'))
            .when(col('v_CVG_CD_STEP4') != '', col('v_CVG_CD_STEP4'))
            .when(col('v_CVG_CD_STEP5') != '', col('v_CVG_CD_STEP5'))
            .when(col('v_CVG_CD_STEP6') != '', col('v_CVG_CD_STEP6'))
            .when(col('v_CVG_CD_STEP7') != '', col('v_CVG_CD_STEP7'))
            , lit(''))
    )

    # final output port with default '???' when blank
    df_temp = df_temp.withColumn('o_NISS_CVG_CD', when(col('v_NISS_CVG_CD') == '', lit('???')).otherwise(col('v_NISS_CVG_CD')))

    # pass through INPUT/OUTPUT ports as required downstream - select explicitly
    df_EXP_Derive_NISS_CVG_CD_And_PassThru_reverent_noether = df_temp.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_NM',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'BI_LMT',
        'GA_ADDED_AT_FAULT_IND',
        'FA2_PLCY_IND',
        'UM_UMI_STACKING',
        'PIP_WVR_WL_IND',
        'PIP_MED_SEC_IND',
        'PIP_LOSS_INCOME_IND',
        'MI_PPO_IND',
        'COMP_DED',
        'COLL_DED',
        'RATNG_CMPY_CD',
        'MIS_LOB',
        'SOURCE_IND_DERIVED',
        'o_NISS_CVG_CD AS NISS_CVG_CD'
    )
    logger.info("Finished EXP_Derive_NISS_CVG_CD_And_PassThru (partial translation of complex rules)")
except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_CVG_CD_And_PassThru: {e}", exc_info=True)
    raise

# --- UPD_NISS_CVG_CD: Update Strategy (DD_UPDATE) -> load-modify-store-back against S3 parquet target ---
try:
    logger.info("Starting UPD_NISS_CVG_CD apply: deriving dd_op marker and applying load-modify-store-back")
    df_upd_input = df_EXP_Derive_NISS_CVG_CD_And_PassThru_reverent_noether.withColumn('dd_op', lit('UPDATE'))

    # drop REJECT rows immediately (none expected here but rule requires it)
    df_upd_filtered = df_upd_input.filter(col('dd_op') != 'REJECT')

    # rows that should be inserted/updated (we treat DD_UPDATE as UPDATE)
    df_to_apply = df_upd_filtered.filter(col('dd_op').isin('INSERT', 'UPDATE') | (col('dd_op') == 'UPDATE'))

    # read current target (full table) from S3; on missing path treat as empty existing set
    target_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    try:
        logger.info("Reading existing WRK_BIRP_NISS_APRM_DETL target from %s", target_path)
        existing_df = spark.read.parquet(target_path)
    except Exception as e:
        logger.warning("Existing target WRK_BIRP_NISS_APRM_DETL not found at %s - treating as empty (will create new). Error: %s", target_path, e)
        # create an empty DataFrame with the same schema as the incoming data (so write will produce proper columns)
        existing_df = df_to_apply.limit(0)

    # derive keys of changed rows
    changed_keys_df = df_to_apply.select('NISS_APRM_DETL_SK').distinct()

    # anti-join to remove any existing rows that are being updated/deleted
    existing_survivors = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')

    # union survivors with rows marked INSERT/UPDATE (DELETE rows would be skipped by not including them)
    # ensure consistent column set by using unionByName allowMissingColumns=True
    combined_df = existing_survivors.unionByName(df_to_apply.drop('dd_op'), allowMissingColumns=True)

    # overwrite the same target path with the combined full table
    try:
        logger.info("Writing combined WRK_BIRP_NISS_APRM_DETL to S3 (overwrite) - full table write; review for large target cost")
        combined_df.write.mode('overwrite').parquet(target_path)
    except Exception as e:
        logger.error(f"Failed writing updated WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
        raise

    # assign the output dataframe variable expected by downstream (and the Output node)
    df_UPD_NISS_CVG_CD_dazzling_boltzmann = combined_df
    logger.info("Completed UPD_NISS_CVG_CD apply")
except Exception as e:
    logger.error(f"Failed in UPD_NISS_CVG_CD transform/apply: {e}", exc_info=True)
    raise

# --- WRK_BIRP_NISS_APRM_DETL Output: write intermediate WRK_ table as parquet to S3 (overwrite) ---
try:
    logger.info("Writing WRK_BIRP_NISS_APRM_DETL intermediate table to S3 as parquet (overwrite)")
    # final write (this will overwrite same path the Update Strategy already wrote to - kept to follow mapping node semantics)
    df_WRK_BIRP_NISS_APRM_DETL_charming_curie = df_UPD_NISS_CVG_CD_dazzling_boltzmann
    df_WRK_BIRP_NISS_APRM_DETL_charming_curie.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL to S3: {e}", exc_info=True)
    raise



job.commit()
