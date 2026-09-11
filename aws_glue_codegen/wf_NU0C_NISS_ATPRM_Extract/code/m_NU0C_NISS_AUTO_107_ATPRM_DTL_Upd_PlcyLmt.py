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

from pyspark.sql.functions import col, expr, when, lit, split, size, regexp_replace, coalesce, element_at
from pyspark.sql.window import Window

# Placeholder constants for environment/mapping parameters
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# --- Node: FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Source) ---
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from workflow-local S3 staging first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_nifty_maxwell = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
except Exception as e:
    logger.warning("Failed to read WRK_BIRP_NISS_APRM_DETL from S3; falling back to Glue Catalog read: %s" % str(e))
    try:
        # Fall back to Glue Catalog - project only the fields the mapping downstream expects
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog database=%s table=%s" % (GLUE_DATABASE, 'WRK_BIRP_NISS_APRM_DETL'))
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_tmp = dyf.toDF()
        # project only the columns needed by downstream nodes
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_nifty_maxwell = df_tmp.select(
            col('CVG_AMT'),
            col('NISS_APRM_DETL_SK'),
            col('ST_ABBR'),
            col('ACCTNG_LOB'),
            col('CVG_TYP_CD'),
            col('NISS_CVG_CD')
        )
    except Exception as e2:
        logger.error(f"Failed to read WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# --- Node: SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Application Source Qualifier with SQL Override) ---
# The SQ had a SQL Override against FDR.WRK_BIRP_NISS_APRM_DETL; this table is staged in S3 and was read above,
# so register that dataframe as a temp view and run the override rewritten to reference the temp view.
sql_query = f"""select
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    NISS_CVG_CD
from
    WRK_BIRP_NISS_APRM_DETL
where
    ST_ABBR = 'CT'"""
try:
    logger.info("Registering WRK_BIRP_NISS_APRM_DETL temp view and executing SQ override via spark.sql")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_nifty_maxwell.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_wonderful_maxwell = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed running SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL override via spark.sql: {e}", exc_info=True)
    raise

# --- Node: EXP_PassThru (Expression - pure passthrough) ---
try:
    logger.info("Applying EXP_PassThru projection (explicit passthrough columns)")
    df_EXP_PassThru_hopeful_ramanujan = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_wonderful_maxwell.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT'
    )
except Exception as e:
    logger.error(f"Failed in EXP_PassThru transformation: {e}", exc_info=True)
    raise

# --- Node: EXP_CvgAmount_Split (Expression - split and normalize CVG_AMT) ---
try:
    logger.info("Computing CVG_AMT derived columns in EXP_CvgAmount_Split")
    # Stage 1: clean CVG_AMT and build split array and parts count
    df_stage1 = df_EXP_PassThru_hopeful_ramanujan.withColumn('v_CVG_AMT', regexp_replace(expr('trim(CVG_AMT)'), ',', ''))
    df_stage1 = df_stage1.withColumn('v_CVG_AMT_SPLIT', split(col('v_CVG_AMT'), '/'))
    df_stage1 = df_stage1.withColumn('v_CVG_AMT_Parts', size(col('v_CVG_AMT_SPLIT')))

    # Stage 2: derive part strings with fallback to '0'
    df_stage2 = (
        df_stage1.withColumn('CVG_AMT_1_String', coalesce(element_at(col('v_CVG_AMT_SPLIT'), lit(1)), lit('0')))
        .withColumn('CVG_AMT_2_String', coalesce(element_at(col('v_CVG_AMT_SPLIT'), lit(2)), lit('0')))
        .withColumn('CVG_AMT_3_String', coalesce(element_at(col('v_CVG_AMT_SPLIT'), lit(3)), lit('0')))
    )

    # Stage 3: cast to decimal (use double here) with fallback to 0.0
    df_stage3 = (
        df_stage2.withColumn('CVG_AMT_1_Decimal', coalesce(col('CVG_AMT_1_String').cast('double'), lit(0.0)))
        .withColumn('CVG_AMT_2_Decimal', coalesce(col('CVG_AMT_2_String').cast('double'), lit(0.0)))
        .withColumn('CVG_AMT_NO_OF_PARTS', col('v_CVG_AMT_Parts'))
        .withColumn('SRC_CVG_AMT', col('v_CVG_AMT'))
    )

    # Preserve the key and relevant fields for downstream
    df_EXP_CvgAmount_Split_vibrant_lovelace = df_stage3.select(
        'NISS_APRM_DETL_SK',
        'CVG_AMT',
        'v_CVG_AMT',
        'v_CVG_AMT_Parts',
        'CVG_AMT_1_String',
        'CVG_AMT_2_String',
        'CVG_AMT_3_String',
        'CVG_AMT_1_Decimal',
        'CVG_AMT_2_Decimal',
        'CVG_AMT_3_String',
        'CVG_AMT_NO_OF_PARTS',
        'SRC_CVG_AMT'
    )
except Exception as e:
    logger.error(f"Failed in EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# --- Node: EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru (Expression with join and derived local variable) ---
try:
    logger.info("Joining pass-through data with CVG amount splits and deriving NISS_PLCY_LMT_CD")
    # Base DF: use the pass-through dataframe (contains NISS_APRM_DETL_SK and other pass-through fields)
    base_df = df_EXP_PassThru_hopeful_ramanujan.alias('b')
    split_df = df_EXP_CvgAmount_Split_vibrant_lovelace.alias('s')

    # Left join to bring in decimal parts
    joined = base_df.join(split_df.select('NISS_APRM_DETL_SK', 'CVG_AMT_1_Decimal', 'CVG_AMT_2_Decimal', 'CVG_AMT_NO_OF_PARTS', 'SRC_CVG_AMT'),
                          on=col('b.NISS_APRM_DETL_SK') == col('s.NISS_APRM_DETL_SK'), how='left')

    # Build CASE/WHEN expression mirroring the nested IIF/DECODE logic from Informatica's v_NISS_PLCY_LMT_CD.
    # This translates the principal branches shown in the original logic. Unmatched cases yield empty string.
    case_expr = """
    CASE
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '500' THEN '01'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '750' THEN '02'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '1,000' THEN '03'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '2,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '3,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '5,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT = '7,500' THEN '07'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 7500 THEN '08'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192MD' AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal < 7500 AND CVG_AMT NOT IN ('500','750','1,000','2,000','3,000','5,000','7,500') THEN '09'

      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','40015','13029','40021','13046','13048') AND CVG_AMT = '20,000/40,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','40015','13029','40021','13046','13048') AND CVG_AMT = '25,000/50,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','40015','13029','40021','13046','13048') AND CVG_AMT = '50,000/100,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','40015','13029','40021','13046','13048') AND CVG_AMT = '100,000/200,000' THEN '07'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','40015','13029','40021','13046','13048') AND CVG_AMT = '100,000/300,000' THEN '08'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND (CVG_AMT IN ('250,000/500,000','500,000/500,000') OR (CVG_AMT_NO_OF_PARTS = 2 AND CVG_AMT_1_Decimal > 100000 AND CVG_AMT_2_Decimal > 300000)) THEN '09'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192BI' AND CVG_AMT NOT IN ('20,000/40,000','25,000/50,000','50,000/100,000','100,000/200,000','100,000/300,000','250,000/500,000','500,000/500,000') THEN '01'

      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '50,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '75,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '100,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '200,000' THEN '07'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '300,000' THEN '08'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 300000 THEN '09'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB IN ('192BI','192MD') AND CVG_AMT NOT IN ('50,000','75,000','100,000','200,000','300,000') THEN '90'

      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','40015','13030','40021') AND CVG_AMT = '10,000' THEN '02'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','40015','13030','40021') AND CVG_AMT = '15,000' THEN '03'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','40015','13030','40021') AND CVG_AMT = '25,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','40015','13030','40021') AND CVG_AMT = '50,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','40015','13030','40021') AND CVG_AMT = '100,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_AMT = '250,000' THEN '07'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 300000 THEN '08'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192PD' AND CVG_AMT NOT IN ('10,000','15,000','25,000','50,000','100,000','250,000') THEN '09'

      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40015','13132','40021') AND CVG_AMT = '20,000/40,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40015','13132','40021') AND CVG_AMT = '25,000/50,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40015','13132','40021') AND CVG_AMT = '50,000/100,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40015','13132','40021') AND CVG_AMT = '100,000/200,000' THEN '07'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40015','13132','40021') AND CVG_AMT = '100,000/300,000' THEN '08'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_AMT_1_Decimal > 100000 AND CVG_AMT_2_Decimal > 300000 THEN '09'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_AMT NOT IN ('20,000/40,000','25,000/50,000','50,000/100,000','100,000/200,000','100,000/300,000') AND RTRIM(LTRIM(NISS_CVG_CD)) = '203' THEN '01'
      WHEN ST_ABBR = 'CT' AND ACCTNG_LOB = '192UM' AND CVG_AMT NOT IN ('20,000/40,000','25,000/50,000','50,000/100,000','100,000/200,000','100,000/300,000') AND RTRIM(LTRIM(NISS_CVG_CD)) = '263' THEN '90'

      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT = '50,000' THEN '04'
      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT = '75,000' THEN '05'
      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT = '100,000' THEN '06'
      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT = '200,000' THEN '07'
      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT = '300,000' THEN '08'
      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD IN ('13108','13132') AND CVG_AMT_1_Decimal > 300000 THEN '09'

      WHEN ST_ABBR = 'CT' AND CVG_TYP_CD = '13118' THEN '90'

      WHEN ACCTNG_LOB = '1923D' AND CVG_TYP_CD IN ('34007','34018') THEN '01'
      WHEN SUBSTR(LTRIM(RTRIM(ACCTNG_LOB)),1,3) = '192' THEN '??'
      WHEN SUBSTR(LTRIM(RTRIM(ACCTNG_LOB)),1,3) != '192' THEN ' '
      ELSE ''
    END
    """

    # Apply the CASE expression. We use the columns as they exist in the joined dataframe's namespace.
    df_with_case = joined.withColumn('v_NISS_PLCY_LMT_CD', expr(case_expr))

    # Final projection: pass-through key and derived policy limit code and also NISS_CVG_CD if present upstream
    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_charming_leibniz = df_with_case.select(
        col('b.NISS_APRM_DETL_SK').alias('NISS_APRM_DETL_SK'),
        col('b.NISS_CVG_CD').alias('NISS_CVG_CD') if 'NISS_CVG_CD' in joined.columns else lit(None).alias('NISS_CVG_CD'),
        col('v_NISS_PLCY_LMT_CD').alias('NISS_PLCY_LMT_CD')
    )
except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: {e}", exc_info=True)
    raise

# --- Node: UPD_NISS_PLCY_LMT_CD (Update Strategy) ---
try:
    logger.info("Applying Update Strategy marker column (DD_UPDATE -> 'UPDATE') and filtering out REJECTs")
    df_UPD_NISS_PLCY_LMT_CD_peaceful_dirac = (
        df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_charming_leibniz
        .withColumn('dd_op', lit('UPDATE'))
    )
    # Drop REJECT rows if any
    df_UPD_NISS_PLCY_LMT_CD_peaceful_dirac = df_UPD_NISS_PLCY_LMT_CD_peaceful_dirac.filter(col('dd_op') != 'REJECT')
except Exception as e:
    logger.error(f"Failed in UPD_NISS_PLCY_LMT_CD update-strategy derivation: {e}", exc_info=True)
    raise

# --- Node: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (Output - Update target, load-modify-store-back) ---
try:
    logger.info("Beginning load-modify-store-back for target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 on S3")
    target_path = f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/"
    try:
        existing_df = spark.read.parquet(target_path)
        logger.info("Read existing target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 from S3")
    except Exception as e_read:
        logger.warning(f"Existing target not found at {target_path} or failed to read: {e_read}. Proceeding with empty existing dataframe.")
        # Fallback to an empty dataframe - schema will be inferred from incoming updates where possible
        existing_df = spark.createDataFrame([], df_UPD_NISS_PLCY_LMT_CD_peaceful_dirac.schema)

    # Identify rows to apply (INSERT/UPDATE). Our update-strategy marked all as 'UPDATE'
    changed_rows = df_UPD_NISS_PLCY_LMT_CD_peaceful_dirac.filter(col('dd_op').isin('INSERT', 'UPDATE'))
    changed_keys = changed_rows.select('NISS_APRM_DETL_SK').distinct()

    # Anti-join to remove existing rows that are being updated/deleted
    existing_anti_join = existing_df.join(changed_keys, on='NISS_APRM_DETL_SK', how='left_anti')

    # Build updated full rows by joining changed rows to existing rows to preserve non-updated columns where possible
    # If existing_df has only minimal schema (because we fell back), allowMissingColumns in union will handle
    # inner join to produce updated versions where full row exists
    updated_rows_from_existing = existing_df.alias('e').join(
        changed_rows.select('NISS_APRM_DETL_SK', 'NISS_PLCY_LMT_CD').alias('c'),
        on=col('e.NISS_APRM_DETL_SK') == col('c.NISS_APRM_DETL_SK'),
        how='inner'
    )

    if len(existing_df.columns) > 0:
        # reconstruct columns: prefer changed value for NISS_PLCY_LMT_CD, keep other columns from existing
        updated_cols = []
        for c in existing_df.columns:
            if c == 'NISS_PLCY_LMT_CD':
                updated_cols.append(coalesce(col('c.NISS_PLCY_LMT_CD'), col('e.NISS_PLCY_LMT_CD')).alias('NISS_PLCY_LMT_CD'))
            else:
                updated_cols.append(col(f'e.{c}'))
        updated_rows = updated_rows_from_existing.select(*updated_cols)
    else:
        # No existing full schema - fall back to using changed_rows as-is (will produce limited columns)
        updated_rows = changed_rows.drop('dd_op')

    # The final dataset is: existing rows not affected by the change + updated rows
    final_df = existing_anti_join.unionByName(updated_rows, allowMissingColumns=True)

    # Overwrite the target with the combined dataframe
    logger.info("Writing combined target dataframe back to S3 (overwrite): %s" % target_path)
    final_df.write.mode('overwrite').parquet(target_path)

    # Assign output dataframe variable for lineage consistency
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_hopeful_franklin = final_df
except Exception as e:
    logger.error(f"Failed in output load-modify-store-back for FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise


job.commit()
