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

from pyspark.sql.functions import col, expr, trim, regexp_replace, split, size, element_at, when, lit

# Top-of-script placeholders
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

# -----------------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL1
# Try S3 parquet read first (staged); on failure fall back to Glue Catalog read.
# -----------------------------------------------------------------------------
try:
    logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: try S3 parquet first")
    try:
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_fancy_newton = spark.read.parquet(
            f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
        )
        logger.info("Read Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from S3 parquet")
    except Exception as e_s3:
        logger.warning(
            f"S3 parquet read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 failed, falling back to Glue Catalog read: {e_s3}"
        )
        try:
            logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Glue Catalog")
            dyf = glueContext.create_dynamic_frame_from_catalog(
                database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL"
            )
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_fancy_newton = dyf.toDF()
            logger.info("Read Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Glue Catalog")
        except Exception as e_cat:
            logger.error(
                f"Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from both S3 and Glue Catalog: {e_cat}",
                exc_info=True,
            )
            raise
except Exception as e:
    logger.error(f"Failed preparing source Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Application Source Qualifier: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL
# The SQ has a SQL Override against FDR.WRK_BIRP_NISS_APRM_DETL but that table is
# staged locally above. Register the staged DF as a temp view named WRK_BIRP_NISS_APRM_DETL
# and run the override via spark.sql against that view.
# -----------------------------------------------------------------------------
try:
    logger.info("Registering staged WRK_BIRP_NISS_APRM_DETL as temp view for SQ")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_fancy_newton.createOrReplaceTempView(
        "WRK_BIRP_NISS_APRM_DETL"
    )

    sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    PRD_GRP_CD,
    LTRIM(RTRIM(COMP_DED)) AS COMP_DED,
    LTRIM(RTRIM(COLL_DED)) AS COLL_DED
FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')"""

    logger.info("Executing SQ override against staged temp view for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_keen_galileo = spark.sql(sql_query)
    logger.info("Completed SQ query for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(
        f"Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL override: {e}",
        exc_info=True,
    )
    raise

# -----------------------------------------------------------------------------
# EXP_PassThru: explicit passthrough projection of SQ output columns
# -----------------------------------------------------------------------------
try:
    logger.info("Transform: EXP_PassThru - projecting explicit passthrough columns")
    df_EXP_PassThru_awesome_kant = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_keen_galileo.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'PRD_GRP_CD',
        'COMP_DED',
        'COLL_DED',
    )
    logger.info("Completed EXP_PassThru")
except Exception as e:
    logger.error(f"Failed EXP_PassThru transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_CvgAmount_Split: clean CVG_AMT, split on '/', produce string & decimal parts
# -----------------------------------------------------------------------------
try:
    logger.info("Transform: EXP_CvgAmount_Split - cleaning and splitting CVG_AMT")

    df_base = df_EXP_PassThru_awesome_kant

    # create cleaned version without commas
    df_step = (
        df_base
        .withColumn('v_CVG_AMT', regexp_replace(trim(col('CVG_AMT')), ',', ''))
        .withColumn('CVG_AMT_NO_OF_PARTS', size(split(col('v_CVG_AMT'), '/')))
        .withColumn(
            'CVG_AMT_1_String',
            when(size(split(col('v_CVG_AMT'), '/')) >= 1, element_at(split(col('v_CVG_AMT'), '/'), 1)).otherwise(lit('0')),
        )
        .withColumn(
            'CVG_AMT_2_String',
            when(size(split(col('v_CVG_AMT'), '/')) >= 2, element_at(split(col('v_CVG_AMT'), '/'), 2)).otherwise(lit('0')),
        )
        .withColumn(
            'CVG_AMT_3_String',
            when(size(split(col('v_CVG_AMT'), '/')) >= 3, element_at(split(col('v_CVG_AMT'), '/'), 3)).otherwise(lit('0')),
        )
        # safe decimal conversion: strip commas (already removed) and cast; null on failure
        .withColumn(
            'CVG_AMT_1_Decimal',
            when(col('CVG_AMT_1_String').isNotNull() & (col('CVG_AMT_1_String') != ''), regexp_replace(col('CVG_AMT_1_String'), ',', '').cast('decimal(18,2)')).otherwise(lit(None)),
        )
        .withColumn(
            'CVG_AMT_2_Decimal',
            when(col('CVG_AMT_2_String').isNotNull() & (col('CVG_AMT_2_String') != ''), regexp_replace(col('CVG_AMT_2_String'), ',', '').cast('decimal(18,2)')).otherwise(lit(None)),
        )
        .withColumn(
            'CVG_AMT_3_Decimal',
            when(col('CVG_AMT_3_String').isNotNull() & (col('CVG_AMT_3_String') != ''), regexp_replace(col('CVG_AMT_3_String'), ',', '').cast('decimal(18,2)')).otherwise(lit(None)),
        )
        .withColumn('SRC_CVG_AMT', col('v_CVG_AMT'))
    )

    df_EXP_CvgAmount_Split_brave_babbage = df_step
    logger.info("Completed EXP_CvgAmount_Split")
except Exception as e:
    logger.error(f"Failed EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru
# Join pass-through and split results, then apply DECODE/CASE logic to derive
# NISS_PLCY_LMT_CD and NISS_DEDUC_CD. Project outputs.
# -----------------------------------------------------------------------------
try:
    logger.info("Transform: EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru - joining and deriving policy limit & deduc codes")

    # bring the split columns into scope by joining on the business key
    df_joined = df_EXP_PassThru_awesome_kant.join(
        df_EXP_CvgAmount_Split_brave_babbage.select(
            'NISS_APRM_DETL_SK',
            'CVG_AMT_1_String',
            'CVG_AMT_2_String',
            'CVG_AMT_3_String',
            'CVG_AMT_1_Decimal',
            'CVG_AMT_2_Decimal',
            'CVG_AMT_3_Decimal',
            'CVG_AMT_NO_OF_PARTS',
            'SRC_CVG_AMT',
        ),
        on='NISS_APRM_DETL_SK',
        how='left',
    )

    # Derive NISS_PLCY_LMT_CD using nested CASE WHEN blocks translated from the original DECODE logic.
    # This mirrors the DECODE(1, ... ) patterns for NJ and NY present in the source logic.
    npl_case = (
        "CASE WHEN ST_ABBR = 'NJ' THEN ("
        "CASE "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '10,000/10,000' AND PRD_GRP_CD = 'BA' THEN '28' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('15,000/30,000','15,000/1,000','15,000/2,000','15,000/2,500','15,000/250') THEN '10' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '20,000/30,000' THEN '11' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '20,000/40,000' THEN '12' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('25,000/50,000','25,000/500') THEN '13' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '35,000/35,000' THEN '14' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('50,000/100,000','50,000/2,500','50,000/500') THEN '15' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('100,000/200,000','100,000/500') THEN '16' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '100,000/300,000' THEN '17' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '300,000/300,000' THEN '18' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('250,000/500,000','250,000/1,000','250,000/2,500','250,000/250') THEN '19' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '300,000/500,000' THEN '29' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('500,000/500,000','500,000/500') THEN '20' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '500,000/1,000,000' THEN '21' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '1,000,000/1,000,000' THEN '22' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '1,000,000/2,000,000' THEN '23' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '1,500,000/3,000,000' THEN '24' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '2,500,000/5,000,000' THEN '25' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '5,000,000/10,000,000' THEN '26' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT = '10,000,000/10,000,000' THEN '27' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_AMT IN ('0','1,000','10,000','120','500','5,000','35,000','50,000','100,000','10,000/500','5,000/500','250,000/2,500','750','25,000','50') THEN '17' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '5,000' THEN '30' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '10,000' THEN '31' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('15,000','15,000/2,500','15,000/30,000') THEN '32' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '20,000' THEN '33' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('25,000','25,000/50,000') THEN '34' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '35,000' THEN '35' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('50,000','50,000/100,000') THEN '36' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('100,000','100,000/300,000') THEN '37' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '150,000' THEN '38' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '200,000' THEN '39' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('250,000','250,000/250','250,000/500,000') THEN '96' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '300,000' THEN '97' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT IN ('500,000','500,000/1,000,000','500,000/500,000') THEN '98' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_AMT = '1,000,000' THEN '99' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('15,000/30,000','1,000','5,000','10,000','15,000/2,500') THEN '40' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '20,000/30,000' THEN '41' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '20,000/40,000' THEN '42' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('25,000/50,000','25,000') THEN '43' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '35,000/35,000' THEN '44' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('50,000/100,000','50,000') THEN '45' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('100,000/200,000','100,000') THEN '46' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '100,000/300,000' THEN '47' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '300,000/300,000' THEN '48' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('250,000/500,000','250,000') THEN '49' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT IN ('500,000/500,000','500,000','500,000/500') THEN '50' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '500,000/1,000,000' THEN '51' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '1,000,000/1,000,000' THEN '52' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '1,000,000/2,000,000' THEN '53' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '1,500,000/3,000,000' THEN '54' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '2,500,000/5,000,000' THEN '55' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '5,000,000/10,000,000' THEN '56' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13106' AND CVG_AMT = '10,000,000/10,000,000' THEN '57' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '12100' AND CVG_AMT_1_Decimal = 5000 THEN '75' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '12100' AND CVG_AMT_1_Decimal = 10000 THEN '76' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '12100' AND CVG_AMT_1_Decimal = 15000 THEN '74' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '12100' AND CVG_AMT = '20,000' THEN '33' "
        "WHEN ACCTNG_LOB = '191' AND PRD_GRP_CD = 'BA' THEN '00' "
        "WHEN ACCTNG_LOB = '191' AND PRD_GRP_CD != 'BA' AND CVG_TYP_CD IN ('35050','35051','35058','35059','35103') AND CVG_AMT_1_Decimal = 15000 THEN '01' "
        "WHEN ACCTNG_LOB = '191' AND PRD_GRP_CD != 'BA' THEN '09' "
        "WHEN SUBSTR(LTRIM(RTRIM(ACCTNG_LOB)),1,3) IN ('191','192') THEN '??' "
        "WHEN NOT SUBSTR(LTRIM(RTRIM(ACCTNG_LOB)),1,3) IN ('191','192') THEN ' ' "
        "ELSE '??' END) "
        "WHEN ST_ABBR = 'NY' THEN ("
        "CASE "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '2,000' THEN '02' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '5,000' THEN '03' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '10,000' THEN '04' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '25,000' THEN '05' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '50,000' THEN '06' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '75,000' THEN '07' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT = '100,000' THEN '08' "
        "WHEN ACCTNG_LOB = '192MD' AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 100000 THEN '09' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','13025') AND CVG_AMT = '25,000/50,000' THEN '05' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','13025') AND CVG_AMT = '50,000/100,000' THEN '06' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','13025') AND CVG_AMT = '100,000/200,000' THEN '07' "
        "WHEN ACCTNG_LOB = '192BI' AND CVG_TYP_CD IN ('13003','13023','13025') AND CVG_AMT = '100,000/300,000' THEN '08' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '60,000' THEN '02' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '75,000' THEN '03' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '100,000' THEN '04' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '200,000' THEN '05' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '300,000' THEN '06' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT = '500,000' THEN '07' "
        "WHEN ACCTNG_LOB IN ('192BI','192PD') AND CVG_TYP_CD IN ('13000','13008') AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 500000 THEN '09' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '10,000' THEN '02' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '15,000' THEN '03' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '20,000' THEN '11' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '25,000' THEN '04' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '50,000' THEN '05' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT = '100,000' THEN '06' "
        "WHEN ACCTNG_LOB = '192PD' AND CVG_TYP_CD IN ('13003','13024','13026') AND CVG_AMT_NO_OF_PARTS = 1 AND CVG_AMT_1_Decimal > 100000 THEN '09' "
        "WHEN ACCTNG_LOB = '192PD' THEN '01' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13100' AND CVG_AMT IN ('25,000/50,000','0','25,000','10,000','100,000') THEN '05' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT = '25,000/50,000' THEN '01' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT = '50,000/100,000' THEN '06' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT = '100,000/200,000' THEN '07' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT = '100,000/300,000' THEN '08' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT = '300,000/300,000' THEN '08' "
        "WHEN ACCTNG_LOB = '192UM' AND CVG_TYP_CD = '13116' AND CVG_AMT_NO_OF_PARTS = 2 AND CVG_AMT_1_Decimal > 100000 AND CVG_AMT_2_Decimal > 300000 THEN '09' "
        "WHEN SUBSTR(ACCTNG_LOB,1,3) = '192' THEN '??' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007','35006') THEN '01' "
        "ELSE '??' END) "
        "ELSE '??' END"
    )

    # Derive NISS_DEDUC_CD similarly using a CASE expression translated from the source logic.
    ded_case = (
        "CASE WHEN ST_ABBR = 'NY' THEN ("
        "CASE "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND (CVG_AMT IN ('50,000','0','1','1,000','10,000','100','100,000','25,000','5,000','50','500','750')) THEN '01' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND CVG_AMT = '50,000/100' THEN '02' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND CVG_AMT = '50,000/200' THEN '03' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD = '35023' AND CVG_AMT IN ('25,000/500','50,000/1,000','100,000/2,000') THEN '01' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD = '35023' AND CVG_AMT IN ('25,000/500/100','50,000/1,000/100','100,000/2,000/100') THEN '02' "
        "WHEN ACCTNG_LOB = '191' THEN '99' "
        "ELSE '99' END) "
        "WHEN ST_ABBR = 'NJ' THEN ("
        "CASE "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35051','35059','35050','35058','35103') AND CVG_AMT IN ('15,000/250','50,000/250','75,000/250','150,000/250','250,000/250') THEN '06' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35051','35059','35050','35058','35103') AND CVG_AMT IN ('15,000/500','50,000/500','75,000/500','150,000/500','250,000/500') THEN '07' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35051','35059','35050','35058','35103') AND CVG_AMT IN ('15,000/1,000','50,000/1,000','75,000/1,000','150,000/1,000','250,000/1,000') THEN '12' "
        "WHEN ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35051','35059','35050','35058','35103') AND LTRIM(RTRIM(CVG_AMT)) IN ('0','') THEN 'E7' "
        "WHEN ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND CVG_AMT = '50' THEN '02' "
        "WHEN ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND CVG_AMT = '100' THEN '03' "
        "WHEN ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND CVG_AMT = '150' THEN '04' "
        "WHEN ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND CVG_AMT = '200' THEN '05' "
        "ELSE '99' END) "
        "ELSE '??' END"
    )

    df_with_codes = (
        df_joined
        .withColumn('NISS_PLCY_LMT_CD', expr(npl_case))
        .withColumn('NISS_DEDUC_CD', expr(ded_case))
    )

    # Final projection for this Expression node: include the requested output ports
    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_reverent_tesla = df_with_codes.select(
        'NISS_APRM_DETL_SK',
        'NISS_DEDUC_CD',
        'NISS_PLCY_LMT_CD',
    )

    logger.info("Completed EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru")
except Exception as e:
    logger.error(f"Failed EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# UPD_NISS_PLCY_LMT_CD: Update Strategy -> derive dd_op, drop REJECT, load-modify-store-back
# -----------------------------------------------------------------------------
try:
    logger.info("Applying Update Strategy UPD_NISS_PLCY_LMT_CD: deriving dd_op and preparing changed keys")

    # All rows marked as DD_UPDATE per mapping configuration
    df_upd_marker = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_reverent_tesla.withColumn('dd_op', lit('UPDATE'))

    # Drop rejected rows if any (none expected here)
    df_upd_marker = df_upd_marker.filter(col('dd_op') != 'REJECT')

    # Load the current full target table (WRK_BIRP_NISS_APRM_DETL1) from S3 to apply updates
    try:
        logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL1 from S3 for load-modify-store-back")
        existing_target_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
        logger.info("Successfully read existing target WRK_BIRP_NISS_APRM_DETL1 from S3")
    except Exception as e_read_target:
        # If the target does not exist yet, treat as empty table (first run)
        logger.warning(f"Existing target WRK_BIRP_NISS_APRM_DETL1 could not be read (will assume empty): {e_read_target}")
        existing_target_df = spark.createDataFrame([], df_upd_marker.schema)

    # Compute the keys of changed rows (INSERT/UPDATE/DELETE) - here dd_op == 'UPDATE'
    changed_keys_df = df_upd_marker.select('NISS_APRM_DETL_SK').distinct()

    # Anti-join to remove any existing rows that will be updated/deleted
    existing_survivors = existing_target_df.join(changed_keys_df, on='NISS_APRM_DETL_SK', how='left_anti')

    # Prepare rows to re-insert: only INSERT/UPDATE (drop DELETE rows). Here we keep UPDATE rows.
    rows_to_apply = df_upd_marker.filter(col('dd_op').isin('INSERT', 'UPDATE'))

    # Union the surviving existing rows with the rows to apply, creating the full new table
    df_UPD_NISS_PLCY_LMT_CD_elated_socrates = existing_survivors.unionByName(rows_to_apply, allowMissingColumns=True)

    logger.info("Completed load-modify-store-back apply for UPD_NISS_PLCY_LMT_CD")
except Exception as e:
    logger.error(f"Failed Update Strategy UPD_NISS_PLCY_LMT_CD: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> write as parquet to S3 (overwrite)
# Strip the FDR_LIB_ prefix when composing the S3 path so it writes to the real table name
# -----------------------------------------------------------------------------
try:
    target_path_name = 'WRK_BIRP_NISS_APRM_DETL1'
    logger.info(f"Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to s3://{S3_OUTPUT_BUCKET}/{target_path_name}/ as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_calm_shannon = df_UPD_NISS_PLCY_LMT_CD_elated_socrates
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_calm_shannon.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/{target_path_name}/")
    logger.info("Completed write of FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3")
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
