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
GLUE_CATALOG_DATABASE = "REPLACE_WITH_GLUE_CATALOG_DATABASE"

from pyspark.sql.functions import col, when, regexp_replace, trim, length, instr, locate, substring, lit, expr, broadcast

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL_1 from S3 parquet first")
    df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_busy_aristotle = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_1/"
    )
except Exception as e:
    logger.warning("Could not read WRK_BIRP_NISS_APRM_DETL_1 from S3, falling back to Glue Catalog read: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog database %s" % GLUE_CATALOG_DATABASE)
        dyf_tmp = glueContext.create_dynamic_frame.from_catalog(database=GLUE_CATALOG_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL_1")
        df_tmp = dyf_tmp.toDF()
        # project exactly the listed fields
        df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_busy_aristotle = df_tmp.select(
            col("NISS_APRM_DETL_SK"),
            col("ST_ABBR"),
            col("ACCTNG_LOB"),
            col("BI_LMT"),
            col("PRD_GRP_CD"),
            col("NJ_NO_LWST_LMT_IND"),
            col("NJ_NMD_DRVR_EXCL_IND"),
            col("CVG_TYP_CD")
        )
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL_1 from Glue Catalog: {e2}", exc_info=True)
        raise

# SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1: SQL Override bypassing the upstream Source node
sql_query = f"""SELECT
NISS_APRM_DETL_SK,
ST_ABBR,
ACCTNG_LOB,
BI_LMT,
PRD_GRP_CD,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
CVG_TYP_CD

FROM FDR.WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')"""

# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 (bypassed — SQL Override reads the table directly)
try:
    logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake via JDBC override query (bypasses Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1)")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_peaceful_newton = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake: {e}", exc_info=True)
    raise

# EXP_BILimit_Split: parse BI_LMT into parts and numeric decimals
try:
    logger.info("Starting EXP_BILimit_Split transformations")
    df_tmp = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_peaceful_newton

    # v_BI_LMT: trim and remove commas
    df_tmp = df_tmp.withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ',', ''))

    # number of parts = length - length(replace(...,'/','')) + 1
    df_tmp = df_tmp.withColumn(
        "v_BI_LMT_Parts",
        (length(trim(col("v_BI_LMT"))) - length(regexp_replace(trim(col("v_BI_LMT")), '/', ''))) + lit(1)
    )

    # positions of first and second '/'
    df_tmp = df_tmp.withColumn("v_BI_LMT_Part1_Pos", locate('/', col("v_BI_LMT"), 1))
    df_tmp = df_tmp.withColumn("v_BI_LMT_Part2_Pos", locate('/', col("v_BI_LMT"), col("v_BI_LMT_Part1_Pos") + 1))

    # v_Limit_FIELD1
    df_tmp = df_tmp.withColumn(
        "v_Limit_FIELD1",
        when(col("v_BI_LMT_Parts") == 1, col("v_BI_LMT"))
        .when((col("v_BI_LMT_Parts") == 2) | (col("v_BI_LMT_Parts") == 3), substring(trim(col("v_BI_LMT")), 1, col("v_BI_LMT_Part1_Pos") - 1))
        .otherwise(lit('0'))
    )

    # v_Limit_FIELD2
    df_tmp = df_tmp.withColumn(
        "v_Limit_FIELD2",
        when(col("v_BI_LMT_Parts") == 1, lit('0'))
        .when(col("v_BI_LMT_Parts") == 2, substring(col("v_BI_LMT"), col("v_BI_LMT_Part1_Pos") + 1, 1000))
        .when(col("v_BI_LMT_Parts") == 3, substring(col("v_BI_LMT"), col("v_BI_LMT_Part1_Pos") + 1, col("v_BI_LMT_Part2_Pos") - col("v_BI_LMT_Part1_Pos") - 1))
        .otherwise(lit('0'))
    )

    # v_Limit_FIELD3
    df_tmp = df_tmp.withColumn(
        "v_Limit_FIELD3",
        when(col("v_BI_LMT_Parts") == 1, lit('0'))
        .when(col("v_BI_LMT_Parts") == 2, lit('0'))
        .when(col("v_BI_LMT_Parts") == 3, substring(trim(col("v_BI_LMT")), col("v_BI_LMT_Part2_Pos") + 1, 1000))
        .otherwise(lit('0'))
    )

    # Cast parts to decimals
    df_tmp = df_tmp.withColumn("BI_LMT_1_Decimal", col("v_Limit_FIELD1").cast("decimal(38,0)"))
    df_tmp = df_tmp.withColumn("BI_LMT_2_Decimal", col("v_Limit_FIELD2").cast("decimal(38,0)"))
    df_tmp = df_tmp.withColumn("BI_LMT_3_Decimal", col("v_Limit_FIELD3").cast("decimal(38,0)"))

    # Outputs: BI_LMT_NO_OF_PARTS and SRC_BI_LMT plus passthroughs retained
    df_EXP_BILimit_Split_jovial_archimedes = df_tmp.select(
        col("NISS_APRM_DETL_SK"),
        col("BI_LMT"),
        col("v_BI_LMT").alias("SRC_BI_LMT"),
        col("BI_LMT_1_Decimal"),
        col("BI_LMT_2_Decimal"),
        col("BI_LMT_3_Decimal"),
        col("v_BI_LMT_Parts").alias("BI_LMT_NO_OF_PARTS")
    )
except Exception as e:
    logger.error(f"Failed in EXP_BILimit_Split transformations: {e}", exc_info=True)
    raise

# EXP_Derive_NISS_SUBLOB_CD_And_PassThru: join BI limit split with SQ and derive NISS_SUBLOB_CD
try:
    logger.info("Starting EXP_Derive_NISS_SUBLOB_CD_And_PassThru transformations (join + derive)")
    df_left = df_EXP_BILimit_Split_jovial_archimedes
    df_right = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_peaceful_newton

    # join on NISS_APRM_DETL_SK
    df_joined = df_left.join(df_right, on=["NISS_APRM_DETL_SK"], how='inner')

    # derive NJ/Y flags
    df_joined = df_joined.withColumn(
        "v_NJ_NO_LWST_LMT_IND",
        when(col("NJ_NO_LWST_LMT_IND") == 1, lit('Y')).otherwise(lit('N'))
    )
    df_joined = df_joined.withColumn(
        "v_NJ_NMD_DRVR_EXCL_IND",
        when(col("NJ_NMD_DRVR_EXCL_IND") == 1, lit('Y')).otherwise(lit('N'))
    )

    # helper: three-char prefix of ACCTNG_LOB after trim
    df_joined = df_joined.withColumn("acct_lob_pref", substring(trim(col("ACCTNG_LOB")), 1, 3))

    # v_NISS_SUBLOB_CD_NewJersey
    df_joined = df_joined.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref").isin('191', '192')) & (col("PRD_GRP_CD") != 'BA') & (col("v_NJ_NO_LWST_LMT_IND") == 'Y'),
            lit('7')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref").isin('191', '192')) & (col("PRD_GRP_CD") != 'BA') & (col("v_NJ_NO_LWST_LMT_IND") != 'Y'),
            lit('8')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref").isin('191', '192')) & (col("PRD_GRP_CD") == 'BA'),
            lit('9')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref") == '211') & (col("PRD_GRP_CD") != 'BA') & (col("v_NJ_NMD_DRVR_EXCL_IND") != 'Y'),
            lit('1')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref") == '211') & (col("PRD_GRP_CD") != 'BA') & (col("v_NJ_NMD_DRVR_EXCL_IND") == 'Y'),
            lit('2')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref") == '211') & (col("PRD_GRP_CD") == 'BA') & (col("v_NJ_NMD_DRVR_EXCL_IND") != 'Y'),
            lit('3')
        ).when(
            (col("ST_ABBR") == 'NJ') & (col("acct_lob_pref") == '211') & (col("PRD_GRP_CD") == 'BA') & (col("v_NJ_NMD_DRVR_EXCL_IND") == 'Y'),
            lit('4')
        ).when(
            col("ST_ABBR") == 'NJ',
            lit('?')
        ).otherwise(lit('?'))
    )

    # v_NISS_SUBLOB_CD_NewYork
    df_joined = df_joined.withColumn(
        "v_NISS_SUBLOB_CD_NewYork",
        when(
            (col("ST_ABBR") == 'NY') & (col("acct_lob_pref").isin('191', '192')) & (~col("CVG_TYP_CD").isin('13000', '13006', '13007', '13008')),
            lit('1')
        ).when(
            (col("ST_ABBR") == 'NY') & (col("acct_lob_pref").isin('191', '192')) & (col("CVG_TYP_CD").isin('13000', '13006', '13007', '13008')),
            lit('2')
        ).when(
            (col("ST_ABBR") == 'NY') & (col("acct_lob_pref") == '211'),
            lit('0')
        ).when(
            col("ST_ABBR") == 'NY',
            lit('?')
        ).otherwise(lit('?'))
    )

    # v_NISS_SUBLOB_CD combined by ST_ABBR
    df_joined = df_joined.withColumn(
        "v_NISS_SUBLOB_CD",
        when(col("ST_ABBR") == 'NY', col("v_NISS_SUBLOB_CD_NewYork"))
        .when(col("ST_ABBR") == 'NJ', col("v_NISS_SUBLOB_CD_NewJersey"))
        .otherwise(lit('?'))
    )

    # final output NISS_SUBLOB_CD
    df_joined = df_joined.withColumn(
        "NISS_SUBLOB_CD",
        when(col("v_NISS_SUBLOB_CD") == '', lit('?')).otherwise(col("v_NISS_SUBLOB_CD"))
    )

    # Select outputs: pass through NISS_APRM_DETL_SK, CVG_TYP_CD and derived NISS_SUBLOB_CD
    df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_humble_shannon = df_joined.select(
        col("NISS_APRM_DETL_SK"),
        col("CVG_TYP_CD"),
        col("NISS_SUBLOB_CD")
    )
except Exception as e:
    logger.error(f"Failed in EXP_Derive_NISS_SUBLOB_CD_And_PassThru: {e}", exc_info=True)
    raise

# UPD_NISS_SUBLOB_CD: Update Strategy - apply UPDATEs back to Snowflake target table
try:
    logger.info("Starting Update Strategy UPD_NISS_SUBLOB_CD: marking dd_op and preparing to apply to target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
    # mark every incoming row as UPDATE per 'DD_UPDATE'
    df_marked = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_humble_shannon.withColumn("dd_op", lit('UPDATE'))

    # drop REJECT rows if any
    df_survivors = df_marked.filter(col("dd_op") != 'REJECT')

    # read current full target table from Snowflake via JDBC
    try:
        logger.info("Reading current full target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 from Snowflake for load-modify-store-back")
        existing_df = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("dbtable", "FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            .load()
        )
    except Exception as e:
        logger.error(f"Failed reading existing target FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 from Snowflake: {e}", exc_info=True)
        raise

    # keys of changed rows
    changed_keys_df = df_survivors.select("NISS_APRM_DETL_SK").dropDuplicates()

    # anti-join to remove existing rows that will be updated/deleted
    existing_remaining = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how='left_anti')

    # prepare incoming rows to re-union: only INSERT/UPDATE (DELETE would be dropped). Here all are UPDATE.
    incoming_for_apply = df_survivors.drop("dd_op")

    # union the surviving existing rows with incoming updates/inserts
    combined_df = existing_remaining.unionByName(incoming_for_apply, allowMissingColumns=True)

    # write the combined full-table dataframe back to Snowflake (overwrite) - this applies the updates
    try:
        logger.info("Writing combined full target back to Snowflake table FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 via JDBC (overwrite) - note: this is a full-table overwrite apply pattern")
        combined_df.write.jdbc(
            url=SNOWFLAKE_URL,
            table='FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1',
            mode='overwrite',
            properties={"user": SNOWFLAKE_USER, "password": SNOWFLAKE_PASSWORD}
        )
    except Exception as e:
        logger.error(f"Failed writing updated full table to Snowflake FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: {e}", exc_info=True)
        raise

    # assign the final combined dataframe (without dd_op) to the node's df_name
    df_UPD_NISS_SUBLOB_CD_gentle_descartes = combined_df

except Exception as e:
    logger.error(f"Failed in Update Strategy UPD_NISS_SUBLOB_CD: {e}", exc_info=True)
    raise

# write intermediate FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)
# write intermediate FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 as parquet to S3 (overwrite)
try:
    logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_relaxed_maxwell = df_UPD_NISS_SUBLOB_CD_gentle_descartes
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_relaxed_maxwell.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/"
    )
except Exception as e:
    logger.error(f"Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
