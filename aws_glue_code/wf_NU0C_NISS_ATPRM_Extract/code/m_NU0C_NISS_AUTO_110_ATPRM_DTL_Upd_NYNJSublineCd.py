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


# placeholder for environment/run-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# -----------------------------------------------------------------------------
# Source: Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 -> df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat
# This is a WRK_ intermediate: try S3 parquet first, fall back to Snowflake JDBC read and project the exact listed ports.
sql_query_src = f"""select
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    BI_LMT,
    PRD_GRP_CD,
    NJ_NO_LWST_LMT_IND,
    NJ_NMD_DRVR_EXCL_IND,
    CVG_TYP_CD
from
    FDR.WRK_BIRP_NISS_APRM_DETL
"""
try:
    logger.info("Attempting to read staged Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from S3 (preferred for WRK_ intermediates)")
    df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL_1/"
    )
except Exception as e_src_s3:
    logger.warning(
        f"Staged S3 read for WRK_BIRP_NISS_APRM_DETL_1 failed or not present; falling back to Snowflake JDBC read: {e_src_s3}"
    )
    try:
        logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake via JDBC as fallback")
        df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat = (
            spark.read.format("jdbc")
            .option("url", SNOWFLAKE_URL)
            .option("user", SNOWFLAKE_USER)
            .option("password", SNOWFLAKE_PASSWORD)
            .option("query", sql_query_src)
            .load()
        )
    except Exception as e_src_jdbc:
        logger.error(f"Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake fallback: {e_src_jdbc}", exc_info=True)
        raise

# create temp view named after the real table used in the SQ override so the SQ can run against the staged dataframe
try:
    df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
except Exception as e:
    logger.error(f"Failed creating temp view WRK_BIRP_NISS_APRM_DETL from df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Source Qualifier: SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 -> df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur
# This SQ had a SQL override referencing FDR.WRK_BIRP_NISS_APRM_DETL. We have a staged temp view above, so run the override via spark.sql against that view (staged case).
sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    BI_LMT,
    PRD_GRP_CD,
    NJ_NO_LWST_LMT_IND,
    NJ_NMD_DRVR_EXCL_IND,
    CVG_TYP_CD

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')"""
try:
    logger.info("Executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 override via spark.sql against staged temp view")
    df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 as spark.sql: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_BILimit_Split -> df_EXP_BILimit_Split_brave_leibniz
try:
    logger.info("Transforming EXP_BILimit_Split: splitting BI_LMT into parts and converting to decimals")
    from pyspark.sql.functions import trim, regexp_replace, split, size, element_at, when, col, lit

    df_src = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur

    # cleaned BI_LMT without commas
    df1 = df_src.withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ",", ""))
    # split into array parts by '/'
    df1 = df1.withColumn("v_BI_LMT_Parts_Array", split(col("v_BI_LMT"), "/"))
    df1 = df1.withColumn("BI_LMT_NO_OF_PARTS", size(col("v_BI_LMT_Parts_Array")))

    # textual tokens with default '0' when missing
    df1 = df1.withColumn(
        "BI_LMT_1_Text",
        when(col("BI_LMT_NO_OF_PARTS") >= 1, element_at(col("v_BI_LMT_Parts_Array"), 1)).otherwise(lit("0")),
    )
    df1 = df1.withColumn(
        "BI_LMT_2_Text",
        when(col("BI_LMT_NO_OF_PARTS") >= 2, element_at(col("v_BI_LMT_Parts_Array"), 2)).otherwise(lit("0")),
    )
    df1 = df1.withColumn(
        "BI_LMT_3_Text",
        when(col("BI_LMT_NO_OF_PARTS") >= 3, element_at(col("v_BI_LMT_Parts_Array"), 3)).otherwise(lit("0")),
    )

    # safe numeric conversion: only convert when token is all digits, otherwise NULL
    df1 = df1.withColumn(
        "BI_LMT_1_Decimal",
        when(col("BI_LMT_1_Text").rlike('^[0-9]+$'), col("BI_LMT_1_Text").cast("decimal(14,0)")).otherwise(lit(None)),
    )
    df1 = df1.withColumn(
        "BI_LMT_2_Decimal",
        when(col("BI_LMT_2_Text").rlike('^[0-9]+$'), col("BI_LMT_2_Text").cast("decimal(14,0)")).otherwise(lit(None)),
    )
    df1 = df1.withColumn(
        "BI_LMT_3_Decimal",
        when(col("BI_LMT_3_Text").rlike('^[0-9]+$'), col("BI_LMT_3_Text").cast("decimal(14,0)")).otherwise(lit(None)),
    )

    # expose cleaned source and final outputs, preserve passthrough input columns needed downstream
    df_EXP_BILimit_Split_brave_leibniz = df1.select(
        "NISS_APRM_DETL_SK",
        "ST_ABBR",
        "ACCTNG_LOB",
        "BI_LMT",
        "PRD_GRP_CD",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "CVG_TYP_CD",
        "BI_LMT_1_Decimal",
        "BI_LMT_2_Decimal",
        "BI_LMT_3_Decimal",
        "BI_LMT_NO_OF_PARTS",
        col("v_BI_LMT").alias("SRC_BI_LMT"),
    )

except Exception as e:
    logger.error(f"Failed transforming EXP_BILimit_Split: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Expression: EXP_Derive_NISS_SUBLOB_CD_And_PassThru -> df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_magical_hilbert
try:
    logger.info("Transforming EXP_Derive_NISS_SUBLOB_CD_And_PassThru: deriving NISS_SUBLOB_CD using NJ/NY logic and passing thru columns")
    from pyspark.sql.functions import trim, substring, col, when, lit

    # start from the BI-split result
    df_in = df_EXP_BILimit_Split_brave_leibniz

    # derive NJ indicator flags
    df2 = df_in.withColumn(
        "v_NJ_NO_LWST_LMT_IND",
        when(col("NJ_NO_LWST_LMT_IND") == 1, lit("Y")).otherwise(lit("N")),
    )
    df2 = df2.withColumn(
        "v_NJ_NMD_DRVR_EXCL_IND",
        when(col("NJ_NMD_DRVR_EXCL_IND") == 1, lit("Y")).otherwise(lit("N")),
    )

    # helper: first 3 chars of trimmed ACCTNG_LOB
    acct_lob_prefix = substring(trim(col("ACCTNG_LOB")), 1, 3)

    # New Jersey DECODE-style chain implemented as ordered when/otherwise updates
    df2 = df2.withColumn("v_NISS_SUBLOB_CD_NewJersey", lit(None).cast("string"))
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (acct_lob_prefix.isin("191", "192"))
            & (col("PRD_GRP_CD") != "BA")
            & (col("v_NJ_NO_LWST_LMT_IND") == "Y"),
        lit("7")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (acct_lob_prefix.isin("191", "192"))
            & (col("PRD_GRP_CD") != "BA")
            & (col("v_NJ_NO_LWST_LMT_IND") != "Y"),
        lit("8")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (acct_lob_prefix.isin("191", "192"))
            & (col("PRD_GRP_CD") == "BA"),
        lit("9")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (substring(trim(col("ACCTNG_LOB")), 1, 3) == "211")
            & (col("PRD_GRP_CD") != "BA")
            & (col("v_NJ_NMD_DRVR_EXCL_IND") != "Y"),
        lit("1")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (substring(trim(col("ACCTNG_LOB")), 1, 3) == "211")
            & (col("PRD_GRP_CD") != "BA")
            & (col("v_NJ_NMD_DRVR_EXCL_IND") == "Y"),
        lit("2")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (substring(trim(col("ACCTNG_LOB")), 1, 3) == "211")
            & (col("PRD_GRP_CD") == "BA")
            & (col("v_NJ_NMD_DRVR_EXCL_IND") != "Y"),
        lit("3")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(
            (col("ST_ABBR") == "NJ")
            & (substring(trim(col("ACCTNG_LOB")), 1, 3) == "211")
            & (col("PRD_GRP_CD") == "BA")
            & (col("v_NJ_NMD_DRVR_EXCL_IND") == "Y"),
        lit("4")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )
    # default for NJ rows is '?'
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewJersey",
        when(col("ST_ABBR") == "NJ", when(col("v_NISS_SUBLOB_CD_NewJersey").isNull(), lit("?")).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))).otherwise(col("v_NISS_SUBLOB_CD_NewJersey"))
    )

    # New York DECODE-style chain
    df2 = df2.withColumn("v_NISS_SUBLOB_CD_NewYork", lit(None).cast("string"))
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewYork",
        when(
            (col("ST_ABBR") == "NY")
            & (acct_lob_prefix.isin("191", "192"))
            & (~col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
        lit("1")).otherwise(col("v_NISS_SUBLOB_CD_NewYork"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewYork",
        when(
            (col("ST_ABBR") == "NY")
            & (acct_lob_prefix.isin("191", "192"))
            & (col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
        lit("2")).otherwise(col("v_NISS_SUBLOB_CD_NewYork"))
    )
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewYork",
        when(
            (col("ST_ABBR") == "NY")
            & (substring(trim(col("ACCTNG_LOB")), 1, 3) == "211"),
        lit("0")).otherwise(col("v_NISS_SUBLOB_CD_NewYork"))
    )
    # default for NY rows is '?'
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD_NewYork",
        when(col("ST_ABBR") == "NY", when(col("v_NISS_SUBLOB_CD_NewYork").isNull(), lit("?")).otherwise(col("v_NISS_SUBLOB_CD_NewYork"))).otherwise(col("v_NISS_SUBLOB_CD_NewYork"))
    )

    # choose between NY and NJ computed values, default '?'
    df2 = df2.withColumn(
        "v_NISS_SUBLOB_CD",
        when(col("ST_ABBR") == "NY", col("v_NISS_SUBLOB_CD_NewYork")).otherwise(when(col("ST_ABBR") == "NJ", col("v_NISS_SUBLOB_CD_NewJersey")).otherwise(lit("?")))
    )

    # final output port enforces '?' when empty
    df2 = df2.withColumn(
        "NISS_SUBLOB_CD",
        when((col("v_NISS_SUBLOB_CD") == "") | col("v_NISS_SUBLOB_CD").isNull(), lit("?")).otherwise(col("v_NISS_SUBLOB_CD")),
    )

    # project final set of ports (preserve passthrough ports used downstream)
    df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_magical_hilbert = df2.select(
        "NISS_APRM_DETL_SK",
        "ST_ABBR",
        "ACCTNG_LOB",
        "BI_LMT",
        "PRD_GRP_CD",
        "NJ_NO_LWST_LMT_IND",
        "NJ_NMD_DRVR_EXCL_IND",
        "CVG_TYP_CD",
        "BI_LMT_1_Decimal",
        "BI_LMT_NO_OF_PARTS",
        "NISS_SUBLOB_CD",
    )

except Exception as e:
    logger.error(f"Failed transforming EXP_Derive_NISS_SUBLOB_CD_And_PassThru: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Update Strategy: UPD_NISS_SUBLOB_CD -> df_UPD_NISS_SUBLOB_CD_eager_hilbert
try:
    logger.info("Applying Update Strategy UPD_NISS_SUBLOB_CD: marking rows with dd_op and dropping REJECTs")
    from pyspark.sql.functions import lit, col

    # metadata indicates DD_UPDATE at transformation level -> mark as UPDATE
    df_UPD_NISS_SUBLOB_CD_eager_hilbert = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_magical_hilbert.withColumn(
        "dd_op",
        lit("UPDATE"),
    )

    # drop REJECT rows if any (none expected at design time but apply rule)
    df_UPD_NISS_SUBLOB_CD_eager_hilbert = df_UPD_NISS_SUBLOB_CD_eager_hilbert.filter(col("dd_op") != "REJECT")

except Exception as e:
    logger.error(f"Failed applying Update Strategy UPD_NISS_SUBLOB_CD: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Output: FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 -> write back to WRK_BIRP_NISS_APRM_DETL (S3 parquet) via load-modify-store-back
try:
    logger.info("Beginning load-modify-store-back to update WRK_BIRP_NISS_APRM_DETL on S3")
    target_s3_path = f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"

    try:
        logger.info(f"Reading existing target from S3: {target_s3_path}")
        df_existing = spark.read.parquet(target_s3_path)
    except Exception as e_read:
        logger.info(f"Existing target not found or unreadable at {target_s3_path}; treating as empty existing set. ({e_read})")
        # create an empty DataFrame with the same schema as the incoming data minus dd_op
        schema_for_empty = df_UPD_NISS_SUBLOB_CD_eager_hilbert.drop("dd_op").schema
        df_existing = spark.createDataFrame([], schema_for_empty)

    # keys changed by this batch
    df_changed_keys = df_UPD_NISS_SUBLOB_CD_eager_hilbert.select("NISS_APRM_DETL_SK").distinct()

    # anti-join to remove rows from existing that are being updated/deleted
    df_existing_anti = df_existing.join(df_changed_keys, on="NISS_APRM_DETL_SK", how="left_anti")

    # keep only INSERT/UPDATE rows from incoming (DD_DELETE rows would be filtered out here)
    df_to_reinsert = df_UPD_NISS_SUBLOB_CD_eager_hilbert.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

    # union existing survivors with incoming inserts/updates (allowMissingColumns to align schemas)
    df_combined = df_existing_anti.unionByName(df_to_reinsert, allowMissingColumns=True)

    # final write back to the same S3 location (overwrite)
    logger.info(f"Writing combined WRK_BIRP_NISS_APRM_DETL to S3 (overwrite): {target_s3_path}")
    df_combined.write.mode("overwrite").parquet(target_s3_path)

    # assign final dataframe variable for downstream lineage
    df_WRK_BIRP_NISS_APRM_DETL_amazing_mendel = df_combined

except Exception as e:
    logger.error(f"Failed applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise



job.commit()
