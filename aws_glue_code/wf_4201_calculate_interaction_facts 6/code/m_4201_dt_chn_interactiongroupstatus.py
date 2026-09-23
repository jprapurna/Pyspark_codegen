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


# JDBC connection and mapping-parameter placeholders
RL_EDW_EINTERACTION_JDBC_URL = "REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_URL"
RL_EDW_EINTERACTION_JDBC_USER = "REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_USER"
RL_EDW_EINTERACTION_JDBC_PASSWORD = "REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_PASSWORD"
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB_VALUE"
LOAD_EVENT_ID = "REPLACE_WITH_LOAD_EVENT_ID_VALUE"
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# -----------------------------------------------------------------------------
# SQ_DateChain: SQL override against rl_EDW_EINTERACTION.InteractionGroupStatus
# (bypassed upstream Source object — the override reads the source directly via JDBC)
# -----------------------------------------------------------------------------
sql_query = f"""SELECT
O.InteractionGroupStatus_Id,
O.InteractionGroup_Id,
O.NewExpiration_Dt AS Expiration_Dt,
O.NewRevision_Ts AS Revision_Ts,
O.NewSequenceEnd_It AS SequenceEnd_It,
O.Source_Cd
FROM
(
SELECT InteractionGroupStatus_Id,
       InteractionGroup_Id,
       Source_Cd,
       COALESCE (
          MIN (
             Effective_Dt)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Effective_Dt ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          DATE '3500-01-01')
          AS NewExpiration_Dt,
       COALESCE (
          MIN (
             Transaction_Ts)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Transaction_Ts ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          TIMESTAMP '3500-01-01 00:00:00.000000')
          AS NewRevision_Ts,
       COALESCE (
          MIN (
             SequenceStart_It)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          350036535003659999)
          AS NewSequenceEnd_It
  FROM {INTERACTION_FND_DB}.InteractionGroupStatus tgt
 WHERE EXISTS
          (SELECT 1
             FROM  {INTERACTION_FND_DB}.InteractionGroupStatus tgt1
            WHERE     tgt.InteractionGroup_Id = tgt1.InteractionGroup_Id
                  AND tgt.Source_Cd = tgt1.Source_Cd
                  AND tgt1.LoadEvent_Id >= {LOAD_EVENT_ID} )
QUALIFY NOT (Expiration_Dt  = NewExpiration_Dt
        AND  Revision_Ts    = NewRevision_Ts
        AND  SequenceEnd_It = NewSequenceEnd_It)
) O
"""

try:
    logger.info("Reading SQ_DateChain (SQL override) from rl_EDW_EINTERACTION via JDBC override query")
    df_SQ_DateChain_modest_rutherford = (
        spark.read.format("jdbc")
        .option("url", RL_EDW_EINTERACTION_JDBC_URL)
        .option("user", RL_EDW_EINTERACTION_JDBC_USER)
        .option("password", RL_EDW_EINTERACTION_JDBC_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_DateChain from rl_EDW_EINTERACTION: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# InteractionGroupStatus (target): project full target schema and perform
# load-modify-store-back upsert to s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/ (overwrite)
# Primary key (design-time): InteractionGroupStatus_Id, InteractionGroup_Id, Source_Cd
# -----------------------------------------------------------------------------
try:
    logger.info("Projecting InteractionGroupStatus target columns from SQ_DateChain output")
    src_df = df_SQ_DateChain_modest_rutherford
    existing_cols = set([c.lower() for c in src_df.columns])

    # Helper to test column presence case-insensitively
    def has_col(col_name):
        return col_name.lower() in existing_cols

    select_exprs = []
    # Required/expected target columns (preserve target names exactly)
    target_columns = [
        "InteractionGroupStatus_Id",
        "InteractionGroup_Id",
        "BusinessStatus_Cd",
        "BusinessStatus_Tp",
        "Status_Cd",
        "Status_Tp",
        "StatusReason_Cd",
        "StatusReason_Tp",
        "Effective_Dt",
        "Expiration_Dt",
        "Transaction_Ts",
        "Revision_Ts",
        "SequenceStart_It",
        "SequenceEnd_It",
        "Source_Cd",
        "LoadEvent_Id",
    ]

    # For each target column decide whether to select from incoming DF (possibly from an alternate name)
    for col in target_columns:
        # handle columns that may be present under alternate names produced by the override
        if col == "Expiration_Dt":
            if has_col("Expiration_Dt"):
                select_exprs.append("Expiration_Dt")
            elif has_col("NewExpiration_Dt"):
                select_exprs.append("NewExpiration_Dt AS Expiration_Dt")
            else:
                select_exprs.append("NULL AS Expiration_Dt")
        elif col == "Revision_Ts":
            if has_col("Revision_Ts"):
                select_exprs.append("Revision_Ts")
            elif has_col("NewRevision_Ts"):
                select_exprs.append("NewRevision_Ts AS Revision_Ts")
            else:
                select_exprs.append("NULL AS Revision_Ts")
        elif col == "SequenceEnd_It":
            if has_col("SequenceEnd_It"):
                select_exprs.append("SequenceEnd_It")
            elif has_col("NewSequenceEnd_It"):
                select_exprs.append("NewSequenceEnd_It AS SequenceEnd_It")
            else:
                select_exprs.append("NULL AS SequenceEnd_It")
        else:
            # generic handling: if source has the same-named column select it, otherwise emit NULL
            if has_col(col):
                select_exprs.append(col)
            else:
                select_exprs.append(f"NULL AS {col}")

    df_projected = src_df.selectExpr(*select_exprs)
except Exception as e:
    logger.error(f"Failed projecting InteractionGroupStatus columns: {e}", exc_info=True)
    raise

# Load-modify-store-back upsert
target_path = f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
pk_cols = ["InteractionGroupStatus_Id", "InteractionGroup_Id", "Source_Cd"]

try:
    logger.info(f"Reading existing InteractionGroupStatus from {target_path} (if present) for upsert")
    try:
        existing_df = spark.read.parquet(target_path)
        logger.info("Existing target read from S3 succeeded")
    except Exception:
        # target does not exist yet; treat as empty DataFrame with same schema as incoming
        logger.warning("Existing InteractionGroupStatus parquet not found at target path; treating as empty for upsert")
        existing_df = spark.createDataFrame([], df_projected.schema)

    # Determine keys of incoming (rows to insert/update/delete). Here Update-else-Insert
    incoming_keys_df = df_projected.select(*pk_cols).distinct()

    logger.info("Applying anti-join to remove rows from existing target that are being updated/deleted by incoming dataset")
    remaining_existing = existing_df.join(incoming_keys_df, on=pk_cols, how="left_anti")

    # Union remaining existing rows with incoming rows (incoming contains the new/updated state)
    logger.info("Unioning incoming rows with remaining existing rows to form the new full target dataset")
    combined_df = remaining_existing.unionByName(df_projected, allowMissingColumns=True)

    # Overwrite the target path with the combined dataset
    logger.info(f"Writing combined InteractionGroupStatus dataset back to {target_path} (overwrite). NOTE: this implements the Update-else-Insert by full overwrite of the target and may be expensive for large tables")
    combined_df.write.mode("overwrite").parquet(target_path)
except Exception as e:
    logger.error(f"Failed performing load-modify-store-back upsert for InteractionGroupStatus: {e}", exc_info=True)
    raise

# assign output df name for downstream lineage
df_InteractionGroupStatus_epic_bohr = df_projected



job.commit()
