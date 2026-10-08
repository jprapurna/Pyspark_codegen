import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)


# Top-of-script placeholder for the mapping parameter $$load_event_id
LOAD_EVENT_ID = "REPLACE_WITH_LOAD_EVENT_ID_VALUE"

# Source: InteractionGroupStatus1 - attempt S3-first read of workflow-staged parquet, fall back to Glue Catalog
try:
    logger.info("Attempting to read workflow-staged InteractionGroupStatus from S3")
    df_InteractionGroupStatus_cool_shannon = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/")
    logger.info("Read InteractionGroupStatus from S3 workflow-staged parquet")
except Exception as e:
    logger.warning("Could not read InteractionGroupStatus from s3; falling back to Glue Catalog read: %s", str(e))
    try:
        logger.info("Reading InteractionGroupStatus from Glue Catalog as fallback")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=GLUE_DATABASE, table_name="InteractionGroupStatus")
        df_InteractionGroupStatus_cool_shannon = dyf.toDF()
        logger.info("Read InteractionGroupStatus from Glue Catalog and converted to DataFrame")
    except Exception as e2:
        logger.error(f"Failed reading InteractionGroupStatus from Glue Catalog fallback: {e2}", exc_info=True)
        raise

# SQ_DateChain: SQL override runs against the staged temp view of InteractionGroupStatus
# Register the staged dataframe as the bare table name the override expects, then run the override via spark.sql
try:
    logger.info("Registering InteractionGroupStatus temp view for SQ_DateChain and executing SQL override")
    df_InteractionGroupStatus_cool_shannon.createOrReplaceTempView("InteractionGroupStatus")

    sql_query = f"""SELECT
    O.InteractionGroupStatus_Id,
    O.InteractionGroup_Id,
    O.NewExpiration_Dt,
    O.NewRevision_Ts,
    O.NewSequenceEnd_It,
    O.Source_Cd
FROM
(
SELECT InteractionGroupStatus_Id,
       InteractionGroup_Id,
       Source_Cd,
       COALESCE(
          MIN(Effective_Dt)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Effective_Dt ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          DATE '3500-01-01') AS NewExpiration_Dt,
       COALESCE(
          MIN(Transaction_Ts)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Transaction_Ts ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          TIMESTAMP '3500-01-01 00:00:00.000000') AS NewRevision_Ts,
       COALESCE(
          MIN(SequenceStart_It)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          350036535003659999) AS NewSequenceEnd_It
  FROM InteractionGroupStatus tgt
 WHERE EXISTS
          (SELECT 1
             FROM InteractionGroupStatus tgt1
            WHERE     tgt.InteractionGroup_Id = tgt1.InteractionGroup_Id
                  AND tgt.Source_Cd = tgt1.Source_Cd
                  AND tgt1.LoadEvent_Id >= {LOAD_EVENT_ID})
QUALIFY NOT (Expiration_Dt  = NewExpiration_Dt
        AND  Revision_Ts    = NewRevision_Ts
        AND  SequenceEnd_It = NewSequenceEnd_It)) O"""

    df_SQ_DateChain_keen_hume = spark.sql(sql_query)
    logger.info("Executed SQ_DateChain override and produced dataframe df_SQ_DateChain_keen_hume")
except Exception as e:
    logger.error(f"Failed running SQ_DateChain SQL override: {e}", exc_info=True)
    raise

# InteractionGroupStatus (Output) - Upsert (Update else Insert) implemented as load-modify-store-back to S3 parquet
try:
    logger.info("Applying upsert to InteractionGroupStatus target (load-modify-store-back)")
    target_path = f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"

    # Read existing target if present; if missing, treat as empty
    try:
        logger.info("Reading existing InteractionGroupStatus target from S3 to apply updates")
        existing_df = spark.read.parquet(target_path)
        logger.info("Read existing InteractionGroupStatus target from S3")
    except Exception:
        logger.warning("Existing InteractionGroupStatus target not found on S3; will treat as empty and create a new one")
        # create an empty DataFrame with the same schema as the incoming rows so unionByName can work
        existing_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_SQ_DateChain_keen_hume.schema)

    # Identify incoming keys (primary key assumed to be InteractionGroupStatus_Id - review if keys differ)
    incoming_keys_df = df_SQ_DateChain_keen_hume.select("InteractionGroupStatus_Id").distinct()

    # Remove any existing rows that are being updated/deleted by incoming rows (anti-join)
    try:
        surviving_existing = existing_df.join(incoming_keys_df, on="InteractionGroupStatus_Id", how="left_anti")
    except Exception as e:
        logger.error(f"Failed while anti-joining existing target with incoming keys: {e}", exc_info=True)
        raise

    # Union surviving existing rows with incoming rows (incoming rows contain the updated/inserted state)
    try:
        combined_df = surviving_existing.unionByName(df_SQ_DateChain_keen_hume, allowMissingColumns=True)
    except Exception as e:
        logger.error(f"Failed while unioning existing and incoming rows for InteractionGroupStatus: {e}", exc_info=True)
        raise

    # Overwrite the target with the combined result
    try:
        logger.info("Writing combined InteractionGroupStatus dataframe back to S3 as parquet (overwrite)")
        combined_df.write.mode("overwrite").parquet(target_path)
        logger.info("Successfully wrote InteractionGroupStatus to S3 (overwrite)")
    except Exception as e:
        logger.error(f"Failed writing InteractionGroupStatus to S3: {e}", exc_info=True)
        raise

    # assign output df name for downstream consumers (if any)
    df_InteractionGroupStatus_fierce_pasteur = combined_df

except Exception as e:
    logger.error(f"Failed applying upsert for InteractionGroupStatus: {e}", exc_info=True)
    raise



job.commit()
