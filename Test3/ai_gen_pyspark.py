import logging
from pyspark.sql import functions as F
import psycopg2
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define constants
SOURCE_TABLE = \"catalog.source_db.interaction_event\"
TARGET_TABLE = \"catalog.target_db.interaction_event_transformed\"

# Start ETL process
try:
    logger.info(\"Starting ETL process for InteractionEvent data.\")

    # Step 1: Load data from Unity Catalog source table
    logger.info(f\"Loading data from Unity Catalog source table: {SOURCE_TABLE}\")
    source_df = spark.table(SOURCE_TABLE)

    # Step 2: Apply transformations
    logger.info(\"Applying transformations to the source data.\")
    
    # Example transformation: Add a new column for processing timestamp
    transformed_df = source_df.withColumn(\"processing_timestamp\", F.lit(datetime.now()))

    # Example transformation: Filter rows where InteractionEvent_Id is not null
    transformed_df = transformed_df.filter(F.col(\"InteractionEvent_Id\").isNotNull())

    # Example transformation: Rename columns for clarity
    transformed_df = transformed_df.withColumnRenamed(\"InteractionEvent_Id\", \"interaction_event_id\") \\
                                   .withColumnRenamed(\"InteractionEventKey_Cd\", \"interaction_event_key_cd\")

    # Example transformation: Add a derived column based on existing data
    transformed_df = transformed_df.withColumn(\"interaction_event_category\",
                                               F.when(F.col(\"ActionType_Tp\") == 1, \"Category A\")
                                                .when(F.col(\"ActionType_Tp\") == 2, \"Category B\")
                                                .otherwise(\"Category C\"))

    # Step 3: Write transformed data to Unity Catalog target table
    logger.info(f\"Writing transformed data to Unity Catalog target table: {TARGET_TABLE}\")
    spark.sql(f\"DROP TABLE IF EXISTS {TARGET_TABLE}\")  # Drop existing table if it exists
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(TARGET_TABLE)

    logger.info(\"ETL process completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise


import logging
from pyspark.sql import functions as F
import psycopg2
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Securely retrieve credentials for external systems
try:
    db_host = dbutils.secrets.get(\"secret_scope\", \"db_host\")
    db_port = dbutils.secrets.get(\"secret_scope\", \"db_port\")
    db_name = dbutils.secrets.get(\"secret_scope\", \"db_name\")
    db_user = dbutils.secrets.get(\"secret_scope\", \"db_user\")
    db_password = dbutils.secrets.get(\"secret_scope\", \"db_password\")
except Exception as e:
    logging.error(f\"Error retrieving database credentials: {e}\")
    raise

# Connect to the external PostgreSQL database and fetch data
try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()
    cursor.execute(\"SELECT * FROM interaction_event\")  # Replace with actual query
    external_data = cursor.fetchall()
    cursor.close()
    conn.close()
except Exception as e:
    logging.error(f\"Error connecting to PostgreSQL database: {e}\")
    raise

# Define schema for external data
schema = StructType([
    StructField(\"InteractionGroup_Id\", DecimalType(18, 0), True),
    StructField(\"BusinessStatus_Cd\", StringType(), True),
    StructField(\"Status_Cd\", StringType(), True),
    StructField(\"StatusReason_Cd\", StringType(), True),
    StructField(\"TransactionEffective_Dt\", TimestampType(), True),
    StructField(\"Transaction_Ts\", TimestampType(), True),
    StructField(\"Sequence_It\", DecimalType(18, 0), True),
    StructField(\"Source_Cd\", StringType(), True),
    StructField(\"LoadEvent_Id\", DecimalType(18, 0), True),
    # Add other fields as needed
])

# Convert external data to a Spark DataFrame
external_df = spark.createDataFrame(external_data, schema)

# Load data from Unity Catalog source table
try:
    source_df = spark.table(\"catalog.source_db.interaction_event\")
except Exception as e:
    logging.error(f\"Error loading Unity Catalog source table: {e}\")
    raise

# Perform transformations
try:
    # Example transformation: Filter and add calculated columns
    transformed_df = source_df.filter(F.col(\"StatusIncludeExclude_Cd\") == \"Include\") \\
        .withColumn(\"BusinessStatus_Cd\", F.when(F.col(\"Source_Cd\").isin(\"$$IGS_excluded_source\"), \"PHONE\")
                    .when(F.col(\"Source_Cd\").isin(\"$$excluded_sources\"), \"Intct\")
                    .otherwise(F.when(F.col(\"TransactionType_Tp\") == 10, \"EMAIL\")
                                .when(F.col(\"TransactionType_Tp\") == 11, \"MOBILE\")
                                .otherwise(\"Notdef\"))) \\
        .withColumn(\"Status_Cd\", F.when(F.col(\"Source_Cd\").isin(\"$$IGS_excluded_source\"), \"A\" + F.trim(F.col(\"Action_Tp\")))
                    .otherwise(\"Notdef\")) \\
        .withColumn(\"StatusReason_Cd\", F.when(F.col(\"Source_Cd\").isin(\"$$IGS_excluded_source\"), \"AR\" + F.trim(F.col(\"ActionResult_Tp\")))
                    .otherwise(\"Notdef\"))

    # Join with external data
    joined_df = transformed_df.join(external_df, [\"InteractionGroup_Id\"], \"left_outer\")
except Exception as e:
    logging.error(f\"Error during transformation: {e}\")
    raise

# Write transformed data to Unity Catalog target table
try:
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.interaction_group_status\")
    joined_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.interaction_group_status\")
except Exception as e:
    logging.error(f\"Error writing to Unity Catalog target table: {e}\")
    raise

logging.info(\"ETL process completed successfully.\")


import logging
from pyspark.sql.functions import col, when

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog table
    logger.info(\"Loading source data from Unity Catalog table: catalog.source_db.exp_get_tp_values\")
    source_df = spark.table(\"catalog.source_db.exp_get_tp_values\")

    # Step 2: Apply transformations as per the lineage plan
    logger.info(\"Applying transformations to set default values for BusinessStatus_Tp, Status_Tp, and StatusReason_Tp fields\")

    transformed_df = (
        source_df
        .withColumn(\"o_BusinessStatus_Tp\", when(col(\"BusinessStatus_Tp\").isNotNull(), col(\"BusinessStatus_Tp\")).otherwise(-2))
        .withColumn(\"o_Status_Tp\", when(col(\"Status_Tp\").isNotNull(), col(\"Status_Tp\")).otherwise(-2))
        .withColumn(\"o_StatusReason_Tp\", when(col(\"StatusReason_Tp\").isNotNull(), col(\"StatusReason_Tp\")).otherwise(-2))
    )

    # Step 3: Drop existing target table if it exists
    logger.info(\"Dropping existing target table if it exists: catalog.target_db.InteractionGroupStatus\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.InteractionGroupStatus\")

    # Step 4: Write transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table: catalog.target_db.InteractionGroupStatus\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.InteractionGroupStatus\")

    logger.info(\"ETL process completed successfully\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {e}\")
    raise


import logging
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog table
    logger.info(\"Loading source data from Unity Catalog table: catalog.source_db.SQ_InteractionEvent\")
    source_df = spark.table(\"catalog.source_db.SQ_InteractionEvent\")

    # Step 2: Perform transformations as per lineage plan
    logger.info(\"Performing transformations as per lineage plan\")

    # Passthrough fields
    transformed_df = source_df.select(
        F.col(\"LoadEvent_Id\"),
        F.col(\"NUMBER_SEQ\"),
        F.col(\"BusinessStatus_Cd\"),
        F.col(\"Status_Cd\"),
        F.col(\"StatusReason_Cd\")
    )

    # Lookup transformations
    logger.info(\"Performing lookup transformations\")
    lookup_xref_xt = spark.table(\"catalog.lookup_db.lkp_xref_xt\")  # Assuming lookup table exists in Unity Catalog

    # Lookup for BusinessStatus_Cd
    business_status_tp_df = lookup_xref_xt.filter(F.col(\"lookup_id\") == 5021).select(
        F.upper(F.trim(F.col(\"lookup_value\"))).alias(\"lookup_value\"),
        F.col(\"lookup_result\").alias(\"o_BusinessStatus_Tp\")
    )
    transformed_df = transformed_df.join(
        business_status_tp_df,
        F.upper(F.trim(transformed_df[\"BusinessStatus_Cd\"])) == business_status_tp_df[\"lookup_value\"],
        \"left\"
    )

    # Lookup for Status_Cd
    status_tp_df = lookup_xref_xt.filter(F.col(\"lookup_id\") == 5022).select(
        F.upper(F.trim(F.col(\"lookup_value\"))).alias(\"lookup_value\"),
        F.col(\"lookup_result\").alias(\"o_Status_Tp\")
    )
    transformed_df = transformed_df.join(
        status_tp_df,
        F.upper(F.trim(transformed_df[\"Status_Cd\"])) == status_tp_df[\"lookup_value\"],
        \"left\"
    )

    # Lookup for StatusReason_Cd
    status_reason_tp_df = lookup_xref_xt.filter(F.col(\"lookup_id\") == 5023).select(
        F.upper(F.trim(F.col(\"lookup_value\"))).alias(\"lookup_value\"),
        F.col(\"lookup_result\").alias(\"o_StatusReason_Tp\")
    )
    transformed_df = transformed_df.join(
        status_reason_tp_df,
        F.upper(F.trim(transformed_df[\"StatusReason_Cd\"])) == status_reason_tp_df[\"lookup_value\"],
        \"left\"
    )

    # Generate InteractionGroupStatus_Id
    logger.info(\"Generating InteractionGroupStatus_Id\")
    transformed_df = transformed_df.withColumn(
        \"o_InteractionGroupStatus_Id\",
        (F.concat_ws(\"\", F.col(\"LoadEvent_Id\").cast(StringType()), F.lit(\"000000000000000000\").substr(0, 18 - F.length(F.col(\"LoadEvent_Id\").cast(StringType()))))
         .cast(DecimalType(38, 0)) + F.col(\"NUMBER_SEQ\").cast(DecimalType(38, 0)))
    )

    # Step 3: Write transformed data to Unity Catalog target table
    target_table = \"catalog.target_db.InteractionGroupStatus\"
    logger.info(f\"Dropping existing target table if exists: {target_table}\")
    spark.sql(f\"DROP TABLE IF EXISTS {target_table}\")

    logger.info(f\"Writing transformed data to Unity Catalog target table: {target_table}\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_table)

    logger.info(\"ETL process completed successfully\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise