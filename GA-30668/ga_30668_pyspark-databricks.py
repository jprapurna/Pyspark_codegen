
# ============================================================================
# AI GENERATED PYSPARK CODE - ISSUE GA-30668
# Processing Mode: Individual Node Processing
# ============================================================================

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_df_info(df, step_name):
    """Log DataFrame information for debugging"""
    logger.info(f"{step_name}: {df.count()} rows, {len(df.columns)} columns")
    logger.info(f"Schema: {[f.name + ':' + str(f.dataType) for f in df.schema.fields]}")


# ============================================================================
# _Analysis (regular)
# ============================================================================
# ============================================================================
# NODE V155S0: CREDIT_USER_INFO1 (Source)
# Container: Not specified (Active)
# Description: The data is been loaded in this file from LDAP server using Unix script.
# Previous Node: None
# Next Node: V72S0
# ============================================================================

try:
    logger.info("Processing Node V155S0: CREDIT_USER_INFO1")
    
    # Extract connection attributes
    file_name = "HANA_ldap_trn_hana_CREDIT_USER_INFO.txt"
    table_name = "catalog.schema.CREDIT_USER_INFO1"  # Replace with actual Unity Catalog table name if available
    
    # Attempt to read data from Unity Catalog or fallback to file
    final_df_V155S0 = read_table(table_name, file_name)
    
    # Safe logging of DataFrame information
    logger.info(f"=== Source: CREDIT_USER_INFO1 - Node V155S0 ===")
    logger.info(f"   └─ Columns: {len(final_df_V155S0.columns)}")
    final_df_V155S0.printSchema()
    final_df_V155S0.show(3, truncate=False)
    
    logger.info("✓ Node V155S0 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V155S0: {str(e)}")
    raise Exception(f"Critical error in Node V155S0: {str(e)}")
# ============================================================================
# NODE V149S0: ref_CREDIT_USER_INFO (Source)
# Container: Not specified (Active)
# Description: This stage is used to lookup target table for checking SCD conditions.
# Previous Node: None
# Next Node: V0S74
# ============================================================================

try:
    logger.info("Processing Node V149S0: ref_CREDIT_USER_INFO")
    
    # Extract connection details and table information
    connection_id = "#GetHANAREPORTINGPROJDEFValues.$hana_DH2_DBNAME#"
    authentication_method = "#GetHANAREPORTINGPROJDEFValues.$hana_DH2_USER#"
    catalog_name = "#GetHANAREPORTINGPROJDEFValues.$hana_FOUNDATION_SCHEMA#"
    table_name = "CREDIT_USER_INFO"
    full_table_name = f"{catalog_name}.{table_name}"
    
    # Attempt to read the table from Unity Catalog
    try:
        logger.info(f"Attempting to read table {full_table_name} from Unity Catalog")
        final_df_V149S0 = spark.table(full_table_name)
        logger.info(f"✓ Successfully read table {full_table_name} from Unity Catalog")
    except Exception as catalog_error:
        logger.error(f"✗ Failed to read table {full_table_name} from Unity Catalog: {str(catalog_error)}")
        raise Exception(f"Critical error: Unable to read source table {full_table_name} from Unity Catalog. Error: {str(catalog_error)}")
    
    # Safe logging of DataFrame information
    logger.info(f"=== Source: ref_CREDIT_USER_INFO - Node V149S0 ===")
    logger.info(f"   └─ Columns: {len(final_df_V149S0.columns)}")
    final_df_V149S0.printSchema()
    final_df_V149S0.show(3, truncate=False)
    
    logger.info("✓ Node V149S0 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V149S0 (ref_CREDIT_USER_INFO): {str(e)}")
    raise Exception(f"Critical error in Node V149S0: {str(e)}")
# ============================================================================
# NODE V153S0: CREDIT_USER_INFO (Source)
# Container: Not specified (Active)
# Description: The data is been loaded in this file from LDAP server using Unix script.
# Previous Node: None
# Next Node: V0S13
# ============================================================================

try:
    logger.info("Processing Node V153S0: CREDIT_USER_INFO (Source)")
    
    # Extract connection attributes
    file_name = "HANA_ldap_trn_hana_CREDIT_USER_INFO.txt"
    table_name = "catalog.schema.CREDIT_USER_INFO"  # Replace with actual Unity Catalog table name if available
    
    # Attempt to read data from Unity Catalog or fallback to local file
    final_df_V153S0 = read_table(table_name, file_name)
    
    # Safe logging of DataFrame information
    logger.info(f"=== Source: CREDIT_USER_INFO - Node V153S0 ===")
    logger.info(f"   └─ Columns: {len(final_df_V153S0.columns)}")
    final_df_V153S0.printSchema()
    final_df_V153S0.show(3, truncate=False)
    
    logger.info("✓ Node V153S0 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V153S0: {str(e)}")
    raise Exception(f"Critical error in Node V153S0: {str(e)}")
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session is pre-initialized in Databricks
# Available as: spark

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_df_info(df, step_name):
    """
    Log DataFrame information safely (without expensive operations)
    NEVER use df.count() - it causes "Connection reset" errors
    """
    logger.info(f"=== {step_name} ===")
    logger.info(f"   └─ Columns: {len(df.columns)}")
    df.printSchema()
    try:
        logger.info(f"   └─ Sample (first 3 rows):")
        df.show(3, truncate=False)
    except Exception as e:
        logger.info(f"   └─ Could not show data: {str(e)}")

def read_table(table_name, local_file_path):
    """
    Universal data reader - tries Unity Catalog first, then local file
    Args:
        table_name: Unity Catalog table name (catalog.schema.table)
        local_file_path: Fallback local CSV path
    Returns:
        Spark DataFrame
    """
    try:
        logger.info(f"Attempting to read from Unity Catalog: {table_name}")
        df = spark.table(table_name)
        logger.info(f"✓ Successfully read from catalog table")
        return df
    except Exception as catalog_error:
        logger.warning(f"Catalog read failed: {str(catalog_error)}")
        logger.info(f"Attempting to read from local file: {local_file_path}")
        try:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(local_file_path)
            logger.info(f"✓ Successfully read from local file")
            return df
        except Exception as file_error:
            logger.error(f"Both catalog and file read failed")
            raise Exception(f"Could not read data. Catalog error: {catalog_error}, File error: {file_error}")

# ============================================================================
# NODE IMPLEMENTATION
# ============================================================================

# ============================================================================
# NODE V154S0: CREDIT_USER_INFO2 (Source)
# Container: Batch 1 (Active)
# Description: The data is been loaded in this file from LDAP server using unix script.
# Previous Node: None
# Next Node: V73S0
# ============================================================================

try:
    logger.info("Processing Node V154S0: CREDIT_USER_INFO2")
    
    # Define source file path and Unity Catalog table name
    table_name = "catalog.schema.CREDIT_USER_INFO2"  # Replace with actual catalog and schema
    local_file_path = "/mnt/data/HANA_ldap_trn_hana_CREDIT_USER_INFO.txt"  # Path to the file
    
    # Read data using the universal read_table helper
    final_df_V154S0 = read_table(table_name, local_file_path)
    
    # Safe logging of DataFrame information
    log_df_info(final_df_V154S0, "Source Node V154S0: CREDIT_USER_INFO2")
    
    logger.info("✓ Node V154S0 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V154S0 (CREDIT_USER_INFO2): {str(e)}")
    raise Exception(f"Critical error in Node V154S0: {str(e)}")
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session is pre-initialized in Databricks
# Available as: spark

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_df_info(df, step_name):
    """
    Log DataFrame information safely (without expensive operations)
    NEVER use df.count() - it causes "Connection reset" errors
    """
    logger.info(f"=== {step_name} ===")
    logger.info(f"   └─ Columns: {len(df.columns)}")
    df.printSchema()
    try:
        logger.info(f"   └─ Sample (first 3 rows):")
        df.show(3, truncate=False)
    except Exception as e:
        logger.info(f"   └─ Could not show data: {str(e)}")

# ============================================================================
# NODE IMPLEMENTATION
# ============================================================================

# ============================================================================
# NODE V0S88: pek_rej_CREDIT_USER_INFO_SCD2 (Output)
# Container: Batch 1 (_Analysis - regular)
# Description: This stage is used to capture error messages.
# Previous Node: V152S0
# Next Node: None
# ============================================================================

try:
    logger.info("Processing Node V0S88: pek_rej_CREDIT_USER_INFO_SCD2")
    
    # Define target location
    target_catalog = "credit_user_info_catalog"
    target_schema = "error_logs"
    target_table = "pek_rej_credit_user_info_scd2"
    
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Schema {target_catalog}.{target_schema} ensured")
    
    # Write to Unity Catalog (overwrite mode handles table replacement automatically)
    final_df_V152S0.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    
    logger.info(f"✓ Data written to {target_catalog}.{target_schema}.{target_table}")
    
    # Safe logging
    logger.info(f"=== Node V0S88: pek_rej_CREDIT_USER_INFO_SCD2 ===")
    logger.info(f"   └─ Columns: {len(final_df_V152S0.columns)}")
    final_df_V152S0.printSchema()
    final_df_V152S0.show(3, truncate=False)
    
    logger.info("✓ Node V0S88 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V0S88 (pek_rej_CREDIT_USER_INFO_SCD2): {str(e)}")
    raise Exception(f"Critical error in Node V0S88: {str(e)}")
# ============================================================================
# NODE V0S86: pek_rej_ins_CREDIT_USER_INFO (Output)
# Container: Batch 1 (_Analysis - regular)
# Description: This stage is used to capture error messages.
# Previous Node: V150S0
# Next Node: None
# ============================================================================

try:
    logger.info("Processing Node V0S86: pek_rej_ins_CREDIT_USER_INFO (Output)")
    
    # Define target location in Unity Catalog
    target_catalog = "hr_data_catalog"
    target_schema = "user_data"
    target_table = "credit_user_info_errors"
    
    # Ensure schema exists before writing the table
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Schema {target_catalog}.{target_schema} ensured")
    
    # Write the DataFrame from the previous node to Unity Catalog
    final_df_V150S0.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    
    logger.info(f"✓ Data written to {target_catalog}.{target_schema}.{target_table}")
    
    # Safe logging of the output DataFrame
    logger.info(f"=== Node V0S86: pek_rej_ins_CREDIT_USER_INFO ===")
    logger.info(f"   └─ Columns: {len(final_df_V150S0.columns)}")
    final_df_V150S0.printSchema()
    final_df_V150S0.show(3, truncate=False)
    
    logger.info("✓ Node V0S86 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V0S86: {str(e)}")
    raise Exception(f"Critical error in Node V0S86: {str(e)}")
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session is pre-initialized in Databricks
# Available as: spark

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_df_info(df, step_name):
    """
    Log DataFrame information safely (without expensive operations)
    NEVER use df.count() - it causes "Connection reset" errors
    """
    logger.info(f"=== {step_name} ===")
    logger.info(f"   └─ Columns: {len(df.columns)}")
    df.printSchema()
    try:
        logger.info(f"   └─ Sample (first 3 rows):")
        df.show(3, truncate=False)
    except Exception as e:
        logger.info(f"   └─ Could not show data: {str(e)}")

# ============================================================================
# NODE IMPLEMENTATIONS
# ============================================================================

# ============================================================================
# NODE V0S87: pek_rej_CREDIT_USER_INFO_SCD1 (Output)
# Container: None (Active)
# Description: This stage is used to capture error messages.
# Previous Node: V151S0
# Next Node: None
# ============================================================================

try:
    logger.info("Processing Node V0S87: pek_rej_CREDIT_USER_INFO_SCD1")
    
    # Define target location
    target_catalog = "credit_user_info_catalog"
    target_schema = "error_logs"
    target_table = "pek_rej_CREDIT_USER_INFO_SCD1"
    
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Schema {target_catalog}.{target_schema} ensured")
    
    # Write to Unity Catalog (overwrite mode handles table replacement automatically)
    final_df_V151S0.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    
    logger.info(f"✓ Data written to {target_catalog}.{target_schema}.{target_table}")
    
    # Safe logging
    logger.info(f"=== Node V0S87: pek_rej_CREDIT_USER_INFO_SCD1 ===")
    logger.info(f"   └─ Columns: {len(final_df_V151S0.columns)}")
    final_df_V151S0.printSchema()
    final_df_V151S0.show(3, truncate=False)
    
    logger.info("✓ Node V0S87 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V0S87 (pek_rej_CREDIT_USER_INFO_SCD1): {str(e)}")
    raise Exception(f"Critical error in Node V0S87: {str(e)}")
# ============================================================================
# NODE V0S98: ldap_hana_ds (Output)
# Container: PxDataSet (Active)
# Description: This stage is used to store SCD2 insert records.
# Previous Node: V0S79
# Next Node: None
# ============================================================================

try:
    logger.info("Processing Node V0S98: ldap_hana_ds (Output)")
    
    # Define target location details
    target_catalog = "HANAREPORTINGPROJDEFValues"
    target_schema = "HANASourceFileDir"
    target_table = "LDAP_HANA_ds"
    
    # Ensure schema exists before writing the table
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Schema {target_catalog}.{target_schema} ensured")
    
    # Write the DataFrame from the previous node (V0S79) to Unity Catalog
    final_df_V0S79.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    
    logger.info(f"✓ Data written to {target_catalog}.{target_schema}.{target_table}")
    
    # Safe logging of the output DataFrame
    logger.info(f"=== Node V0S98: ldap_hana_ds ===")
    logger.info(f"   └─ Columns: {len(final_df_V0S79.columns)}")
    final_df_V0S79.printSchema()
    final_df_V0S79.show(3, truncate=False)
    
    logger.info("✓ Node V0S98 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V0S98 (ldap_hana_ds): {str(e)}")
    raise Exception(f"Critical error in Node V0S98: {str(e)}")
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark session is pre-initialized in Databricks
# Available as: spark

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_df_info(df, step_name):
    """
    Log DataFrame information safely (without expensive operations)
    NEVER use df.count() - it causes "Connection reset" errors
    """
    logger.info(f"=== {step_name} ===")
    logger.info(f"   └─ Columns: {len(df.columns)}")
    df.printSchema()
    try:
        logger.info(f"   └─ Sample (first 3 rows):")
        df.show(3, truncate=False)
    except Exception as e:
        logger.info(f"   └─ Could not show data: {str(e)}")

# ============================================================================
# NODE IMPLEMENTATION
# ============================================================================

# ============================================================================
# NODE V150S0: ins_CREDIT_USER_INFO (Output)
# Container: Batch 1 (Active)
# Description: This stage inserts (pure insert and SCD2 insert) values to CREDIT_USER_INFO table.
# Previous Node: V0S79
# Next Node: None
# ============================================================================

try:
    logger.info("Processing Node V150S0: ins_CREDIT_USER_INFO")
    
    # Define target location
    target_catalog = "hana_FOUNDATION_SCHEMA"
    target_schema = "default"  # Assuming default schema for simplicity
    target_table = "CREDIT_USER_INFO"
    
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Schema {target_catalog}.{target_schema} ensured")
    
    # Retrieve the DataFrame from the previous node
    previous_node_id = "V0S79"
    final_df_V0S79 = spark.table(f"{target_catalog}.{target_schema}.previous_table")  # Replace with actual table name
    
    # Write to Unity Catalog (overwrite mode handles table replacement automatically)
    final_df_V0S79.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    
    logger.info(f"✓ Data written to {target_catalog}.{target_schema}.{target_table}")
    
    # Safe logging
    logger.info(f"=== Node V150S0: ins_CREDIT_USER_INFO ===")
    log_df_info(final_df_V0S79, "Output DataFrame")
    
    logger.info("✓ Node V150S0 completed successfully")
    
except Exception as e:
    logger.error(f"✗ Error in Node V150S0 (ins_CREDIT_USER_INFO): {str(e)}")
    raise Exception(f"Critical error in Node V150S0: {str(e)}")


