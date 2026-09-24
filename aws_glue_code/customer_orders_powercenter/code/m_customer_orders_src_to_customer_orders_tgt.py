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


# Top-of-script placeholders for environment-specific values
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
CATALOG_DATABASE = "REPLACE_WITH_CATALOG_DATABASE"
CATALOG_TABLE = "REPLACE_WITH_CATALOG_TABLE"

# Source: customer_orders_src - read from Glue Data Catalog and convert to Spark DataFrame
try:
    logger.info("Reading source customer_orders_src from Glue Data Catalog")
    dyf_customer_orders_src_fierce_heisenberg = glueContext.create_dynamic_frame_from_catalog(
        database=CATALOG_DATABASE,
        table_name=CATALOG_TABLE,
    )
    df_customer_orders_src_fierce_heisenberg = dyf_customer_orders_src_fierce_heisenberg.toDF()
    # Project only the columns the mapping downstream requires (explicit list)
    df_customer_orders_src_fierce_heisenberg = df_customer_orders_src_fierce_heisenberg.selectExpr(
        "order_id",
        "customer_id",
        "order_date",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "order_status",
        "country"
    )
except Exception as e:
    logger.error(f"Failed reading customer_orders_src from Glue Catalog: {e}", exc_info=True)
    raise

# Source Qualifier: SQ_customer_orders_src - explicit projection of upstream source
try:
    logger.info("Applying Source Qualifier SQ_customer_orders_src projection")
    df_SQ_customer_orders_src_modest_shannon = df_customer_orders_src_fierce_heisenberg.selectExpr(
        "order_id",
        "customer_id",
        "order_date",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "order_status",
        "country"
    )
except Exception as e:
    logger.error(f"Failed in SQ_customer_orders_src projection: {e}", exc_info=True)
    raise

# Expression: EXP_customer_orders - derive customer_id_clean, order_status_norm, order_total, source_file_name (NULL for PM variable)
try:
    logger.info("Applying Expression EXP_customer_orders deriving customer_id_clean, order_status_norm, order_total, source_file_name(NULL)")
    # Use selectExpr to explicitly list passthrough columns and compute derived expressions; PM variable becomes NULL
    df_EXP_customer_orders_mighty_spinoza = df_SQ_customer_orders_src_modest_shannon.selectExpr(
        "order_id",
        "customer_id",
        "order_date",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "order_status",
        "country",
        "TRIM(customer_id) AS customer_id_clean",
        "UPPER(TRIM(order_status)) AS order_status_norm",
        "quantity * unit_price AS order_total",
        "NULL AS source_file_name"
    )
except Exception as e:
    logger.error(f"Failed in EXP_customer_orders transformation: {e}", exc_info=True)
    raise

# Filter: FIL_customer_orders - keep only valid rows per the condition
try:
    logger.info("Applying Filter FIL_customer_orders to remove invalid rows")
    # Translated filter condition: order_id IS NOT NULL AND customer_id IS NOT NULL AND length(trim(customer_id)) > 0 AND quantity > 0 AND unit_price >= 0
    df_FIL_customer_orders_hopeful_socrates = df_EXP_customer_orders_mighty_spinoza.filter(
        "order_id IS NOT NULL AND customer_id IS NOT NULL AND length(trim(customer_id)) > 0 AND quantity > 0 AND unit_price >= 0"
    )
except Exception as e:
    logger.error(f"Failed in FIL_customer_orders filtering: {e}", exc_info=True)
    raise

# Final Output assignment and write to S3 as Parquet
# assign the output df_name as required by the mapping plan
df_customer_orders_tgt_elegant_euclid = df_FIL_customer_orders_hopeful_socrates

# write customer_orders_tgt as parquet to S3 (overwrite)
try:
    logger.info("Writing customer_orders_tgt to S3 as parquet (overwrite)")
    df_customer_orders_tgt_elegant_euclid.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/customer_orders_tgt/"
    )
except Exception as e:
    logger.error(f"Failed writing customer_orders_tgt to S3: {e}", exc_info=True)
    raise



job.commit()
