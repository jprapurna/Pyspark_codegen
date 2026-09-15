import json
import boto3


def get_snowflake_connection(secret_name):
    """Fetch this job's Snowflake JDBC connection details from AWS Secrets Manager."""
    secret = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)["SecretString"])
    return secret["url"], secret["username"], secret["password"]


def read_staged_table_with_fallback(spark, glueContext, s3_bucket, table_name, glue_database, select_columns=None, logger=None, s3_prefix=None):
    """Attempt to read a staged parquet table from S3 first, falling back to the Glue Data Catalog.

    Args:
        spark: Spark session
        glueContext: GlueContext
        s3_bucket: S3 bucket or prefix (string)
        table_name: physical table name (WRK_...)
        glue_database: Glue database name
        select_columns: optional list of column names to project via selectExpr
        logger: optional logger; if None a module logger will be created
        s3_prefix: optional S3 prefix to override using table_name in the path

    Returns:
        Spark DataFrame

    Raises:
        Re-raises the exception if both S3 and Glue Catalog reads fail.
    """
    if logger is None:
        import logging
        logger = logging.getLogger("utils")

    path = f"s3://{s3_bucket}/{s3_prefix or table_name}/"
    try:
        logger.info(f"Attempting staged parquet read from {path}")
        df = spark.read.parquet(path)
        if select_columns:
            df = df.selectExpr(*select_columns)
        logger.info(f"Read {table_name} from S3")
        return df
    except Exception as e:
        logger.warning(f"Failed to read staged parquet from {path}, falling back to Glue Data Catalog: {e}")

    try:
        logger.info(f"Reading {table_name} from Glue Data Catalog {glue_database}.{table_name}")
        dyf = glueContext.create_dynamic_frame_from_catalog(database=glue_database, table_name=table_name)
        df = dyf.toDF()
        if select_columns:
            df = df.selectExpr(*select_columns)
        logger.info(f"Read {table_name} from Glue Catalog")
        return df
    except Exception:
        logger.error(f"Failed reading {table_name} from Glue Data Catalog", exc_info=True)
        raise
