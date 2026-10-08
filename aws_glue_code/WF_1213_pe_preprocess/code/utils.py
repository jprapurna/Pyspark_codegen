import json
import boto3


def get_snowflake_connection(secret_name):
    """Fetch this job's Snowflake JDBC connection details from AWS Secrets Manager."""
    secret = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)["SecretString"])
    return secret["url"], secret["username"], secret["password"]


def read_glue_table_select(glueContext, database, table_name, required_cols):
    """Read a Glue Catalog table into a Spark DataFrame and project the required columns.

    Parameters:
    - glueContext: the AWS GlueContext instance
    - database: Glue catalog database name (string)
    - table_name: Glue catalog table name (string)
    - required_cols: iterable of column names or column expressions to pass to DataFrame.select()

    Returns:
    - pyspark.sql.DataFrame with only the requested columns selected
    """
    dyf = glueContext.create_dynamic_frame_from_catalog(database=database, table_name=table_name)
    return dyf.toDF().select(*required_cols)
