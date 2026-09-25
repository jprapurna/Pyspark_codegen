import json
import boto3


def get_snowflake_connection(secret_name):
    """Fetch this job's Snowflake JDBC connection details from AWS Secrets Manager."""
    secret = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)["SecretString"])
    return secret["url"], secret["username"], secret["password"]


# --- Utilities appended by deduper ---
def read_staged_parquet_or_catalog(spark, glueContext, s3_bucket, table_name, glue_database, select_cols=None, logger=None):
    """Try reading a staged parquet at s3://{s3_bucket}/{table_name}/ and fall back to Glue Catalog if missing.

    Returns a Spark DataFrame. If select_cols is provided, projects those columns after read.
    logger is optional and, if given, will receive info/warning messages.
    """
    try:
        if logger:
            logger.info(f"Attempting to read staged {table_name} from S3 as parquet")
        df = spark.read.parquet(f"s3://{s3_bucket}/{table_name}/")
    except Exception as e:
        if logger:
            logger.warning(f"Staged parquet for {table_name} not found on S3; falling back to Glue Catalog read: %s", e)
        dyf = glueContext.create_dynamic_frame_from_catalog(database=glue_database, table_name=table_name)
        df = dyf.toDF()
    if select_cols:
        df = df.select(*select_cols)
    return df


def write_parquet_to_s3(df, s3_bucket, table_name, mode='overwrite', logger=None):
    """Write DataFrame to s3://{s3_bucket}/{table_name}/ as parquet with the chosen mode and optional logging."""
    try:
        if logger:
            logger.info(f"Writing {table_name} to S3 as parquet ({mode})")
        df.write.mode(mode).parquet(f"s3://{s3_bucket}/{table_name}/")
    except Exception as e:
        if logger:
            logger.error(f"Failed writing {table_name} to S3: {e}", exc_info=True)
        raise


def apply_update_strategy_s3_path(spark, rows_to_apply_df, target_path, pk_col, logger=None):
    """Apply Update-Strategy (anti-join existing by PK, union in incoming apply rows) where target is specified by a full s3 path.

    rows_to_apply_df: DataFrame containing rows to INSERT/UPDATE (already filtered and without dd_op column).
    target_path: full s3 path string such as 's3://bucket/tablename/' or a variable already containing such string.
    pk_col: primary key column name (string) used to anti-join existing.
    Returns the combined DataFrame written out (and writes by overwrite to the given path).
    """
    try:
        existing = spark.read.parquet(target_path)
    except Exception:
        # no existing target, create empty DF with same schema as rows_to_apply_df
        existing = spark.createDataFrame([], rows_to_apply_df.schema)
    changed_keys = rows_to_apply_df.select(pk_col).distinct()
    surviving = existing.join(changed_keys, on=pk_col, how='left_anti')
    combined = surviving.unionByName(rows_to_apply_df, allowMissingColumns=True)
    combined.write.mode('overwrite').parquet(target_path)
    if logger:
        logger.info(f"Applied update strategy and overwrote {target_path}")
    return combined


def apply_update_strategy_s3(spark, rows_to_apply_df, s3_bucket, target_table, pk_col, logger=None):
    """Convenience wrapper: target is specified by s3_bucket and target_table name (the function writes to s3://{s3_bucket}/{target_table}/).

    rows_to_apply_df: DataFrame of rows to INSERT/UPDATE (already filtered, without dd_op).
    """
    target_path = f"s3://{s3_bucket}/{target_table}/"
    return apply_update_strategy_s3_path(spark, rows_to_apply_df, target_path, pk_col, logger=logger)


def apply_update_strategy_jdbc(spark, rows_to_apply_df, jdbc_url, target_dbtable, connection_properties, pk_col, logger=None):
    """Apply Update-Strategy against a JDBC target table.

    rows_to_apply_df: DataFrame containing rows to INSERT/UPDATE (already filtered and without dd_op)
    jdbc_url: JDBC URL string
    target_dbtable: database table name (string) or SQL-wrapped select as appropriate
    connection_properties: dict with keys expected by Spark .jdbc write/read (e.g. {'user':..., 'password':...})
    pk_col: primary key column name used for anti-join
    Returns the new full DataFrame which was written to the JDBC target (overwrite mode).
    """
    # read existing table
    try:
        existing = spark.read.format('jdbc').options(url=jdbc_url, dbtable=target_dbtable, **connection_properties).load()
    except Exception:
        # if read fails, treat as empty
        existing = spark.createDataFrame([], rows_to_apply_df.schema)
    if rows_to_apply_df.rdd.isEmpty():
        remaining = existing
    else:
        changed_keys = rows_to_apply_df.select(pk_col).distinct()
        remaining = existing.join(changed_keys, on=pk_col, how='left_anti')
    new_full = remaining.unionByName(rows_to_apply_df, allowMissingColumns=True)
    # Write back via jdbc (overwrite)
    new_full.write.jdbc(url=jdbc_url, table=target_dbtable, mode='overwrite', properties=connection_properties)
    if logger:
        logger.info(f"Applied JDBC update strategy and overwrote {target_dbtable}")
    return new_full


def read_dedup_csv_lookup(spark, path, dedup_keys=None, header=True, normalize_col=None, cache=False):
    """Read a flat-file CSV lookup, optionally deduplicate on dedup_keys, optionally normalize a column, and optionally cache.

    normalize_col: tuple (src_col, dst_col) to produce dst_col trimmed from src_col if present, otherwise dst_col=NULL.
    """
    from pyspark.sql.functions import trim, col, lit
    df = spark.read.option('header', 'true').csv(path) if header else spark.read.csv(path)
    if normalize_col and isinstance(normalize_col, tuple):
        src, dst = normalize_col
        if src in df.columns:
            df = df.withColumn(dst, trim(col(src)))
        else:
            df = df.withColumn(dst, lit(None))
    if dedup_keys:
        df = df.dropDuplicates(dedup_keys)
    if cache:
        df = df.cache()
    return df


def load_jdbc_lookup_dedup(spark, jdbc_options, sql_override, dedup_keys):
    """Load a reference lookup via JDBC using an SQL override (wrapped as a subquery), dropDuplicates on dedup_keys and return a DataFrame suited for broadcasting.

    jdbc_options: dict of options accepted by spark.read.format('jdbc').options(...) excluding dbtable
    sql_override: string SQL that selects the lookup rows (will be wrapped as (sql_override) t for dbtable)
    dedup_keys: list of column names to dropDuplicates on
    """
    dbtable_expr = f"({sql_override}) t"
    df = spark.read.format('jdbc').options(**jdbc_options).option('dbtable', dbtable_expr).load()
    if dedup_keys:
        df = df.dropDuplicates(dedup_keys)
    from pyspark.sql.functions import broadcast
    return df


def split_and_cast_delimited_amount(df, src_col='CVG_AMT', prefix='CVG_AMT', delimiter='/', cast_to='decimal(14,0)', treat_zero_as_null=True):
    """Generic helper to clean commas from a delimited amount string, split into parts and produce typed numeric part columns.

    Produces columns named {prefix}_NO_OF_PARTS, {prefix}_1_String, {prefix}_2_String, {prefix}_3_String, and corresponding decimal columns {prefix}_1_Decimal etc.
    """
    from pyspark.sql.functions import regexp_replace, trim, split, size, element_at, coalesce, lit, col, when
    clean = regexp_replace(trim(col(src_col)), ',', '')
    parts_col = split(clean, delimiter)
    df2 = (
        df.withColumn(f"{prefix}_SRC", clean)
          .withColumn(f"{prefix}_NO_OF_PARTS", size(parts_col))
          .withColumn(f"{prefix}_1_String", coalesce(element_at(parts_col, 1), lit('0')))
          .withColumn(f"{prefix}_2_String", coalesce(element_at(parts_col, 2), lit('0')))
          .withColumn(f"{prefix}_3_String", coalesce(element_at(parts_col, 3), lit('0')))
          .withColumn(f"{prefix}_1_Decimal", when((col(f"{prefix}_1_String").isNull()) | (col(f"{prefix}_1_String") == '0') & lit(treat_zero_as_null), lit(None)).otherwise(regexp_replace(col(f"{prefix}_1_String"), ',', '').cast(cast_to)))
          .withColumn(f"{prefix}_2_Decimal", when((col(f"{prefix}_2_String").isNull()) | (col(f"{prefix}_2_String") == '0') & lit(treat_zero_as_null), lit(None)).otherwise(regexp_replace(col(f"{prefix}_2_String"), ',', '').cast(cast_to)))
          .withColumn(f"{prefix}_3_Decimal", when((col(f"{prefix}_3_String").isNull()) | (col(f"{prefix}_3_String") == '0') & lit(treat_zero_as_null), lit(None)).otherwise(regexp_replace(col(f"{prefix}_3_String"), ',', '').cast(cast_to)))
    )
    return df2


def apply_sorter(src_df, sort_keys, output_mappings):
    """Generic helper: orderBy then select/alias ports (preserving missing columns as NULL).

    sort_keys: list of column names to sort by (ascending)
    output_mappings: list of tuples (source_col, target_col) used to build the final projection; if source_col is not present we emit NULL as target_col
    """
    from pyspark.sql.functions import col
    sorted_df = src_df.orderBy(*[col(k).asc() for k in sort_keys])
    existing = set(sorted_df.columns)
    select_exprs = [f"{s} AS {t}" if s in existing else f"NULL AS {t}" for s, t in output_mappings]
    return sorted_df.selectExpr(*select_exprs)


def run_sql_override_with_staged_view(spark, df_staged, view_name, sql_query):
    """Register df_staged as a temp view with view_name and run sql_query against it, returning the resulting DataFrame."""
    df_staged.createOrReplaceTempView(view_name)
    return spark.sql(sql_query)
