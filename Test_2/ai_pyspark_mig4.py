import json, sys, logging, traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, coalesce, trim, ltrim, rtrim
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper functions
def iif(cond, t, f): return when(cond, t).otherwise(f)
def decode(key_col, pairs, default_val):
    expr = None
    for k, v in pairs.items():
        expr = when(key_col == k, v) if expr is None else expr.when(key_col == k, v)
    return expr.otherwise(default_val)
def to_int(c): return col(c).cast(\"int\")
def non_empty(c): return when((c.isNull()) | (trim(c) == \"\"), None).otherwise(c)

def main(migration_plan_str: str):
    try:
        # Load migration plan
        migration_plan = json.loads(migration_plan_str)
        logger.info(\"Loaded migration plan.\")

        # Validate minimal keys
        if not migration_plan.get(\"sources\") and not migration_plan.get(\"targets\"):
            logger.error(\"Migration plan must contain at least sources or targets.\")
            sys.exit(1)

        # Initialize Spark session
        spark = SparkSession.builder.appName(\"DataMigration\").getOrCreate()
        dfs = {}

        # Process sources
        for source in migration_plan.get(\"sources\", []):
            node_name = source[\"node_name\"]
            if source.get(\"sql_override\"):
                df = spark.sql(source[\"sql_override\"])
            else:
                table_name = \".\".join(filter(None, [source.get(\"catalog\"), source.get(\"schema\"), source.get(\"table\")]))
                df = spark.table(table_name)
            if source.get(\"filters\"):
                for filter_expr in source[\"filters\"]:
                    df = df.filter(expr(filter_expr))
            dfs[node_name] = df
            df.createOrReplaceTempView(node_name)
            logger.info(f\"Source {node_name} loaded.\")

        # Process transformations
        for transform in migration_plan.get(\"transformations\", []):
            node_name = transform[\"node_name\"]
            prev_node = transform[\"prev\"][0]
            df = dfs[prev_node]

            if transform.get(\"derivations\"):
                for derivation in transform[\"derivations\"]:
                    df = df.withColumn(derivation[\"output\"], expr(derivation[\"expression\"]))
            if transform.get(\"passthrough\"):
                df = df.select(*transform[\"passthrough\"], *[d[\"output\"] for d in transform.get(\"derivations\", [])])
            if transform.get(\"condition\"):
                df = df.filter(expr(transform[\"condition\"]))
            dfs[node_name] = df
            df.createOrReplaceTempView(node_name)
            logger.info(f\"Transformation {node_name} applied.\")

        # Process targets
        for target in migration_plan.get(\"targets\", []):
            node_name = target[\"node_name\"]
            df = dfs[target[\"upstream\"]]
            df = df.selectExpr(*[f\"{assignment['expression']} as {assignment['target_column']}\" for assignment in target[\"assignments\"]])

            table_name = \".\".join(filter(None, [target.get(\"catalog\"), target.get(\"schema\"), target.get(\"table\")]))
            write_strategy = target[\"write_strategy\"]

            if write_strategy == \"append\":
                df.write.format(\"delta\").mode(\"append\").saveAsTable(table_name)
            elif write_strategy == \"overwrite\":
                df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(table_name)
            elif write_strategy in [\"merge_update\", \"merge_upsert\"]:
                tgt = DeltaTable.forName(spark, table_name)
                src = df.alias(\"src\")
                cond = \" AND \".join([f\"t.{k}=src.{k}\" for k in target[\"merge_on\"]])
                merge_builder = tgt.alias(\"t\").merge(src, cond)
                merge_builder = merge_builder.whenMatchedUpdate(set={col[\"target_column\"]: col[\"expression\"] for col in target[\"assignments\"]})
                if write_strategy == \"merge_upsert\":
                    merge_builder = merge_builder.whenNotMatchedInsert(values={col[\"target_column\"]: col[\"expression\"] for col in target[\"assignments\"]})
                merge_builder.execute()
            else:
                logger.error(f\"Unsupported write strategy: {write_strategy}\")
                sys.exit(1)
            logger.info(f\"Target {node_name} written to {table_name}.\")

        logger.info(\"Migration plan executed successfully.\")
    except Exception as e:
        logger.error(\"Error executing migration plan.\")
        logger.error(traceback.format_exc())
        sys.exit(2)

if __name__ == \"__main__\":
    try:
        migration_plan_str = sys.stdin.read().strip()
        if not migration_plan_str:
            raise ValueError(\"No migration plan provided. Pass the plan via stdin.\")
        main(migration_plan_str)
    except Exception as e:
        logger.error(\"Fatal error in main execution.\")
        logger.error(traceback.format_exc())
        sys.exit(2)