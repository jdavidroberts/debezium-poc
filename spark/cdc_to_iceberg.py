"""
CDC Pipeline: PostgreSQL -> Kafka (Debezium) -> Apache Iceberg
==============================================================
Spark Structured Streaming job that reads Debezium CDC events from Kafka,
flattens the JSON envelope, deduplicates within each micro-batch, and
performs MERGE INTO on Iceberg tables (insert / update / delete).

Why micro-batch + foreachBatch?
  Iceberg's MERGE INTO is a scan-then-write operation that requires batch
  semantics.  Spark Structured Streaming's micro-batch engine is the standard
  approach for this.  The trigger interval (default 10s) controls the latency /
  throughput tradeoff — set it as low as 1s for near-real-time, or higher
  (30-60s) in production to reduce the number of small Iceberg data files.
  (For true record-level streaming into Iceberg, use Apache Flink instead.)

Run with:
    spark-submit --master local[*] /opt/spark-app/cdc_to_iceberg.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, coalesce, row_number
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:29092"
ICEBERG_WAREHOUSE = "/tmp/iceberg_warehouse"
CHECKPOINT_BASE = "/tmp/checkpoints"
TRIGGER_INTERVAL = "10 seconds"    # tune: 1s = lowest latency, 60s = fewer files

# Maps each Debezium topic to its Iceberg table definition.
# - pk:      primary key column (used for MERGE ON + dedup)
# - columns: ordered dict of column_name -> Spark SQL type
#
# Type notes (with schemas.enable=false in Debezium):
#   TIMESTAMPTZ -> ISO-8601 string  ->  stored as STRING here
#   NUMERIC     -> double           ->  via decimal.handling.mode=double in connector
TABLE_DEFS = {
    "dbserver1.public.merchants": {
        "iceberg_table": "iceberg.cdc.merchants",
        "pk": "id",
        "columns": {
            "id":         "INT",
            "name":       "STRING",
            "created_at": "STRING",
        },
    },
    "dbserver1.public.payments": {
        "iceberg_table": "iceberg.cdc.payments",
        "pk": "id",
        "columns": {
            "id":          "INT",
            "merchant_id": "INT",
            "amount":      "DOUBLE",
            "status":      "STRING",
            "created_at":  "STRING",
            "updated_at":  "STRING",
        },
    },
}


# ---------------------------------------------------------------------------
# Spark session with Iceberg catalog
# ---------------------------------------------------------------------------
def create_spark():
    return (
        SparkSession.builder
        .appName("CDC-to-Iceberg")
        .master("local[*]")
        # Iceberg SQL extensions — enables MERGE INTO, time-travel queries, etc.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Hadoop-based Iceberg catalog (filesystem; no metastore required)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
        # Keep shuffle partitions low for single-node POC
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Iceberg DDL — create namespace + tables on first run
# ---------------------------------------------------------------------------
def init_iceberg_tables(spark):
    """Create the 'cdc' namespace and destination tables if they don't exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.cdc")

    for _topic, defn in TABLE_DEFS.items():
        col_defs = ", ".join(
            f"{name} {dtype}" for name, dtype in defn["columns"].items()
        )
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {defn['iceberg_table']} (
                {col_defs},
                op     STRING  COMMENT 'Debezium op: c=create, u=update, d=delete, r=snapshot-read',
                ts_ms  BIGINT  COMMENT 'Debezium event timestamp in epoch milliseconds'
            ) USING iceberg
        """
        spark.sql(ddl)
        print(f"  [init] {defn['iceberg_table']} ready")


# ---------------------------------------------------------------------------
# foreachBatch processor factory
# ---------------------------------------------------------------------------
def make_batch_processor(spark, table_def):
    """
    Return a function(batch_df, batch_id) that:
      1. Parses the Debezium JSON envelope
      2. Deduplicates by PK (keeps latest ts_ms)
      3. MERGEs into the Iceberg table

    The MERGE handles all CDC operations in one statement:
      - INSERT  (op 'c' or 'r')  -> WHEN NOT MATCHED ... INSERT *
      - UPDATE  (op 'u')         -> WHEN MATCHED ... UPDATE SET *
      - DELETE  (op 'd')         -> WHEN MATCHED ... DELETE
    """
    iceberg_table = table_def["iceberg_table"]
    columns = table_def["columns"]
    pk = table_def["pk"]

    def _process(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # ── 1. Parse the Debezium JSON envelope ──────────────────────
        #
        # Event structure (schemas.enable=false):
        #   { "before": {...}|null, "after": {...}|null,
        #     "op": "c"|"u"|"d"|"r", "ts_ms": 1234567890, ... }
        #
        # INSERT/UPDATE/SNAPSHOT (c/u/r): "after" has the new row
        # DELETE                 (d):     "after" is null; "before" has the old row
        select_exprs = [
            get_json_object(col("value"), "$.op").alias("op"),
            get_json_object(col("value"), "$.ts_ms").cast("bigint").alias("ts_ms"),
        ]

        for name, dtype in columns.items():
            after_val = get_json_object(col("value"), f"$.after.{name}")
            before_val = get_json_object(col("value"), f"$.before.{name}")

            if name == pk:
                # PK: prefer 'after', fall back to 'before' (for deletes)
                expr = coalesce(after_val, before_val).cast(dtype)
            else:
                # Non-PK: only need 'after' (null for deletes is fine —
                # the MERGE DELETE branch ignores column values)
                expr = after_val.cast(dtype)

            select_exprs.append(expr.alias(name))

        parsed = batch_df.select(*select_exprs)

        # ── 2. Deduplicate within the micro-batch ────────────────────
        # Multiple events for the same PK in one batch (e.g. rapid INSERT
        # then UPDATE) — keep only the latest by ts_ms.
        window = Window.partitionBy(pk).orderBy(col("ts_ms").desc())
        deduped = (
            parsed
            .withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # ── 3. MERGE INTO the Iceberg table ──────────────────────────
        # Use a *global* temp view — session-scoped temp views are invisible
        # to MERGE INTO when the Iceberg catalog is the active catalog.
        view_name = f"__cdc_batch_{iceberg_table.split('.')[-1]}"
        deduped.createOrReplaceGlobalTempView(view_name)

        merge_sql = f"""
            MERGE INTO {iceberg_table} t
            USING global_temp.{view_name} s
            ON t.{pk} = s.{pk}
            WHEN MATCHED AND s.op = 'd'
                THEN DELETE
            WHEN MATCHED AND s.op IN ('u', 'c', 'r')
                THEN UPDATE SET *
            WHEN NOT MATCHED AND s.op != 'd'
                THEN INSERT *
        """
        spark.sql(merge_sql)
        print(f"  [batch {batch_id}] Merged into {iceberg_table}")

    return _process


# ---------------------------------------------------------------------------
# Main — wire up one streaming query per table
# ---------------------------------------------------------------------------
def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Initialising Iceberg tables ===")
    init_iceberg_tables(spark)

    queries = []
    for topic, defn in TABLE_DEFS.items():
        table_name = topic.split(".")[-1]   # "merchants" or "payments"
        print(f"\n=== Starting stream: {topic} -> {defn['iceberg_table']} ===")

        # Read from Kafka — value is binary, cast to UTF-8 string
        stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
            .filter(col("value").isNotNull())   # drop Debezium tombstones
        )

        query = (
            stream.writeStream
            .foreachBatch(make_batch_processor(spark, defn))
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table_name}")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(query)

    print("\n=== All streams running. Ctrl+C to stop. ===\n")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
