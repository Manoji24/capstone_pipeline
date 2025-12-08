from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, \
    lit, desc, initcap, trim, regexp_replace, \
    to_utc_timestamp, to_timestamp 


def add_audit_columns(df: DataFrame) -> DataFrame:
    # withColumn returns a new DataFrame; make sure we return the augmented DataFrame
    df = df.withColumn("_ingest_timestamp", current_timestamp())
    df = df.withColumn("_source_file_name", col("_metadata.file_path"))
    return df


def create_table_bronze(spark, schema, file_path, chk_pnt_path, read_options, table_name) -> None:
    bronze_txn_df = (spark.readStream
                     .format("cloudFiles")
                     .options(**read_options)
                     .schema(schema)
                     .load(file_path))

    # Add additional columns for ingestion timestamp and source file name
    bronze_txn_write = (bronze_txn_df.transform(add_audit_columns))

    # Create the Delta Table
    (bronze_txn_write.writeStream
        .format("delta")
        .option("checkpointLocation", chk_pnt_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(once=True)
        .toTable(table_name))

def audit_log(spark, tablename, deltapath):
    dfhist = spark.sql(f"describe history {tablename}")

    (dfhist.orderBy(desc(col("timestamp")))
        .limit(1)
        .select("timestamp", "userName", "operation", "job.jobRunId", "notebook.notebookId", "operationMetrics.numOutputRows")
        .withColumn("table_name", lit(tablename))
        .write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(deltapath))


def transform_inticap(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, initcap(col(column_name)))

def transform_clean_timestamp(df: DataFrame, column_name: str) -> DataFrame:
    dftrim = df.withColumn("order_timestamp_trim", trim(col(column_name)))

    dftz = dftrim.withColumn(
                "order_timestamp_tz",
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(col("order_timestamp_trim"), "PST", "-08:00"),
                        "PDT", "-07:00"),
                    "EST", "-05:00"),
                "EDT", "-04:00")
            )
    
    dftzcvt = dftz.withColumn(
                "order_timestamp",
                to_utc_timestamp(to_timestamp(col("order_timestamp_tz"), "yyyy-MM-dd HH:mm:ss XXX"), "UTC")
            )
    
    return dftzcvt

def transform_clean_digits(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, regexp_replace(col(column_name), "[^0-9]", ""))

def transform_clean_currency(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, regexp_replace(col(column_name), "[$,]", ""))

def fiter_corrupt_records(df: DataFrame, corrupt_column: str) -> DataFrame:
    return df.filter(col(corrupt_column).isNull() | (col(corrupt_column) != lit("Y")))

def transform_hashvalue(df: DataFrame, column_names: list) -> DataFrame:
    from pyspark.sql.functions import sha2, concat_ws
    return df.withColumn("hash_value", sha2(concat_ws("|", *column_names), 256))
