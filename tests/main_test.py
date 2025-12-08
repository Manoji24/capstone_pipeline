import sys
import types
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, DataFrame
import pytest

sys.path.append("../src")

import capstone_pipeline.main as main

@pytest.fixture(scope="session")
def get_spark() -> SparkSession:
    """Create and return a Spark session for testing."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        return SparkSession.builder.appName("test").getOrCreate()



def test_add_audit_columns_calls_withcolumn_in_order():
    # Create chainable DataFrame mocks: initial -> after first -> after second
    df_initial = MagicMock()
    df_after_first = MagicMock()
    df_after_second = MagicMock()

    # initial.withColumn(...) -> df_after_first
    df_initial.withColumn.return_value = df_after_first
    # df_after_first.withColumn(...) -> df_after_second
    df_after_first.withColumn.return_value = df_after_second

    res = main.add_audit_columns(df_initial)

    # Final returned object should be the result of the second withColumn
    assert res is df_after_second

    # Validate the two withColumn calls and their column names
    assert df_initial.withColumn.call_count == 1
    assert df_after_first.withColumn.call_count == 1
    assert df_initial.withColumn.call_args[0][0] == "_ingest_timestamp"
    assert df_after_first.withColumn.call_args[0][0] == "_source_file_name"


def test_create_table_bronze_invokes_toTable_and_checkpoint():
    spark = MagicMock()

    # Configure the readStream chain: spark.readStream.format(...).options(...).schema(...).load(...) -> bronze_txn_df
    format_ret = MagicMock()
    spark.readStream.format.return_value = format_ret
    format_ret.options.return_value = format_ret
    format_ret.schema.return_value = format_ret
    bronze_txn_df = MagicMock()
    format_ret.load.return_value = bronze_txn_df

    # bronze_txn_df.transform(add_audit_columns) -> bronze_txn_write
    bronze_txn_write = MagicMock()
    bronze_txn_df.transform.return_value = bronze_txn_write

    # bronze_txn_write.writeStream.format(...).option(...).toTable(table_name)
    writer_holder = MagicMock()
    bronze_txn_write.writeStream.format.return_value = writer_holder
    writer_holder.option.return_value = writer_holder
    writer_holder.outputMode.return_value = writer_holder
    writer_holder.trigger.return_value = writer_holder
    writer_holder.toTable.return_value = None

    schema = MagicMock()
    file_path = "/tmp/input"
    chk_pnt_path = "/tmp/checkpoint"
    read_options = {"opt1": "val"}
    table_name = "test_table"

    # Call the function under test
    main.create_table_bronze(spark, schema, file_path, chk_pnt_path, read_options, table_name)

    # Ensure the stream load and toTable were invoked
    format_ret.load.assert_called_once_with(file_path)
    bronze_txn_df.transform.assert_called_once()
    bronze_txn_write.writeStream.format.assert_called_once_with("delta")
    writer_holder.toTable.assert_called_once_with(table_name)


def test_audit_log_writes_delta_and_calls_display(monkeypatch):
    spark = MagicMock()

    # Create a dfhist mock with chained methods
    dfhist = MagicMock()
    dfhist.orderBy.return_value = dfhist
    dfhist.limit.return_value = dfhist
    dfhist.select.return_value = dfhist
    dfhist.withColumn.return_value = dfhist

    # Provide a writer chain returned from dfhist.withColumn().write
    writer = MagicMock()
    dfhist.withColumn.return_value.write.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer

    spark.sql.return_value = dfhist


    deltapath = "/tmp/delta_out"
    tablename = "dummy_table"

    main.audit_log(spark, tablename, deltapath)

    # verify spark.sql called and writer.save called with the path
    spark.sql.assert_called_once()
    writer.save.assert_called_once_with(deltapath)


def test_transform_inticap_applies_initcap():
    """Test that transform_inticap applies initcap transformation to specified column."""
    df = MagicMock()
    df_result = MagicMock()
    df.withColumn.return_value = df_result

    result = main.transform_inticap(df, "name_column")

    assert result is df_result
    df.withColumn.assert_called_once()
    # Check that withColumn was called with column name
    call_args = df.withColumn.call_args
    assert call_args[0][0] == "name_column"


def test_transform_clean_digits_removes_non_numeric():
    """Test that transform_clean_digits removes all non-digit characters."""
    df = MagicMock()
    df_result = MagicMock()
    df.withColumn.return_value = df_result

    result = main.transform_clean_digits(df, "phone_column")

    assert result is df_result
    df.withColumn.assert_called_once()
    call_args = df.withColumn.call_args
    assert call_args[0][0] == "phone_column"


def test_transform_clean_currency_removes_symbols():
    """Test that transform_clean_currency removes $ and , characters."""
    df = MagicMock()
    df_result = MagicMock()
    df.withColumn.return_value = df_result

    result = main.transform_clean_currency(df, "price_column")

    assert result is df_result
    df.withColumn.assert_called_once()
    call_args = df.withColumn.call_args
    assert call_args[0][0] == "price_column"


def test_transform_clean_timestamp_applies_timezone_and_utc():
    """Test that transform_clean_timestamp applies multiple transformations for timezone handling."""
    df = MagicMock()
    
    # Chain the withColumn calls
    df_trim = MagicMock()
    df_tz = MagicMock()
    df_tzcvt = MagicMock()
    
    df.withColumn.return_value = df_trim
    df_trim.withColumn.return_value = df_tz
    df_tz.withColumn.return_value = df_tzcvt

    result = main.transform_clean_timestamp(df, "timestamp_column")

    assert result is df_tzcvt
    # Should have 3 withColumn calls
    assert df.withColumn.call_count == 1
    assert df_trim.withColumn.call_count == 1
    assert df_tz.withColumn.call_count == 1


def test_fiter_corrupt_records_filters_nulls_and_y():
    """Test that fiter_corrupt_records filters null and 'Y' values."""
    df = MagicMock()
    df_result = MagicMock()
    df.filter.return_value = df_result

    result = main.fiter_corrupt_records(df, "corrupt_flag")

    assert result is df_result
    df.filter.assert_called_once()


def test_transform_hashvalue_creates_sha2_hash():
    """Test that transform_hashvalue creates a hash_value column using sha2."""
    df = MagicMock()
    df_result = MagicMock()
    df.withColumn.return_value = df_result

    column_list = ["col1", "col2", "col3"]
    result = main.transform_hashvalue(df, column_list)

    assert result is df_result
    df.withColumn.assert_called_once()
    call_args = df.withColumn.call_args
    assert call_args[0][0] == "hash_value"


# Integration tests using real Spark session with sample data
def test_transform_inticap_with_real_data(get_spark):
    """Integration test: transform_inticap with real sample data."""
    spark = get_spark
    data = [("john doe",), ("jane smith",), ("bob johnson",)]
    df = spark.createDataFrame(data, ["name"])
    
    result = main.transform_inticap(df, "name")
    
    assert result.collect() is not None
    assert "name" in result.columns


def test_transform_clean_digits_with_real_data(get_spark):
    """Integration test: transform_clean_digits removes non-digits."""
    spark = get_spark
    data = [("123-456-7890",), ("(555) 123-4567",), ("no digits here",)]
    df = spark.createDataFrame(data, ["phone"])
    
    result = main.transform_clean_digits(df, "phone")
    
    assert result.collect() is not None
    rows = result.collect()
    assert rows[0]["phone"] == "1234567890"
    assert rows[1]["phone"] == "5551234567"


def test_transform_clean_currency_with_real_data(get_spark):
    """Integration test: transform_clean_currency removes $ and commas."""
    spark = get_spark
    data = [("$1,234.56",), ("$99.99",), ("$1,000,000.00",)]
    df = spark.createDataFrame(data, ["price"])
    
    result = main.transform_clean_currency(df, "price")
    
    assert result.collect() is not None
    rows = result.collect()
    assert rows[0]["price"] == "1234.56"
    assert rows[1]["price"] == "99.99"


def test_transform_clean_timestamp_with_real_data(get_spark):
    """Integration test: transform_clean_timestamp converts timezones to UTC."""
    spark = get_spark
    data = [
        ("2024-01-15 10:30:00 PST",),
        ("2024-01-15 13:45:00 EST",),
        ("2024-06-15 14:00:00 PDT",),
    ]
    df = spark.createDataFrame(data, ["timestamp_col"])
    
    result = main.transform_clean_timestamp(df, "timestamp_col")
    
    assert result.collect() is not None
    assert "order_timestamp" in result.columns


def test_fiter_corrupt_records_with_real_data(get_spark):
    """Integration test: fiter_corrupt_records keeps null and non-'Y' values."""
    spark = get_spark
    data = [
        ("record1", None),
        ("record2", "N"),
        ("record3", "Y"),
        ("record4", None),
        ("record5", "N"),
    ]
    df = spark.createDataFrame(data, ["record_id", "corrupt_flag"])
    
    result = main.fiter_corrupt_records(df, "corrupt_flag")
    
    rows = result.collect()
    # Filter keeps: isNull (records 1, 4) OR != "Y" (records 2, 5)
    # So records 1, 2, 4, 5 are kept; record 3 (where corrupt_flag == "Y") is removed
    assert len(rows) == 4
    assert all(row["corrupt_flag"] != "Y" or row["corrupt_flag"] is None for row in rows)


def test_transform_hashvalue_with_real_data(get_spark):
    """Integration test: transform_hashvalue creates sha2 hash from columns."""
    spark = get_spark
    data = [
        ("john", "doe", "john@example.com"),
        ("jane", "smith", "jane@example.com"),
    ]
    df = spark.createDataFrame(data, ["first_name", "last_name", "email"])
    
    result = main.transform_hashvalue(df, ["first_name", "last_name", "email"])
    
    assert result.collect() is not None
    assert "hash_value" in result.columns
    rows = result.collect()
    # Verify hash values are created (64 char SHA2-256 hex string)
    assert all(len(row["hash_value"]) == 64 for row in rows)

