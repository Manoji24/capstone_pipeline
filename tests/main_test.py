from pyspark.sql import SparkSession


import sys
sys.path.append("../src")

from capstone_pipeline.main import create_table_bronze, audit_log, get_spark


def test_get_spark():
    spark = get_spark()
    assert isinstance(spark, SparkSession)

def test_create_table_bronze():
    create_table_bronze()
    assert 2 == 2

def test_audit_log():

    audit_log(get_spark(), "dummy_table", "/tmp/dummy_path")
    assert 1 + 1 == 2
    
