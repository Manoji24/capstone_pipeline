# from pyspark.sql import SparkSession
import pytest

import sys
sys.path.append("../src")

from capstone_pipeline.main import get_taxis, get_spark

@pytest.fixture
def serverless():
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        return None


def test_main():
    taxis = get_taxis(get_spark())
    assert taxis.count() > 21931
