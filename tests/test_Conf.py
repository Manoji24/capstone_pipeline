"""Pytest configuration and fixtures for capstone_pipeline tests."""
import sys
from unittest.mock import MagicMock

# Mock PySpark imports if running in CI without Java/Spark
try:
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    # Create mock objects for PySpark types
    SparkSession = MagicMock
    DataFrame = MagicMock
    
    # Inject mocks into sys.modules to prevent import errors
    sys.modules['pyspark'] = MagicMock()
    sys.modules['pyspark.sql'] = MagicMock()
    sys.modules['pyspark.sql.types'] = MagicMock()
    sys.modules['pyspark.sql.functions'] = MagicMock()
    sys.modules['pyspark.sql.window'] = MagicMock()
    
    # Add them to the test module namespace
    import tests.test_main
    if hasattr(tests.test_main, 'SparkSession'):
        tests.test_main.SparkSession = SparkSession
    if hasattr(tests.test_main, 'DataFrame'):
        tests.test_main.DataFrame = DataFrame
