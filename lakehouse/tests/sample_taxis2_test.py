import random
from databricks.sdk.runtime import spark
import pytest

from lakehouse.src.utils.utils import (
    find_all_taxis,
)
# from lakehouse.src.etl.bronze.source_to_raw import (
#     get_loader_options, 
#     write_data_to_bronze, 
#     read_stream
# )
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# --- Setup Test Environment ---

catalog_name = "tests"
schema_name = f"test_schema_{random.randint(1,int(1e9))}"
test_schema = f"{catalog_name}.{schema_name}"

@pytest.fixture(scope="session")
def spark_session(request):
    print("Starting integration tests...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
    yield spark

    def teardown():
        spark.sql(f"DROP SCHEMA IF EXISTS {test_schema} CASCADE")
    request.addfinalizer(teardown)



def test_przyklad():
    assert 1==1