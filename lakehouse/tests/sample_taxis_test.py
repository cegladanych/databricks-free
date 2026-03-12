import random
from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
import pytest
import os
from lakehouse.src.utils.utils import find_all_taxis
from lakehouse.src.etl.bronze.source_to_raw import get_loader_options, write_data_to_bronze, read_stream
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# --- Setup Test Environment ---

catalog_name = "tests" #os.environ.get("test_env_variable", "default")
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



def test_find_all_taxis():
    results = find_all_taxis()
    assert results.count() > 5

def test_get_loader_options():
    csv_options = get_loader_options("CSV")
    assert csv_options["cloudFiles.format"] == "csv"
    assert csv_options["header"] == "true"

    json_options = get_loader_options("JSON")
    assert json_options["cloudFiles.format"] == "json"
    assert json_options["multiLine"] == "true"

    parquet_options = get_loader_options("PARQUET")
    assert parquet_options["cloudFiles.format"] == "parquet"

def test_write_data_to_bronze(spark_session):
    # Przygotowanie testowego DataFrames
    test_data = [("2024-01-01", "NYC", 100), ("2024-01-02", "LA", 150)]
    columns = ["date", "city", "count"]
    test_df = spark.createDataFrame(test_data, columns)

    # zbuduj pełną nazwę tabeli w utworzonym schemacie
    table_name = f"{test_schema}.test_table"

    # Wywołanie funkcji zapisu
    write_data_to_bronze(test_df, 0, table_name)

    # Sprawdzenie, czy dane zostały zapisane i nazwy kolumn się zgadzają
    saved_df = spark.table(table_name)
    assert saved_df.count() == 2
    assert set(saved_df.columns) == set(columns)