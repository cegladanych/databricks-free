from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def get_data(catalog: str, schema: str, table: str) -> DataFrame:

    return spark.sql(f"SELECT * FROM {catalog}.{schema}.{table}")
