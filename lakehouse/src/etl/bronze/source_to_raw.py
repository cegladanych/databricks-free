# Databricks notebook source
# DBTITLE 1,Ustawianie widgetów dla ścieżki i tabeli
dbutils.widgets.text("catalog", "lakehouse", "Katalog docelowy")
dbutils.widgets.text("schema", "dev", "Schemat docelowy")
dbutils.widgets.text("table", "readings", "Tabela")
dbutils.widgets.text("file_type", "CSV", "Typ pliku")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
file_type = dbutils.widgets.get("file_type")

destination_full_name = f"{catalog}.{schema}.{table}"
raw_path = "/Volumes/lakehouse/raw/data"
log_path = "/Volumes/lakehouse/raw/log"
checkpointLocation_path = f"{log_path}/checkpoints/{table}"

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name,col
import os 

# COMMAND ----------

# DBTITLE 1,Dynamiczne ustawianie opcji loadera wg file_type
# Funkcja pomocnicza – dynamiczne opcje w zależności od typu pliku

def get_loader_options(file_type: str):
    # Ustawienia standardowe
    base_options = {
        "cloudFiles.schemaLocation": log_path,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.allowOverwrites": "true",
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.maxFilesPerTrigger": "10",
        "cloudFiles.validateOptions": "false",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }
    # Dodatki w zależności od typu danych
    if file_type == "CSV":
        base_options["cloudFiles.format"] = "csv"
        base_options["header"] = "true"
    elif file_type == "JSON":
        base_options["cloudFiles.format"] = "json"
        base_options["multiLine"] = "true"
    else:
        base_options["cloudFiles.format"] = "parquet"
    return base_options

# Połączenie z widgetem
options = get_loader_options(file_type)

print(f"file_type: {file_type}")

# COMMAND ----------

def write_data_to_bronze(batch_df, epochId, destination_full_name):
    record_count = batch_df.count()
    print(f"Writing epoch {epochId} with {record_count} records to {destination_full_name}")
    if record_count > 0:
        batch_df.write.format("delta").mode("append").saveAsTable(destination_full_name)
    else:
        print(f"No records for epoch {epochId}, skipping write")

# COMMAND ----------

# DBTITLE 1,Auto Loader: obsługa CSV i JSON (stream)
# Funkcja strumieniowego odczytu danych via CloudFiles

def read_stream(source_path: str, options: dict) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .option("checkpointLocation", checkpointLocation_path)
        .load(source_path)
        .withColumn("source_file", col("_metadata.file_path"))
    )

# Funkcja zapisu streamu do tabeli bronze

def write_stream_to_table(df: DataFrame, bronze_table: str):
    (
        df.writeStream
        .trigger(availableNow=True)
        .format("delta")
        .option("checkpointLocation", checkpointLocation_path)
        .outputMode("append")
        .foreachBatch(lambda batch_df, epochId: write_data_to_bronze(
            batch_df, 
            epochId, 
            destination_full_name)
        ) 
        .start()
        .awaitTermination()
    )

# Wykonanie strumieniowego odczytu i zapisu
raw_stream_df = read_stream(raw_path, options)
write_stream_to_table(raw_stream_df, destination_full_name)
