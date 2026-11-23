# Import modules
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Define the path to the source data
file_path = f"/Volumes/raw/default/data"

# Schema for the books_*.json source files (multi-line JSON arrays, 37 fields)
books_schema = StructType(
    [
        StructField("asin", StringType(), True),
        StructField("ISBN10", StringType(), True),
        StructField("answered_questions", LongType(), True),
        StructField("availability", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("buybox_seller", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("date_first_available", StringType(), True),
        StructField("department", StringType(), True),
        StructField("description", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("domain", StringType(), True),
        StructField("final_price", DoubleType(), True),
        StructField("image_url", StringType(), True),
        StructField("images_count", StringType(), True),
        StructField("initial_price", DoubleType(), True),
        StructField("item_weight", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("model_number", StringType(), True),
        StructField("plus_content", BooleanType(), True),
        StructField("product_dimensions", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("reviews_count", LongType(), True),
        StructField("root_bs_rank", LongType(), True),
        StructField("seller_id", StringType(), True),
        StructField("seller_name", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("title", StringType(), True),
        StructField("upc", StringType(), True),
        StructField("url", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("video_count", LongType(), True),
        StructField(
            "delivery",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "features",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "format",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("price", StringType(), True),
                        StructField("url", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "categories",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "best_sellers_rank",
            ArrayType(
                StructType(
                    [
                        StructField("category", StringType(), True),
                        StructField("rank", LongType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)


@dp.table(
  name="bronze.dbo.books_raw",
  comment="Dane z Amazon o książkach. Same ciekawe rzeczy :)"
)
def books_raw():
  return (spark.readStream
    .format("cloudFiles")
    .schema(books_schema)
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .load(file_path))


# Pobiera definicje oczekiwań z tabeli raw.main.expectations
def get_expectations_from_table():
  return {
    row["name"]: row["expression"]
    for row in spark.read.table("raw.main.expectations").collect()
    if row["action"] == "expect"
  }


@dp.materialized_view(
  name="bronze.dbo.books_prepared",
  comment="Dane zwalidowane do bronze."
)
@dp.expect_all(get_expectations_from_table())
@dp.expect_or_drop("valid_ISBN10", "ISBN10 IS NOT NULL")
@dp.expect_or_fail("positive_answered_questions", "answered_questions >= 0")
@dp.expect("date_first_available_format", "date_first_available RLIKE '^\\d{4}-\\d{2}-\\d{2}$'")
@dp.expect("department_present", "department IS NOT NULL")
@dp.expect_or_drop("description_present", "description IS NOT NULL")
@dp.expect("discount_non_negative", "discount >= 0")
@dp.expect("domain_allowed", "domain IN ('amazon.com','amazon.co.uk','amazon.de')")
@dp.expect("final_price_positive", "final_price > 0")
@dp.expect("image_url_valid", "image_url RLIKE '^https?://.*'")
@dp.expect("images_count_valid", "images_count IS NOT NULL")
@dp.expect("initial_price_positive", "initial_price > 0")
@dp.expect("item_weight_non_negative", "item_weight >= 0")
@dp.expect("manufacturer_present", "manufacturer IS NOT NULL")
@dp.expect("model_number_present", "model_number IS NOT NULL")
@dp.expect("plus_content_boolean", "plus_content IS NOT NULL")
@dp.expect("product_dimensions_present", "product_dimensions IS NOT NULL")
@dp.expect("rating_format", "rating RLIKE '^\\d+(\\.\\d+)?'")
@dp.expect("reviews_count_non_negative", "reviews_count >= 0")
@dp.expect("root_bs_rank_non_negative", "root_bs_rank >= 0")
@dp.expect("seller_id_present", "seller_id IS NOT NULL")
@dp.expect("seller_name_present", "seller_name IS NOT NULL")
@dp.expect("timestamp_format", "timestamp RLIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*'")
# @dp.expect_or_fail("title_allowed", "title IN ('Book One','Book Two','Book Three')")
@dp.expect("upc_present", "upc IS NOT NULL")
@dp.expect("url_valid", "url RLIKE '^https?://.*'")
@dp.expect("video_boolean", "video IS NOT NULL")
@dp.expect("video_count_non_negative", "video_count >= 0")
@dp.expect("delivery_array", "delivery IS NOT NULL")
@dp.expect("features_array", "features IS NOT NULL")
@dp.expect("format_array", "format IS NOT NULL")
@dp.expect("categories_array", "categories IS NOT NULL")
@dp.expect("best_sellers_rank_array", "best_sellers_rank IS NOT NULL")

def books_prepared():
  return (
    spark.read.table("bronze.dbo.books_raw")
      .select("asin", "ISBN10", "answered_questions", "availability", "brand", "buybox_seller", "currency", "date_first_available", "department", "description", "discount", "domain", "final_price", "image_url", "images_count", "initial_price", "item_weight", "manufacturer", "model_number", "plus_content", "product_dimensions", "rating", "reviews_count", "root_bs_rank", "seller_id", "seller_name", "timestamp", "title", "upc", "url", "video", "video_count", "delivery", "features", "format", "categories", "best_sellers_rank")
  )

