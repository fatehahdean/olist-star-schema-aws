import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DB = "olist_raw"
OUT = "s3://fatehah-olist-dw-2026/curated"

def read(name):
    return (spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"s3://fatehah-olist-dw-2026/raw/{name}/{name}.csv"))

orders = read("olist_orders_dataset")
customers = read("olist_customers_dataset")
items = read("olist_order_items_dataset")
products = read("olist_products_dataset")
categories = read("product_category_name_translation")

# Cast timestamps (crawler may have read them as strings)
ts_cols = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for c in ts_cols:
    orders = orders.withColumn(c, F.to_timestamp(F.col(c)))

# Cast numeric columns
items = (items
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double")))

# dim_customer: one row per customer_id (per order), with unique customer for repeat analysis
dim_customer = customers.select(
    "customer_id", "customer_unique_id",
    "customer_zip_code_prefix", "customer_city", "customer_state",
).dropDuplicates(["customer_id"])

# dim_product: translated category names
dim_product = (products
    .join(categories, "product_category_name", "left")
    .select(
        "product_id",
        F.col("product_category_name_english").alias("category_name"),
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ).dropDuplicates(["product_id"]))

# fact_orders: grain = one order item
fact_orders = (items
    .join(orders, "order_id", "inner")
    .select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "customer_id", "order_status",
        F.col("order_purchase_timestamp").alias("purchase_ts"),
        F.col("order_delivered_customer_date").alias("delivered_ts"),
        F.col("order_estimated_delivery_date").alias("estimated_delivery_ts"),
        "price", "freight_value",
    )
    .withColumn("purchase_date_key",
        F.date_format("purchase_ts", "yyyyMMdd").cast("int"))
    .withColumn("total_item_value", F.col("price") + F.col("freight_value")))

# dim_date: one row per calendar day covering the order range
bounds = orders.agg(
    F.min(F.to_date("order_purchase_timestamp")).alias("start"),
    F.max(F.to_date("order_purchase_timestamp")).alias("end"),
).collect()[0]

dim_date = (spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['start']}'), "
        f"to_date('{bounds['end']}'), interval 1 day)) AS full_date")
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("month_name", F.date_format("full_date", "MMMM")))

# Write everything as Parquet
for name, df in [
    ("fact_orders", fact_orders),
    ("dim_customer", dim_customer),
    ("dim_product", dim_product),
    ("dim_date", dim_date),
]:
    df.write.mode("overwrite").parquet(f"{OUT}/{name}/")

job.commit()
