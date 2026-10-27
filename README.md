# Olist Star Schema on AWS

A star-schema warehouse built from the Brazilian E-commerce (Olist) dataset, using S3, AWS Glue (PySpark), and Amazon Athena. Redshift Serverless is the planned production target. The queries here run on Athena.

## Pipeline
1. Raw CSVs uploaded to S3, one folder per table under `raw/`
2. Glue crawler catalogs the raw tables in the `olist_raw` database
3. Glue job `glue/olist_build_star.py` cleans and joins the tables, then writes `fact_orders`, `dim_customer`, `dim_product`, and `dim_date` as Parquet to `curated/`
4. A second crawler registers the curated tables in Athena
5. Business queries are in `sql/`, and their results are in `results/`

## Data model
See `docs/erd.png`. `fact_orders` holds one row per order item and joins to `dim_customer`, `dim_product`, and `dim_date`. Payments are excluded from the fact table on purpose: an order can have several payment rows, and joining them would multiply revenue.

## Findings
Source: Olist dataset, about 113k order items from 2016 to 2018.

### 1. Revenue by state
São Paulo (SP) has the most delivered orders by a wide margin (40,494, against 12,350 for RJ). Revenue is split into item price, freight, and total, so freight can be compared across states. The first months of data are small because the dataset starts in late September 2016.

### 2. Top product categories
| Category | Items sold | Revenue (BRL) |
|---|---|---|
| health_beauty | 9,670 | 1,258,681.34 |
| watches_gifts | 11,115 | 1,036,988.68 |
| bed_bath_table | 8,641 | 988,048.97 |
| sports_leisure | 7,827 | 911,954.32 |
| computers_accessories | 8,334 | 729,762.49 |

`watches_gifts` sells the most units, but `health_beauty` earns the most revenue, so its average item price is higher.

### 3. Repeat-purchase rate
Of 95,420 unique customers (by `customer_unique_id`), 2,913 placed more than one order, a repeat rate of **3.05%**. Most customers buy once.

### 4. Delivery timing by state
`avg_days_late` is delivered date minus estimated delivery date. A negative value means the order arrived early. Every state shown has a negative average, so orders generally arrived ahead of the estimate. The most negative averages include AM (-20.01 days, 243 orders), AP (-19.08, 145), and RR (-18.98, 67). These are small northern states, so treat their averages with caution.

## Notes
- `customer_id` changes with each order in Olist, so repeat purchases use `customer_unique_id`.
- The Glue job reads the raw CSVs directly, because the crawler did not detect the header row.
- The Olist dataset is licensed CC BY-NC-SA 4.0 (non-commercial). The raw data is not included in this repo.

