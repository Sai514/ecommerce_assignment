# Databricks notebook source
# Import necessary libraries from PySpark
from pyspark.sql.functions import col, year, round as spark_round, expr, to_date, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType

# -----------------------------
# Step 1: Read and load input data
# -----------------------------

# Reading product data from a CSV file into a Spark DataFrame.
# Assumes the CSV file has a header row for column names.
product_df = spark.read.format("csv").option("header", True).load(
    "/Volumes/workspace/default/peiassignemnt/Products.csv"
)
# Display the product DataFrame for validation
display(product_df)

# Define the schema for the order data (JSON format)
# Assumes that the order data is in JSON format and has specific fields.
schema = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

# Reading order data from a JSON file with a predefined schema
# The `multiLine` option handles multi-line JSON files.
order_df = spark.read.format("json").schema(schema).option("multiLine", True).load(
    "/Volumes/workspace/default/peiassignemnt/Orders.json"
)
# Display the orders DataFrame for validation
display(order_df)






# COMMAND ----------

# -----------------------------
# Step 2: Read and load customer data
# -----------------------------

# Install and import pandas to work with Excel files
# MAGIC %pip install openpyxl
import pandas as pd

# Read the Excel file into a pandas DataFrame
# Assumes the Excel file contains a 'phone' column that should be treated as a string.
customer_pd = pd.read_excel("/Volumes/workspace/default/peiassignemnt/Customer.xlsx")
customer_pd['phone'] = customer_pd['phone'].astype(str)

# Convert the pandas DataFrame to a Spark DataFrame for distributed processing
customers_df = spark.createDataFrame(customer_pd)
# Display the customers DataFrame for validation
display(customers_df)


# COMMAND ----------

# -----------------------------
# Step 3: Register temporary views for SQL queries
# -----------------------------

# Register the DataFrames as temporary SQL views to enable SQL-based querying
order_df.createOrReplaceTempView("raw_orders")
product_df.createOrReplaceTempView("raw_products")
customers_df.createOrReplaceTempView("raw_customers")

# COMMAND ----------

# -----------------------------
# Step 4: Clean and Transform Data
# -----------------------------

# Join the raw customer and product data to the order data and clean it.
# Ensure correct date formatting, rounding, and calculating profit.
enriched_customers_df = customers_df.select(
    "Customer ID", "Customer Name", "Country"
)

enriched_products_df = product_df.select(
    "Product ID", "Category", "Sub-Category"
)

# Join the orders with enriched customer and product data
enriched_orders_df = order_df \
    .join(enriched_customers_df, on="Customer ID", how="inner") \
    .join(enriched_products_df, on="Product ID", how="inner") \
    .withColumn("Order Date", to_date(regexp_replace(col("Order Date"), "/", "-"), "d-M-yyyy")) \
    .withColumn("Profit", spark_round(
        expr("Quantity * Price * (1 - Discount)"), 2
    ).cast(DecimalType(10, 2))) \
    .withColumn("Discount", spark_round(col("Discount"), 2)) \
    .select(
        "Order ID", "Order Date", "Customer ID", "Customer Name", "Country",
        "Product ID", "Category", "Sub-Category", "Quantity", "Price", "Discount", "Profit"
    )


# COMMAND ----------

# -----------------------------
# Step 5: Aggregated Profit View
# -----------------------------

# Aggregate the profit by year, category, sub-category, and customer name
aggregated_profit_df = enriched_orders_df \
    .withColumn("Year", year(col("Order Date"))) \
    .groupBy("Year", "Category", "Sub-Category", "Customer Name") \
    .sum("Profit") \
    .withColumnRenamed("sum(Profit)", "Total Profit")

# Register the aggregated profit DataFrame as a temporary SQL view for further analysis
aggregated_profit_df.createOrReplaceTempView("aggregated_profit")



# COMMAND ----------

# -----------------------------
# Step 6: SQL Queries for Profit Analysis
# -----------------------------

# Query to calculate total profit by year
profit_by_year = spark.sql("""
    SELECT year(`Order Date`) AS Year, ROUND(SUM(Profit), 2) AS Profit
    FROM enriched_orders
    GROUP BY year(`Order Date`)
""")

# Query to calculate profit by year and category
profit_by_year_category = spark.sql("""
    SELECT year(`Order Date`) AS Year, Category, ROUND(SUM(Profit), 2) AS Profit
    FROM enriched_orders
    GROUP BY year(`Order Date`), Category
""")

# Query to calculate total profit by customer
profit_by_customer = spark.sql("""
    SELECT `Customer Name`, ROUND(SUM(Profit), 2) AS Profit
    FROM enriched_orders
    GROUP BY `Customer Name`
""")

# Query to calculate total profit by customer and year
profit_by_customer_year = spark.sql("""
    SELECT `Customer Name`, year(`Order Date`) AS Year, ROUND(SUM(Profit), 2) AS Profit
    FROM enriched_orders
    GROUP BY `Customer Name`, year(`Order Date`)
""")
