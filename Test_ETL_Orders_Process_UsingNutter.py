# Databricks notebook source
# MAGIC %run /Workspace/Users/sk9441429057@gmail.com/ETL_Orders_Processing

# COMMAND ----------

# COMMAND ----------

# MAGIC %pip install nutter --quiet

# COMMAND ----------

from runtime.nutterfixture import NutterFixture
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from IPython.display import display, HTML

# COMMAND ----------

class TestETLOrdersProcess(NutterFixture):
    def __init__(self):
        NutterFixture.__init__(self)
        self.spark = SparkSession.getActiveSession()
        assert self.spark is not None, "❌ No active Spark session"
    
    # ------------------------
    # Test 1: enriched_orders exists and is not empty
    def assertion_enriched_orders_not_empty(self):
        df = self.spark.sql("SELECT * FROM enriched_orders")
        assert df.count() > 0, "❌ enriched_orders is empty"

    # ------------------------
    # Test 2: No nulls in key fields
    def assertion_no_nulls_in_key_fields(self):
        df = self.spark.sql("SELECT * FROM enriched_orders")
        for column in ["Order ID", "Customer ID", "Product ID", "Profit"]:
            nulls = df.filter(col(column).isNull()).count()
            assert nulls == 0, f"❌ Nulls found in column '{column}'"

    # ------------------------
    # Test 3: Discount is within [0, 1]
    def assertion_discount_valid_range(self):
        df = self.spark.sql("SELECT Discount FROM enriched_orders")
        bad = df.filter((col("Discount") < 0) | (col("Discount") > 1)).count()
        assert bad == 0, "❌ Discount values out of range [0,1]"

    # ------------------------
    # Test 4: Quantity should be non-negative
    def assertion_quantity_non_negative(self):
        df = self.spark.sql("SELECT Quantity FROM enriched_orders")
        negatives = df.filter(col("Quantity") < 0).count()
        assert negatives == 0, "❌ Negative quantities found"

    # ------------------------
    # Test 5: Profit is rounded to 2 decimals
    def assertion_profit_is_rounded(self):
        df = spark.sql("SELECT Profit FROM enriched_orders")
        from pyspark.sql.functions import expr

    # Compare string representation with rounded version to catch precision mismatches
        unrounded_df = df.filter(expr("Profit != ROUND(Profit, 2)"))

        unrounded = unrounded_df.count()
        if unrounded > 0:
           sample = unrounded_df.limit(5).toPandas().to_string(index=False)
           raise AssertionError(f"❌ Unrounded Profit values found: {unrounded}\nSample:\n{sample}")


    # ------------------------
    # Test 6: aggregated_profit exists and has data
    def assertion_aggregated_profit_exists(self):
        df = self.spark.sql("SELECT * FROM aggregated_profit")
        assert df.count() > 0, "❌ aggregated_profit is empty"

    # ------------------------
    # Test 7: Year exists in aggregated_profit
    def assertion_year_column_exists(self):
        df = self.spark.sql("SELECT * FROM aggregated_profit")
        assert "Year" in df.columns, "❌ Year column missing in aggregated_profit"

# COMMAND ----------

# %run your main ETL notebook before testing
# MAGIC %run ./Order_etl

# COMMAND ----------

test = TestETLOrdersProcess()
result = test.execute_tests()




# COMMAND ----------

print(result.to_string())

# COMMAND ----------

# Display result in clean HTML format
display(HTML(f"<pre>{result.to_string()}</pre>"))

# COMMAND ----------

from runtime.nutterfixture import NutterFixture
from pyspark.sql.functions import col, year, round as spark_round, abs

class TestETLPipeline(NutterFixture):
    def __init__(self):
        NutterFixture.__init__(self)

    # Check 1: enriched_orders exists and has data
    def assertion_enriched_orders_not_empty(self):
        df = spark.sql("SELECT * FROM enriched_orders")
        assert df.count() > 0, "❌ enriched_orders table is empty"

    # Check 2: no nulls in critical fields
    def assertion_no_nulls_in_key_fields(self):
        df = spark.sql("SELECT `Order ID`, `Customer ID`, `Order Date` FROM enriched_orders")
        nulls = df.filter(
            col("Order ID").isNull() |
            col("Customer ID").isNull() |
            col("Order Date").isNull()
        ).count()
        assert nulls == 0, "❌ Nulls found in key fields"

    # Check 3: profit is rounded to 2 decimal places
    def assertion_profit_is_rounded(self):
        df = spark.sql("SELECT Profit FROM enriched_orders")
        unrounded = df.filter((col("Profit") * 100) % 1 != 0).count()
        assert unrounded == 0, "❌ Unrounded Profit values found"

    # Check 4: discount is between 0 and 1
    def assertion_discount_valid_range(self):
        df = spark.sql("SELECT Discount FROM enriched_orders")
        out_of_range = df.filter((col("Discount") < 0) | (col("Discount") > 1)).count()
        assert out_of_range == 0, "❌ Discount values out of range (0–1)"

    # Check 5: quantity must be non-negative
    def assertion_quantity_non_negative(self):
        df = spark.sql("SELECT Quantity FROM enriched_orders")
        negatives = df.filter(col("Quantity") < 0).count()
        assert negatives == 0, "❌ Negative quantities found"

    # Check 6: year column should exist in aggregated_profit
    def assertion_year_column_exists(self):
        df = spark.sql("SELECT * FROM aggregated_profit")
        assert "Year" in df.columns, "❌ 'Year' column not found in aggregated_profit"

    # Check 7: aggregated_profit should not be empty
    def assertion_aggregated_profit_exists(self):
        df = spark.sql("SELECT * FROM aggregated_profit")
        assert df.count() > 0, "❌ aggregated_profit is empty"

    # Check 8: all customer names must be present
    def assertion_customer_names_exist(self):
        df = spark.sql("SELECT `Customer Name` FROM enriched_orders")
        nulls = df.filter(col("Customer Name").isNull() | (col("Customer Name") == "")).count()
        assert nulls == 0, "❌ Missing Customer Names"

    # Check 9: Product category/subcategory should exist
    def assertion_product_category_valid(self):
        df = spark.sql("SELECT Category, `Sub-Category` FROM enriched_orders")
        nulls = df.filter(col("Category").isNull() | col("Sub-Category").isNull()).count()
        assert nulls == 0, "❌ Missing Category or Sub-Category info"

    # Check 10: Order ID + Product ID should be unique
    def assertion_unique_order_product_combo(self):
        df = spark.sql("SELECT `Order ID`, `Product ID` FROM enriched_orders")
        total = df.count()
        unique = df.dropDuplicates(["Order ID", "Product ID"]).count()
        assert total == unique, "❌ Duplicate Order ID + Product ID combinations found"

    # Check 11: Profit formula validation (within tolerance)
    def assertion_profit_calculation_correct(self):
        df = spark.sql("SELECT Quantity, Price, Discount, Profit FROM enriched_orders")
        calc_df = df.withColumn(
            "Expected_Profit",
            spark_round(col("Quantity") * col("Price") * (1 - col("Discount")), 2)
        )
        mismatches = calc_df.filter(abs(col("Profit") - col("Expected_Profit")) > 0.01).count()
        assert mismatches == 0, "❌ Profit values mismatch with calculated formula"



# COMMAND ----------

# Run all tests
test = TestETLPipeline()
result = test.execute_tests()
print(result.to_string())  # Show output in Databricks cell
