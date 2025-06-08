# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df= spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("path", "/FileStore/Spend Data/spend_analysis_dataset.csv")\
        .load()

# COMMAND ----------

df.printSchema()
df.show(5)

# COMMAND ----------

df.count()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean The Data**

# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df_clean = df.filter((col("UnitPrice").isNotNull()) & (col("UnitPrice") > 0))

# COMMAND ----------

critical_cols = ["TransactionID", "ItemName", "Quantity", "UnitPrice", "TotalCost", "PurchaseDate", "Supplier"]

df_clean = df.dropna(subset=critical_cols)

# COMMAND ----------

from pyspark.sql.functions import trim, lower

df_clean = df_clean.withColumn("Supplier", trim(lower(col("Supplier")))) \
                   .withColumn("Category", trim(lower(col("Category")))) \
                   .withColumn("ItemName", trim(lower(col("ItemName"))))

# COMMAND ----------

display(df_clean)

# COMMAND ----------

df_transformed = df_clean \
    .withColumn("Year", year(col("PurchaseDate"))) \
    .withColumn("Month", month(col("PurchaseDate"))) \
    .withColumn("Weekday", dayofweek(col("PurchaseDate")))

# COMMAND ----------

df_transformed = df_transformed.withColumn(
    "SpendCategory",
    when(col("TotalCost") < 100, "Low") \
    .when(col("TotalCost").between(100, 1000), "Medium") \
    .otherwise("High")
)

# COMMAND ----------

display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC **Total Spend per Supplier**

# COMMAND ----------

df_transformed.groupBy("Supplier")
.agg(sum("TotalCost").alias("TotalSpend")).orderBy(col("TotalSpend").desc()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Monthly Spend Trend**

# COMMAND ----------

df_transformed.groupBy("Month").agg(sum(col("TotalCost")).alias("MonthlySpend")).orderBy(col("MonthlySpend").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Top 10 Most Frequently Purchased Items**

# COMMAND ----------

df_transformed.groupBy("ItemName").agg(count(col("ItemName")).alias("FrequentItems")).orderBy(col("FrequentItems").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Avg. Order Value Per Buyer**

# COMMAND ----------

df_transformed.groupBy("Buyer").agg(avg(col("TotalCost")).alias("AverageOrderValue")).orderBy(col("AverageOrderValue").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Category-wise Spend Distribution**

# COMMAND ----------

df_transformed.groupBy("Category").agg(sum(col("TotalCost")).alias("CategorySpend")).orderBy(col("CategorySpend").desc()).show()