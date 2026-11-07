import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F

D = "file:///C:/Users/USER/AUCA_BigData_Project/"

spark = (SparkSession.builder.appName("AUCA_batch").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Read raw data (files are JSON arrays, so multiLine is required)
txn = spark.read.option("multiLine", True).json(D + "transactions.json")
print("Raw transactions:", txn.count())
txn.printSchema()

# 2. Clean and normalize
txn_clean = (txn.dropDuplicates(["transaction_id"])
    .filter(F.col("user_id").isNotNull() & F.col("items").isNotNull())
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("session_id", F.when(F.col("session_id") == "", None).otherwise(F.col("session_id")))
    .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
    .withColumn("status", F.lower(F.trim(F.col("status")))))
print("Clean transactions:", txn_clean.count())

# 3. Recommendation indicator: products frequently bought together
items = (txn_clean.select("transaction_id", F.explode("items").alias("it"))
         .select("transaction_id", F.col("it.product_id").alias("product_id"))
         .distinct())
a = items.alias("a")
b = items.alias("b")
cond = ((F.col("a.transaction_id") == F.col("b.transaction_id"))
        & (F.col("a.product_id") < F.col("b.product_id")))
pairs = (a.join(b, cond)
         .groupBy(F.col("a.product_id").alias("product_x"),
                  F.col("b.product_id").alias("product_y"))
         .count()
         .orderBy(F.desc("count")))
print("Top 10 product pairs bought together:")
pairs.show(10, truncate=False)

spark.stop()
