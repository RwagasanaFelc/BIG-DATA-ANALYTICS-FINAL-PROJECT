import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession

D = "file:///C:/Users/USER/AUCA_BigData_Project/"
spark = (SparkSession.builder.appName("AUCA_sql").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

def load(name, view):
    spark.read.option("multiLine", True).json(D + name).createOrReplaceTempView(view)

load("users.json", "users_raw")
load("products.json", "products_raw")
load("transactions.json", "txn_raw")
load("sessions_0.json", "sessions")

spark.sql("SELECT user_id, to_timestamp(registration_date) AS reg_date FROM users_raw").createOrReplaceTempView("users")
spark.sql("SELECT product_id, category_id FROM products_raw").createOrReplaceTempView("products")
spark.sql("SELECT transaction_id, user_id, to_timestamp(timestamp) AS ts, total, items FROM txn_raw").createOrReplaceTempView("txn")
spark.sql("SELECT t.user_id, t.ts, i.product_id, i.quantity, i.subtotal FROM txn t LATERAL VIEW explode(t.items) x AS i").createOrReplaceTempView("txn_items")

print("\n=== Q1: Monthly revenue and month-over-month growth ===")
spark.sql("""
WITH m AS (
  SELECT date_format(ts, 'yyyy-MM') AS month, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
  FROM txn GROUP BY date_format(ts, 'yyyy-MM'))
SELECT month, orders, revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month), 1) AS growth_pct
FROM m ORDER BY month
""").show(30, truncate=False)

print("\n=== Q2: Top 3 products per category by revenue (first 30 rows) ===")
spark.sql("""
SELECT * FROM (
  SELECT p.category_id, ti.product_id, ROUND(SUM(ti.subtotal), 2) AS revenue,
         RANK() OVER (PARTITION BY p.category_id ORDER BY SUM(ti.subtotal) DESC) AS rnk
  FROM txn_items ti JOIN products p ON ti.product_id = p.product_id
  GROUP BY p.category_id, ti.product_id)
WHERE rnk <= 3 ORDER BY category_id, rnk
""").show(30, truncate=False)

print("\n=== Q3: Cohort analysis - revenue by registration month and months since registration ===")
spark.sql("""
SELECT date_format(u.reg_date, 'yyyy-MM') AS cohort,
       CAST(months_between(trunc(t.ts, 'MM'), trunc(u.reg_date, 'MM')) AS INT) AS month_offset,
       COUNT(DISTINCT t.user_id) AS buyers, ROUND(SUM(t.total), 0) AS revenue
FROM users u JOIN txn t ON u.user_id = t.user_id
WHERE t.ts >= u.reg_date
GROUP BY date_format(u.reg_date, 'yyyy-MM'),
         CAST(months_between(trunc(t.ts, 'MM'), trunc(u.reg_date, 'MM')) AS INT)
ORDER BY cohort, month_offset
""").show(30, truncate=False)

print("\n=== Q4: Conversion rate by device type (session data, as if loaded from HBase) ===")
spark.sql("""
SELECT device_profile.type AS device, COUNT(*) AS sessions,
       ROUND(100 * AVG(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END), 2) AS conversion_pct
FROM sessions GROUP BY device_profile.type ORDER BY sessions DESC
""").show(truncate=False)

spark.stop()
