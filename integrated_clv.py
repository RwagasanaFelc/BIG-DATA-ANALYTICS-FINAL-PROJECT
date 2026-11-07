import os, sys, getpass
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from urllib.parse import quote_plus
from collections import defaultdict
import happybase
from pymongo import MongoClient
from pyspark.sql import SparkSession

# 1. MongoDB: purchase history per user
pwd = getpass.getpass("MongoDB admin password: ")
uri = "mongodb://admin:%s@localhost:27017/?authSource=admin" % quote_plus(pwd)
db = MongoClient(uri)["ecommerce"]
pipe = [{"$group": {"_id": "$user_id",
                    "txn_count": {"$sum": 1},
                    "total_spent": {"$sum": "$total"}}}]
mongo_rows = list(db.transactions.aggregate(pipe, allowDiskUse=True))
span_pipe = [{"$group": {"_id": None, "mn": {"$min": "$timestamp"},
                         "mx": {"$max": "$timestamp"}}}]
span = list(db.transactions.aggregate(span_pipe))[0]
window_days = max((span["mx"] - span["mn"]).days, 1)
print("MongoDB users with purchases:", len(mongo_rows))
print("Observation window (days):", window_days)

# 2. HBase: engagement metrics per user (sessions, duration, conversions)
conn = happybase.Connection("127.0.0.1", 9090, timeout=120000)
table = conn.table("user_sessions")
eng = defaultdict(lambda: [0, 0, 0])
cols = [b"meta:duration_seconds", b"meta:conversion_status"]
for key, d in table.scan(columns=cols, batch_size=2000):
    uid = key.decode().split("#")[0]
    e = eng[uid]
    e[0] += 1
    e[1] += int(d.get(b"meta:duration_seconds", b"0") or 0)
    if d.get(b"meta:conversion_status") == b"converted":
        e[2] += 1
conn.close()
print("HBase users with sessions:", len(eng))

# 3. Spark: join both sources and compute CLV segments
spark = (SparkSession.builder.appName("AUCA_integrated").master("local[*]")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
m_rows = [(r["_id"], r["txn_count"], float(r["total_spent"])) for r in mongo_rows]
h_rows = [(u, v[0], v[1], v[2]) for u, v in eng.items()]
spark.createDataFrame(m_rows, ["user_id", "txn_count", "total_spent"]).createOrReplaceTempView("purchases")
spark.createDataFrame(h_rows, ["user_id", "sessions", "total_duration", "converted"]).createOrReplaceTempView("engagement")

clv = spark.sql(f"""
WITH base AS (
  SELECT p.user_id, p.txn_count, p.total_spent,
         ROUND(p.total_spent / {window_days} * 365, 2) AS clv_annual,
         COALESCE(e.sessions, 0) AS sessions,
         COALESCE(e.total_duration, 0) AS total_duration,
         COALESCE(e.converted, 0) AS converted
  FROM purchases p LEFT JOIN engagement e ON p.user_id = e.user_id)
SELECT *, NTILE(4) OVER (ORDER BY clv_annual) AS clv_quartile FROM base
""")
clv.createOrReplaceTempView("clv")

print("\n=== Top 10 customers by estimated annual CLV ===")
clv.orderBy(clv.clv_annual.desc()).select("user_id", "txn_count", "clv_annual", "sessions").show(10, truncate=False)

print("\n=== CLV quartiles (1 = lowest value, 4 = highest) vs engagement ===")
spark.sql("""
SELECT clv_quartile, COUNT(*) AS users,
       ROUND(AVG(clv_annual), 0) AS avg_clv,
       ROUND(AVG(sessions), 1) AS avg_sessions,
       ROUND(SUM(total_duration) / NULLIF(SUM(sessions), 0), 0) AS avg_session_sec,
       ROUND(100 * SUM(converted) / NULLIF(SUM(sessions), 0), 1) AS session_conv_pct
FROM clv GROUP BY clv_quartile ORDER BY clv_quartile
""").show(truncate=False)

clv.toPandas().to_csv("integrated_clv.csv", index=False)
print("Saved integrated_clv.csv")
spark.stop()
