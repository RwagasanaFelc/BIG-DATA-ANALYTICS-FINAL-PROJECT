# AUCA Big Data Analytics Final Project
Distributed multi-model analytics for e-commerce data using MongoDB (document model), HBase (wide-column model) and Apache Spark (distributed processing).

## Architecture
- **MongoDB**: users, categories, products (category and subcategory embedded), transactions (line items embedded), sessions (page views embedded). Used for aggregation analytics.
- **HBase**: `user_sessions` (row key `user_id#reverse_timestamp#session_id`, families `meta`, `device`, `views`) for fast per-user session retrieval, and `product_metrics` (row key `product_id#YYYYMMDD`, family `m`) for daily product counters.
- **Spark (PySpark)**: batch cleaning, co-purchase recommendations, Spark SQL analytics, and the integrated CLV query joining MongoDB and HBase data.

## Requirements
Python 3.12, Java 17, Docker, and: `pip install faker pandas pymongo happybase pyspark matplotlib`

## How to reproduce
1. Generate the data (several GB, not stored in the repo): `python dataset_generator_clean.py`
2. Start MongoDB: `docker run -d --name auca-mongodb -p 127.0.0.1:27017:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=<your_password> mongo:8.0`
3. Start HBase: `docker run -d --name auca-hbase -p 127.0.0.1:9090:9090 -p 127.0.0.1:16010:16010 harisekhon/hbase` (wait about 90 seconds; if the Thrift port hangs, run `docker restart auca-hbase`)
4. Load MongoDB (prompts for the password): `python load_mongo.py`
5. Run the MongoDB aggregations: `python mongo_queries.py`
6. Create the HBase tables (shell): `create 'user_sessions','meta','device','views'` and `create 'product_metrics','m'`, or with happybase as in the report
7. Load HBase: `python load_hbase.py`, then query it: `python hbase_queries.py`
8. Spark batch job: `python spark_batch.py`
9. Spark SQL: `python spark_sql.py` (output saved in `spark_sql_output.txt`)
10. Integrated CLV query (MongoDB + HBase + Spark): `python integrated_clv.py` (writes `integrated_clv.csv`)
11. Charts: `python visualizations.py` (writes PNG files to `figures/`)

## Key results
- 500,000 transactions, 10,000 users, 5,000 products, 25 categories; 200,000 sessions loaded in MongoDB and 50,000 in HBase.
- Revenue grew from about 115M (Aug) to 156M (Sep) and 161M (Oct); November is a partial month (data to 7 Nov).
- Funnel: 98.7% of sessions view a product, 65.0% add to cart, 20.2% convert (the largest drop is from cart to purchase).
- Higher-value customers (CLV quartile 4) convert more per session (about 22.1%) than the lowest quartile (about 18.1%).

## Limitations
The data is synthetic and generated randomly, so category revenue, device conversion and retention are nearly flat. Annual CLV is a simple extrapolation of spend over the observed window, not a predictive model. MongoDB and HBase hold subsets of the sessions.
