import getpass
from urllib.parse import quote_plus
from pymongo import MongoClient

pwd = getpass.getpass("MongoDB admin password: ")
client = MongoClient("mongodb://admin:%s@localhost:27017/?authSource=admin" % quote_plus(pwd))
db = client["ecommerce"]

print("\n--- Transaction status counts (sanity check) ---")
for r in db.transactions.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]):
    print(r)

print("\n--- Pipeline 1: Top 10 categories by revenue ---")
p1 = [
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id",
                 "foreignField": "product_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": "$p.category.name",
                "revenue": {"$sum": "$items.subtotal"},
                "units_sold": {"$sum": "$items.quantity"}}},
    {"$project": {"revenue": {"$round": ["$revenue", 2]}, "units_sold": 1}},
    {"$sort": {"revenue": -1}},
    {"$limit": 10},
]
for r in db.transactions.aggregate(p1, allowDiskUse=True):
    print(r)

print("\n--- Pipeline 2: User segmentation by purchase frequency (4 equal-size segments) ---")
p2 = [
    {"$group": {"_id": "$user_id", "txn_count": {"$sum": 1}, "total_spent": {"$sum": "$total"}}},
    {"$bucketAuto": {"groupBy": "$txn_count", "buckets": 4,
                     "output": {"users": {"$sum": 1},
                                "avg_spent": {"$avg": "$total_spent"}}}},
]
for r in db.transactions.aggregate(p2, allowDiskUse=True):
    r["avg_spent"] = round(r["avg_spent"], 2)
    print(r)
