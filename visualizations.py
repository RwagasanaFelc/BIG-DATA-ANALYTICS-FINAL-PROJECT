import os, getpass
from urllib.parse import quote_plus
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

os.makedirs("figures", exist_ok=True)
pwd = getpass.getpass("MongoDB admin password: ")
uri = "mongodb://admin:%s@localhost:27017/?authSource=admin" % quote_plus(pwd)
db = MongoClient(uri)["ecommerce"]
BLUE, GREY = "#2a6f97", "#adb5bd"

# Chart 1: monthly revenue
m = list(db.transactions.aggregate([
    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "revenue": {"$sum": "$total"}}},
    {"$sort": {"_id": 1}}]))
months = [r["_id"] for r in m]
rev = [r["revenue"] / 1e6 for r in m]
colors = [BLUE] * len(months)
colors[-1] = GREY
fig, ax = plt.subplots(figsize=(7, 4.5))
bars = ax.bar(months, rev, color=colors)
for b_, v in zip(bars, rev):
    ax.text(b_.get_x() + b_.get_width() / 2, v + 2, "%.0fM" % v, ha="center")
ax.set_title("Monthly revenue (grey = partial month, data to 7 Nov)")
ax.set_ylabel("Revenue (millions)")
fig.tight_layout()
fig.savefig("figures/1_monthly_revenue.png", dpi=150)
print("saved chart 1")

# Chart 2: top 10 categories by revenue
cats = list(db.transactions.aggregate([
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id",
                 "foreignField": "product_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": "$p.category.name", "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 10}], allowDiskUse=True))
names = [c["_id"] for c in cats][::-1]
vals = [c["revenue"] / 1e6 for c in cats][::-1]
fig, ax = plt.subplots(figsize=(8, 5))
ax.barh(names, vals, color=BLUE)
for i, v in enumerate(vals):
    ax.text(v + 0.1, i, "%.1fM" % v, va="center")
ax.set_title("Top 10 categories by revenue")
ax.set_xlabel("Revenue (millions)")
fig.tight_layout()
fig.savefig("figures/2_top_categories.png", dpi=150)
print("saved chart 2")

# Chart 3: CLV quartiles vs engagement
d = pd.read_csv("integrated_clv.csv")
g = d.groupby("clv_quartile").agg(avg_clv=("clv_annual", "mean"),
                                  conv=("converted", "sum"),
                                  sess=("sessions", "sum"))
g["conv_pct"] = 100 * g["conv"] / g["sess"]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 4.5))
a1.bar(g.index.astype(str), g["avg_clv"] / 1000, color=BLUE)
a1.set_title("Average annual CLV")
a1.set_xlabel("CLV quartile (4 = highest value)")
a1.set_ylabel("Thousands")
a2.bar(g.index.astype(str), g["conv_pct"], color=BLUE)
a2.set_title("Session conversion rate (%)")
a2.set_xlabel("CLV quartile (4 = highest value)")
fig.suptitle("Customer value vs engagement (MongoDB + HBase + Spark)")
fig.tight_layout()
fig.savefig("figures/3_clv_quartiles.png", dpi=150)
print("saved chart 3")

# Chart 4: conversion funnel (sessions collection)
S = db.sessions
total = S.count_documents({})
viewed = S.count_documents({"page_views.page_type": "product_detail"})
cart_expr = {"$gt": [{"$size": {"$objectToArray": {"$ifNull": ["$cart_contents", {}]}}}, 0]}
carted = S.count_documents({"$expr": cart_expr})
bought = S.count_documents({"conversion_status": "converted"})
labels = ["All sessions", "Viewed a product", "Added to cart", "Converted"]
counts = [total, viewed, carted, bought]
print("funnel:", counts)
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh(labels[::-1], counts[::-1], color=BLUE)
for i, v in enumerate(counts[::-1]):
    ax.text(v + total * 0.01, i, "%d (%.1f%%)" % (v, 100 * v / total), va="center")
ax.set_xlim(0, total * 1.25)
ax.set_title("Conversion funnel (200,000 sessions loaded in MongoDB)")
fig.tight_layout()
fig.savefig("figures/4_funnel.png", dpi=150)
print("saved chart 4")
