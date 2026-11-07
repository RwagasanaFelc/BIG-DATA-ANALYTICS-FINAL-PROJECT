import json, getpass
from datetime import datetime
from pymongo import MongoClient, ASCENDING

DATA_DIR = r"C:\Users\USER\AUCA_BigData_Project"
SESSION_FILES = 2      # number of sessions_N.json files to load (subset)
BATCH = 5000

def dt(s):
    return datetime.fromisoformat(s) if s else None

def load(name):
    with open(f"{DATA_DIR}\\{name}", encoding="utf-8") as f:
        return json.load(f)

def insert_batches(col, docs):
    for i in range(0, len(docs), BATCH):
        col.insert_many(docs[i:i + BATCH], ordered=False)
    print(f"  {col.name}: {col.count_documents({})} documents")

pwd = getpass.getpass("MongoDB admin password: ")
client = MongoClient("mongodb://admin:%s@localhost:27017/?authSource=admin" % __import__("urllib.parse").parse.quote_plus(pwd))
db = client["ecommerce"]
for c in ["users", "categories", "products", "transactions", "sessions"]:
    db[c].drop()

# Categories (kept as-is: category with embedded subcategories)
categories = load("categories.json")
insert_batches(db.categories, categories)
sub_lookup = {}
for c in categories:
    for s in c.get("subcategories", []):
        sub_lookup[s["subcategory_id"]] = {"name": s.get("name"), "profit_margin": s.get("profit_margin")}
cat_names = {c["category_id"]: c["name"] for c in categories}

# Users
users = load("users.json")
for u in users:
    u["registration_date"] = dt(u.get("registration_date"))
    u["last_active"] = dt(u.get("last_active"))
insert_batches(db.users, users)

# Products (category and subcategory details embedded to avoid joins)
products = load("products.json")
for p in products:
    p["creation_date"] = dt(p.get("creation_date"))
    for h in p.get("price_history", []):
        h["date"] = dt(h.get("date"))
    p["category"] = {"category_id": p.get("category_id"), "name": cat_names.get(p.get("category_id"))}
    p["subcategory"] = dict(sub_lookup.get(p.get("subcategory_id"), {}), subcategory_id=p.get("subcategory_id"))
insert_batches(db.products, products)

# Transactions (line items embedded)
txns = load("transactions.json")
for t in txns:
    t["timestamp"] = dt(t.get("timestamp"))
insert_batches(db.transactions, txns)
del txns

# Sessions (subset; page_views embedded)
for n in range(SESSION_FILES):
    print(f"Loading sessions_{n}.json ...")
    sess = load(f"sessions_{n}.json")
    for s in sess:
        s["start_time"] = dt(s.get("start_time"))
        s["end_time"] = dt(s.get("end_time"))
        for pv in s.get("page_views", []):
            pv["timestamp"] = dt(pv.get("timestamp"))
    insert_batches(db.sessions, sess)
    del sess

# Indexes for the planned queries
db.users.create_index("user_id", unique=True)
db.products.create_index("product_id", unique=True)
db.products.create_index("category.category_id")
db.transactions.create_index("user_id")
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")
db.sessions.create_index([("user_id", ASCENDING), ("start_time", ASCENDING)])
print("Done.")
