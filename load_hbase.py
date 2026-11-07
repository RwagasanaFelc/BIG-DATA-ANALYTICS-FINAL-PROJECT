import json
from datetime import datetime, timezone
from collections import defaultdict
import happybase

DATA_FILE = r"C:\Users\USER\AUCA_BigData_Project\sessions_0.json"
LIMIT = 50000          # number of sessions to load (subset); raise it later if you want more
BATCH = 1000
MAX_TS = 9999999999    # for reverse timestamp: newest session sorts first

def b(x):
    return str(x if x is not None else "").encode("utf-8")

def epoch(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())

conn = happybase.Connection("127.0.0.1", 9090, timeout=60000)
sess_tbl = conn.table("user_sessions")
prod_tbl = conn.table("product_metrics")

print("Reading sessions_0.json ...")
with open(DATA_FILE, encoding="utf-8") as f:
    sessions = json.load(f)[:LIMIT]
print(f"  {len(sessions)} sessions read")

metrics = defaultdict(lambda: {"views": 0, "cart_adds": 0, "purchases": 0})

with sess_tbl.batch(batch_size=BATCH) as bt:
    for i, s in enumerate(sessions, 1):
        ts = epoch(s["start_time"])
        rowkey = f"{s['user_id']}#{MAX_TS - ts:010d}#{s['session_id']}".encode()
        geo = s.get("geo_data") or {}
        dev = s.get("device_profile") or {}
        data = {
            b"meta:session_id": b(s.get("session_id")),
            b"meta:start_time": b(s.get("start_time")),
            b"meta:end_time": b(s.get("end_time")),
            b"meta:duration_seconds": b(s.get("duration_seconds")),
            b"meta:conversion_status": b(s.get("conversion_status")),
            b"meta:referrer": b(s.get("referrer")),
            b"meta:city": b(geo.get("city")),
            b"meta:state": b(geo.get("state")),
            b"meta:country": b(geo.get("country")),
            b"device:type": b(dev.get("type")),
            b"device:os": b(dev.get("os")),
            b"device:browser": b(dev.get("browser")),
            b"views:viewed_products": b(",".join(s.get("viewed_products") or [])),
            b"views:cart_contents": b(json.dumps(s.get("cart_contents") or {})),
        }
        # one sparse column per page view
        for j, pv in enumerate(s.get("page_views") or []):
            data[f"views:pv_{j:03d}".encode()] = b(json.dumps(
                [pv.get("timestamp"), pv.get("page_type"), pv.get("product_id"),
                 pv.get("category_id"), pv.get("view_duration")]))
            if pv.get("page_type") == "product_detail" and pv.get("product_id"):
                day = s["start_time"][:10].replace("-", "")
                metrics[(pv["product_id"], day)]["views"] += 1
        # cart adds and purchases
        day = s["start_time"][:10].replace("-", "")
        for pid in (s.get("cart_contents") or {}):
            metrics[(pid, day)]["cart_adds"] += 1
            if s.get("conversion_status") == "converted":
                metrics[(pid, day)]["purchases"] += 1
        bt.put(rowkey, data)
        if i % 10000 == 0:
            print(f"  user_sessions: {i} rows written")

print("Writing product_metrics ...")
with prod_tbl.batch(batch_size=BATCH) as bt:
    for (pid, day), m in metrics.items():
        bt.put(f"{pid}#{day}".encode(), {
            b"m:views": b(m["views"]),
            b"m:cart_adds": b(m["cart_adds"]),
            b"m:purchases": b(m["purchases"]),
        })

n1 = sum(1 for _ in sess_tbl.scan(filter=b"FirstKeyOnlyFilter()"))
n2 = sum(1 for _ in prod_tbl.scan(filter=b"FirstKeyOnlyFilter()"))
print(f"Done. user_sessions rows: {n1}, product_metrics rows: {n2}")
conn.close()
