import happybase

def show(d):
    return ", ".join("%s=%s" % (k.decode(), v.decode()) for k, v in d.items())

conn = happybase.Connection("127.0.0.1", 9090, timeout=60000)
sess = conn.table("user_sessions")
prod = conn.table("product_metrics")

first_key, _ = next(sess.scan(limit=1, filter=b"FirstKeyOnlyFilter()"))
user_id = first_key.decode().split("#")[0]
prefix = (user_id + "#").encode()

print("\n--- Query 1: all sessions for " + user_id + " (prefix scan, newest first) ---")
rows = list(sess.scan(row_prefix=prefix))
print(len(rows), "sessions found")
for key, d in rows[:5]:
    n_views = sum(1 for c in d if c.startswith(b"views:pv_"))
    print(key.decode(), "|", d[b"meta:start_time"].decode(), "|",
          d[b"meta:duration_seconds"].decode() + "s", "|",
          d[b"meta:conversion_status"].decode(), "|",
          d[b"device:type"].decode(), "|", n_views, "page views")

print("\n--- Query 2: 3 most recent sessions for " + user_id + " ---")
cols = [b"meta:start_time", b"meta:referrer", b"device:type"]
for key, d in sess.scan(row_prefix=prefix, limit=3, columns=cols):
    print(key.decode(), show(d))

print("\n--- Query 3: daily metrics for one product ---")
pk, _ = next(prod.scan(limit=1, filter=b"FirstKeyOnlyFilter()"))
pid = pk.decode().split("#")[0]
for key, d in prod.scan(row_prefix=(pid + "#").encode(), limit=7):
    print(key.decode(), show(d))
conn.close()
