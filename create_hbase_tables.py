# HBase schema for the AUCA project (run after HBase is up on port 9090)
#
# Equivalent HBase shell commands:
#   create 'user_sessions', {NAME => 'meta', VERSIONS => 1}, {NAME => 'device', VERSIONS => 1}, {NAME => 'views', VERSIONS => 1}
#   create 'product_metrics', {NAME => 'm', VERSIONS => 1}
#
# Row keys:
#   user_sessions   : user_id#reverse_timestamp#session_id   (newest session first for each user)
#   product_metrics : product_id#YYYYMMDD                    (one row per product per day)
import happybase

conn = happybase.Connection("127.0.0.1", 9090, timeout=30000)
existing = conn.tables()
if b"user_sessions" not in existing:
    conn.create_table("user_sessions", {
        "meta": dict(max_versions=1),
        "device": dict(max_versions=1),
        "views": dict(max_versions=1)})
if b"product_metrics" not in existing:
    conn.create_table("product_metrics", {"m": dict(max_versions=1)})
print(conn.tables())
conn.close()
