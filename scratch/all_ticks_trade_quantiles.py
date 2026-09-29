import clickhouse_connect

client = clickhouse_connect.get_client(
    host="ra5fptcofl.ap-south-1.aws.clickhouse.cloud",
    port=8443,
    username="default",
    password="BhhYrZvtF3lA~",
    secure=True
)

# Query for executed trades / active trades where delta is realistic (sub-10s) across ALL 53.8M ticks
query = """
SELECT
    count() as n,
    round(quantile(0.50)(ts_recv - ts_exchange), 2) as p50,
    round(quantile(0.90)(ts_recv - ts_exchange), 2) as p90,
    round(quantile(0.95)(ts_recv - ts_exchange), 2) as p95,
    round(quantile(0.99)(ts_recv - ts_exchange), 2) as p99,
    round(quantile(0.999)(ts_recv - ts_exchange), 2) as p99_9,
    round(avg(ts_recv - ts_exchange), 2) as mean
FROM market_ticks
WHERE ts_recv > 0 AND ts_exchange > 0
  AND ts_recv >= ts_exchange
  AND formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00'
  AND (ts_recv - ts_exchange) <= 10000 -- Real-time trade stream (within 10s of trade)
"""
res = client.query(query)
r = res.result_rows[0]
print("=========================================================================")
print("  ALL CONCURRENT TRADE TICKS (All 53.8M ticks, delta <= 10s)             ")
print("=========================================================================")
print(f"Active Ticks Count   : {r[0]:,}")
print(f"p50 (Median)         : {r[1]:,} ms")
print(f"p90                  : {r[2]:,} ms")
print(f"p95                  : {r[3]:,} ms")
print(f"p99                  : {r[4]:,} ms")
print(f"p99.9                : {r[5]:,} ms")
print(f"Mean                 : {r[6]:,} ms")
print("=========================================================================")
