import clickhouse_connect

client = clickhouse_connect.get_client(
    host="ra5fptcofl.ap-south-1.aws.clickhouse.cloud",
    port=8443,
    username="default",
    password="BhhYrZvtF3lA~",
    secure=True
)

query = """
SELECT
    multiIf(
        delta <= 100, '<= 100ms',
        delta <= 250, '101 - 250ms',
        delta <= 500, '251 - 500ms',
        delta <= 1000, '501 - 1000ms',
        delta <= 2000, '1001 - 2000ms',
        '> 2000ms'
    ) as latency_bucket,
    count() as tick_count,
    round(count() * 100.0 / sum(count()) OVER(), 2) as pct
FROM (
    SELECT (ts_recv - ts_exchange) as delta
    FROM market_ticks
    WHERE toDate(timestamp) = '2026-09-07'
      AND formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00'
      AND ts_recv > 0 AND ts_exchange > 0
      AND ts_recv >= ts_exchange
      AND delta <= 5000
)
GROUP BY latency_bucket
ORDER BY min(delta)
"""

res = client.query(query)
print("=== Latency Distribution Buckets Today (2026-09-07) ===")
for r in res.result_rows:
    print(f"{r[0]:<15} | {r[1]:>10,} ticks | {r[2]:>6.2f}%")

# Same for Index / NIFTY Fut / ATM
query2 = """
SELECT
    multiIf(
        delta <= 100, '<= 100ms',
        delta <= 250, '101 - 250ms',
        delta <= 500, '251 - 500ms',
        delta <= 1000, '501 - 1000ms',
        delta <= 2000, '1001 - 2000ms',
        '> 2000ms'
    ) as latency_bucket,
    count() as tick_count,
    round(count() * 100.0 / sum(count()) OVER(), 2) as pct
FROM (
    SELECT (ts_recv - ts_exchange) as delta
    FROM market_ticks
    WHERE toDate(timestamp) = '2026-09-07'
      AND formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00'
      AND underlying = 'NIFTY'
      AND strike BETWEEN 23800 AND 24200
      AND ts_recv > 0 AND ts_exchange > 0
      AND ts_recv >= ts_exchange
      AND delta <= 5000
)
GROUP BY latency_bucket
ORDER BY min(delta)
"""
res2 = client.query(query2)
print("\n=== NIFTY ATM Strikes Latency Buckets Today ===")
for r in res2.result_rows:
    print(f"{r[0]:<15} | {r[1]:>10,} ticks | {r[2]:>6.2f}%")
