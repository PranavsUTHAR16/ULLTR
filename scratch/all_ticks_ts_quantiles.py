import clickhouse_connect

client = clickhouse_connect.get_client(
    host="ra5fptcofl.ap-south-1.aws.clickhouse.cloud",
    port=8443,
    username="default",
    password="BhhYrZvtF3lA~",
    secure=True
)

# 1. Overview of entire dataset: date range & total ticks
overview_query = """
SELECT
    min(toDate(timestamp)) as min_date,
    max(toDate(timestamp)) as max_date,
    count() as total_ticks,
    countIf(ts_recv > 0 AND ts_exchange > 0) as valid_ts_ticks,
    countIf(formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00') as market_hours_ticks
FROM market_ticks
"""
res_overview = client.query(overview_query)
min_d, max_d, total_cnt, valid_cnt, mkt_cnt = res_overview.result_rows[0]
print(f"Dataset Range      : {min_d} to {max_d}")
print(f"Total Ticks Stored : {total_cnt:,}")
print(f"Valid Timestamp Ticks : {valid_cnt:,}")
print(f"Market Hours Ticks    : {mkt_cnt:,}\n")

# 2. ALL trading tick data (Market Hours: 09:15 - 15:30 across ALL dates)
all_data_query = """
SELECT
    count() as n,
    round(quantile(0.50)(ts_recv - ts_exchange), 2) as p50,
    round(quantile(0.90)(ts_recv - ts_exchange), 2) as p90,
    round(quantile(0.95)(ts_recv - ts_exchange), 2) as p95,
    round(quantile(0.99)(ts_recv - ts_exchange), 2) as p99,
    round(quantile(0.999)(ts_recv - ts_exchange), 2) as p99_9,
    round(avg(ts_recv - ts_exchange), 2) as mean,
    min(ts_recv - ts_exchange) as min_val,
    max(ts_recv - ts_exchange) as max_val
FROM market_ticks
WHERE ts_recv > 0 AND ts_exchange > 0
  AND ts_recv >= ts_exchange
  AND formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00'
"""
res_all = client.query(all_data_query)
r = res_all.result_rows[0]
print("=========================================================================")
print("  ALL TRADING TICK DATA (Market Hours 09:15 - 15:30, Uncapped All Dates) ")
print("=========================================================================")
print(f"Total Ticks Analyzed : {r[0]:,}")
print(f"p50 (Median)         : {r[1]:,} ms ({r[1]/1000.0:.3f} s)")
print(f"p90                  : {r[2]:,} ms ({r[2]/1000.0:.3f} s)")
print(f"p95                  : {r[3]:,} ms ({r[3]/1000.0:.3f} s)")
print(f"p99                  : {r[4]:,} ms ({r[4]/1000.0:.3f} s)")
print(f"p99.9                : {r[5]:,} ms ({r[5]/1000.0:.3f} s)")
print(f"Mean                 : {r[6]:,} ms ({r[6]/1000.0:.3f} s)")
print(f"Min                  : {r[7]:,} ms")
print(f"Max                  : {r[8]:,} ms ({r[8]/1000.0:.2f} s)")
print("=========================================================================\n")

# 3. Completely Unrestricted (24-Hour, All dates, Zero filters except valid timestamps)
completely_raw_query = """
SELECT
    count() as n,
    round(quantile(0.50)(ts_recv - ts_exchange), 2) as p50,
    round(quantile(0.90)(ts_recv - ts_exchange), 2) as p90,
    round(quantile(0.95)(ts_recv - ts_exchange), 2) as p95,
    round(quantile(0.99)(ts_recv - ts_exchange), 2) as p99,
    round(quantile(0.999)(ts_recv - ts_exchange), 2) as p99_9,
    round(avg(ts_recv - ts_exchange), 2) as mean,
    min(ts_recv - ts_exchange) as min_val,
    max(ts_recv - ts_exchange) as max_val
FROM market_ticks
WHERE ts_recv > 0 AND ts_exchange > 0
  AND ts_recv >= ts_exchange
"""
res_raw = client.query(completely_raw_query)
r2 = res_raw.result_rows[0]
print("=========================================================================")
print("  COMPLETELY UNFILTERED (Every Single Tick in DB, 24 Hours, All Dates)   ")
print("=========================================================================")
print(f"Total Ticks Analyzed : {r2[0]:,}")
print(f"p50 (Median)         : {r2[1]:,} ms ({r2[1]/1000.0:.3f} s)")
print(f"p90                  : {r2[2]:,} ms ({r2[2]/1000.0:.3f} s)")
print(f"p95                  : {r2[3]:,} ms ({r2[3]/1000.0:.3f} s)")
print(f"p99                  : {r2[4]:,} ms ({r2[4]/1000.0:.3f} s)")
print(f"p99.9                : {r2[5]:,} ms ({r2[5]/1000.0:.3f} s)")
print(f"Mean                 : {r2[6]:,} ms ({r2[6]/1000.0:.3f} s)")
print(f"Min                  : {r2[7]:,} ms")
print(f"Max                  : {r2[8]:,} ms ({r2[8]/1000.0:.2f} s)")
print("=========================================================================\n")

# 4. Day-by-Day Breakdown for recent trading days
day_by_day_query = """
SELECT
    toDate(timestamp) as trade_date,
    count() as n,
    round(quantile(0.50)(ts_recv - ts_exchange), 2) as p50,
    round(quantile(0.90)(ts_recv - ts_exchange), 2) as p90,
    round(quantile(0.99)(ts_recv - ts_exchange), 2) as p99,
    round(quantile(0.999)(ts_recv - ts_exchange), 2) as p99_9,
    round(avg(ts_recv - ts_exchange), 2) as mean
FROM market_ticks
WHERE ts_recv > 0 AND ts_exchange > 0
  AND ts_recv >= ts_exchange
  AND formatDateTime(timestamp, '%H:%M:%S') BETWEEN '09:15:00' AND '15:30:00'
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT 10
"""
res_days = client.query(day_by_day_query)
print("=== Day-by-Day Breakdown (Market Hours 09:15 - 15:30) ===")
print(f"{'Trade Date':<12} | {'Ticks Count':<12} | {'p50 (ms)':<10} | {'p90 (ms)':<10} | {'p99 (ms)':<12} | {'p99.9 (ms)':<12} | {'Mean (ms)':<10}")
print("-" * 88)
for row in res_days.result_rows:
    print(f"{str(row[0]):<12} | {row[1]:>12,} | {row[2]:>10.2f} | {row[3]:>10.2f} | {row[4]:>12.2f} | {row[5]:>12.2f} | {row[6]:>10.2f}")
