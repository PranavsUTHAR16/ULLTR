#!/usr/bin/env python3
"""
ClickHouse Cloud to ClickHouse Cloud Full Migration Script.
============================================================
Migrates all tables, schemas, and data from old ClickHouse Cloud server
(ra5fptcofl.ap-south-1.aws.clickhouse.cloud) to new ClickHouse Cloud server
(uwmxpy364g.ap-south-1.aws.clickhouse.cloud).

Uses high-speed server-to-server native streaming via remoteSecure()
across AWS ap-south-1 backbone.
"""

import sys
import time
import clickhouse_connect

OLD_HOST = "ra5fptcofl.ap-south-1.aws.clickhouse.cloud"
OLD_PORT = 8443
OLD_NATIVE_PORT = 9440
OLD_USER = "default"
OLD_PASS = "BhhYrZvtF3lA~"

NEW_HOST = "uwmxpy364g.ap-south-1.aws.clickhouse.cloud"
NEW_PORT = 8443
NEW_USER = "default"
NEW_PASS = "WqJbsqU5_Odq4"


def get_clients():
    print(f"Connecting to Old Server ({OLD_HOST})...")
    old_client = clickhouse_connect.get_client(
        host=OLD_HOST,
        port=OLD_PORT,
        user=OLD_USER,
        password=OLD_PASS,
        secure=True,
        connect_timeout=60,
        send_receive_timeout=1800
    )

    print(f"Connecting to New Server ({NEW_HOST})...")
    new_client = clickhouse_connect.get_client(
        host=NEW_HOST,
        port=NEW_PORT,
        user=NEW_USER,
        password=NEW_PASS,
        secure=True,
        connect_timeout=60,
        send_receive_timeout=1800
    )
    return old_client, new_client


def migrate_table(old_client, new_client, table_name: str):
    print(f"\n=======================================================")
    print(f"Starting Migration for: {table_name}")
    print(f"=======================================================")

    # Get old count
    old_cnt = old_client.query(f"SELECT count() FROM default.{table_name}").result_rows[0][0]
    print(f"Old Server Total Rows: {old_cnt:,}")

    # Check if table is partitioned
    part_query = f"""
        SELECT DISTINCT partition 
        FROM system.parts 
        WHERE table = '{table_name}' AND database = 'default' AND active = 1
        ORDER BY partition
    """
    parts = [r[0] for r in old_client.query(part_query).result_rows if r[0] != 'tuple()']

    if parts:
        print(f"Table is partitioned. Partitions to migrate: {parts}")
        for p in parts:
            p_old_cnt = old_client.query(
                f"SELECT count() FROM default.{table_name} WHERE toYYYYMM(timestamp) = {p}"
            ).result_rows[0][0]

            # Check if partition is already on new server
            p_new_cnt = new_client.query(
                f"SELECT count() FROM default.{table_name} WHERE toYYYYMM(timestamp) = {p}"
            ).result_rows[0][0]

            if p_new_cnt == p_old_cnt and p_old_cnt > 0:
                print(f"  • Partition {p}: Already migrated ({p_new_cnt:,} rows). Skipping.")
                continue

            if p_new_cnt > 0 and p_new_cnt != p_old_cnt:
                print(f"  • Partition {p}: Partial rows ({p_new_cnt:,} vs {p_old_cnt:,}). Dropping partition on new...")
                new_client.command(f"ALTER TABLE default.{table_name} DROP PARTITION {p}")

            print(f"  • Partition {p}: Migrating {p_old_cnt:,} rows server-to-server...")
            t0 = time.time()
            sql = f"""
                INSERT INTO default.{table_name}
                SELECT * FROM remoteSecure('{OLD_HOST}:{OLD_NATIVE_PORT}', 'default', '{table_name}', '{OLD_USER}', '{OLD_PASS}')
                WHERE toYYYYMM(timestamp) = {p}
                SETTINGS max_execution_time = 3600
            """
            new_client.command(sql)
            elapsed = time.time() - t0
            p_migrated = new_client.query(
                f"SELECT count() FROM default.{table_name} WHERE toYYYYMM(timestamp) = {p}"
            ).result_rows[0][0]
            print(f"  • Partition {p}: Complete in {elapsed:.1f}s ({p_migrated:,} / {p_old_cnt:,} rows)")
            assert p_migrated == p_old_cnt, f"Partition {p} count mismatch!"
    else:
        # Non-partitioned table
        cur_new_cnt = new_client.query(f"SELECT count() FROM default.{table_name}").result_rows[0][0]
        if cur_new_cnt == old_cnt and old_cnt > 0:
            print(f"  • Table {table_name}: Already fully migrated ({cur_new_cnt:,} rows).")
            return

        print(f"  • Migrating all {old_cnt:,} rows server-to-server...")
        t0 = time.time()
        sql = f"""
            INSERT INTO default.{table_name}
            SELECT * FROM remoteSecure('{OLD_HOST}:{OLD_NATIVE_PORT}', 'default', '{table_name}', '{OLD_USER}', '{OLD_PASS}')
            SETTINGS max_execution_time = 3600
        """
        new_client.command(sql)
        elapsed = time.time() - t0
        print(f"  • Complete in {elapsed:.1f}s")

    # Final verification
    new_final_cnt = new_client.query(f"SELECT count() FROM default.{table_name}").result_rows[0][0]
    print(f"Verification: Old={old_cnt:,} | New={new_final_cnt:,} -> {'MATCH' if old_cnt == new_final_cnt else 'MISMATCH'}")
    assert old_cnt == new_final_cnt, f"Total count mismatch for {table_name}!"


def main():
    old_client, new_client = get_clients()

    tables = ["index_weights", "cas_index_estimates", "market_ticks", "stock_orderbook_depth"]
    for t in tables:
        migrate_table(old_client, new_client, t)

    print("\n🎉 ALL TABLES MIGRATED AND VERIFIED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
