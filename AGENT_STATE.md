# AGENT STATE: FORWARD TEST ENGINE REPAIR

## 1. CURRENT STAGE & LIFECYCLE
- **Active Lifecycle Stage**: Phase 7 (Deploy to AWS VM & ClickHouse Migration) **COMPLETE**.
- **Active Models**:
  - Model 06 (Strategy 6: Volatility-Adaptive Regime Engine)
  - Model 07 (Strategy 7: Pure Nifty OTM VRP Harvesting)
- **Retired Models**: Model 0216, Dynamic DTE, Ultra-TSMOM, CAS Arbitrage (Deleted per DEC-006, verified clean on VM).

---

## 2. CLICKHOUSE CLOUD MIGRATION (COMPLETE)
- **Source Cluster**: `ra5fptcofl.ap-south-1.aws.clickhouse.cloud:8443` (Database: `default`)
- **Target Cluster**: `uwmxpy364g.ap-south-1.aws.clickhouse.cloud:8443` (Database: `default`, User: `default`)
- **Data Transfer Protocol**: High-speed server-to-server native streaming via `remoteSecure()` over AWS ap-south-1 backbone.
- **Verification Table Scorecard**:
  1. `index_weights`: 80 / 80 rows -> **MATCH (100.0%)**
  2. `cas_index_estimates`: 314,710 / 314,710 rows -> **MATCH (100.0%)**
  3. `market_ticks`: 247,063,826 / 247,063,826 rows -> **MATCH (100.0%)**
  4. `stock_orderbook_depth`: 118,076,626 / 118,076,626 rows -> **MATCH (100.0%)**
  - **Total Rows Migrated**: **365,455,242 rows** with zero loss.
- **Old Server Decommissioning**:
  - `TRUNCATE TABLE default.market_ticks`: Executed (0 rows remaining).
  - `TRUNCATE TABLE default.stock_orderbook_depth`: Executed (0 rows remaining).
  - `TRUNCATE TABLE default.cas_index_estimates`: Executed (0 rows remaining).
  - Old server storage fully reclaimed.
- **Collector Services Updated**:
  - `clickhouse_streamer.py`: Updated default `CH_HOST` and `CH_PASSWORD`.
  - `stock_depth_collector.py`: Updated `CH_HOST` and `CH_PASS`.
  - `market_replay_engine.py` & `scripts/market_replay.py`: Updated credentials.
  - VM services (`ulltr-clickhouse-streamer`, `ulltr-depth-collector`) synchronized and verified on VM.

---

## 3. VERIFIED REPLAY PARITY RESULTS

### A. Model 07 Multi-Session Parity (10 Historical Trading Days)
Verified between backtest (`shadow/main07.py`) and forward test replay (`web/forward_tester/run_model_07.py`):
1. **2026-09-28 (Monday, DTE=1)**: Gross +10.90 pts, Friction -2.50 pts, Net +8.40 pts -> **+₹546.00** (100.000% EXACT MATCH)
2. **2026-09-25 (Friday, DTE=4)**: Gross +20.89 pts, Friction -2.50 pts, Net +18.39 pts -> **+₹1,195.35** (100.000% EXACT MATCH)
3. **2026-09-24 (Thursday, DTE=5)**: Gross +8.09 pts, Friction -3.75 pts, Net +4.34 pts -> **+₹282.10** (100.000% EXACT MATCH)
4. **2026-09-23 (Wednesday, DTE=6)**: Gross +4.60 pts, Friction -3.75 pts, Net +0.85 pts -> **+₹55.25** (100.000% EXACT MATCH)
5. **2026-09-22 (Tuesday, DTE=0)**: Gross -11.00 pts, Friction -3.75 pts, Net -14.75 pts -> **-₹958.75** (100.000% EXACT MATCH)
6. **2026-09-21 (Monday, DTE=1)**: Gross +10.20 pts, Friction -5.00 pts, Net +5.20 pts -> **+₹338.00** (100.000% EXACT MATCH)
7. **2026-09-18 (Friday, DTE=4)**: Gross +30.00 pts, Friction -2.50 pts, Net +27.50 pts -> **+₹1,787.50** (100.000% EXACT MATCH)
8. **2026-09-17 (Thursday, DTE=5)**: Gross +28.83 pts, Friction -6.25 pts, Net +22.58 pts -> **+₹1,467.38** (100.000% EXACT MATCH)
9. **2026-09-16 (Wednesday, DTE=6)**: Gross +12.45 pts, Friction -2.50 pts, Net +9.95 pts -> **+₹646.75** (100.000% EXACT MATCH)
10. **2026-09-15 (Tuesday, DTE=0)**: Gross -16.82 pts, Friction -6.25 pts, Net -23.07 pts -> **-₹1,499.23** (100.000% EXACT MATCH)

### B. Strategy 6 Tick Replay (ClickHouse Cloud 154M+ `market_ticks` to Redis)
1. **2026-10-08 (Thursday)**: Realized PnL: **-₹25,151.75** (PE stop loss hit, CE EOD exit)
2. **2026-10-07 (Wednesday)**: Front Expiry: `2026-10-13` (DTE=6). Realized PnL: **+₹36,169.25** (4 Wins / 0 Losses)
3. **2026-10-06 (Tuesday)**: Front Expiry: `2026-10-06` (DTE=0). Realized PnL: **-₹6,919.25** (CE stop loss hit, PE EOD exit)

---

## 4. REGRESSION TEST STATUS
- **Automated Test Suite**: `pytest -v tests/test_parity_lock.py tests/test_tick_pipeline_wal.py`
- **Local Result**: **14 PASSED / 0 FAILED** (in 3.23s)
- **AWS VM Result**: **14 PASSED / 0 FAILED** (in 3.54s on `ULLTR-System-VM`)

---

## 5. TICK DATA COLLECTION PIPELINE OPTIMIZATION (P0-P4 COMPLETE)
- **P0 (Option B) & P1 (Local WAL)**:
  - C++ Ingestor writes ticks directly to local append-only WAL files in `data/tick_wal/` (`wal_ticks_<YYYYMMDD>_<HH>.jsonl`).
  - `clickhouse_streamer.py` tails the WAL files using a persistent `.watermark.json` offset.
  - Redis polling eliminated for database persistence: **Zero tick loss on high-frequency market spikes**.
  - Local disk buffering protects against network disconnects and ClickHouse Cloud resets.
- **P2 (C++ Hot Path Zero-Allocation & Redis Pipelining)**:
  - Replaced heap `nlohmann::json` parsing with stack-allocated `MarketTickRecord`.
  - Pipelined `redisAppendCommandArgv()` batches all feeds in a WebSocket frame into a single network round-trip.
  - Strategy engine quote latency dropped from ~5-10ms to <0.3ms.
- **P3 (ClickHouse Table TTL & Ingestion Settings)**:
  - Configured 90-day automatic retention TTL on `market_ticks`, `stock_orderbook_depth`, and `cas_index_estimates`.
  - Configured `async_insert: 1, wait_for_async_insert: 0` in ClickHouse connections for sub-millisecond client turnaround.
- **P4 (Nanosecond Timestamping & Sanity Filters)**:
  - High-resolution hardware/kernel receive timestamping (`ts_recv_ns`) captured at frame arrival.
  - Sanity filter rejects corrupted/inverted books (`bid > ask`) and invalid prices.
- **Root Cause & Fix for Morning Setup C++ Compilation Bug**:
  - **Issue**: Remote VM had branched git state on `origin/main` (an old commit from weeks ago) instead of the active development branch, and CMake incremental cache reused old object files.
  - **Fix 1 (Baked Version)**: Baked Git commit hash into C++ binary during build (`-DGIT_COMMIT_HASH="${GIT_COMMIT_HASH}"`). Added `./collector --version` flag.
  - **Fix 2 (Clean Rebuild)**: Updated `setup_aws_vm.sh`, `deploy.sh`, and `recover_services.py` to strictly wipe `build/` (`rm -rf build && mkdir -p build`) before running CMake.
  - **Fix 3 (Branch Alignment)**: Fast-forward merged `feat/strategy-engine-rehydration-and-fixes` into `main` and pushed both to `origin`.
  - **Verification**: Built and verified `./collector --version` on the AWS VM -> outputs exact commit `9cb17ef`. Ran full 14-test suite on VM -> **14 PASSED / 0 FAILED**.
  - **VM State**: `ULLTR-System-VM` (`i-0c3e67b057e7b3e00`) powered down (`stopped`).

---

## 5. REPOSITORY COMMITS
- **Web Repo** (`/Users/prana/Desktop/open_source/web` on `feat/strategy-engine-rehydration-and-fixes`, pushed to `origin`):
  - `83dc82a`: feat(parity): isolate pure quantitative calculation cores and add parity regression lock tests
  - `e15385c`: fix(def-008): remove premature 80% decay TP exit for Strategy 6 to match backtest
  - `bc15b06`: fix(def-001, def-002): enforce zero synthetic fallback and strict expiry resolution in Model 07
  - `49ad8cb`: fix(def-005, def-006, def-007): implement correct rolling RV5 slope, dynamic percentiles, and reject silent fallback in Strategy 6
  - `72dad10`: feat(telemetry): add high-throughput append-only Parquet raw input logger and replayer
  - `1a8b544`: fix(def-004): reject futures order-flow CSVs in load_session_from_csv to preserve spot parity
  - `2ffe94e`: refactor(def-011): remove retired models (Model 0216, Dynamic DTE, Ultra-TSMOM, CAS Arbitrage) and dead code (-2,849 lines)
  - `6e86d8a`: test(def-012): add lock regression test for PE breach wing harvest parity on 2026-09-24
  - `70d5ace`: fix(strategy_6): resolve DEF-013 expiry selection failure on DTE >= 6
  - `ffd3fc5`: test(model_07): add multi-day replay parity lock test covering 5 historical sessions
  - `869e472`: docs(registers): update AGENT_STATE, DECISIONS, and DEFECTS with DEF-013 and multi-day parity logs
  - `832ea5e`: docs(agent_state): mark phase 7 VM deployment complete and instance stopped
  - `48827c3`: feat(clickhouse): migrate cloud database to new cluster uwmxpy364g and update all collectors
  - **Git Tag**: `backup-pre-model-cleanup`
- **Shadow Repo** (`/Users/prana/Desktop/black_box/shadow` on `main`):
  - `1a3890f`: fix(def-003): remove double-counted rollover friction in Model 07 backtest
  - `94e5e08`: fix(def-012): fix typo in opposite CE leg harvest on PE breach (p_pe_0 -> p_ce_0)
  - `36ca42d`: fix(vix): support both timestamp/close and date/vix_close schemas in ClickHouse

---

## 6. INFRASTRUCTURE & VM STATE
- **AWS Host**: `ULLTR-System-VM` (`i-0c3e67b057e7b3e00`, Elastic IP `65.1.219.83`).
- **VM State**: `stopped` (zero idle AWS compute billing).
- **Deployment Status**: Verified and fully synchronized with new ClickHouse cluster. Systemd timers enabled. Ready for scheduled auto-boot Monday morning 08:30 IST.
