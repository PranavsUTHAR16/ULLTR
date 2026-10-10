# AGENT STATE: FORWARD TEST ENGINE REPAIR

## 1. CURRENT STAGE & LIFECYCLE
- **Active Lifecycle Stage**: Phase 7 (Deploy to AWS VM) **COMPLETE**.
- **Active Models**:
  - Model 06 (Strategy 6: Volatility-Adaptive Regime Engine)
  - Model 07 (Strategy 7: Pure Nifty OTM VRP Harvesting)
- **Retired Models**: Model 0216, Dynamic DTE, Ultra-TSMOM, CAS Arbitrage (Deleted per DEC-006, verified clean on VM).

---

## 2. VERIFIED REPLAY PARITY RESULTS

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

## 3. REGRESSION TEST STATUS
- **Automated Test Suite**: `pytest -v tests/test_parity_lock.py`
- **Local Result**: **11 PASSED / 0 FAILED** (in 2.73s)
- **AWS VM Result**: **11 PASSED / 0 FAILED** (in 2.63s on `ULLTR-System-VM`)

---

## 4. REPOSITORY COMMITS
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
  - **Git Tag**: `backup-pre-model-cleanup`
- **Shadow Repo** (`/Users/prana/Desktop/black_box/shadow` on `main`):
  - `1a3890f`: fix(def-003): remove double-counted rollover friction in Model 07 backtest
  - `94e5e08`: fix(def-012): fix typo in opposite CE leg harvest on PE breach (p_pe_0 -> p_ce_0)
  - `36ca42d`: fix(vix): support both timestamp/close and date/vix_close schemas in ClickHouse

---

## 5. INFRASTRUCTURE & VM STATE
- **AWS Host**: `ULLTR-System-VM` (`i-0c3e67b057e7b3e00`, Elastic IP `65.1.219.83`).
- **VM State**: `stopped` (zero idle AWS compute billing).
- **Deployment Status**: Verified and fully synchronized. Systemd timers enabled. Ready for scheduled auto-boot Monday morning 08:30 IST.
