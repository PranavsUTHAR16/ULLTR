#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cassert>

#include "../src/strategy_types.hpp"
#include "../src/fifo_pool.hpp"
#include "../src/strategy_engine.hpp"

extern "C" {
    void freeReplyObject(void* reply) {
        if (reply) free(reply);
    }
    void* redisCommand(redisContext* c, const char* format, ...) {
        return nullptr;
    }
}

void run_latency_benchmark() {
    std::cout << "================================================================" << std::endl;
    std::cout << "  ULLTR C++ StrategyEngine & FIFO Margin Pool Latency Benchmark " << std::endl;
    std::cout << "================================================================" << std::endl;

    FIFOPool pool(20000.0);
    StrategyEngine engine(pool);

    // 1. Benchmark evaluate_and_allocate
    const int N = 100000;
    std::vector<double> alloc_latencies;
    alloc_latencies.reserve(N);

    TradeSignal test_sig;
    test_sig.model_name = "ModelDualVWAP";
    test_sig.option_type = OptionType::PE;
    test_sig.strike = 23950;
    test_sig.symbol = "NIFTY_23950_PE";
    test_sig.option_ask = 138.20;
    test_sig.fut_price = 24000.0;
    test_sig.sl_fut = 24025.0;
    test_sig.tp_fut = 23950.0;
    test_sig.timestamp_str = "11:04";

    for (int i = 0; i < N; ++i) {
        pool.reset_day(20000.0);
        UnifiedPosition pos;
        std::string reason;

        auto t0 = std::chrono::high_resolution_clock::now();
        bool ok = pool.evaluate_and_allocate(test_sig, pos, reason);
        auto t1 = std::chrono::high_resolution_clock::now();

        double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
        alloc_latencies.push_back(ns);
        (void)ok;
    }

    std::sort(alloc_latencies.begin(), alloc_latencies.end());
    double alloc_mean = std::accumulate(alloc_latencies.begin(), alloc_latencies.end(), 0.0) / N;
    double alloc_p50 = alloc_latencies[N * 0.50];
    double alloc_p95 = alloc_latencies[N * 0.95];
    double alloc_p99 = alloc_latencies[N * 0.99];

    std::cout << "\n[1] FIFOPool::evaluate_and_allocate Latency (" << N << " runs):" << std::endl;
    std::cout << "    Mean Latency : " << std::fixed << std::setprecision(2) << alloc_mean << " ns (" << alloc_mean / 1000.0 << " µs)" << std::endl;
    std::cout << "    Median (p50) : " << alloc_p50 << " ns" << std::endl;
    std::cout << "    95th % (p95) : " << alloc_p95 << " ns" << std::endl;
    std::cout << "    99th % (p99) : " << alloc_p99 << " ns" << std::endl;

    // 2. Benchmark on_tick Hot-Path Exit Evaluator
    std::vector<double> tick_latencies;
    tick_latencies.reserve(N);

    // Setup active position in pool
    pool.reset_day(20000.0);
    UnifiedPosition active_pos;
    std::string reason;
    pool.evaluate_and_allocate(test_sig, active_pos, reason);

    MicrostructureMetrics metrics;
    metrics.vwap_buy = 23980.0;
    metrics.vwap_sell = 24010.0;
    metrics.roll_buy_vol = 500000.0;
    metrics.roll_sell_vol = 550000.0;
    metrics.cvd_15m = -20000.0;
    metrics.delta_oi_15m = 1200.0;

    int64_t tick_ts_ms = 1725697440000; // ~11:14 IST

    for (int i = 0; i < N; ++i) {
        auto t0 = std::chrono::high_resolution_clock::now();
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 24002.0, 24001.0, 24003.0, tick_ts_ms, metrics);
        auto t1 = std::chrono::high_resolution_clock::now();

        double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
        tick_latencies.push_back(ns);
    }

    std::sort(tick_latencies.begin(), tick_latencies.end());
    double tick_mean = std::accumulate(tick_latencies.begin(), tick_latencies.end(), 0.0) / N;
    double tick_p50 = tick_latencies[N * 0.50];
    double tick_p95 = tick_latencies[N * 0.95];
    double tick_p99 = tick_latencies[N * 0.99];

    std::cout << "\n[2] StrategyEngine::on_tick Hot-Path Exit Latency (" << N << " ticks with active position):" << std::endl;
    std::cout << "    Mean Latency : " << std::fixed << std::setprecision(2) << tick_mean << " ns (" << tick_mean / 1000.0 << " µs)" << std::endl;
    std::cout << "    Median (p50) : " << tick_p50 << " ns" << std::endl;
    std::cout << "    95th % (p95) : " << tick_p95 << " ns" << std::endl;
    std::cout << "    99th % (p99) : " << tick_p99 << " ns" << std::endl;
}

void verify_logic_and_parity() {
    std::cout << "\n================================================================" << std::endl;
    std::cout << "  Verifying Mathematical Logic, Rules & Parity                  " << std::endl;
    std::cout << "================================================================" << std::endl;

    FIFOPool pool(20000.0);
    StrategyEngine engine(pool);

    // Test 1: Sizing formula: Rs 20,000 capital, 138.20 ask -> 2 lots
    TradeSignal sig1;
    sig1.model_name = "ModelDualVWAP";
    sig1.option_type = OptionType::PE;
    sig1.strike = 23950;
    sig1.symbol = "NIFTY_23950_PE";
    sig1.option_ask = 138.20;
    sig1.fut_price = 24000.0;
    sig1.sl_fut = 24025.0;
    sig1.tp_fut = 23950.0;
    sig1.timestamp_str = "11:04";

    UnifiedPosition pos1;
    std::string reason;
    bool ok1 = pool.evaluate_and_allocate(sig1, pos1, reason);
    assert(ok1);
    assert(pos1.lots == 2);
    assert(pos1.quantity == 130);
    assert(pool.get_locked_margin() == 20000.0); // 2 * max(10000, 138.2*65=8983) = 20000
    std::cout << "✅ Test 1 Passed: Sizing allocated exactly 2 lots (130 qty) and locked Rs 20,000 margin." << std::endl;

    // Test 2: Directional Conflict Filter (Cannot buy CE while holding PE)
    TradeSignal sig2;
    sig2.model_name = "ModelPOC";
    sig2.option_type = OptionType::CE;
    sig2.strike = 24050;
    sig2.symbol = "NIFTY_24050_CE";
    sig2.option_ask = 150.0;
    sig2.fut_price = 24000.0;
    sig2.sl_fut = 23970.0;
    sig2.tp_fut = 24060.0;
    sig2.timestamp_str = "11:05";

    UnifiedPosition pos2;
    bool ok2 = pool.evaluate_and_allocate(sig2, pos2, reason);
    assert(!ok2);
    assert(reason.find("BLOCKED: Directional conflict") != std::string::npos);
    std::cout << "✅ Test 2 Passed: Directional conflict filter blocked CE order while holding PE." << std::endl;

    // Test 2b: Same-Model Reversal Flip (ModelDualVWAP reverses from PE to CE)
    TradeSignal sig_rev;
    sig_rev.model_name = "ModelDualVWAP";
    sig_rev.option_type = OptionType::CE;
    sig_rev.strike = 24050;
    sig_rev.symbol = "NIFTY_24050_CE";
    sig_rev.option_ask = 150.0;
    sig_rev.fut_price = 24000.0;
    sig_rev.sl_fut = 23975.0;
    sig_rev.tp_fut = 24050.0;
    sig_rev.timestamp_str = "11:06";

    UnifiedPosition pos_rev;
    bool ok_rev = pool.evaluate_and_allocate(sig_rev, pos_rev, reason);
    assert(ok_rev);
    assert(pool.active_positions.size() == 1);
    assert(pool.active_positions[0].option_type == OptionType::CE);
    assert(pool.closed_positions.size() == 1);
    assert(pool.closed_positions[0].exit_reason.find("Reversal Flip") != std::string::npos);
    std::cout << "✅ Test 2b Passed: Same-model trend reversal cleanly flipped position from PE to CE." << std::endl;

    // Test 3: Take Profit Execution (+50 pts)
    pool.reset_day(20000.0);
    pool.evaluate_and_allocate(sig1, pos1, reason);
    MicrostructureMetrics m_tp;
    m_tp.vwap_buy = 23980.0;
    m_tp.vwap_sell = 24010.0;
    // Futures hits 23950.0 (TP hit for PE)
    engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23950.0, 23949.0, 23951.0, 1725698000000, m_tp);
    assert(pool.active_positions.empty());
    assert(pool.closed_positions.size() == 1);
    assert(pool.closed_positions[0].exit_reason.find("Take Profit") != std::string::npos);
    std::cout << "✅ Test 3 Passed: Take profit (+50 pts) triggered instantly." << std::endl;

    // Test 4: Dynamic 6-point VWAP Invalidation Defense
    pool.reset_day(20000.0);
    UnifiedPosition pos4;
    pool.evaluate_and_allocate(sig1, pos4, reason);
    assert(pool.active_positions.size() == 1);

    // PE position: institutional defense line is v_sell (24010.0). If futures breaches > 24016.0, exit!
    MicrostructureMetrics m_breach;
    m_breach.vwap_buy = 23980.0;
    m_breach.vwap_sell = 24010.0;
    // Price spikes to 24016.5 (> 24016.0)
    engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 24016.5, 24016.0, 24017.0, 1725698500000, m_breach);
    assert(pool.active_positions.empty());
    assert(pool.closed_positions.size() == 1);
    assert(pool.closed_positions[0].exit_reason.find("VWAP Defense Failed") != std::string::npos);
    std::cout << "✅ Test 4 Passed: Dynamic VWAP breach (> 6 pts) invalidated and closed position instantly." << std::endl;

    // Test 5: EOD 15:20 Square-Off
    pool.reset_day(20000.0);
    UnifiedPosition pos5;
    pool.evaluate_and_allocate(sig1, pos5, reason);
    assert(pool.active_positions.size() == 1);

    // 15:21 IST timestamp: 15*3600 + 21*60 = 55260s IST -> UTC 35460s
    int64_t eod_ts_ms = 35460LL * 1000LL;
    engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 24000.0, 23999.0, 24001.0, eod_ts_ms, m_breach);
    assert(pool.active_positions.empty());
    assert(pool.closed_positions.size() == 1);
    assert(pool.closed_positions[0].exit_reason.find("EOD Square-Off") != std::string::npos);
    std::cout << "✅ Test 5 Passed: Hard EOD 15:20 square-off executed cleanly." << std::endl;

    std::cout << "\n>>> ALL 5 UNIT & INTEGRATION TESTS PASSED WITH 100% PARITY! <<<\n" << std::endl;
}

int main() {
    verify_logic_and_parity();
    run_latency_benchmark();
    return 0;
}
