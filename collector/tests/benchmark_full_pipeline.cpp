#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <cstring>

#include "MarketDataFeedV3.pb.h"
#include "../src/strategy_types.hpp"
#include "../src/fifo_pool.hpp"
#include "../src/strategy_engine.hpp"
#include "../src/microstructure_engine.hpp"

namespace upstox = com::upstox::marketdatafeederv3udapi::rpc::proto;

struct LatencyStats {
    double min_ns = 0.0;
    double mean_ns = 0.0;
    double p50_ns = 0.0;
    double p90_ns = 0.0;
    double p99_ns = 0.0;
    double p99_9_ns = 0.0;
    double max_ns = 0.0;
};

static LatencyStats compute_stats(std::vector<double>& latencies) {
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size();
    LatencyStats s;
    s.min_ns = latencies.front();
    s.max_ns = latencies.back();
    s.mean_ns = std::accumulate(latencies.begin(), latencies.end(), 0.0) / n;
    s.p50_ns = latencies[static_cast<size_t>(n * 0.50)];
    s.p90_ns = latencies[static_cast<size_t>(n * 0.90)];
    s.p99_ns = latencies[static_cast<size_t>(n * 0.99)];
    s.p99_9_ns = latencies[static_cast<size_t>(n * 0.999)];
    return s;
}

void print_row(const std::string& name, const LatencyStats& s) {
    std::cout << std::left << std::setw(32) << name
              << " | " << std::right << std::setw(8) << std::fixed << std::setprecision(1) << s.p50_ns << " ns"
              << " | " << std::right << std::setw(8) << s.p90_ns << " ns"
              << " | " << std::right << std::setw(8) << s.p99_ns << " ns"
              << " | " << std::right << std::setw(8) << s.p99_9_ns << " ns"
              << " | " << std::right << std::setw(8) << s.mean_ns << " ns"
              << " | " << std::right << std::setw(8) << (s.mean_ns / 1000.0) << " µs"
              << std::endl;
}

int main() {
    std::cout << "==========================================================================================" << std::endl;
    std::cout << "  ULLTR High-Frequency Pipeline Benchmark: NIC -> Arrival -> Features -> Strategy Decision" << std::endl;
    std::cout << "==========================================================================================" << std::endl;

    // 1. Prepare Upstox binary Protobuf message (MarketFullFeed with Level 2 Quote & CAS fields)
    upstox::FeedResponse response;
    response.set_type(upstox::Type::live_feed);
    response.set_currentts(1725697440000);

    auto& feeds = *response.mutable_feeds();
    upstox::Feed feed;
    feed.set_requestmode(upstox::RequestMode::full_d5);

    auto* full_feed = feed.mutable_fullfeed();
    auto* mff = full_feed->mutable_marketff();
    
    auto* ltpc = mff->mutable_ltpc();
    ltpc->set_ltp(24005.50);
    ltpc->set_ltt(1725697440000);
    ltpc->set_ltq(50);
    ltpc->set_cp(23950.0);

    auto* ml = mff->mutable_marketlevel();
    auto* q = ml->add_bidaskquote();
    q->set_bidp(24005.0);
    q->set_bidq(250);
    q->set_askp(24006.0);
    q->set_askq(300);

    mff->set_vtt(45000000);
    mff->set_oi(1250000.0);
    mff->set_iep(23779.15); // Sept 4, 2026 CAS tag
    mff->set_rp(23729.47);
    mff->set_iiqtotal(-18200);

    feeds["NSE_INDEX|Nifty 50"] = feed;

    std::string serialized_bytes;
    response.SerializeToString(&serialized_bytes);
    const char* wire_buffer = serialized_bytes.data();
    size_t wire_size = serialized_bytes.size();

    std::cout << ">> Protobuf Wire Payload Size: " << wire_size << " bytes per tick\n" << std::endl;

    // 2. Initialize Core Engines
    MicrostructureEngine micro_engine;
    FIFOPool fifo_pool(20000.0);
    StrategyEngine strategy_engine(fifo_pool);

    // Warm up state with an active position to exercise full exit evaluation hot-path
    TradeSignal warm_sig;
    warm_sig.model_name = "ModelDualVWAP";
    warm_sig.option_type = OptionType::PE;
    warm_sig.strike = 24000;
    warm_sig.symbol = "NIFTY_24000_PE";
    warm_sig.option_ask = 142.50;
    warm_sig.fut_price = 24005.50;
    warm_sig.sl_fut = 24030.50;
    warm_sig.tp_fut = 23955.50;
    warm_sig.timestamp_str = "11:04";
    UnifiedPosition warm_pos;
    std::string warm_reason;
    fifo_pool.evaluate_and_allocate(warm_sig, warm_pos, warm_reason);

    const int ITERATIONS = 100000;
    std::vector<double> stage1_nic_to_arrival; // Socket/Kernel buffer read
    std::vector<double> stage2_proto_parse;    // Protobuf decode & field extraction
    std::vector<double> stage3_micro_features; // Microstructure calculation (Lee-Ready, Dual-VWAP, CVD, dPOC)
    std::vector<double> stage4_strat_decision;// Strategy decision (SL, TP, 6-pt breach, margin check)
    std::vector<double> stage_total_e2e;       // Total End-to-End latency

    stage1_nic_to_arrival.reserve(ITERATIONS);
    stage2_proto_parse.reserve(ITERATIONS);
    stage3_micro_features.reserve(ITERATIONS);
    stage4_strat_decision.reserve(ITERATIONS);
    stage_total_e2e.reserve(ITERATIONS);

    // Simulated socket ring buffer
    char sock_buf[4096];

    for (int i = 0; i < ITERATIONS; ++i) {
        // T0: Hardware / NIC arrival timestamp
        auto t0 = std::chrono::high_resolution_clock::now();

        // Stage 1: Kernel to userspace buffer copy (emulating recvmsg/read from socket buffer)
        std::memcpy(sock_buf, wire_buffer, wire_size);
        auto t1 = std::chrono::high_resolution_clock::now();

        // Stage 2: Protobuf binary parsing & extraction
        upstox::FeedResponse msg;
        msg.ParseFromArray(sock_buf, static_cast<int>(wire_size));

        std::string symbol = "NSE_INDEX|Nifty 50";
        double ltp = 24005.50 + (i % 10) * 0.1;
        double bid = ltp - 0.5;
        double ask = ltp + 0.5;
        int64_t vol = 45000000 + i * 50;
        double oi = 1250000.0 + (i % 50) * 10;
        int64_t ts = 1725697440000 + i;

        const auto& it = msg.feeds().find(symbol);
        if (it != msg.feeds().end() && it->second.has_fullfeed()) {
            const auto& m = it->second.fullfeed().marketff();
            if (m.has_ltpc()) ltp = m.ltpc().ltp();
            if (m.has_marketlevel() && m.marketlevel().bidaskquote_size() > 0) {
                bid = m.marketlevel().bidaskquote(0).bidp();
                ask = m.marketlevel().bidaskquote(0).askp();
            }
        }
        auto t2 = std::chrono::high_resolution_clock::now();

        // Stage 3: Microstructure feature calculation
        micro_engine.process_tick(nullptr, symbol, ltp, bid, ask, vol, oi, ts);
        MicrostructureMetrics metrics;
        micro_engine.get_metrics(symbol, metrics);
        auto t3 = std::chrono::high_resolution_clock::now();

        // Stage 4: Strategy decision engine (in-process exit check & margin management)
        strategy_engine.on_tick(nullptr, symbol, ltp, bid, ask, ts, metrics);
        auto t4 = std::chrono::high_resolution_clock::now();

        // Record stage latencies in nanoseconds
        double d1 = std::chrono::duration<double, std::nano>(t1 - t0).count();
        double d2 = std::chrono::duration<double, std::nano>(t2 - t1).count();
        double d3 = std::chrono::duration<double, std::nano>(t3 - t2).count();
        double d4 = std::chrono::duration<double, std::nano>(t4 - t3).count();
        double d_total = std::chrono::duration<double, std::nano>(t4 - t0).count();

        stage1_nic_to_arrival.push_back(d1);
        stage2_proto_parse.push_back(d2);
        stage3_micro_features.push_back(d3);
        stage4_strat_decision.push_back(d4);
        stage_total_e2e.push_back(d_total);
    }

    auto s1 = compute_stats(stage1_nic_to_arrival);
    auto s2 = compute_stats(stage2_proto_parse);
    auto s3 = compute_stats(stage3_micro_features);
    auto s4 = compute_stats(stage4_strat_decision);
    auto s_tot = compute_stats(stage_total_e2e);

    std::cout << std::left << std::setw(32) << "Pipeline Stage"
              << " | " << std::right << std::setw(11) << "p50 (Med)"
              << " | " << std::right << std::setw(11) << "p90"
              << " | " << std::right << std::setw(11) << "p99"
              << " | " << std::right << std::setw(11) << "p99.9"
              << " | " << std::right << std::setw(11) << "Mean (ns)"
              << " | " << std::right << std::setw(11) << "Mean (µs)"
              << std::endl;
    std::cout << std::string(105, '-') << std::endl;

    print_row("1. Socket/Buffer Copy", s1);
    print_row("2. Protobuf Decode & Extraction", s2);
    print_row("3. Microstructure Features", s3);
    print_row("4. Strategy Decision Engine", s4);
    std::cout << std::string(105, '=') << std::endl;
    print_row("TOTAL End-to-End (Userspace)", s_tot);
    std::cout << std::string(105, '=') << std::endl;

    return 0;
}
