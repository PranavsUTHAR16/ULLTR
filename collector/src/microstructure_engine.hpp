#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <cmath>
#include <chrono>
#include <hiredis/hiredis.h>

struct MinuteBucket {
    int64_t minute_timestamp = 0; // Epoch seconds rounded to 60s
    double buy_vol = 0.0;
    double sell_vol = 0.0;
    double buy_dollar = 0.0;
    double sell_dollar = 0.0;
    double qb_delta = 0.0; // Quote Boundary delta for ModelPOC
    double close_oi = 0.0;
    double close_price = 0.0;
};

struct MicrostructureMetrics {
    std::string symbol;
    double ltp = 0.0;
    double bid = 0.0;
    double ask = 0.0;
    double vwap_buy = 0.0;
    double vwap_sell = 0.0;
    double roll_buy_vol = 0.0;
    double roll_sell_vol = 0.0;
    double cvd_15m = 0.0;
    double delta_oi_15m = 0.0;
    double delta_price_15m = 0.0;
    double dpoc = 0.0;
    double cum_cvd = 0.0;
    double session_vwap = 0.0;
    int64_t updated_at_ms = 0;
};

class MicrostructureEngine {
public:
    static constexpr size_t VWAP_WINDOW_MINS = 90;
    static constexpr size_t CVD_WINDOW_MINS = 15;

    struct SymbolState {
        std::string symbol;
        std::string session_date;
        double last_ltp = 0.0;
        double last_bid = 0.0;
        double last_ask = 0.0;
        int64_t last_cum_vol = 0;
        double last_sign = 1.0;
        double last_oi = 0.0;
        int64_t last_redis_write_ms = 0;

        // Cumulative day tracking
        double cum_cvd = 0.0;
        double cum_session_dollar = 0.0;
        double cum_session_vol = 0.0;

        // 90-minute rolling accumulators
        double roll_buy_vol_90m = 0.0;
        double roll_sell_vol_90m = 0.0;
        double roll_buy_dollar_90m = 0.0;
        double roll_sell_dollar_90m = 0.0;

        // 90-minute circular ring buffer of 1-minute buckets
        MinuteBucket buckets_90m[VWAP_WINDOW_MINS];
        int64_t current_minute_ts = 0;

        // 15-minute OI & Price history
        double oi_ring[CVD_WINDOW_MINS];
        int64_t oi_ts_ring[CVD_WINDOW_MINS];
        double initial_oi = 0.0;
        bool initial_oi_set = false;

        double price_ring[CVD_WINDOW_MINS];
        double initial_price = 0.0;
        bool initial_price_set = false;

        // 5.0 pt dPOC Volume Profile
        std::unordered_map<int64_t, double> poc_bins; // bin_index = round(price / 5.0)
        int64_t max_poc_bin = 0;
        double max_poc_vol = 0.0;

        MicrostructureMetrics last_metrics;

        SymbolState() {
            for (size_t i = 0; i < VWAP_WINDOW_MINS; ++i) buckets_90m[i] = MinuteBucket();
            for (size_t i = 0; i < CVD_WINDOW_MINS; ++i) { 
                oi_ring[i] = 0.0; 
                oi_ts_ring[i] = 0; 
                price_ring[i] = 0.0;
            }
        }
    };

    MicrostructureEngine();
    ~MicrostructureEngine() = default;

    // Hot-path tick processor (O(1) nanosecond execution)
    void process_tick(
        redisContext* redis,
        const std::string& symbol,
        double ltp,
        double bid,
        double ask,
        int64_t cum_vol,
        double oi,
        int64_t ts_exchange_ms
    );

    // Read current state
    bool get_metrics(const std::string& symbol, MicrostructureMetrics& out) const;

private:
    std::unordered_map<std::string, SymbolState> m_states;

    void restore_session_from_redis(redisContext* redis, const std::string& symbol, SymbolState& state);
    void update_minute_boundary(SymbolState& state, int64_t minute_ts, double current_oi, double ltp);
    void write_to_redis(redisContext* redis, SymbolState& state, const MicrostructureMetrics& m);
};
