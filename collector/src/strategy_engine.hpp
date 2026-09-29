#pragma once

#include "strategy_types.hpp"
#include "fifo_pool.hpp"
#include "microstructure_engine.hpp"
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <functional>

struct Candle1M {
    int64_t minute_ts = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    int64_t volume = 0;
};

struct TargetStrikeResult {
    int64_t strike = 0;
    std::string instrument_key;
    double ask = 0.0;
    double bid = 0.0;
};

class StrategyEngine {
public:
    StrategyEngine(FIFOPool& pool);
    ~StrategyEngine() = default;

    // Callbacks for order execution and notifications
    using TradeCallback = std::function<void(const std::string& event_type, const UnifiedPosition& pos, const std::string& msg)>;
    void set_trade_callback(TradeCallback cb) { m_trade_callback = cb; }

    // Core Tick Evaluator (Sub-microsecond hot-path)
    void on_tick(
        redisContext* redis,
        const std::string& symbol,
        double ltp,
        double bid,
        double ask,
        int64_t ts_ms,
        const MicrostructureMetrics& metrics
    );

    // 1-Minute Bar Evaluator
    void on_1m_bar(
        redisContext* redis,
        const std::string& symbol,
        const Candle1M& bar,
        const Candle1M& prev_bar,
        const MicrostructureMetrics& metrics
    );

    // Model POC V2 Evaluator
    void evaluate_model_poc_v2(
        redisContext* redis,
        const Candle1M& bar,
        const MicrostructureMetrics& m,
        const std::string& time_str
    );

    // Model Spatial Box (AVWAP Arm Gate) Evaluator
    void evaluate_model_spatial_box(
        redisContext* redis,
        const Candle1M& bar,
        const MicrostructureMetrics& m,
        const std::string& time_str
    );

    // TPO Market Profile POC Reversion Evaluator (Legacy/Disabled)
    void evaluate_model_tpo_poc(
        redisContext* redis,
        const Candle1M& bar,
        const MicrostructureMetrics& m,
        const std::string& time_str
    );

    // Horizon 3: Causal Dalton Value Area Traverse Evaluator (10:15 - 13:30 IST)
    void evaluate_model_dalton_va(
        redisContext* redis,
        const Candle1M& bar,
        const MicrostructureMetrics& m,
        const std::string& time_str
    );

    FIFOPool& get_pool() { return m_pool; }
    void publish_portfolio_state(redisContext* redis, const std::string& current_time_str = "");
    void set_front_expiry(const std::string& chain_key) { m_cached_front_expiry = chain_key; }
    void init_front_expiry(redisContext* redis);

    /**
     * MANDATORY STARTUP CALL — invoke immediately after construction, before any
     * on_tick / on_1m_bar calls.
     *
     * Reads all today's 1m candles for the NIFTY spot index from Redis
     * (md:candle:NSE_INDEX|Nifty 50:1m:*), replays them through the historical
     * path to reconstruct:
     *   - Initial Balance high/low and TPO bitmasks (09:15–10:15)
     *   - IB lock: VAH, VAL, POC (called automatically at the 10:15 bar)
     *   - m_was_below_val / m_was_above_vah probes
     *   - POC V2 baseline (m_prev_poc) from the most recent historical bar
     *   - Spatial Box rolling OHLC (box reset on >50pt range breaks)
     *
     * Running this after a process restart means the engine sees the same
     * Initial Balance as if it had been running since 09:15. This eliminates
     * the corrupted-IB root cause of the 2026-09-29 ₹-3.5k live loss.
     */
    void rehydrate_from_redis(redisContext* redis);

    // Strategy Execution Toggles
    bool enable_model_poc_v2 = true;
    bool enable_model_spatial_box = true;
    bool enable_model_tpo_poc = false; // Blocked as requested: TPO POC Reversion disabled
    bool enable_model_dalton_va = true; // Horizon 3 Causal Dalton Value Area Traverse

private:
    FIFOPool& m_pool;
    TradeCallback m_trade_callback;

    // Model POC V2 State
    double m_prev_poc = 0.0;
    bool m_poc_initialized = false;
    int64_t m_last_evaluated_minute = 0;

    struct PendingSignal {
        bool has_signal = false;
        OptionType chosen_type = OptionType::CE;
        std::string regime;
    };
    PendingSignal m_pending_poc_signal;

    void execute_pending_poc_signal(
        redisContext* redis,
        const Candle1M& bar,
        const std::string& time_str
    );

    // Model Spatial Box (AVWAP Arm Gate) State
    double m_box_high = 0.0;
    double m_box_low = 0.0;
    double m_box_anchor_cvd = 0.0;
    double m_box_anchor_pv = 0.0;
    double m_box_anchor_vol = 0.0;
    double m_cum_pv = 0.0;
    double m_cum_vol = 0.0;
    bool m_box_armed = false;
    int m_box_armed_dir = 0; // +1 for UP (CE), -1 for DOWN (PE)
    bool m_box_initialized = false;
    int64_t m_box_last_exit_minute = -20;
    double get_box_avwap(double current_tp) const;

    // TPO Market Profile Engine State (20-pt price bins, 13 periods A..M)
    static constexpr double TPO_BIN_SIZE = 20.0;
    static constexpr int64_t TPO_BASE_PRICE = 20000;
    static constexpr size_t TPO_NUM_BINS = 350; // 20000 to 27000 covers NIFTY comfortably
    uint16_t m_tpo_bracket_masks[TPO_NUM_BINS] = {0};
    double m_current_tpo_poc = 0.0;
    int m_current_tpo_max_count = 0;
    int get_tpo_bracket_index(const std::string& time_str) const;
    void update_tpo_profile(double high, double low, int bracket_idx);

    // Helper evaluation methods
    void check_active_exits(
        redisContext* redis,
        double fut_p,
        double high,
        double low,
        double session_vwap,
        double cvd_15m,
        int64_t now_ms,
        const std::string& time_str,
        bool is_bar_close = false
    );

    // Causal Dalton Value Area Engine State (Frozen permanently at 10:15:00 IST)
    bool m_ib_locked = false;
    double m_ib_high = 0.0;
    double m_ib_low = 1e9;
    double m_ib_poc = 0.0;
    double m_ib_vah = 0.0;
    double m_ib_val = 0.0;
    bool m_was_below_val = false;
    bool m_was_above_vah = false;
    int m_dalton_trades_today = 0;
    double m_cached_pcr = 1.0;
    int64_t m_cached_pcr_time_ms = 0;

    void lock_initial_balance_value_area();
    double calculate_chain_pcr(redisContext* redis);

    double resolve_option_price(redisContext* redis, int64_t strike, OptionType type, bool is_ask);
    std::string resolve_option_instrument_key(redisContext* redis, int64_t strike, OptionType type);
    TargetStrikeResult resolve_target_strike(redisContext* redis, double fut_price, OptionType type, double target_premium = 155.0);
    void publish_trade_event(redisContext* redis, const std::string& type, const UnifiedPosition& pos, const std::string& details);
    int get_current_dte() const;
    std::string m_cached_front_expiry;
};
