#pragma once

#include "strategy_types.hpp"
#include "position_manager.hpp"
#include "strike_resolver.hpp"
#include "redis_utils.hpp"
#include "microstructure_engine.hpp"
#include <hiredis/hiredis.h>
#include <string>
#include <functional>
#include <cstdint>

struct Candle1M {
    int64_t minute_ts = 0;
    double open  = 0.0;
    double high  = 0.0;
    double low   = 0.0;
    double close = 0.0;
    int64_t volume = 0;
};

/**
 * StrategyEngine (Coordinator)
 * ----------------------------
 * Thin routing layer.  Receives 1m bars and live ticks and dispatches to the
 * per-model evaluate_* methods.  All capital, pricing, and event publishing
 * is delegated to PositionManager and StrikeResolver.
 *
 * CRITICAL INVARIANT: call rehydrate_from_redis() immediately after
 * construction (or after any process restart) before processing live ticks.
 * This rebuilds the full Initial Balance, TPO profile, POC baseline, and any
 * open positions from Redis so restarts are transparent.
 */
class StrategyEngine {
public:
    using TradeCallback = std::function<void(const std::string& event_type,
                                             const UnifiedPosition& pos,
                                             const std::string& msg)>;

    StrategyEngine(double starting_capital, int lot_size, redisContext* redis);
    ~StrategyEngine() = default;

    // -----------------------------------------------------------------------
    // Startup: must be called before any on_tick / on_1m_bar
    // -----------------------------------------------------------------------
    /**
     * Replay all today's 1m candles from Redis (md:candle:{spot_key}:1m:*)
     * between 09:15 and the current time.  Reconstructs:
     *   - Initial Balance High/Low and TPO masks (09:15-10:15)
     *   - IB lock and Value Area (VAH/VAL/POC)
     *   - POC baseline for POC V2
     *   - Spatial Box rolling state
     *   - Any open positions from ulltr:portfolio:state
     *
     * This is idempotent.  Running it when already up-to-date is safe.
     */
    void rehydrate_from_redis();

    // -----------------------------------------------------------------------
    // Live feed handlers
    // -----------------------------------------------------------------------
    void on_tick(double ltp, double bid, double ask,
                 int64_t ts_ms, const MicrostructureMetrics& metrics);

    void on_1m_bar(const std::string& symbol,
                   const Candle1M& bar,
                   const Candle1M& prev_bar,
                   const MicrostructureMetrics& metrics);

    // -----------------------------------------------------------------------
    // Config toggles
    // -----------------------------------------------------------------------
    bool enable_model_poc_v2    = true;
    bool enable_model_spatial_box = true;
    bool enable_model_dalton_va  = true;
    bool enable_model_tpo_poc    = false;

    void set_trade_callback(TradeCallback cb) { m_trade_callback = cb; }

    // Access to sub-objects for main.cpp wiring
    PositionManager& pool() { return m_pool; }
    StrikeResolver& resolver() { return m_resolver; }

    // Convenience passthrough
    void publish_portfolio_state(const std::string& t = "") {
        m_pool.publish_portfolio_state(t);
    }

private:
    // -----------------------------------------------------------------------
    // Core sub-objects (owned here)
    // -----------------------------------------------------------------------
    PositionManager m_pool;
    StrikeResolver  m_resolver;
    redisContext*   m_redis;
    TradeCallback   m_trade_callback;

    // -----------------------------------------------------------------------
    // Model POC V2 state
    // -----------------------------------------------------------------------
    double  m_prev_poc       = 0.0;
    bool    m_poc_initialized = false;
    int64_t m_last_evaluated_minute = 0;

    struct PendingSignal {
        bool       has_signal   = false;
        OptionType chosen_type  = OptionType::CE;
        std::string regime;
    } m_pending_poc_signal;

    void evaluate_model_poc_v2(const Candle1M& bar,
                               const MicrostructureMetrics& m,
                               const std::string& time_str);

    void execute_pending_poc_signal(const Candle1M& bar,
                                    const std::string& time_str);

    // -----------------------------------------------------------------------
    // TPO Market Profile state (shared with Dalton IB)
    // -----------------------------------------------------------------------
    static constexpr double   TPO_BIN_SIZE  = 20.0;
    static constexpr int64_t  TPO_BASE_PRICE = 20000;
    static constexpr size_t   TPO_NUM_BINS   = 350;
    uint16_t m_tpo_bracket_masks[TPO_NUM_BINS] = {0};
    double   m_current_tpo_poc = 0.0;
    int      m_current_tpo_max_count = 0;

    int  get_tpo_bracket_index(const std::string& time_str) const;
    void update_tpo_profile(double high, double low, int bracket_idx);

    void evaluate_model_tpo_poc(const Candle1M& bar,
                                const MicrostructureMetrics& m,
                                const std::string& time_str);

    // -----------------------------------------------------------------------
    // Spatial Box state
    // -----------------------------------------------------------------------
    double  m_box_high          = 0.0;
    double  m_box_low           = 0.0;
    double  m_box_anchor_cvd    = 0.0;
    double  m_box_anchor_pv     = 0.0;
    double  m_box_anchor_vol    = 0.0;
    double  m_cum_pv            = 0.0;
    double  m_cum_vol           = 0.0;
    bool    m_box_initialized   = false;
    bool    m_box_armed         = false;
    int     m_box_armed_dir     = 0;
    int64_t m_box_last_exit_minute = -20;

    double get_box_avwap(double tp) const;
    void   evaluate_model_spatial_box(const Candle1M& bar,
                                      const MicrostructureMetrics& m,
                                      const std::string& time_str);

    // -----------------------------------------------------------------------
    // Causal Dalton Value Area state (locked once at 10:15)
    // -----------------------------------------------------------------------
    bool   m_ib_locked        = false;
    double m_ib_high          = 0.0;
    double m_ib_low           = 1e9;
    double m_ib_poc           = 0.0;
    double m_ib_vah           = 0.0;
    double m_ib_val           = 0.0;
    bool   m_was_below_val    = false;
    bool   m_was_above_vah    = false;
    int    m_dalton_trades_today = 0;

    double m_cached_pcr         = 1.0;
    int64_t m_cached_pcr_time_ms = 0;

    void   lock_initial_balance_value_area();
    double calculate_chain_pcr();
    void   evaluate_model_dalton_va(const Candle1M& bar,
                                    const MicrostructureMetrics& m,
                                    const std::string& time_str);

    // -----------------------------------------------------------------------
    // Exit management
    // -----------------------------------------------------------------------
    void check_active_exits(double fut_p, double high, double low,
                            double session_vwap, double cvd_15m,
                            int64_t now_ms, const std::string& time_str,
                            bool is_bar_close);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    /// Rehydrate a single historical 1m bar (no live data lookup, no entry signals).
    void replay_historical_bar(const Candle1M& bar, const std::string& time_str);

    std::string m_spot_key;   ///< e.g. "NSE_INDEX|Nifty 50"
};
