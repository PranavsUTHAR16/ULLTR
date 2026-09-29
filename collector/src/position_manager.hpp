#pragma once

#include "strategy_types.hpp"
#include "redis_utils.hpp"
#include "strike_resolver.hpp"
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <cstdint>

/**
 * PositionManager
 * ---------------
 * Single responsibility: owns the SEBI T+1 capital pool, allocates/closes
 * UnifiedPosition objects, and publishes portfolio state + trade audit events
 * to Redis. It has NO strategy logic — strategies call it to request capital.
 *
 * Key invariants:
 *   - Never accepts an entry if live option price is 0.0 (DATA_STALE guard).
 *   - Lot allocation is floored to available settled cash minus locked margin.
 *   - Realized losses are immediately deducted; gains are T+1 locked.
 */
class PositionManager {
public:
    PositionManager(double starting_capital, int lot_size, redisContext* redis)
        : starting_capital(starting_capital),
          lot_size(lot_size),
          m_redis(redis) {}

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /**
     * Attempt to allocate a new position for a TradeSignal.
     * Returns true and populates out_pos if capital is available and the live
     * option price is valid (> 0.0). Returns false with reason string otherwise.
     */
    bool evaluate_and_allocate(const TradeSignal& sig,
                               UnifiedPosition& out_pos,
                               std::string& reject_reason);

    /**
     * Bank Lot 1 of a two-tier POC V2 position at T1 target price.
     * Frees half the margin immediately.
     */
    bool bank_lot1(const std::string& position_id,
                   double exit_opt_price,
                   double exit_fut_price,
                   const std::string& time_str);

    /**
     * Close a position fully at given prices.
     * Returns true if found and closed.
     */
    bool close_position(const std::string& position_id,
                        double exit_opt_price,
                        double exit_fut_price,
                        const std::string& exit_reason,
                        const std::string& time_str);

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------
    bool has_active_position_for_model(const std::string& model_name) const;
    double get_realized_pnl_today() const;
    double get_total_portfolio_value() const;
    double get_free_cash() const;
    double get_locked_margin() const;

    // -----------------------------------------------------------------------
    // Publish
    // -----------------------------------------------------------------------

    /// Push current portfolio state to Redis (ulltr:portfolio:state) and
    /// broadcast on ulltr:events:portfolio.
    void publish_portfolio_state(const std::string& time_str = "") const;

    /// Append a POSITION_OPENED / POSITION_CLOSED event to ulltr:trades:audit
    /// and broadcast on ulltr:trades:stream.
    void publish_trade_event(const std::string& event_type,
                             const UnifiedPosition& pos,
                             const std::string& details) const;

    /// Update redis context (e.g., after reconnect).
    void set_redis(redisContext* redis) { m_redis = redis; }

    // -----------------------------------------------------------------------
    // Public state (read-only for strategy engines)
    // -----------------------------------------------------------------------
    std::vector<UnifiedPosition> active_positions;
    std::vector<UnifiedPosition> closed_positions;
    const double starting_capital;
    const int lot_size;

private:
    redisContext* m_redis = nullptr;
    int m_next_position_idx = 1;

    double m_realized_pnl_today = 0.0;
    double m_locked_margin = 0.0;

    /// Generate position IDs: UP_1, UP_2, …
    std::string next_position_id();

    std::string build_portfolio_json(const std::string& time_str) const;
};
