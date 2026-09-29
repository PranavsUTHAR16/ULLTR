#pragma once

#include "strategy_types.hpp"
#include <vector>
#include <string>
#include <memory>
#include <cmath>
#include <algorithm>

struct redisContext;

class FIFOPool {
public:
    FIFOPool(double starting_capital = 25000.0);
    ~FIFOPool() = default;

    // Config parameters
    double starting_capital = 25000.0;
    double current_equity = 25000.0;
    int lot_size = 65;
    double lot_cost_divisor = 10000.0;
    int max_strike_lots = 3;
    bool enforce_conflict_filter = true;

    // Running accounting
    double realized_losses_today = 0.0;
    double realized_profits_today = 0.0;
    double t1_locked_profits = 0.0;

    std::vector<UnifiedPosition> active_positions;
    std::vector<UnifiedPosition> closed_positions;
    std::vector<RejectedSignal> rejected_signals;

    // Capacity & Valuation
    double get_locked_margin() const;
    double get_free_cash() const;
    double get_total_portfolio_value() const;
    double get_realized_pnl_today() const;

    // Allocation & Exit
    bool evaluate_and_allocate(
        const TradeSignal& signal,
        UnifiedPosition& out_position,
        std::string& out_reason
    );

    bool bank_lot1(
        const std::string& position_id,
        double exit_opt_price,
        double exit_fut_price,
        const std::string& exit_time
    );

    bool close_position(
        const std::string& position_id,
        double exit_opt_price,
        double exit_fut_price,
        const std::string& reason,
        const std::string& exit_time
    );

    void reset_day(double new_starting_capital = -1.0);
    void restore_state(redisContext* redis);

    bool has_active_position_for_model(const std::string& model_name) const {
        for (const auto& p : active_positions) {
            if (p.model_name == model_name) return true;
        }
        return false;
    }

    bool has_same_direction_position(const std::string& model_name, OptionType type) const {
        for (const auto& p : active_positions) {
            if (p.model_name == model_name && p.option_type == type) return true;
        }
        return false;
    }

private:
    int m_trade_counter = 0;
    void record_rejection(
        const std::string& timestamp,
        const std::string& model_name,
        int64_t strike,
        OptionType option_type,
        double option_ask,
        const std::string& reason
    );
};
