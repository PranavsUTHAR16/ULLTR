#include "fifo_pool.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>

FIFOPool::FIFOPool(double starting_capital_)
    : starting_capital(starting_capital_),
      current_equity(starting_capital_) {
}

double FIFOPool::get_locked_margin() const {
    double total = 0.0;
    for (const auto& pos : active_positions) {
        total += pos.margin_locked;
    }
    return total;
}

double FIFOPool::get_free_cash() const {
    double base = starting_capital - realized_losses_today;
    double avail = base - get_locked_margin();
    return std::max(0.0, std::round(avail * 100.0) / 100.0);
}

double FIFOPool::get_realized_pnl_today() const {
    return realized_profits_today - realized_losses_today;
}

double FIFOPool::get_total_portfolio_value() const {
    double unrealized = 0.0;
    for (const auto& pos : active_positions) {
        unrealized += pos.get_unrealized_pnl();
    }
    return std::round((current_equity + get_realized_pnl_today() + unrealized) * 100.0) / 100.0;
}

void FIFOPool::record_rejection(
    const std::string& timestamp,
    const std::string& model_name,
    int64_t strike,
    OptionType option_type,
    double option_ask,
    const std::string& reason
) {
    RejectedSignal rej;
    rej.timestamp = timestamp;
    rej.model_name = model_name;
    rej.strike = strike;
    rej.option_type = option_type;
    rej.option_ask = option_ask;
    rej.reason = reason;
    rejected_signals.push_back(rej);

    std::cout << "⚠️ [FIFOPool REJECTION] " << timestamp << " | " << model_name
              << " " << strike << " " << option_type_to_string(option_type)
              << " @ Rs " << option_ask << " | Reason: " << reason << std::endl;
}

bool FIFOPool::evaluate_and_allocate(
    const TradeSignal& signal,
    UnifiedPosition& out_position,
    std::string& out_reason
) {
    // 1. Directional Conflict Filter & Model Reversal Flip
    if (enforce_conflict_filter) {
        OptionType opp_type = (signal.option_type == OptionType::CE) ? OptionType::PE : OptionType::CE;
        std::vector<UnifiedPosition> opp_positions;
        bool has_other_model_opp = false;

        for (const auto& pos : active_positions) {
            if (pos.option_type == opp_type) {
                opp_positions.push_back(pos);
                if (pos.model_name != signal.model_name) {
                    has_other_model_opp = true;
                }
            }
        }

        if (!opp_positions.empty()) {
            if (!has_other_model_opp) {
                // Same model reversal: close opposing positions to release margin
                for (const auto& opp_pos : opp_positions) {
                    close_position(
                        opp_pos.position_id,
                        opp_pos.current_option_price,
                        signal.fut_price,
                        "Model " + signal.model_name + " Trend Reversal Flip -> " + option_type_to_string(signal.option_type),
                        signal.timestamp_str
                    );
                }
            } else {
                std::string opp_str = option_type_to_string(opp_type);
                std::string sig_str = option_type_to_string(signal.option_type);
                out_reason = "BLOCKED: Directional conflict (Long " + opp_str + " active, cannot buy " + sig_str + ")";
                record_rejection(signal.timestamp_str, signal.model_name, signal.strike, signal.option_type, signal.option_ask, out_reason);
                return false;
            }
        }
    }

    // 2. Strike-Level Concentration Cap
    int current_strike_lots = 0;
    for (const auto& pos : active_positions) {
        if (pos.strike == signal.strike && pos.option_type == signal.option_type) {
            current_strike_lots += pos.lots;
        }
    }
    if (current_strike_lots >= max_strike_lots) {
        std::ostringstream oss;
        oss << "REJECTED: Max strike concentration reached (" << current_strike_lots << "/" << max_strike_lots << " lots on " << signal.strike << " " << option_type_to_string(signal.option_type) << ")";
        out_reason = oss.str();
        record_rejection(signal.timestamp_str, signal.model_name, signal.strike, signal.option_type, signal.option_ask, out_reason);
        return false;
    }

    // 3. FIFO Margin Sizing Calculation
    double avail_cash = get_free_cash();
    double cost_per_lot = signal.option_ask * lot_size;

    if (avail_cash < cost_per_lot || avail_cash < 1000.0) {
        std::ostringstream oss;
        oss << "REJECTED: Insufficient margin (Free Cash: Rs " << std::fixed << std::setprecision(2) << avail_cash << " < 1 Lot Cost: Rs " << cost_per_lot << ")";
        out_reason = oss.str();
        record_rejection(signal.timestamp_str, signal.model_name, signal.strike, signal.option_type, signal.option_ask, out_reason);
        return false;
    }

    int lots_by_capital = static_cast<int>(std::floor(avail_cash / lot_cost_divisor));
    int lots_by_cost = static_cast<int>(std::floor(avail_cash / cost_per_lot));
    int lots = std::min(lots_by_capital, lots_by_cost);

    int remaining_strike_capacity = max_strike_lots - current_strike_lots;
    lots = std::min(lots, remaining_strike_capacity);

    if (lots <= 0) {
        if (avail_cash >= cost_per_lot) {
            lots = 1;
        } else {
            std::ostringstream oss;
            oss << "REJECTED: Margin sizing yielded 0 lots (Free Cash: Rs " << std::fixed << std::setprecision(2) << avail_cash << ")";
            out_reason = oss.str();
            record_rejection(signal.timestamp_str, signal.model_name, signal.strike, signal.option_type, signal.option_ask, out_reason);
            return false;
        }
    }

    // 4. Create and Approve Position
    double margin_required = lots * cost_per_lot;
    m_trade_counter++;

    std::ostringstream pid_oss;
    pid_oss << "UP_" << m_trade_counter;

    out_position.position_id = pid_oss.str();
    out_position.model_name = signal.model_name;
    out_position.symbol = signal.symbol;
    out_position.strike = signal.strike;
    out_position.option_type = signal.option_type;
    out_position.lots = lots;
    out_position.quantity = lots * lot_size;
    out_position.entry_time = signal.timestamp_str;
    out_position.entry_option_price = signal.option_ask;
    out_position.entry_futures_price = signal.fut_price;
    out_position.current_option_price = signal.option_ask;
    out_position.current_futures_price = signal.fut_price;
    out_position.margin_locked = margin_required;
    out_position.sl_futures_price = signal.sl_fut;
    out_position.tp_futures_price = signal.tp_fut;
    out_position.is_rolled = false;
    out_position.is_active = true;
    out_position.remaining_lots = lots;

    active_positions.push_back(out_position);
    out_reason = "ALLOCATED";
    return true;
}

bool FIFOPool::bank_lot1(
    const std::string& position_id,
    double exit_opt_price,
    double exit_fut_price,
    const std::string& exit_time
) {
    for (auto& pos : active_positions) {
        if (pos.position_id == position_id) {
            pos.t1_hit = true;
            pos.lot1_exit_time = exit_time;
            pos.lot1_exit_opt = exit_opt_price;
            pos.lot1_exit_fut = exit_fut_price;
            double pnl_1 = (exit_opt_price - pos.entry_option_price - 1.0) * lot_size;
            pos.lot1_realized_pnl = pnl_1;

            if (pnl_1 >= 0.0) {
                realized_profits_today += pnl_1;
                t1_locked_profits += pnl_1;
            } else {
                realized_losses_today += std::abs(pnl_1);
            }

            pos.remaining_lots = std::max(0, pos.lots - 1);
            pos.margin_locked = pos.remaining_lots * (pos.entry_option_price * lot_size);
            return true;
        }
    }
    return false;
}

bool FIFOPool::close_position(
    const std::string& position_id,
    double exit_opt_price,
    double exit_fut_price,
    const std::string& reason,
    const std::string& exit_time
) {
    for (auto it = active_positions.begin(); it != active_positions.end(); ++it) {
        if (it->position_id == position_id) {
            UnifiedPosition closed_pos = *it;
            closed_pos.is_active = false;
            closed_pos.exit_option_price = exit_opt_price;
            closed_pos.exit_futures_price = exit_fut_price;
            closed_pos.exit_reason = reason;
            closed_pos.exit_time = exit_time;

            if (closed_pos.t1_hit) {
                int runner_lots = closed_pos.remaining_lots;
                if (runner_lots > 0 && closed_pos.lots >= 2) {
                    double runner_pnl = (exit_opt_price - closed_pos.entry_option_price - 1.0) * (runner_lots * lot_size);
                    if (runner_pnl < 0.0) {
                        realized_losses_today += std::abs(runner_pnl);
                    } else {
                        realized_profits_today += runner_pnl;
                        t1_locked_profits += runner_pnl;
                    }
                    closed_pos.realized_pnl = closed_pos.lot1_realized_pnl + runner_pnl;
                } else {
                    // 1-lot position already accounted for in bank_lot1
                    closed_pos.realized_pnl = closed_pos.lot1_realized_pnl;
                }
            } else {
                closed_pos.realized_pnl = (exit_opt_price - closed_pos.entry_option_price - 1.0) * closed_pos.quantity;
                if (closed_pos.realized_pnl < 0.0) {
                    realized_losses_today += std::abs(closed_pos.realized_pnl);
                } else {
                    realized_profits_today += closed_pos.realized_pnl;
                    t1_locked_profits += closed_pos.realized_pnl;
                }
            }

            active_positions.erase(it);
            closed_positions.push_back(closed_pos);
            return true;
        }
    }
    return false;
}

void FIFOPool::reset_day(double new_starting_capital) {
    if (new_starting_capital > 0.0) {
        starting_capital = new_starting_capital;
    } else {
        starting_capital = starting_capital + t1_locked_profits - realized_losses_today;
    }
    current_equity = starting_capital;
    realized_losses_today = 0.0;
    realized_profits_today = 0.0;
    t1_locked_profits = 0.0;
    active_positions.clear();
    closed_positions.clear();
    rejected_signals.clear();
    m_trade_counter = 0;
}

void FIFOPool::restore_state(redisContext* redis) {
    if (!redis) return;
    redisReply* reply = (redisReply*)redisCommand(redis, "GET ulltr:portfolio:recovery");
    if (!reply) return;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        try {
            auto j = nlohmann::json::parse(reply->str);
            if (j.contains("trade_counter")) m_trade_counter = j["trade_counter"].get<int>();
            if (j.contains("realized_losses_today")) realized_losses_today = j["realized_losses_today"].get<double>();
            if (j.contains("realized_profits_today")) realized_profits_today = j["realized_profits_today"].get<double>();
            if (j.contains("t1_locked_profits")) t1_locked_profits = j["t1_locked_profits"].get<double>();
            
            if (j.contains("active_positions")) {
                active_positions.clear();
                for (const auto& pj : j["active_positions"]) {
                    UnifiedPosition p;
                    p.position_id = pj.value("position_id", "");
                    p.model_name = pj.value("model_name", "");
                    p.symbol = pj.value("symbol", "");
                    p.strike = pj.value("strike", (int64_t)0);
                    p.option_type = string_to_option_type(pj.value("option_type", "PE"));
                    p.lots = pj.value("lots", 1);
                    p.quantity = pj.value("quantity", (int64_t)65);
                    p.entry_time = pj.value("entry_time", "");
                    p.entry_option_price = pj.value("entry_option_price", 0.0);
                    p.entry_futures_price = pj.value("entry_futures_price", 0.0);
                    p.current_option_price = pj.value("current_option_price", p.entry_option_price);
                    p.current_futures_price = pj.value("current_futures_price", p.entry_futures_price);
                    p.sl_futures_price = pj.value("sl_futures_price", 0.0);
                    p.tp_futures_price = pj.value("tp_futures_price", 0.0);
                    p.margin_locked = pj.value("margin_locked", 0.0);
                    p.is_active = true;
                    p.t1_hit = pj.value("t1_hit", false);
                    p.lot1_exit_time = pj.value("lot1_exit_time", "");
                    p.lot1_exit_opt = pj.value("lot1_exit_opt", 0.0);
                    p.lot1_exit_fut = pj.value("lot1_exit_fut", 0.0);
                    p.lot1_realized_pnl = pj.value("lot1_realized_pnl", 0.0);
                    p.lot2_sl_futures_price = pj.value("lot2_sl_futures_price", 0.0);
                    if (p.t1_hit && p.lot2_sl_futures_price <= 0.0 && p.entry_futures_price > 0.0) {
                        p.lot2_sl_futures_price = (p.option_type == OptionType::CE) ? (p.entry_futures_price + 2.0) : (p.entry_futures_price - 2.0);
                    }
                    p.remaining_lots = pj.value("remaining_lots", 1);
                    p.bars_held = pj.value("bars_held", 0);
                    p.active_box_avwap = pj.value("active_box_avwap", 0.0);
                    p.target_opt_price = pj.value("target_opt_price", 0.0);
                    p.sl_opt_price = pj.value("sl_opt_price", 0.0);
                    p.tpo_poc_at_entry = pj.value("tpo_poc_at_entry", 0.0);
                    p.tpo_target_futures = pj.value("tpo_target_futures", 0.0);
                    active_positions.push_back(p);
                }
            }

            if (j.contains("closed_positions")) {
                closed_positions.clear();
                for (const auto& cj : j["closed_positions"]) {
                    UnifiedPosition cp;
                    cp.position_id = cj.value("position_id", "");
                    cp.model_name = cj.value("model_name", "");
                    cp.symbol = cj.value("symbol", "");
                    cp.strike = cj.value("strike", (int64_t)0);
                    cp.option_type = string_to_option_type(cj.value("option_type", "CE"));
                    cp.lots = cj.value("lots", 1);
                    cp.quantity = cj.value("quantity", (int64_t)65);
                    cp.entry_time = cj.value("entry_time", "");
                    cp.exit_time = cj.value("exit_time", "");
                    cp.entry_option_price = cj.value("entry_option_price", 0.0);
                    cp.exit_option_price = cj.value("exit_option_price", 0.0);
                    cp.entry_futures_price = cj.value("entry_futures_price", 0.0);
                    cp.exit_futures_price = cj.value("exit_futures_price", 0.0);
                    cp.realized_pnl = cj.value("realized_pnl", 0.0);
                    cp.exit_reason = cj.value("exit_reason", "");
                    cp.bars_held = cj.value("bars_held", 0);
                    cp.active_box_avwap = cj.value("active_box_avwap", 0.0);
                    cp.target_opt_price = cj.value("target_opt_price", 0.0);
                    cp.sl_opt_price = cj.value("sl_opt_price", 0.0);
                    cp.tpo_poc_at_entry = cj.value("tpo_poc_at_entry", 0.0);
                    cp.tpo_target_futures = cj.value("tpo_target_futures", 0.0);
                    cp.is_active = false;
                    closed_positions.push_back(cp);
                }
            }
            std::cout << "✅ [FIFOPool] Successfully restored portfolio state from Redis: "
                      << active_positions.size() << " active (runner), "
                      << closed_positions.size() << " closed, trade_counter=" << m_trade_counter
                      << std::endl;
        } catch (const std::exception& e) {
            std::cout << "⚠️ [FIFOPool] Failed to parse recovery state: " << e.what() << std::endl;
        }
    }
    freeReplyObject(reply);
}
