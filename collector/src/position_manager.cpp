#include "position_manager.hpp"
#include <sstream>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <cstring>

// ---------------------------------------------------------------------------
// next_position_id
// ---------------------------------------------------------------------------
std::string PositionManager::next_position_id() {
    return "UP_" + std::to_string(m_next_position_idx++);
}

// ---------------------------------------------------------------------------
// evaluate_and_allocate
// ---------------------------------------------------------------------------
bool PositionManager::evaluate_and_allocate(
    const TradeSignal& sig,
    UnifiedPosition& out_pos,
    std::string& reject_reason)
{
    // 1. DATA_STALE guard: refuse if live price is zero.
    if (sig.option_ask <= 0.0) {
        reject_reason = "DATA_STALE: option_ask is 0.0 — live feed unavailable";
        return false;
    }

    // 2. Directional conflict: reject if an opposite position of same model exists.
    for (const auto& p : active_positions) {
        if (p.is_active && p.option_type != sig.option_type) {
            reject_reason = "CONFLICT: opposite direction position " + p.position_id + " active";
            return false;
        }
    }

    // 3. Capital calculation under SEBI T+1 rules:
    //    Available = starting_capital - realized_losses_today - locked_margin
    //    Gains from today are NOT yet available (T+1 settlement).
    double losses = 0.0;
    for (const auto& cp : closed_positions) {
        if (cp.realized_pnl < 0.0) losses += std::abs(cp.realized_pnl);
    }
    double locked = get_locked_margin();
    double available = starting_capital - losses - locked;

    double cost_per_lot = sig.option_ask * static_cast<double>(lot_size);
    if (cost_per_lot <= 0.0 || available < cost_per_lot) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2)
            << "INSUFFICIENT_CAPITAL: need ₹" << cost_per_lot
            << ", available ₹" << available;
        reject_reason = oss.str();
        return false;
    }

    int lots = static_cast<int>(available / cost_per_lot);
    lots = std::max(1, std::min(lots, 2)); // cap at 2 lots per the ₹25k pool
    double margin = lots * cost_per_lot;

    // 4. Build position
    UnifiedPosition pos;
    pos.position_id        = next_position_id();
    pos.model_name         = sig.model_name;
    pos.symbol             = sig.symbol;
    pos.strike             = sig.strike;
    pos.option_type        = sig.option_type;
    pos.lots               = lots;
    pos.remaining_lots     = lots;
    pos.quantity           = static_cast<int64_t>(lots) * lot_size;
    pos.entry_option_price = sig.option_ask;
    pos.entry_futures_price = sig.fut_price;
    pos.current_option_price = sig.option_ask;
    pos.current_futures_price = sig.fut_price;
    pos.margin_locked      = margin;
    pos.sl_futures_price   = sig.sl_fut;
    pos.tp_futures_price   = sig.tp_fut;
    pos.entry_time         = sig.timestamp_str;
    pos.is_active          = true;
    pos.t1_hit             = false;
    pos.bars_held          = 0;

    m_locked_margin += margin;
    active_positions.push_back(pos);
    out_pos = pos;

    std::cout << "✅ [PositionManager] Allocated " << pos.position_id
              << " | " << pos.model_name << " | " << pos.lots << " lot(s)"
              << " | Margin ₹" << margin
              << " | Free ₹" << get_free_cash() << std::endl;

    return true;
}

// ---------------------------------------------------------------------------
// bank_lot1
// ---------------------------------------------------------------------------
bool PositionManager::bank_lot1(
    const std::string& position_id,
    double exit_opt_price,
    double exit_fut_price,
    const std::string& time_str)
{
    for (auto& p : active_positions) {
        if (p.position_id != position_id) continue;
        if (p.t1_hit || p.lots < 2) return false;

        int banked_lots = p.lots / 2;
        double pnl = (exit_opt_price - p.entry_option_price) * banked_lots * lot_size - 1.0; // friction

        p.t1_hit            = true;
        p.remaining_lots    = p.lots - banked_lots;
        p.lot1_exit_time    = time_str;
        p.lot1_exit_opt     = exit_opt_price;
        p.lot1_exit_fut     = exit_fut_price;
        p.lot1_realized_pnl = pnl;

        // Free lot1 margin immediately
        double freed = banked_lots * p.entry_option_price * lot_size;
        m_locked_margin = std::max(0.0, m_locked_margin - freed);
        m_realized_pnl_today += pnl;

        std::cout << "🎯 [PositionManager] Lot1 banked for " << position_id
                  << " @ ₹" << exit_opt_price << " | PnL ₹" << pnl << std::endl;
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// close_position
// ---------------------------------------------------------------------------
bool PositionManager::close_position(
    const std::string& position_id,
    double exit_opt_price,
    double exit_fut_price,
    const std::string& exit_reason,
    const std::string& time_str)
{
    for (auto it = active_positions.begin(); it != active_positions.end(); ++it) {
        if (it->position_id != position_id) continue;

        int active_lots = it->t1_hit ? it->remaining_lots : it->lots;
        double pnl = (exit_opt_price - it->entry_option_price) * active_lots * lot_size - 1.0;
        if (it->t1_hit) pnl += it->lot1_realized_pnl;

        it->exit_option_price  = exit_opt_price;
        it->exit_futures_price = exit_fut_price;
        it->exit_reason        = exit_reason;
        it->exit_time          = time_str;
        it->realized_pnl       = pnl;
        it->is_active          = false;

        // Release remaining margin
        double freed = it->remaining_lots * it->entry_option_price * lot_size;
        m_locked_margin = std::max(0.0, m_locked_margin - freed);
        m_realized_pnl_today += (exit_opt_price - it->entry_option_price) * active_lots * lot_size - 1.0;

        closed_positions.push_back(*it);
        active_positions.erase(it);

        std::cout << "🔴 [PositionManager] Closed " << position_id
                  << " | Reason: " << exit_reason
                  << " | PnL ₹" << pnl << std::endl;
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------
bool PositionManager::has_active_position_for_model(const std::string& model_name) const {
    for (const auto& p : active_positions) {
        if (p.model_name == model_name && p.is_active) return true;
    }
    return false;
}

double PositionManager::get_realized_pnl_today() const {
    double total = 0.0;
    for (const auto& cp : closed_positions) total += cp.realized_pnl;
    return total;
}

double PositionManager::get_total_portfolio_value() const {
    return starting_capital + get_realized_pnl_today();
}

double PositionManager::get_free_cash() const {
    double losses = 0.0;
    for (const auto& cp : closed_positions) {
        if (cp.realized_pnl < 0.0) losses += std::abs(cp.realized_pnl);
    }
    return starting_capital - losses - m_locked_margin;
}

double PositionManager::get_locked_margin() const {
    return m_locked_margin;
}

// ---------------------------------------------------------------------------
// build_portfolio_json
// ---------------------------------------------------------------------------
std::string PositionManager::build_portfolio_json(const std::string& time_str) const {
    std::ostringstream o;
    o << std::fixed << std::setprecision(2);

    double unrealized = 0.0;
    for (const auto& p : active_positions) unrealized += p.get_unrealized_pnl();

    o << "{"
      << "\"starting_capital\":" << starting_capital << ","
      << "\"total_portfolio_value\":" << get_total_portfolio_value() << ","
      << "\"realized_pnl_today\":" << get_realized_pnl_today() << ","
      << "\"unrealized_pnl\":" << unrealized << ","
      << "\"free_cash\":" << get_free_cash() << ","
      << "\"locked_margin\":" << get_locked_margin() << ","
      << "\"active_count\":" << active_positions.size() << ","
      << "\"closed_count\":" << closed_positions.size() << ","
      << "\"active_positions\":[";

    for (size_t i = 0; i < active_positions.size(); ++i) {
        const auto& p = active_positions[i];
        if (i > 0) o << ",";
        o << "{\"position_id\":\"" << p.position_id << "\","
          << "\"model_name\":\"" << p.model_name << "\","
          << "\"symbol\":\"" << p.symbol << "\","
          << "\"option_type\":\"" << option_type_to_string(p.option_type) << "\","
          << "\"strike\":" << p.strike << ","
          << "\"lots\":" << p.lots << ","
          << "\"remaining_lots\":" << p.remaining_lots << ","
          << "\"t1_hit\":" << (p.t1_hit ? "true" : "false") << ","
          << "\"quantity\":" << p.quantity << ","
          << "\"entry_opt\":" << p.entry_option_price << ","
          << "\"current_opt\":" << p.current_option_price << ","
          << "\"points\":" << (p.current_option_price - p.entry_option_price) << ","
          << "\"unrealized_pnl\":" << p.get_unrealized_pnl() << ","
          << "\"entry_time\":\"" << p.entry_time << "\"}";
    }

    o << "],\"closed_positions\":[";

    for (size_t i = 0; i < closed_positions.size(); ++i) {
        const auto& p = closed_positions[i];
        if (i > 0) o << ",";
        o << "{\"position_id\":\"" << p.position_id << "\","
          << "\"model_name\":\"" << p.model_name << "\","
          << "\"symbol\":\"" << p.symbol << "\","
          << "\"option_type\":\"" << option_type_to_string(p.option_type) << "\","
          << "\"strike\":" << p.strike << ","
          << "\"lots\":" << p.lots << ","
          << "\"entry_opt\":" << p.entry_option_price << ","
          << "\"exit_opt\":" << p.exit_option_price << ","
          << "\"pnl\":" << p.realized_pnl << ","
          << "\"reason\":\"" << p.exit_reason << "\","
          << "\"entry_time\":\"" << p.entry_time << "\","
          << "\"exit_time\":\"" << p.exit_time << "\"}";
    }

    o << "],\"updated_at\":\"" << time_str << "\"}";
    return o.str();
}

// ---------------------------------------------------------------------------
// publish_portfolio_state
// ---------------------------------------------------------------------------
void PositionManager::publish_portfolio_state(const std::string& time_str) const {
    if (!m_redis) return;
    std::string t = time_str.empty() ? redis_utils::now_ist_time() : time_str;
    std::string json = build_portfolio_json(t);

    redisReply* r1 = (redisReply*)redisCommand(m_redis, "SET ulltr:portfolio:state %s", json.c_str());
    if (r1) freeReplyObject(r1);

    redisReply* r2 = (redisReply*)redisCommand(m_redis, "PUBLISH ulltr:events:portfolio %s", json.c_str());
    if (r2) freeReplyObject(r2);
}

// ---------------------------------------------------------------------------
// publish_trade_event
// ---------------------------------------------------------------------------
void PositionManager::publish_trade_event(
    const std::string& event_type,
    const UnifiedPosition& pos,
    const std::string& details) const
{
    if (!m_redis) return;

    std::ostringstream audit;
    audit << std::fixed << std::setprecision(2);
    audit << "{\"position_id\":\"" << pos.position_id << "\","
          << "\"model_name\":\"" << pos.model_name << "\","
          << "\"symbol\":\"" << pos.symbol << "\","
          << "\"strike\":" << pos.strike << ","
          << "\"option_type\":\"" << option_type_to_string(pos.option_type) << "\","
          << "\"lots\":" << pos.lots << ","
          << "\"remaining_lots\":" << pos.remaining_lots << ","
          << "\"t1_hit\":" << (pos.t1_hit ? "true" : "false") << ","
          << "\"quantity\":" << pos.quantity << ","
          << "\"entry_opt\":" << pos.entry_option_price << ","
          << "\"exit_opt\":" << pos.exit_option_price << ","
          << "\"pnl\":" << pos.realized_pnl << ","
          << "\"entry_time\":\"" << pos.entry_time << "\","
          << "\"exit_time\":\"" << pos.exit_time << "\","
          << "\"reason\":\"" << details << "\"}";

    std::string payload = audit.str();
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    redisReply* r = (redisReply*)redisCommand(
        m_redis,
        "XADD ulltr:trades:audit * event %s data %s timestamp %lld",
        event_type.c_str(), payload.c_str(), (long long)now_ms);
    if (r) freeReplyObject(r);

    // Telegram sidecar stream
    std::ostringstream tg;
    tg << std::fixed << std::setprecision(2);
    tg << "{\"event\":\"" << event_type << "\","
       << "\"model\":\"" << pos.model_name << "\","
       << "\"symbol\":\"" << pos.symbol << "\","
       << "\"strike\":" << pos.strike << ","
       << "\"option_type\":\"" << option_type_to_string(pos.option_type) << "\","
       << "\"lots\":" << pos.lots << ","
       << "\"quantity\":" << pos.quantity << ","
       << "\"entry_opt\":" << pos.entry_option_price << ","
       << "\"entry_fut\":" << pos.entry_futures_price << ","
       << "\"exit_opt\":" << pos.exit_option_price << ","
       << "\"pnl\":" << pos.realized_pnl << ","
       << "\"reason\":\"" << details << "\","
       << "\"free_cash\":" << get_free_cash() << ","
       << "\"margin_locked\":" << get_locked_margin() << "}";

    redisReply* r2 = (redisReply*)redisCommand(m_redis, "PUBLISH ulltr:trades:stream %s", tg.str().c_str());
    if (r2) freeReplyObject(r2);
}
