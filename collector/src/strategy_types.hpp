#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <cmath>
#include <algorithm>

enum class OptionType { CE, PE };

inline std::string option_type_to_string(OptionType t) {
    return (t == OptionType::CE) ? "CE" : "PE";
}

inline OptionType string_to_option_type(const std::string& s) {
    return (s == "CE") ? OptionType::CE : OptionType::PE;
}

struct UnifiedPosition {
    std::string position_id;
    std::string model_name;
    std::string symbol;
    int64_t strike = 0;
    OptionType option_type = OptionType::CE;
    int lots = 0;
    int64_t quantity = 0;
    std::string entry_time;
    int64_t entry_time_ms = 0;
    double entry_option_price = 0.0;
    double entry_futures_price = 0.0;
    double current_option_price = 0.0;
    double current_futures_price = 0.0;
    double margin_locked = 0.0;
    double sl_futures_price = 0.0;
    double tp_futures_price = 0.0;
    bool is_rolled = false;
    bool is_active = true;
    std::string exit_time;
    double exit_option_price = 0.0;
    double exit_futures_price = 0.0;
    std::string exit_reason;
    double realized_pnl = 0.0;

    // Two-Tier ModelPOCV2 Execution State
    bool t1_hit = false;
    std::string lot1_exit_time;
    double lot1_exit_opt = 0.0;
    double lot1_exit_fut = 0.0;
    double lot1_realized_pnl = 0.0;
    double lot2_sl_futures_price = 0.0;
    int remaining_lots = 0;

    // Spatial Box Tracking State
    int bars_held = 0;
    double active_box_avwap = 0.0;
    double target_opt_price = 0.0;
    double sl_opt_price = 0.0;

    // TPO POC Reversion Tracking State
    double tpo_poc_at_entry = 0.0;
    double tpo_target_futures = 0.0;

    // Causal Dalton Value Area Tracking State
    double dalton_ib_vah = 0.0;
    double dalton_ib_val = 0.0;
    double dalton_ib_poc = 0.0;
    double dalton_pcr = 0.0;

    double get_unrealized_pnl() const {
        if (!is_active) return 0.0;
        int active_l = (t1_hit && remaining_lots > 0) ? remaining_lots : lots;
        return (current_option_price - entry_option_price) * (active_l * 65);
    }

    double get_points() const {
        if (entry_option_price <= 0.0) return 0.0;
        double eff_exit = (exit_option_price > 0.0) ? exit_option_price : current_option_price;
        return eff_exit - entry_option_price - 1.0; // 1.0 pt friction
    }

    void update_market_price(double opt_p, double fut_p) {
        if (opt_p > 0.0) current_option_price = opt_p;
        if (fut_p > 0.0) current_futures_price = fut_p;
    }
};

struct TradeSignal {
    std::string model_name;
    OptionType option_type = OptionType::CE;
    int64_t strike = 0;
    std::string symbol;
    double option_ask = 0.0;
    double fut_price = 0.0;
    double sl_fut = 0.0;
    double tp_fut = 0.0;
    std::string timestamp_str;
};

struct RejectedSignal {
    std::string timestamp;
    std::string model_name;
    int64_t strike = 0;
    OptionType option_type = OptionType::CE;
    double option_ask = 0.0;
    std::string reason;
};
