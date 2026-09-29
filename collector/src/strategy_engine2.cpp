#include "strategy_engine2.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cmath>
#include <cstring>

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
StrategyEngine::StrategyEngine(double starting_capital, int lot_size, redisContext* redis)
    : m_pool(starting_capital, lot_size, redis)
    , m_resolver(redis)
    , m_redis(redis)
{
    std::memset(m_tpo_bracket_masks, 0, sizeof(m_tpo_bracket_masks));
}

// ===========================================================================
// CRITICAL: rehydrate_from_redis()
//
// This is the startup routine that makes restarts transparent.
// It reads every today's 1m candle stored in Redis between 09:15 and NOW,
// replays them through the historical-bar path (no live lookups, no entries),
// and rebuilds:
//   - m_ib_high / m_ib_low / m_tpo_bracket_masks for Initial Balance
//   - m_ib_locked + m_ib_vah / m_ib_val / m_ib_poc (Dalton)
//   - m_prev_poc, m_poc_initialized (POC V2 baseline)
//   - m_box_* (Spatial Box rolling state)
// ===========================================================================
void StrategyEngine::rehydrate_from_redis() {
    if (!m_redis) {
        std::cerr << "❌ [StrategyEngine] rehydrate_from_redis: no Redis context" << std::endl;
        return;
    }

    // Resolve spot key from Redis (defaults to NSE_INDEX|Nifty 50)
    redisReply* r_spot = (redisReply*)redisCommand(m_redis, "GET spot:NIFTY");
    if (r_spot && r_spot->type == REDIS_REPLY_STRING && r_spot->str) {
        m_spot_key = r_spot->str;
    } else {
        m_spot_key = "NSE_INDEX|Nifty 50";
    }
    if (r_spot) freeReplyObject(r_spot);

    // Ensure front expiry is resolved
    m_resolver.init_front_expiry();

    // Get today's date in IST
    std::string today = redis_utils::now_ist_date();

    // Fetch all today's 1m candle keys for the spot index
    std::string pattern = "md:candle:" + m_spot_key + ":1m:*";
    redisReply* r_keys = (redisReply*)redisCommand(m_redis, "KEYS %s", pattern.c_str());
    if (!r_keys) {
        std::cerr << "⚠️ [StrategyEngine] rehydrate_from_redis: KEYS returned null" << std::endl;
        return;
    }

    // Parse timestamps and sort chronologically
    struct HistBar {
        int64_t ts;
        std::string key;
    };
    std::vector<HistBar> bars;

    if (r_keys->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < r_keys->elements; ++i) {
            if (!r_keys->element[i]->str) continue;
            std::string k = r_keys->element[i]->str;
            // Extract timestamp from key suffix
            size_t last_colon = k.rfind(':');
            if (last_colon == std::string::npos) continue;
            try {
                int64_t ts = std::stoll(k.substr(last_colon + 1));
                // Convert to IST date for filtering
                std::string bar_date = redis_utils::format_ist_date(ts * 1000);
                if (bar_date != today) continue;

                std::string bar_time = redis_utils::format_ist_time(ts * 1000);
                if (bar_time < "09:15") continue;

                bars.push_back({ts, k});
            } catch (...) {
                continue;
            }
        }
    }
    freeReplyObject(r_keys);

    std::sort(bars.begin(), bars.end(), [](const HistBar& a, const HistBar& b) {
        return a.ts < b.ts;
    });

    std::cout << "🔄 [StrategyEngine] Rehydrating " << bars.size()
              << " historical 1m bars for " << today << "..." << std::endl;

    for (const auto& hb : bars) {
        // Fetch OHLCV from Redis hash
        redisReply* r = (redisReply*)redisCommand(m_redis, "HMGET %s open high low close volume",
                                                   hb.key.c_str());
        if (!r || r->type != REDIS_REPLY_ARRAY || r->elements < 5) {
            if (r) freeReplyObject(r);
            continue;
        }

        Candle1M bar;
        bar.minute_ts = hb.ts;
        bar.open   = r->element[0]->str ? std::atof(r->element[0]->str) : 0.0;
        bar.high   = r->element[1]->str ? std::atof(r->element[1]->str) : 0.0;
        bar.low    = r->element[2]->str ? std::atof(r->element[2]->str) : 0.0;
        bar.close  = r->element[3]->str ? std::atof(r->element[3]->str) : 0.0;
        bar.volume = r->element[4]->str ? std::strtoll(r->element[4]->str, nullptr, 10) : 0;
        freeReplyObject(r);

        if (bar.high <= 0.0 || bar.low <= 0.0) continue;

        std::string time_str = redis_utils::format_ist_time(hb.ts * 1000);
        replay_historical_bar(bar, time_str);
    }

    std::cout << "✅ [StrategyEngine] Rehydration complete."
              << " IB_locked=" << (m_ib_locked ? "YES" : "NO")
              << " VAH=" << m_ib_vah << " VAL=" << m_ib_val
              << " POC_init=" << (m_poc_initialized ? "YES" : "NO")
              << " prev_poc=" << m_prev_poc << std::endl;
}

// ---------------------------------------------------------------------------
// replay_historical_bar
// NO live Redis lookups (no option pricing), NO trade entries.
// Only state reconstruction.
// ---------------------------------------------------------------------------
void StrategyEngine::replay_historical_bar(const Candle1M& bar, const std::string& time_str) {
    // 1. TPO bitmasks
    int bracket_idx = get_tpo_bracket_index(time_str);
    update_tpo_profile(bar.high, bar.low, bracket_idx);

    // 2. Initial Balance tracking (09:15 to 10:15)
    if (!m_ib_locked) {
        if (time_str < "10:15") {
            if (bar.high > m_ib_high) m_ib_high = bar.high;
            if (bar.low  < m_ib_low)  m_ib_low  = bar.low;
        } else {
            lock_initial_balance_value_area();
        }
    }

    // 3. POC V2 baseline (track dpoc shifts — no signal generation on historical bars)
    // We store the most recent POC value seen during 09:15-10:30 as the baseline.
    // During rehydration we don't have microstructure, so we skip actual signal logic
    // but we mark poc_initialized=true with the last close as a rough proxy if needed.
    if (!m_poc_initialized && time_str >= "09:20") {
        m_prev_poc = bar.close; // Will be overwritten by real dpoc on first live bar
        m_poc_initialized = true;
    }

    // 4. Spatial Box rolling OHLC (no armed/triggered logic on history)
    double tp = (bar.high + bar.low + bar.close) / 3.0;
    m_cum_pv  += tp * bar.volume;
    m_cum_vol += bar.volume;

    if (!m_box_initialized) {
        m_box_initialized   = true;
        m_box_high          = bar.high;
        m_box_low           = bar.low;
        m_box_anchor_cvd    = 0.0;
        m_box_anchor_pv     = m_cum_pv;
        m_box_anchor_vol    = m_cum_vol;
    } else {
        double range = std::max(m_box_high, bar.high) - std::min(m_box_low, bar.low);
        if (range > 50.0) {
            // Box was broken — reset to current bar
            m_box_high = bar.high;
            m_box_low  = bar.low;
            m_box_anchor_pv  = m_cum_pv;
            m_box_anchor_vol = m_cum_vol;
            m_box_armed      = false;
            m_box_armed_dir  = 0;
        } else {
            m_box_high = std::max(m_box_high, bar.high);
            m_box_low  = std::min(m_box_low,  bar.low);
        }
    }

    // 5. Dalton probe tracking (was_below_val / was_above_vah)
    if (m_ib_locked && m_ib_vah > 0.0 && m_ib_val > 0.0) {
        if (bar.low  <= m_ib_val - 5.0) m_was_below_val = true;
        if (bar.high >= m_ib_vah + 5.0) m_was_above_vah = true;
    }
}

// ===========================================================================
// on_tick — hot path, sub-microsecond exit evaluation only
// ===========================================================================
void StrategyEngine::on_tick(
    double ltp, double bid, double ask,
    int64_t ts_ms, const MicrostructureMetrics& metrics)
{
    if (m_pool.active_positions.empty()) return;
    std::string time_str = redis_utils::format_ist_time(ts_ms);

    // Update current prices on active positions
    for (auto& p : m_pool.active_positions) {
        double live_bid = m_resolver.resolve_price(p.strike, p.option_type, false);
        if (live_bid > 0.0) p.update_market_price(live_bid, ltp);
    }

    check_active_exits(ltp, ltp, ltp, metrics.session_vwap, metrics.cvd_15m,
                       ts_ms, time_str, /*is_bar_close=*/false);
}

// ===========================================================================
// on_1m_bar — entry evaluation + IB / TPO state advancement
// ===========================================================================
void StrategyEngine::on_1m_bar(
    const std::string& /*symbol*/,
    const Candle1M& bar,
    const Candle1M& /*prev_bar*/,
    const MicrostructureMetrics& metrics)
{
    std::string time_str = redis_utils::format_ist_time(bar.minute_ts * 1000);

    // 1. Advance TPO profile
    int bracket_idx = get_tpo_bracket_index(time_str);
    update_tpo_profile(bar.high, bar.low, bracket_idx);

    // 2. Initial Balance advancement (if not yet locked by rehydrate)
    if (!m_ib_locked) {
        if (time_str < "10:15") {
            if (bar.high > m_ib_high) m_ib_high = bar.high;
            if (bar.low  < m_ib_low)  m_ib_low  = bar.low;
        } else {
            lock_initial_balance_value_area();
        }
    }

    // 3. Pending POC V2 signal: execute at open of this bar
    if (m_pending_poc_signal.has_signal) {
        execute_pending_poc_signal(bar, time_str);
    }

    // 4. Bar-close exits
    check_active_exits(bar.close, bar.high, bar.low,
                       metrics.session_vwap, metrics.cvd_15m,
                       bar.minute_ts * 1000, time_str, /*is_bar_close=*/true);

    // 5. Hard EOD cutoff
    if (time_str >= "15:00") return;

    // 6. Model evaluations
    if (enable_model_poc_v2 && time_str >= "09:20" && time_str <= "10:30")
        evaluate_model_poc_v2(bar, metrics, time_str);

    if (enable_model_spatial_box && time_str >= "09:20" && time_str <= "15:00")
        evaluate_model_spatial_box(bar, metrics, time_str);

    if (enable_model_dalton_va && time_str >= "10:15" && time_str <= "13:30")
        evaluate_model_dalton_va(bar, metrics, time_str);

    if (enable_model_tpo_poc && time_str >= "10:30" && time_str <= "14:45")
        evaluate_model_tpo_poc(bar, metrics, time_str);
}

// ===========================================================================
// check_active_exits
// ===========================================================================
void StrategyEngine::check_active_exits(
    double fut_p, double high, double low,
    double session_vwap, double cvd_15m,
    int64_t now_ms, const std::string& time_str,
    bool is_bar_close)
{
    if (m_pool.active_positions.empty()) return;

    // Refresh option prices for all active positions
    for (auto& p : m_pool.active_positions) {
        double live_bid = m_resolver.resolve_price(p.strike, p.option_type, false);
        if (live_bid > 0.0) p.update_market_price(live_bid, fut_p);
        if (is_bar_close) p.bars_held++;
    }

    // Evaluate exit conditions per position (iterate over a copy)
    std::vector<UnifiedPosition> copy = m_pool.active_positions;

    for (auto& pos : copy) {
        bool exit_triggered = false;
        std::string exit_reason;
        double exit_fut = fut_p;
        // Current live option price; do NOT fall back to entry price for exit math.
        double exit_opt = (pos.current_option_price > 0.0) ? pos.current_option_price : 0.0;

        // ── Hard EOD ────────────────────────────────────────────────────────
        if (time_str >= "15:20") {
            if (exit_opt <= 0.0) {
                // DATA_STALE at EOD: still close at last known price
                exit_opt = pos.current_option_price > 0.0 ? pos.current_option_price : pos.entry_option_price;
            }
            exit_triggered = true;
            exit_reason = "EOD Square-Off (15:20 IST)";
        }

        // ── Model POC V2 ────────────────────────────────────────────────────
        else if (pos.model_name == "Model POC V2") {
            bool just_banked_t1 = false;
            if (!pos.t1_hit) {
                double mfe = (pos.option_type == OptionType::CE)
                             ? (high - pos.entry_futures_price)
                             : (pos.entry_futures_price - low);
                if (mfe >= 30.0) {
                    double live_bid = m_resolver.resolve_price(pos.strike, pos.option_type, false);
                    if (live_bid > 0.0) {
                        m_pool.bank_lot1(pos.position_id, live_bid,
                                         (pos.option_type == OptionType::CE)
                                             ? pos.entry_futures_price + 30.0
                                             : pos.entry_futures_price - 30.0,
                                         time_str);
                        m_pool.publish_trade_event("T1_BANKED", pos, "Model POC V2 T1 Bank (+30pt FUT)");
                        just_banked_t1 = true;
                    }
                }
            }
            if (!pos.t1_hit && !just_banked_t1) {
                bool hit_sl = (pos.option_type == OptionType::CE && low  <= pos.entry_futures_price - 20.0)
                           || (pos.option_type == OptionType::PE && high >= pos.entry_futures_price + 20.0);
                if (hit_sl && exit_opt > 0.0) {
                    exit_triggered = true;
                    exit_reason = "Initial SL (-20pt FUT)";
                    exit_fut = (pos.option_type == OptionType::CE)
                               ? pos.entry_futures_price - 20.0
                               : pos.entry_futures_price + 20.0;
                }
            } else if (pos.t1_hit && !just_banked_t1) {
                double active_sl = pos.lot2_sl_futures_price;
                if (pos.option_type == OptionType::CE) {
                    if (session_vwap > 0.0)
                        active_sl = std::max(active_sl, session_vwap - 5.0);
                    if (low <= active_sl && exit_opt > 0.0) {
                        exit_triggered = true;
                        exit_reason = "VWAP Trail Exit";
                        exit_fut = active_sl;
                    }
                } else {
                    if (session_vwap > 0.0)
                        active_sl = std::min(active_sl, session_vwap + 5.0);
                    if (high >= active_sl && exit_opt > 0.0) {
                        exit_triggered = true;
                        exit_reason = "VWAP Trail Exit";
                        exit_fut = active_sl;
                    }
                }
                if (!exit_triggered && time_str >= "15:20") {
                    exit_triggered = true;
                    exit_reason = "EOD Squareoff";
                    if (exit_opt <= 0.0) exit_opt = pos.entry_option_price; // last resort
                }
            }
        }

        // ── Spatial Box ─────────────────────────────────────────────────────
        else if (pos.model_name == "Model Spatial Box") {
            double opt_bid = m_resolver.resolve_price(pos.strike, pos.option_type, false);
            if (opt_bid <= 0.0) opt_bid = pos.current_option_price;
            if (opt_bid > 0.0) {
                if (opt_bid >= pos.target_opt_price) {
                    exit_triggered = true; exit_reason = "Target Reached (+45pt)"; exit_opt = pos.target_opt_price;
                } else if (opt_bid <= pos.sl_opt_price) {
                    exit_triggered = true; exit_reason = "Stop Loss Hit (-15pt)"; exit_opt = pos.sl_opt_price;
                } else if (pos.bars_held >= 45) {
                    exit_triggered = true; exit_reason = "Time Exit (45m)"; exit_opt = opt_bid;
                }
            }
        }

        // ── Causal Dalton VA ─────────────────────────────────────────────────
        else if (pos.model_name == "Causal Dalton VA") {
            double opt_bid = m_resolver.resolve_price(pos.strike, pos.option_type, false);
            if (opt_bid > 0.0) exit_opt = opt_bid;
            // DATA_STALE: skip this exit evaluation if exit_opt is unknown.
            if (exit_opt <= 0.0) goto skip_exit;

            if (pos.option_type == OptionType::CE) {
                if (fut_p >= pos.tpo_target_futures)        { exit_triggered = true; exit_reason = "Target Reached (VAH)"; }
                else if (fut_p <= pos.sl_futures_price)     { exit_triggered = true; exit_reason = "Stop Loss Hit (VAL - 15pt)"; }
                else if (time_str >= "13:30")                { exit_triggered = true; exit_reason = "Window Close (13:30 IST)"; }
            } else {
                if (fut_p <= pos.tpo_target_futures)        { exit_triggered = true; exit_reason = "Target Reached (VAL)"; }
                else if (fut_p >= pos.sl_futures_price)     { exit_triggered = true; exit_reason = "Stop Loss Hit (VAH + 15pt)"; }
                else if (time_str >= "13:30")                { exit_triggered = true; exit_reason = "Window Close (13:30 IST)"; }
            }
        }

        skip_exit:
        if (exit_triggered && exit_opt > 0.0) {
            bool closed = m_pool.close_position(pos.position_id, exit_opt, exit_fut,
                                                 exit_reason, time_str);
            if (closed) {
                if (pos.model_name == "Model Spatial Box")
                    m_box_last_exit_minute = (now_ms > 0) ? (now_ms / 60000) : 0;

                // Find snapshot from closed list
                for (const auto& cp : m_pool.closed_positions) {
                    if (cp.position_id == pos.position_id) {
                        m_pool.publish_trade_event("POSITION_CLOSED", cp, exit_reason);
                        if (m_trade_callback) m_trade_callback("POSITION_CLOSED", cp, exit_reason);
                        break;
                    }
                }
                m_pool.publish_portfolio_state(time_str);
            }
        }
    }
}

// ===========================================================================
// TPO Profile
// ===========================================================================
int StrategyEngine::get_tpo_bracket_index(const std::string& time_str) const {
    if (time_str.size() < 5) return 0;
    int hour = 0, minute = 0;
    try {
        hour   = std::stoi(time_str.substr(0, 2));
        minute = std::stoi(time_str.substr(3, 2));
    } catch (...) { return 0; }
    int mins = (hour - 9) * 60 + minute - 15;
    if (mins < 0) return 0;
    return std::min(mins / 30, 12);
}

void StrategyEngine::update_tpo_profile(double high, double low, int bracket_idx) {
    if (bracket_idx < 0 || bracket_idx > 12 || high <= 0.0 || low <= 0.0) return;

    int min_bin = std::max(0, std::min((int)std::floor((low  - TPO_BASE_PRICE) / TPO_BIN_SIZE), (int)TPO_NUM_BINS - 1));
    int max_bin = std::max(0, std::min((int)std::floor((high - TPO_BASE_PRICE) / TPO_BIN_SIZE), (int)TPO_NUM_BINS - 1));

    uint16_t bracket_bit = static_cast<uint16_t>(1 << bracket_idx);
    for (int b = min_bin; b <= max_bin; ++b) m_tpo_bracket_masks[b] |= bracket_bit;

    int max_count = 0; double best_poc = 0.0;
    for (int b = 0; b < (int)TPO_NUM_BINS; ++b) {
        if (m_tpo_bracket_masks[b] > 0) {
            int cnt = __builtin_popcount(m_tpo_bracket_masks[b]);
            if (cnt > max_count) { max_count = cnt; best_poc = TPO_BASE_PRICE + (b + 0.5) * TPO_BIN_SIZE; }
        }
    }
    if (max_count > 0) { m_current_tpo_max_count = max_count; m_current_tpo_poc = best_poc; }
}

// ===========================================================================
// Causal Dalton IB Lock
// ===========================================================================
void StrategyEngine::lock_initial_balance_value_area() {
    if (m_ib_locked) return;
    m_ib_locked = true;

    int tot_ib_tpos = 0, max_ib_tpos = 0, ib_poc_bin = -1;
    std::vector<int> active_bins;

    for (size_t b = 0; b < TPO_NUM_BINS; ++b) {
        int count = __builtin_popcount(m_tpo_bracket_masks[b] & 0x03);
        if (count > 0) {
            tot_ib_tpos += count;
            active_bins.push_back((int)b);
            if (count > max_ib_tpos) { max_ib_tpos = count; ib_poc_bin = (int)b; }
        }
    }

    if (tot_ib_tpos > 0 && ib_poc_bin >= 0) {
        m_ib_poc = TPO_BASE_PRICE + (ib_poc_bin + 0.5) * TPO_BIN_SIZE;
        double target_tpos = tot_ib_tpos * 0.70;
        int cur_tpos = __builtin_popcount(m_tpo_bracket_masks[ib_poc_bin] & 0x03);
        int min_va_bin = ib_poc_bin, max_va_bin = ib_poc_bin;

        auto it = std::find(active_bins.begin(), active_bins.end(), ib_poc_bin);
        int poc_idx = (int)std::distance(active_bins.begin(), it);
        int u = poc_idx + 1, d = poc_idx - 1;

        while (cur_tpos < target_tpos && (u < (int)active_bins.size() || d >= 0)) {
            int u_val = (u < (int)active_bins.size()) ? __builtin_popcount(m_tpo_bracket_masks[active_bins[u]] & 0x03) : 0;
            int d_val = (d >= 0) ? __builtin_popcount(m_tpo_bracket_masks[active_bins[d]] & 0x03) : 0;
            if (u_val >= d_val && u_val > 0) { cur_tpos += u_val; max_va_bin = std::max(max_va_bin, active_bins[u]); u++; }
            else if (d_val > 0)               { cur_tpos += d_val; min_va_bin = std::min(min_va_bin, active_bins[d]); d--; }
            else break;
        }
        m_ib_vah = TPO_BASE_PRICE + (max_va_bin + 1.0) * TPO_BIN_SIZE;
        m_ib_val = TPO_BASE_PRICE + min_va_bin         * TPO_BIN_SIZE;
    } else {
        m_ib_poc = (m_ib_high > 0.0 && m_ib_low < 1e8) ? (m_ib_high + m_ib_low) / 2.0 : 0.0;
        m_ib_vah = m_ib_high;
        m_ib_val = (m_ib_low < 1e8) ? m_ib_low : 0.0;
    }

    std::cout << "🔒 [Dalton VA] IB Locked → High:" << m_ib_high
              << " Low:" << m_ib_low
              << " VAH:" << m_ib_vah
              << " VAL:" << m_ib_val
              << " POC:" << m_ib_poc << std::endl;
}

// ===========================================================================
// PCR Calculation
// ===========================================================================
double StrategyEngine::calculate_chain_pcr() {
    if (!m_redis) return 1.0;

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (m_cached_pcr_time_ms > 0 && (now_ms - m_cached_pcr_time_ms) < 15000)
        return m_cached_pcr;

    const std::string& chain_key = m_resolver.front_expiry();
    if (chain_key.empty()) return 1.0;

    redisReply* r_all = (redisReply*)redisCommand(m_redis, "HGETALL %s", chain_key.c_str());
    if (!r_all || r_all->type != REDIS_REPLY_ARRAY) { if (r_all) freeReplyObject(r_all); return 1.0; }

    int64_t tot_pe = 0, tot_ce = 0;
    std::vector<std::pair<std::string, bool>> tokens;

    for (size_t i = 0; i + 1 < r_all->elements; i += 2) {
        if (!r_all->element[i]->str || !r_all->element[i + 1]->str) continue;
        std::string field = r_all->element[i]->str;
        std::string token = r_all->element[i + 1]->str;
        bool is_pe = (field.find(":PE") != std::string::npos);
        bool is_ce = (field.find(":CE") != std::string::npos);
        if (is_pe || is_ce) {
            tokens.push_back({token, is_pe});
            redisAppendCommand(m_redis, "HGET md:quote:%s oi", token.c_str());
        }
    }
    freeReplyObject(r_all);

    for (const auto& item : tokens) {
        redisReply* r_oi = nullptr;
        if (redisGetReply(m_redis, (void**)&r_oi) == REDIS_OK && r_oi) {
            if (r_oi->str) {
                int64_t oi = std::strtoll(r_oi->str, nullptr, 10);
                if (oi > 0) { if (item.second) tot_pe += oi; else tot_ce += oi; }
            }
            freeReplyObject(r_oi);
        }
    }

    m_cached_pcr = (tot_ce > 0) ? (double)tot_pe / (double)tot_ce : 1.0;
    m_cached_pcr_time_ms = now_ms;
    return m_cached_pcr;
}

// ===========================================================================
// Model POC V2
// ===========================================================================
void StrategyEngine::evaluate_model_poc_v2(
    const Candle1M& bar, const MicrostructureMetrics& m, const std::string& time_str)
{
    if (bar.minute_ts == m_last_evaluated_minute) return;
    m_last_evaluated_minute = bar.minute_ts;

    double curr_poc = m.dpoc;
    if (curr_poc <= 0.0) return;

    if (!m_poc_initialized) { m_prev_poc = curr_poc; m_poc_initialized = true; return; }
    if (curr_poc == m_prev_poc) return;

    double poc_shift = curr_poc - m_prev_poc;
    double old_poc   = m_prev_poc;
    m_prev_poc       = curr_poc;

    std::string oi_regime;
    if      (m.delta_price_15m >= 0 && m.delta_oi_15m >= 0) oi_regime = "Long Buildup";
    else if (m.delta_price_15m >= 0 && m.delta_oi_15m < 0)  oi_regime = "Short Covering";
    else if (m.delta_price_15m < 0  && m.delta_oi_15m >= 0) oi_regime = "Short Buildup";
    else                                                      oi_regime = "Long Unwinding";

    OptionType chosen = OptionType::CE;
    bool has_signal   = false;
    std::string regime;

    if (poc_shift > 0.0) {
        if (m.cvd_15m < 0.0 || oi_regime == "Short Buildup" || oi_regime == "Long Unwinding") {
            chosen = OptionType::PE; has_signal = true; regime = "Bull Trap Fade (UP + CVD Neg / " + oi_regime + ")";
        } else if (m.cvd_15m > 0.0 && oi_regime == "Long Buildup") {
            chosen = OptionType::CE; has_signal = true; regime = "True Bull Breakout (UP + CVD Pos + Long Buildup)";
        }
    } else if (poc_shift < 0.0) {
        if (m.cvd_15m > 0.0 || oi_regime == "Long Buildup") {
            chosen = OptionType::CE; has_signal = true; regime = "Absorption Bottom (DOWN + CVD Pos / " + oi_regime + ")";
        } else if (m.cvd_15m < 0.0 && oi_regime == "Short Buildup") {
            chosen = OptionType::PE; has_signal = true; regime = "True Bear Breakdown (DOWN + CVD Neg + Short Buildup)";
        }
    }

    std::cout << "🔍 [POC V2] Shift @ " << time_str
              << " | " << poc_shift << " pts (New:" << curr_poc << " Old:" << old_poc << ")"
              << " | CVD:" << m.cvd_15m << " | " << oi_regime
              << " | Signal: " << (has_signal ? regime : "None") << std::endl;

    if (!has_signal) return;

    m_pending_poc_signal.has_signal   = true;
    m_pending_poc_signal.chosen_type  = chosen;
    m_pending_poc_signal.regime       = regime;
}

void StrategyEngine::execute_pending_poc_signal(const Candle1M& bar, const std::string& time_str) {
    if (!m_pending_poc_signal.has_signal) return;
    m_pending_poc_signal.has_signal = false;

    OptionType chosen = m_pending_poc_signal.chosen_type;
    std::string regime = m_pending_poc_signal.regime;

    TargetStrikeResult target = m_resolver.resolve_target_strike(bar.open, chosen, 155.0);
    if (target.ask <= 0.0) {
        std::cout << "⚠️ [POC V2] DATA_STALE: no live ask for strike resolution @ " << time_str << std::endl;
        return;
    }

    TradeSignal sig;
    sig.model_name  = "Model POC V2";
    sig.option_type = chosen;
    sig.strike      = target.strike;
    sig.symbol      = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen);
    sig.option_ask  = target.ask;
    sig.fut_price   = bar.open;
    sig.sl_fut      = (chosen == OptionType::CE) ? bar.open - 20.0 : bar.open + 20.0;
    sig.tp_fut      = (chosen == OptionType::CE) ? bar.open + 30.0 : bar.open - 30.0;
    sig.timestamp_str = time_str;

    UnifiedPosition out_pos; std::string reason;
    bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);
    if (allocated) {
        m_pool.publish_trade_event("POSITION_OPENED", out_pos, regime + " (" + time_str + ")");
        if (m_trade_callback) m_trade_callback("POSITION_OPENED", out_pos, regime);
        m_pool.publish_portfolio_state(time_str);
    } else {
        std::cout << "⚠️ [POC V2] Rejected: " << reason << std::endl;
    }
}

// ===========================================================================
// Spatial Box
// ===========================================================================
double StrategyEngine::get_box_avwap(double current_tp) const {
    double vol_delta = m_cum_vol - m_box_anchor_vol;
    double pv_delta  = m_cum_pv  - m_box_anchor_pv;
    return (vol_delta > 0.0) ? pv_delta / vol_delta : current_tp;
}

void StrategyEngine::evaluate_model_spatial_box(
    const Candle1M& bar, const MicrostructureMetrics& m, const std::string& time_str)
{
    double tp = (bar.high + bar.low + bar.close) / 3.0;
    m_cum_pv  += tp * bar.volume;
    m_cum_vol += bar.volume;

    if (!m_box_initialized) {
        m_box_initialized = true;
        m_box_high = bar.high; m_box_low = bar.low;
        m_box_anchor_cvd = m.cum_cvd;
        m_box_anchor_pv = m_cum_pv; m_box_anchor_vol = m_cum_vol;
        return;
    }

    if (m_pool.has_active_position_for_model("Model Spatial Box")) return;

    m_box_high = std::max(m_box_high, bar.high);
    m_box_low  = std::min(m_box_low,  bar.low);

    double box_avwap = get_box_avwap(tp);
    bool should_reset = ((m_box_high - m_box_low) > 50.0);
    if (!should_reset && m_box_armed) {
        if (m_box_armed_dir == 1  && (bar.close - box_avwap) > 15.0) should_reset = true;
        if (m_box_armed_dir == -1 && (box_avwap - bar.close) > 15.0) should_reset = true;
    }

    if (should_reset) {
        m_box_high = bar.high; m_box_low = bar.low;
        m_box_anchor_cvd = m.cum_cvd; m_box_anchor_pv = m_cum_pv; m_box_anchor_vol = m_cum_vol;
        m_box_armed = false; m_box_armed_dir = 0;
    } else {
        double dcvd = m.cum_cvd - m_box_anchor_cvd;
        if      (dcvd >=  55000.0 && bar.close <= box_avwap + 15.0) { m_box_armed = true; m_box_armed_dir =  1; }
        else if (dcvd <= -55000.0 && bar.close >= box_avwap - 15.0) { m_box_armed = true; m_box_armed_dir = -1; }
    }

    int64_t cur_minute = bar.minute_ts / 60;
    if (!m_box_armed || (cur_minute - m_box_last_exit_minute < 15)) return;

    OptionType chosen = OptionType::CE;
    bool triggered = false; std::string regime;
    if      (m_box_armed_dir ==  1 && bar.close >= m_box_high) { chosen = OptionType::CE; triggered = true; regime = "50-pt Box Breakout + AVWAP Arm Gate (CE)"; }
    else if (m_box_armed_dir == -1 && bar.close <= m_box_low)  { chosen = OptionType::PE; triggered = true; regime = "50-pt Box Breakdown + AVWAP Arm Gate (PE)"; }

    if (!triggered) return;

    std::string exec_time = redis_utils::format_ist_time((bar.minute_ts + 60) * 1000);
    TargetStrikeResult target = m_resolver.resolve_target_strike(bar.close, chosen, 155.0);
    if (target.ask <= 0.0) {
        std::cout << "⚠️ [Spatial Box] DATA_STALE: no live ask @ " << exec_time << std::endl;
        return;
    }

    TradeSignal sig;
    sig.model_name = "Model Spatial Box"; sig.option_type = chosen;
    sig.strike = target.strike;
    sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen);
    sig.option_ask = target.ask; sig.fut_price = bar.close;
    sig.sl_fut = (chosen == OptionType::CE) ? bar.close - 20.0 : bar.close + 20.0;
    sig.tp_fut = (chosen == OptionType::CE) ? bar.close + 45.0 : bar.close - 45.0;
    sig.timestamp_str = exec_time;

    UnifiedPosition out_pos; std::string reason;
    bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);
    if (allocated) {
        for (auto& p : m_pool.active_positions) {
            if (p.position_id == out_pos.position_id) {
                p.target_opt_price = target.ask + 45.0;
                p.sl_opt_price     = target.ask - 15.0;
                p.active_box_avwap = box_avwap;
                p.bars_held        = 0;
                break;
            }
        }
        m_box_armed = false; m_box_armed_dir = 0;
        m_box_high = bar.high; m_box_low = bar.low;
        m_box_anchor_cvd = m.cum_cvd; m_box_anchor_pv = m_cum_pv; m_box_anchor_vol = m_cum_vol;

        m_pool.publish_trade_event("POSITION_OPENED", out_pos, regime + " (" + exec_time + ")");
        if (m_trade_callback) m_trade_callback("POSITION_OPENED", out_pos, regime);
        m_pool.publish_portfolio_state(exec_time);
    } else {
        std::cout << "⚠️ [Spatial Box] Rejected: " << reason << std::endl;
    }
}

// ===========================================================================
// Causal Dalton VA
// ===========================================================================
void StrategyEngine::evaluate_model_dalton_va(
    const Candle1M& bar, const MicrostructureMetrics& m, const std::string& time_str)
{
    if (!m_ib_locked) { if (time_str >= "10:15") lock_initial_balance_value_area(); else return; }
    if (time_str < "10:15" || time_str > "13:30") return;
    if (m_pool.has_active_position_for_model("Causal Dalton VA")) return;

    if (bar.low  <= m_ib_val - 5.0) m_was_below_val = true;
    if (bar.high >= m_ib_vah + 5.0) m_was_above_vah = true;

    OptionType chosen = OptionType::CE;
    bool triggered = false; std::string regime;
    double target_fut = 0.0, sl_fut = 0.0;

    if (m_was_below_val && bar.close >= m_ib_val && m.cvd_15m > 0.0) {
        double pcr = calculate_chain_pcr();
        if (pcr < 0.70) {
            std::cout << "⚠️ [Dalton VA] REJECTED LONG CE: PCR " << pcr << " < 0.70 @ " << time_str << std::endl;
            return;
        }
        chosen = OptionType::CE; triggered = true;
        target_fut = m_ib_vah; sl_fut = m_ib_val - 15.0;
        regime = "Dalton 80% Bullish VA Traverse (Buy CE)";
    } else if (m_was_above_vah && bar.close <= m_ib_vah && m.cvd_15m < 0.0) {
        double pcr = calculate_chain_pcr();
        if (pcr > 1.35) {
            std::cout << "⚠️ [Dalton VA] REJECTED SHORT PE: PCR " << pcr << " > 1.35 @ " << time_str << std::endl;
            return;
        }
        chosen = OptionType::PE; triggered = true;
        target_fut = m_ib_val; sl_fut = m_ib_vah + 15.0;
        regime = "Dalton 80% Bearish VA Traverse (Buy PE)";
    }

    if (!triggered) return;

    std::string exec_time = redis_utils::format_ist_time((bar.minute_ts + 60) * 1000);
    TargetStrikeResult target = m_resolver.resolve_target_strike(bar.close, chosen, 155.0);
    if (target.ask <= 0.0) {
        std::cout << "⚠️ [Dalton VA] DATA_STALE: no live ask @ " << exec_time << std::endl;
        return;
    }

    TradeSignal sig;
    sig.model_name = "Causal Dalton VA"; sig.option_type = chosen;
    sig.strike = target.strike;
    sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen);
    sig.option_ask = target.ask; sig.fut_price = bar.close;
    sig.sl_fut = sl_fut; sig.tp_fut = target_fut;
    sig.timestamp_str = exec_time;

    UnifiedPosition out_pos; std::string reason;
    bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);
    if (allocated) {
        for (auto& p : m_pool.active_positions) {
            if (p.position_id == out_pos.position_id) {
                p.dalton_ib_vah = m_ib_vah; p.dalton_ib_val = m_ib_val;
                p.dalton_ib_poc = m_ib_poc; p.dalton_pcr = m_cached_pcr;
                p.tpo_target_futures = target_fut; p.sl_futures_price = sl_fut;
                p.bars_held = 0; break;
            }
        }
        if (chosen == OptionType::CE) m_was_below_val = false;
        else                           m_was_above_vah = false;
        m_dalton_trades_today++;

        m_pool.publish_trade_event("POSITION_OPENED", out_pos, regime + " (" + exec_time + ")");
        if (m_trade_callback) m_trade_callback("POSITION_OPENED", out_pos, regime);
        m_pool.publish_portfolio_state(exec_time);
        std::cout << "🚀 [Dalton VA] " << out_pos.position_id << " | " << regime
                  << " @ " << exec_time << " | Target:" << target_fut << " SL:" << sl_fut
                  << " PCR:" << m_cached_pcr << std::endl;
    } else {
        std::cout << "⚠️ [Dalton VA] Rejected: " << reason << std::endl;
    }
}

// ===========================================================================
// TPO POC Reversion (legacy, disabled by default)
// ===========================================================================
void StrategyEngine::evaluate_model_tpo_poc(
    const Candle1M& bar, const MicrostructureMetrics& m, const std::string& time_str)
{
    if (!enable_model_tpo_poc) return;
    if (m_pool.has_active_position_for_model("TPO POC Reversion")) return;

    int tpo_count = 0;
    for (const auto& p : m_pool.active_positions)  if (p.model_name == "TPO POC Reversion") tpo_count++;
    for (const auto& p : m_pool.closed_positions)  if (p.model_name == "TPO POC Reversion") tpo_count++;
    if (tpo_count >= 1 || m_current_tpo_poc <= 0.0) return;

    double dist = bar.close - m_current_tpo_poc;
    OptionType chosen = OptionType::CE; bool triggered = false; std::string regime;

    if (dist <= -25.0 && (bar.close > bar.open || m.cvd_15m > 0.0)) { chosen = OptionType::CE; triggered = true; regime = "TPO POC Oversold Fade (CE)"; }
    else if (dist >= 25.0 && (bar.close < bar.open || m.cvd_15m < 0.0)) { chosen = OptionType::PE; triggered = true; regime = "TPO POC Overbought Fade (PE)"; }

    if (!triggered) return;

    TargetStrikeResult target = m_resolver.resolve_target_strike(bar.close, chosen, 155.0);
    if (target.ask <= 0.0) return; // DATA_STALE

    TradeSignal sig;
    sig.model_name = "TPO POC Reversion"; sig.option_type = chosen;
    sig.strike = target.strike;
    sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen);
    sig.option_ask = target.ask; sig.fut_price = bar.close;
    sig.sl_fut = (chosen == OptionType::CE) ? bar.close - 20.0 : bar.close + 20.0;
    sig.tp_fut = m_current_tpo_poc; sig.timestamp_str = time_str;

    UnifiedPosition out_pos; std::string reason;
    if (m_pool.evaluate_and_allocate(sig, out_pos, reason)) {
        for (auto& p : m_pool.active_positions) {
            if (p.position_id == out_pos.position_id) {
                p.tpo_poc_at_entry = m_current_tpo_poc;
                p.tpo_target_futures = m_current_tpo_poc;
                p.sl_futures_price = sig.sl_fut; p.bars_held = 0; break;
            }
        }
        m_pool.publish_trade_event("POSITION_OPENED", out_pos, regime + " (" + time_str + ")");
        if (m_trade_callback) m_trade_callback("POSITION_OPENED", out_pos, regime);
        m_pool.publish_portfolio_state(time_str);
    }
}
