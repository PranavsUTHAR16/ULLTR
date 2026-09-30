#include "strategy_engine.hpp"
#include <sstream>
#include <iomanip>
#include <iostream>
#include <ctime>
#include <cmath>
#include <algorithm>
#include <chrono>

StrategyEngine::StrategyEngine(FIFOPool& pool)
    : m_pool(pool) {
}

static inline std::string format_ist_time(int64_t epoch_ms) {
    int64_t ist_sec = (epoch_ms / 1000) + 19800; // UTC + 5:30
    int sec_of_day = ist_sec % 86400;
    if (sec_of_day < 0) sec_of_day += 86400;
    int hour = sec_of_day / 3600;
    int min = (sec_of_day % 3600) / 60;
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%02d:%02d", hour, min);
    return std::string(buf);
}

static inline std::string format_ist_date(int64_t epoch_ms) {
    std::time_t raw = (epoch_ms / 1000) + 19800; // UTC + 5:30
    std::tm tm_buf;
#if defined(_WIN32)
    gmtime_s(&tm_buf, &raw);
#else
    gmtime_r(&raw, &tm_buf);
#endif
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday);
    return std::string(buf);
}

// ===========================================================================
// STARTUP REHYDRATION — rebuilds all in-memory state from today's Redis 1m candles.
// Must be called BEFORE processing any live ticks or 1m bars.
// This eliminates the "corrupted Initial Balance on restart" bug.
// ===========================================================================
void StrategyEngine::rehydrate_from_redis(redisContext* redis) {
    if (!redis) {
        std::cerr << "❌ [Rehydrate] No Redis context — skipping rehydration" << std::endl;
        return;
    }

    // Resolve spot key (e.g. "NSE_INDEX|Nifty 50")
    std::string spot_key = "NSE_INDEX|Nifty 50";
    redisReply* r_spot = (redisReply*)redisCommand(redis, "GET spot:NIFTY");
    if (r_spot) {
        if (r_spot->type == REDIS_REPLY_STRING && r_spot->str)
            spot_key = r_spot->str;
        freeReplyObject(r_spot);
    }

    // Ensure front expiry is populated
    if (m_cached_front_expiry.empty()) init_front_expiry(redis);

    // Today's IST date for filtering
    std::string today_date = format_ist_date(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    // Fetch all 1m candle keys for the spot index
    std::string pattern = "md:candle:" + spot_key + ":1m:*";
    redisReply* r_keys = (redisReply*)redisCommand(redis, "KEYS %s", pattern.c_str());
    if (!r_keys) {
        std::cerr << "⚠️ [Rehydrate] KEYS returned null for pattern: " << pattern << std::endl;
        return;
    }

    struct HistBar { int64_t ts; std::string key; };
    std::vector<HistBar> bars;

    if (r_keys->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < r_keys->elements; ++i) {
            if (!r_keys->element[i]->str) continue;
            std::string k = r_keys->element[i]->str;
            size_t last_colon = k.rfind(':');
            if (last_colon == std::string::npos) continue;
            try {
                int64_t ts = std::stoll(k.substr(last_colon + 1));
                // Filter to today's session (09:15 IST and onwards)
                std::string bar_date = format_ist_date(ts * 1000);
                if (bar_date != today_date) continue;
                std::string bar_time = format_ist_time(ts * 1000);
                if (bar_time < "09:15") continue;
                bars.push_back({ts, k});
            } catch (...) { continue; }
        }
    }
    freeReplyObject(r_keys);

    std::sort(bars.begin(), bars.end(), [](const HistBar& a, const HistBar& b) {
        return a.ts < b.ts;
    });

    std::cout << "🔄 [Rehydrate] Replaying " << bars.size()
              << " historical 1m bars for " << today_date << "..." << std::endl;

    for (const auto& hb : bars) {
        redisReply* r = (redisReply*)redisCommand(redis, "HMGET %s open high low close volume",
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

        std::string time_str = format_ist_time(hb.ts * 1000);

        // ── 1. TPO profile ──────────────────────────────────────────────
        int bracket_idx = get_tpo_bracket_index(time_str);
        update_tpo_profile(bar.high, bar.low, bracket_idx);

        // ── 2. Initial Balance accumulation (09:15 to 10:15) ────────────
        if (!m_ib_locked) {
            if (time_str < "10:15") {
                if (bar.high > m_ib_high) m_ib_high = bar.high;
                if (bar.low  < m_ib_low)  m_ib_low  = bar.low;
            } else {
                lock_initial_balance_value_area();
            }
        }

        // ── 3. POC V2 baseline: track last close as proxy for dpoc baseline
        //       (real dpoc comes from microstructure_engine on live bars)
        if (time_str >= "09:20" && !m_poc_initialized) {
            m_prev_poc = bar.close;
            m_poc_initialized = true;
        }

        // ── 4. Spatial Box rolling OHLC (no armed/triggered logic on history)
        double tp = (bar.high + bar.low + bar.close) / 3.0;
        m_cum_pv  += tp * static_cast<double>(bar.volume);
        m_cum_vol += static_cast<double>(bar.volume);

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
                // Box broken — reset
                m_box_high       = bar.high;
                m_box_low        = bar.low;
                m_box_anchor_pv  = m_cum_pv;
                m_box_anchor_vol = m_cum_vol;
                m_box_armed      = false;
                m_box_armed_dir  = 0;
            } else {
                m_box_high = std::max(m_box_high, bar.high);
                m_box_low  = std::min(m_box_low,  bar.low);
            }
        }

        // ── 5. Dalton probe flags ────────────────────────────────────────
        if (m_ib_locked && m_ib_vah > 0.0 && m_ib_val > 0.0) {
            if (bar.low  <= m_ib_val - 5.0) m_was_below_val = true;
            if (bar.high >= m_ib_vah + 5.0) m_was_above_vah = true;
        }
    }

    // If we passed 10:15 but lock was never called (sparse data), call it now
    std::string now_time = format_ist_time(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    if (!m_ib_locked && now_time >= "10:15" && m_ib_high > 0.0 && m_ib_low < 1e8) {
        lock_initial_balance_value_area();
    }

    std::cout << "✅ [Rehydrate] Complete."
              << " IB_locked=" << (m_ib_locked ? "YES" : "NO")
              << " | High=" << m_ib_high << " Low=" << (m_ib_low < 1e8 ? m_ib_low : 0.0)
              << " | VAH=" << m_ib_vah << " VAL=" << m_ib_val << " POC=" << m_ib_poc
              << " | POC_init=" << (m_poc_initialized ? "YES" : "NO")
              << " | prev_poc=" << m_prev_poc
              << " | bars_replayed=" << bars.size()
              << std::endl;
}

void StrategyEngine::on_tick(
    redisContext* redis,
    const std::string& symbol,
    double ltp,
    double bid,
    double ask,
    int64_t ts_ms,
    const MicrostructureMetrics& metrics
) {
    if (m_pool.active_positions.empty()) {
        return;
    }

    std::string time_str = format_ist_time(ts_ms);

    // Instant microsecond exit evaluation on live tick arrival
    check_active_exits(
        redis,
        ltp,
        ltp,
        ltp,
        metrics.session_vwap,
        metrics.cvd_15m,
        ts_ms,
        time_str,
        false
    );
}

void StrategyEngine::on_1m_bar(
    redisContext* redis,
    const std::string& symbol,
    const Candle1M& bar,
    const Candle1M& prev_bar,
    const MicrostructureMetrics& metrics
) {
    std::string bar_date = format_ist_date(bar.minute_ts * 1000);
    std::string today_date = format_ist_date(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    if (bar_date != today_date) {
        return; // Guard against stale bar closes from previous sessions
    }

    std::string time_str = format_ist_time(bar.minute_ts * 1000);

    // Update TPO Market Profile brackets (09:15 to 15:30 IST)
    int bracket_idx = get_tpo_bracket_index(time_str);
    update_tpo_profile(bar.high, bar.low, bracket_idx);

    // Track Initial Balance (Periods A & B: 09:15 to 10:15 IST) and lock at 10:15:00
    if (!m_ib_locked) {
        if (time_str < "10:15") {
            if (bar.high > m_ib_high) m_ib_high = bar.high;
            if (bar.low < m_ib_low) m_ib_low = bar.low;
        } else {
            lock_initial_balance_value_area();
        }
    }

    // 0. Execute any pending POC V2 signal on bar open
    if (m_pending_poc_signal.has_signal) {
        execute_pending_poc_signal(redis, bar, time_str);
    }

    // 1. Evaluate exits across all active positions on bar close
    check_active_exits(
        redis,
        bar.close,
        bar.high,
        bar.low,
        metrics.session_vwap,
        metrics.cvd_15m,
        bar.minute_ts * 1000,
        time_str,
        true
    );

    // Hard EOD Cutoff: No new entries past 15:00
    if (time_str >= "15:00" || time_str >= "15:20") {
        return;
    }

    // 2. Model POC V2 Evaluation (09:20 - 10:30 IST)
    if (enable_model_poc_v2 && time_str >= "09:20" && time_str <= "10:30") {
        evaluate_model_poc_v2(redis, bar, metrics, time_str);
    }

    // 3. Model Spatial Box with AVWAP Arm Gate Evaluation (09:20 - 15:00 IST)
    if (enable_model_spatial_box && time_str >= "09:20" && time_str <= "15:00") {
        evaluate_model_spatial_box(redis, bar, metrics, time_str);
    }

    // 4. Horizon 3: Causal Dalton Value Area Traverse Evaluation (10:15 - 13:30 IST)
    if (enable_model_dalton_va && time_str >= "10:15" && time_str <= "13:30") {
        evaluate_model_dalton_va(redis, bar, metrics, time_str);
    }

    // 5. Model TPO Market Profile POC Reversion Evaluation (Legacy / Disabled)
    if (enable_model_tpo_poc && time_str >= "10:30" && time_str <= "14:45") {
        evaluate_model_tpo_poc(redis, bar, metrics, time_str);
    }
}

void StrategyEngine::check_active_exits(
    redisContext* redis,
    double fut_p,
    double high,
    double low,
    double session_vwap,
    double cvd_15m,
    int64_t now_ms,
    const std::string& time_str,
    bool is_bar_close
) {
    if (m_pool.active_positions.empty()) return;

    for (auto& p : m_pool.active_positions) {
        double opt_bid = resolve_option_price(redis, p.strike, p.option_type, false);
        if (opt_bid > 0.0) {
            p.update_market_price(opt_bid, fut_p);
        }
        if (is_bar_close) {
            p.bars_held++;
        }
    }

    std::vector<UnifiedPosition> active_copy = m_pool.active_positions;

    for (auto& pos : active_copy) {
        bool exit_triggered = false;
        std::string exit_reason;
        double exit_fut_price = fut_p;
        double exit_opt_price = (pos.current_option_price > 0.0) ? pos.current_option_price : pos.entry_option_price;

        // 1. Hard EOD Square-off at 15:20 IST
        if (time_str >= "15:20") {
            exit_triggered = true;
            exit_reason = "EOD Square-Off (15:20 IST)";
            exit_fut_price = fut_p;
            exit_opt_price = pos.current_option_price;
        }
        // 2. Model POC V2: Two-Tier Execution & Session VWAP Trailing
        else if (pos.model_name == "Model POC V2" || pos.model_name == "ModelPOCV2" || pos.model_name == "POC V2") {
            bool just_banked_t1 = false;
            // A. Check Lot 1 Target (+30.0 pts FUT)
            if (!pos.t1_hit) {
                double mfe = (pos.option_type == OptionType::CE) ? (high - pos.entry_futures_price) : (pos.entry_futures_price - low);
                if (mfe >= 30.0) {
                    double exit_fut = (pos.option_type == OptionType::CE) ? (pos.entry_futures_price + 30.0) : (pos.entry_futures_price - 30.0);
                    double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
                    if (opt_bid <= 0.0) {
                        double delta_est = (get_current_dte() <= 1) ? 0.75 : 0.50;
                        opt_bid = std::max(0.5, pos.entry_option_price + (30.0 * delta_est));
                    }
                    m_pool.bank_lot1(pos.position_id, opt_bid, exit_fut, time_str);
                    pos.t1_hit = true;
                    just_banked_t1 = true;
                    pos.lot2_sl_futures_price = (pos.option_type == OptionType::CE) ? (pos.entry_futures_price + 2.0) : (pos.entry_futures_price - 2.0);
                    for (auto& p : m_pool.active_positions) {
                        if (p.position_id == pos.position_id) {
                            p.t1_hit = true;
                            p.lot2_sl_futures_price = pos.lot2_sl_futures_price;
                            break;
                        }
                    }
                    publish_trade_event(redis, "T1_BANKED", pos, "Model POC V2 T1 Bank (+30pt FUT)");
                    std::cout << "🎯 [Model POC V2] Banked Lot 1 @ +30pt FUT | Lot 2 SL locked at Breakeven (" << pos.lot2_sl_futures_price << ")" << std::endl;
                }
            }

            // B. If T1 NOT hit: Check Initial SL (-20 pts FUT) or EOD
            if (!pos.t1_hit) {
                bool hit_sl = (pos.option_type == OptionType::CE && low <= (pos.entry_futures_price - 20.0)) ||
                              (pos.option_type == OptionType::PE && high >= (pos.entry_futures_price + 20.0));
                if (hit_sl) {
                    exit_triggered = true;
                    exit_reason = "Initial SL (-20pt FUT)";
                    exit_fut_price = (pos.option_type == OptionType::CE) ? (pos.entry_futures_price - 20.0) : (pos.entry_futures_price + 20.0);
                    double delta_est = (get_current_dte() <= 1) ? 0.75 : 0.50;
                    double fb_bid = std::max(0.5, pos.entry_option_price - (20.0 * delta_est));
                    double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
                    exit_opt_price = (opt_bid > 0.0) ? std::min(opt_bid, fb_bid) : fb_bid;
                } else if (time_str >= "15:20") {
                    exit_triggered = true;
                    exit_reason = "EOD Squareoff";
                    exit_fut_price = fut_p;
                    exit_opt_price = pos.current_option_price;
                }
            }
            // C. If T1 WAS hit: Manage Lot 2 Trailing Session VWAP (offset by 5.0 pts buffer)
            else if (pos.t1_hit && !just_banked_t1) {
                if (pos.option_type == OptionType::CE) {
                    double active_sl = pos.lot2_sl_futures_price;
                    if (session_vwap > 0.0) {
                        active_sl = std::max(pos.lot2_sl_futures_price, session_vwap - 5.0);
                    }
                    if (low <= active_sl) {
                        exit_triggered = true;
                        exit_reason = "VWAP Trail Exit";
                        exit_fut_price = active_sl;
                        double fut_pts = (exit_fut_price - pos.entry_futures_price);
                        double delta_est = (get_current_dte() <= 1) ? 0.75 : 0.50;
                        double fb_bid = std::max(0.5, pos.entry_option_price + (fut_pts * delta_est));
                        double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
                        exit_opt_price = (opt_bid > 0.0) ? opt_bid : fb_bid;
                    }
                } else {
                    double active_sl = pos.lot2_sl_futures_price;
                    if (session_vwap > 0.0) {
                        active_sl = std::min(pos.lot2_sl_futures_price, session_vwap + 5.0);
                    }
                    if (high >= active_sl) {
                        exit_triggered = true;
                        exit_reason = "VWAP Trail Exit";
                        exit_fut_price = active_sl;
                        double fut_pts = (pos.entry_futures_price - exit_fut_price);
                        double delta_est = (get_current_dte() <= 1) ? 0.75 : 0.50;
                        double fb_bid = std::max(0.5, pos.entry_option_price + (fut_pts * delta_est));
                        double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
                        exit_opt_price = (opt_bid > 0.0) ? opt_bid : fb_bid;
                    }
                }
                if (!exit_triggered && time_str >= "15:20") {
                    exit_triggered = true;
                    exit_reason = "EOD Squareoff";
                    exit_fut_price = fut_p;
                    exit_opt_price = pos.current_option_price;
                }
            }
        }
        // 3. Model Spatial Box: +45pt Option Target, -15pt Option SL, 45m Time Stop
        else if (pos.model_name == "Model Spatial Box") {
            double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
            if (opt_bid <= 0.0) opt_bid = pos.current_option_price;

            if (opt_bid >= pos.target_opt_price) {
                exit_triggered = true;
                exit_reason = "Target Reached (+45pt)";
                exit_opt_price = pos.target_opt_price;
                exit_fut_price = fut_p;
            } else if (opt_bid <= pos.sl_opt_price) {
                exit_triggered = true;
                exit_reason = "Stop Loss Hit (-15pt)";
                exit_opt_price = pos.sl_opt_price;
                exit_fut_price = fut_p;
            } else if (pos.bars_held >= 45) {
                exit_triggered = true;
                exit_reason = "Time Exit (45m)";
                exit_opt_price = opt_bid;
                exit_fut_price = fut_p;
            }
        }
        // 4. Horizon 3: Causal Dalton Value Area Traverse (Target VAH/VAL, SL VAL-15/VAH+15, Window Close 13:30)
        else if (pos.model_name == "Causal Dalton VA" || pos.model_name == "Dalton VA" || pos.model_name == "DALTON_VA") {
            if (pos.option_type == OptionType::CE) {
                if (fut_p >= pos.tpo_target_futures) {
                    exit_triggered = true;
                    exit_reason = "Target Reached (VAH)";
                    exit_fut_price = pos.tpo_target_futures;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (fut_p <= pos.sl_futures_price) {
                    exit_triggered = true;
                    exit_reason = "Stop Loss Hit (VAL - 15pt)";
                    exit_fut_price = pos.sl_futures_price;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (time_str >= "13:30") {
                    exit_triggered = true;
                    exit_reason = "Window Close (13:30 IST)";
                    exit_fut_price = fut_p;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                }
            } else { // PE
                if (fut_p <= pos.tpo_target_futures) {
                    exit_triggered = true;
                    exit_reason = "Target Reached (VAL)";
                    exit_fut_price = pos.tpo_target_futures;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (fut_p >= pos.sl_futures_price) {
                    exit_triggered = true;
                    exit_reason = "Stop Loss Hit (VAH + 15pt)";
                    exit_fut_price = pos.sl_futures_price;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (time_str >= "13:30") {
                    exit_triggered = true;
                    exit_reason = "Window Close (13:30 IST)";
                    exit_fut_price = fut_p;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                }
            }
        }
        // 5. Model TPO POC Reversion: Futures Mean Reversion to TPO POC, -20pt SL (Legacy)
        else if (pos.model_name == "TPO POC Reversion") {
            if (pos.option_type == OptionType::CE) {
                if (fut_p >= pos.tpo_target_futures - 2.0) {
                    exit_triggered = true;
                    exit_reason = "Target Reached (TPO POC)";
                    exit_fut_price = pos.tpo_target_futures;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (fut_p <= pos.sl_futures_price) {
                    exit_triggered = true;
                    exit_reason = "Stop Loss Hit (-20pt FUT)";
                    exit_fut_price = pos.sl_futures_price;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                }
            } else { // PE
                if (fut_p <= pos.tpo_target_futures + 2.0) {
                    exit_triggered = true;
                    exit_reason = "Target Reached (TPO POC)";
                    exit_fut_price = pos.tpo_target_futures;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                } else if (fut_p >= pos.sl_futures_price) {
                    exit_triggered = true;
                    exit_reason = "Stop Loss Hit (-20pt FUT)";
                    exit_fut_price = pos.sl_futures_price;
                    exit_opt_price = resolve_option_price(redis, pos.strike, pos.option_type, false);
                }
            }
        }

        if (exit_triggered) {
            double exit_opt = exit_opt_price;
            if (exit_opt <= 0.0) {
                exit_opt = resolve_option_price(redis, pos.strike, pos.option_type, false);
            }
            if (exit_opt <= 0.0) {
                double fut_pts = (pos.option_type == OptionType::CE) ? (exit_fut_price - pos.entry_futures_price) : (pos.entry_futures_price - exit_fut_price);
                double delta_est = (get_current_dte() <= 1) ? 0.75 : 0.50;
                exit_opt = std::max(0.5, pos.entry_option_price + (fut_pts * delta_est));
            }
            UnifiedPosition closed_snapshot = pos;
            bool closed = m_pool.close_position(
                pos.position_id,
                exit_opt,
                exit_fut_price,
                exit_reason,
                time_str
            );
            if (closed) {
                if (pos.model_name == "Model Spatial Box") {
                    m_box_last_exit_minute = (now_ms > 0) ? (now_ms / 60000) : 0;
                }
                for (const auto& cp : m_pool.closed_positions) {
                    if (cp.position_id == pos.position_id) {
                        closed_snapshot = cp;
                        break;
                    }
                }
                publish_trade_event(redis, "POSITION_CLOSED", closed_snapshot, exit_reason);
                if (m_trade_callback) {
                    m_trade_callback("POSITION_CLOSED", closed_snapshot, exit_reason);
                }
                publish_portfolio_state(redis, time_str);
            }
        }
    }
}

std::string StrategyEngine::resolve_option_instrument_key(
    redisContext* redis,
    int64_t strike,
    OptionType type
) {
    if (m_cached_front_expiry.empty()) {
        init_front_expiry(redis);
    }
    if (m_cached_front_expiry.empty()) return "";

    std::string field = std::to_string(strike) + ":" + option_type_to_string(type);
    redisReply* r_tok = (redisReply*)redisCommand(redis, "HGET %s %s", m_cached_front_expiry.c_str(), field.c_str());
    std::string token = "";
    if (r_tok) {
        if (r_tok->type == REDIS_REPLY_STRING && r_tok->str) {
            token = r_tok->str;
        }
        freeReplyObject(r_tok);
    }
    return token;
}

double StrategyEngine::resolve_option_price(
    redisContext* redis,
    int64_t strike,
    OptionType type,
    bool is_ask
) {
    if (!redis) return 0.0;

    std::string token = resolve_option_instrument_key(redis, strike, type);
    std::string field = is_ask ? "ask" : "bid";
    double price = 0.0;

    if (!token.empty()) {
        redisReply* reply = (redisReply*)redisCommand(redis, "HGET md:quote:%s %s", token.c_str(), field.c_str());
        if (reply) {
            if (reply->type == REDIS_REPLY_STRING && reply->str) {
                price = std::atof(reply->str);
            }
            freeReplyObject(reply);
        }
        if (price <= 0.0) {
            redisReply* ltp_reply = (redisReply*)redisCommand(redis, "HGET md:quote:%s ltp", token.c_str());
            if (ltp_reply) {
                if (ltp_reply->type == REDIS_REPLY_STRING && ltp_reply->str) {
                    price = std::atof(ltp_reply->str);
                }
                freeReplyObject(ltp_reply);
            }
        }
    }

    // Fallback: direct pattern match if token resolution was empty
    if (price <= 0.0) {
        std::string type_str = option_type_to_string(type);
        std::ostringstream sym_oss;
        sym_oss << "NSE_FO|NIFTY" << strike << type_str;
        redisReply* reply = (redisReply*)redisCommand(redis, "HGET md:quote:%s %s", sym_oss.str().c_str(), field.c_str());
        if (reply) {
            if (reply->type == REDIS_REPLY_STRING && reply->str) {
                price = std::atof(reply->str);
            }
            freeReplyObject(reply);
        }
    }

    return price;
}

void StrategyEngine::init_front_expiry(redisContext* redis) {
    if (!redis) return;
    std::string today_date = format_ist_date(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    std::string min_chain_key = "chain:NIFTY:" + today_date;

    redisReply* r_keys = (redisReply*)redisCommand(redis, "KEYS chain:NIFTY:*");
    if (r_keys) {
        if (r_keys->type == REDIS_REPLY_ARRAY && r_keys->elements > 0) {
            std::vector<std::string> chains;
            for (size_t i = 0; i < r_keys->elements; ++i) {
                if (r_keys->element[i]->str) {
                    std::string k_str = r_keys->element[i]->str;
                    if (k_str >= min_chain_key && k_str.find(":meta") == std::string::npos) {
                        chains.push_back(k_str);
                    }
                }
            }
            if (chains.empty()) {
                for (size_t i = 0; i < r_keys->elements; ++i) {
                    if (r_keys->element[i]->str) {
                        std::string k_str = r_keys->element[i]->str;
                        if (k_str.find(":meta") == std::string::npos) {
                            chains.push_back(k_str);
                        }
                    }
                }
            }
            std::sort(chains.begin(), chains.end());
            if (!chains.empty()) {
                m_cached_front_expiry = chains.front();
                std::cout << "🎯 [StrategyEngine] Initialized Front Expiry: " << m_cached_front_expiry << std::endl;
            }
        }
        freeReplyObject(r_keys);
    }
}

int StrategyEngine::get_current_dte() const {
    if (m_cached_front_expiry.empty()) return 2;
    size_t last_colon = m_cached_front_expiry.rfind(':');
    if (last_colon == std::string::npos || last_colon + 11 > m_cached_front_expiry.size()) return 2;
    std::string exp_date = m_cached_front_expiry.substr(last_colon + 1, 10);

    std::string today_date = format_ist_date(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    if (exp_date == today_date) return 0;

    int y1, m1, d1, y2, m2, d2;
    if (sscanf(today_date.c_str(), "%d-%d-%d", &y1, &m1, &d1) == 3 &&
        sscanf(exp_date.c_str(), "%d-%d-%d", &y2, &m2, &d2) == 3) {
        std::tm tm1 = {};
        tm1.tm_year = y1 - 1900;
        tm1.tm_mon = m1 - 1;
        tm1.tm_mday = d1;
        std::tm tm2 = {};
        tm2.tm_year = y2 - 1900;
        tm2.tm_mon = m2 - 1;
        tm2.tm_mday = d2;
        std::time_t t1 = std::mktime(&tm1);
        std::time_t t2 = std::mktime(&tm2);
        if (t1 != -1 && t2 != -1) {
            double diff_days = std::difftime(t2, t1) / 86400.0;
            return std::max(0, static_cast<int>(std::round(diff_days)));
        }
    }
    return 2;
}

TargetStrikeResult StrategyEngine::resolve_target_strike(
    redisContext* redis,
    double fut_price,
    OptionType type,
    double target_premium
) {
    TargetStrikeResult res;
    if (!redis) return res;

    if (m_cached_front_expiry.empty()) {
        init_front_expiry(redis);
    }
    int64_t atm = static_cast<int64_t>(std::round(fut_price / 50.0)) * 50;

    if (m_cached_front_expiry.empty()) {
        res.strike = atm;
        res.ask = resolve_option_price(redis, atm, type, true);
        res.bid = resolve_option_price(redis, atm, type, false);
        return res;
    }

    std::string type_suffix = (type == OptionType::CE) ? ":CE" : ":PE";

    redisReply* r_all = (redisReply*)redisCommand(redis, "HGETALL %s", m_cached_front_expiry.c_str());
    if (!r_all || r_all->type != REDIS_REPLY_ARRAY) {
        if (r_all) freeReplyObject(r_all);
        res.strike = atm;
        res.ask = resolve_option_price(redis, atm, type, true);
        res.bid = resolve_option_price(redis, atm, type, false);
        return res;
    }

    struct Candidate {
        int64_t strike;
        std::string token;
        double bid;
        double ask;
    };
    std::vector<Candidate> candidates;

    for (size_t i = 0; i + 1 < r_all->elements; i += 2) {
        if (!r_all->element[i]->str || !r_all->element[i + 1]->str) continue;
        std::string field = r_all->element[i]->str;
        std::string token = r_all->element[i + 1]->str;

        if (field.size() > type_suffix.size() && 
            field.rfind(type_suffix) == (field.size() - type_suffix.size())) {
            
            try {
                int64_t strike = std::stoll(field.substr(0, field.size() - type_suffix.size()));

                double ask = 0.0;
                double bid = 0.0;
                redisReply* r_quote = (redisReply*)redisCommand(redis, "HMGET md:quote:%s ask bid ltp", token.c_str());
                if (r_quote && r_quote->type == REDIS_REPLY_ARRAY && r_quote->elements >= 3) {
                    if (r_quote->element[0]->str) ask = std::atof(r_quote->element[0]->str);
                    if (r_quote->element[1]->str) bid = std::atof(r_quote->element[1]->str);
                    if (ask <= 0.0 && r_quote->element[2]->str) ask = std::atof(r_quote->element[2]->str);
                    if (bid <= 0.0) bid = ask;
                    freeReplyObject(r_quote);
                }

                // Match Python candidate filter: ask >= 15.0 INR
                if (ask >= 15.0) {
                    candidates.push_back({strike, token, bid, ask});
                }
            } catch (...) {
                continue;
            }
        }
    }
    freeReplyObject(r_all);

    if (candidates.empty()) {
        int64_t atm = static_cast<int64_t>(std::round(fut_price / 50.0)) * 50;
        res.strike = atm;
        res.ask = resolve_option_price(redis, atm, type, true);
        res.bid = resolve_option_price(redis, atm, type, false);
        return res;
    }

    std::vector<Candidate> under_prem;
    for (const auto& c : candidates) {
        if (c.ask <= target_premium) {
            under_prem.push_back(c);
        }
    }

    Candidate chosen;
    if (!under_prem.empty()) {
        chosen = *std::max_element(under_prem.begin(), under_prem.end(), [](const Candidate& a, const Candidate& b) {
            return a.ask < b.ask;
        });
    } else {
        chosen = *std::min_element(candidates.begin(), candidates.end(), [](const Candidate& a, const Candidate& b) {
            return a.ask < b.ask;
        });
    }

    res.strike = chosen.strike;
    res.instrument_key = chosen.token;
    res.ask = chosen.ask;
    res.bid = chosen.bid;
    return res;
}

void StrategyEngine::publish_portfolio_state(
    redisContext* redis,
    const std::string& current_time_str
) {
    if (!redis) return;

    std::string t_str = current_time_str.empty() ? format_ist_time(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()
    ) : current_time_str;

    double pnl = m_pool.get_realized_pnl_today();
    double tot_val = m_pool.get_total_portfolio_value();
    double free_c = m_pool.get_free_cash();
    double locked_m = m_pool.get_locked_margin();
    double un_pnl = 0.0;
    for (const auto& p : m_pool.active_positions) {
        un_pnl += p.get_unrealized_pnl();
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "{"
        << "\"starting_capital\":" << m_pool.starting_capital << ","
        << "\"total_portfolio_value\":" << tot_val << ","
        << "\"realized_pnl_today\":" << pnl << ","
        << "\"unrealized_pnl\":" << un_pnl << ","
        << "\"free_cash\":" << free_c << ","
        << "\"locked_margin\":" << locked_m << ","
        << "\"active_count\":" << m_pool.active_positions.size() << ","
        << "\"closed_count\":" << m_pool.closed_positions.size() << ","
        << "\"active_positions\":[";

    for (size_t i = 0; i < m_pool.active_positions.size(); ++i) {
        const auto& p = m_pool.active_positions[i];
        if (i > 0) oss << ",";
        oss << "{"
            << "\"position_id\":\"" << p.position_id << "\","
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
            << "\"entry_time\":\"" << p.entry_time << "\""
            << "}";
    }

    oss << "],\"closed_positions\":[";
    for (size_t i = 0; i < m_pool.closed_positions.size(); ++i) {
        const auto& p = m_pool.closed_positions[i];
        if (i > 0) oss << ",";
        oss << "{"
            << "\"position_id\":\"" << p.position_id << "\","
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
            << "\"exit_time\":\"" << p.exit_time << "\""
            << "}";
    }

    oss << "],\"dalton_state\":{"
        << "\"ib_locked\":" << (m_ib_locked ? "true" : "false") << ","
        << "\"ib_vah\":" << m_ib_vah << ","
        << "\"ib_val\":" << m_ib_val << ","
        << "\"ib_poc\":" << m_ib_poc << ","
        << "\"ib_high\":" << m_ib_high << ","
        << "\"ib_low\":" << (m_ib_low < 1e8 ? m_ib_low : 0.0) << ","
        << "\"pcr\":" << m_cached_pcr
        << "},\"updated_at\":\"" << t_str << "\"}";

    std::string json_payload = oss.str();

    // 1. SET ulltr:portfolio:state
    redisReply* r_set = (redisReply*)redisCommand(redis, "SET ulltr:portfolio:state %s", json_payload.c_str());
    if (r_set) freeReplyObject(r_set);

    // 2. PUBLISH ulltr:events:portfolio
    redisReply* r_pub = (redisReply*)redisCommand(redis, "PUBLISH ulltr:events:portfolio %s", json_payload.c_str());
    if (r_pub) freeReplyObject(r_pub);
}

void StrategyEngine::publish_trade_event(
    redisContext* redis,
    const std::string& type,
    const UnifiedPosition& pos,
    const std::string& details
) {
    if (!redis) return;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "{"
        << "\"position_id\":\"" << pos.position_id << "\","
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
        << "\"reason\":\"" << details << "\""
        << "}";

    std::string payload = oss.str();
    redisReply* r = (redisReply*)redisCommand(
        redis,
        "XADD ulltr:trades:audit * event %s data %s timestamp %ld",
        type.c_str(),
        payload.c_str(),
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()
    );
    if (r) freeReplyObject(r);

    // 2. Format payload for Telegram Sidecar Daemon & PUBLISH ulltr:trades:stream
    std::ostringstream tg_oss;
    tg_oss << std::fixed << std::setprecision(2);
    tg_oss << "{"
           << "\"event\":\"" << type << "\","
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
           << "\"free_cash\":" << m_pool.get_free_cash() << ","
           << "\"margin_locked\":" << m_pool.get_locked_margin()
           << "}";
    std::string tg_payload = tg_oss.str();
    redisReply* r_pub_tg = (redisReply*)redisCommand(redis, "PUBLISH ulltr:trades:stream %s", tg_payload.c_str());
    if (r_pub_tg) freeReplyObject(r_pub_tg);
}

void StrategyEngine::evaluate_model_poc_v2(
    redisContext* redis,
    const Candle1M& bar,
    const MicrostructureMetrics& m,
    const std::string& time_str
) {
    if (bar.minute_ts == m_last_evaluated_minute) {
        return;
    }
    m_last_evaluated_minute = bar.minute_ts;

    double curr_poc = m.dpoc;
    if (curr_poc <= 0.0) {
        return;
    }

    // Initialize baseline POC on start
    if (!m_poc_initialized) {
        m_prev_poc = curr_poc;
        m_poc_initialized = true;
        return;
    }

    if (curr_poc == m_prev_poc) {
        return;
    }

    double poc_shift = curr_poc - m_prev_poc;
    double old_poc = m_prev_poc;
    m_prev_poc = curr_poc; // Advance state continuously on every shift!

    // Order flow context: 15m rolling lookback
    std::string oi_regime = "";
    if (m.delta_price_15m >= 0.0 && m.delta_oi_15m >= 0.0) {
        oi_regime = "Long Buildup";
    } else if (m.delta_price_15m >= 0.0 && m.delta_oi_15m < 0.0) {
        oi_regime = "Short Covering";
    } else if (m.delta_price_15m < 0.0 && m.delta_oi_15m >= 0.0) {
        oi_regime = "Short Buildup";
    } else {
        oi_regime = "Long Unwinding";
    }

    OptionType chosen_type = OptionType::CE;
    bool has_signal = false;
    std::string regime = "";

    if (poc_shift > 0.0) {
        // 1. Bull Trap Fade (UP Jump + CVD Neg / Short Buildup / Long Unwinding)
        if (m.cvd_15m < 0.0 || oi_regime == "Short Buildup" || oi_regime == "Long Unwinding") {
            chosen_type = OptionType::PE;
            has_signal = true;
            regime = "Bull Trap Fade (UP Jump + CVD Neg / " + oi_regime + ")";
        }
        // 2. True Bull Breakout (UP Jump + CVD Pos + Long Buildup)
        else if (m.cvd_15m > 0.0 && oi_regime == "Long Buildup") {
            chosen_type = OptionType::CE;
            has_signal = true;
            regime = "True Bull Breakout (UP Jump + CVD Pos + Long Buildup)";
        }
    } else if (poc_shift < 0.0) {
        // 3. Absorption Bottom (DOWN Jump + CVD Pos / Long Buildup)
        if (m.cvd_15m > 0.0 || oi_regime == "Long Buildup") {
            chosen_type = OptionType::CE;
            has_signal = true;
            regime = "Absorption Bottom (DOWN Jump + CVD Pos / " + oi_regime + ")";
        }
        // 4. True Bear Breakdown (DOWN Jump + CVD Neg + Short Buildup)
        else if (m.cvd_15m < 0.0 && oi_regime == "Short Buildup") {
            chosen_type = OptionType::PE;
            has_signal = true;
            regime = "True Bear Breakdown (DOWN Jump + CVD Neg + Short Buildup)";
        }
    }

    std::cout << "🔍 [Model POC V2] POC Shift @ " << time_str 
              << " | Shift: " << poc_shift << " pts (New: " << curr_poc 
              << ", Old: " << old_poc << ") | CVD 15m: " << m.cvd_15m 
              << " | Delta P: " << m.delta_price_15m << " | Delta OI: " << m.delta_oi_15m 
              << " | Regime: " << oi_regime 
              << " | Signal: " << (has_signal ? regime : "None") << std::endl;

    if (!has_signal) {
        return;
    }

    // Queue signal for execution at the open of next bar (bar.minute_ts + 60)
    m_pending_poc_signal.has_signal = true;
    m_pending_poc_signal.chosen_type = chosen_type;
    m_pending_poc_signal.regime = regime;
}

void StrategyEngine::execute_pending_poc_signal(
    redisContext* redis,
    const Candle1M& bar,
    const std::string& time_str
) {
    if (!m_pending_poc_signal.has_signal) return;

    double entry_fut_price = bar.open;
    OptionType chosen_type = m_pending_poc_signal.chosen_type;
    std::string regime = m_pending_poc_signal.regime;

    // Check if an opposite position is active in Model POC V2
    for (auto& pos : m_pool.active_positions) {
        if (pos.model_name == "Model POC V2" && pos.is_active && pos.option_type != chosen_type) {
            // Reversal exit at bar open
            double fut_pts = (pos.option_type == OptionType::CE) 
                ? (entry_fut_price - pos.entry_futures_price) 
                : (pos.entry_futures_price - entry_fut_price);
            int dte = get_current_dte();
            double delta_est = (dte <= 1) ? 0.75 : 0.50;
            double fb_bid = std::max(0.5, pos.entry_option_price + (fut_pts * delta_est));
            double opt_bid = resolve_option_price(redis, pos.strike, pos.option_type, false);
            double exit_opt = (opt_bid > 0.0) ? opt_bid : fb_bid;

            m_pool.close_position(pos.position_id, exit_opt, entry_fut_price, "Reversal Exit", time_str);
            publish_trade_event(redis, "TRADE_CLOSE", pos, "Reversal Exit (" + time_str + ")");
            if (m_trade_callback) {
                m_trade_callback("REVERSAL_EXIT", pos, "Reversal Exit");
            }
        }
    }

    // Resolve target strike with premium closest to and <= 155.0 INR (less but not over)
    TargetStrikeResult target = resolve_target_strike(redis, entry_fut_price, chosen_type, 155.0);
    double entry_ask = (target.ask > 0.0) ? target.ask : 155.0;

    TradeSignal sig;
    sig.model_name = "Model POC V2";
    sig.option_type = chosen_type;
    sig.strike = target.strike;
    sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen_type);
    sig.option_ask = entry_ask;
    sig.fut_price = entry_fut_price;
    sig.sl_fut = (chosen_type == OptionType::CE) ? (entry_fut_price - 20.0) : (entry_fut_price + 20.0);
    sig.tp_fut = (chosen_type == OptionType::CE) ? (entry_fut_price + 30.0) : (entry_fut_price - 30.0);
    sig.timestamp_str = time_str;

    UnifiedPosition out_pos;
    std::string reason;
    bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);

    if (allocated) {
        publish_trade_event(redis, "POSITION_OPENED", out_pos, regime + " (" + time_str + ")");
        if (m_trade_callback) {
            m_trade_callback("POSITION_OPENED", out_pos, regime);
        }
        publish_portfolio_state(redis, time_str);
        std::cout << "🚀 [Model POC V2] Allocated " << out_pos.position_id 
                  << " (" << out_pos.lots << " lots) | Margin: Rs " << out_pos.margin_locked
                  << " | " << regime << " @ " << time_str << std::endl;
    } else {
        std::cout << "⚠️ [Model POC V2] Allocation rejected @ " << time_str << " | Reason: " << reason << std::endl;
    }

    m_pending_poc_signal.has_signal = false;
}

// -----------------------------------------------------------------------------
// TPO Market Profile Engine (Sub-Nanosecond Bit-Mask Matrix)
// -----------------------------------------------------------------------------
int StrategyEngine::get_tpo_bracket_index(const std::string& time_str) const {
    if (time_str.size() < 5) return 0;
    int hour = 0;
    int minute = 0;
    try {
        hour = std::stoi(time_str.substr(0, 2));
        minute = std::stoi(time_str.substr(3, 2));
    } catch (...) {
        return 0;
    }
    int minutes_from_open = (hour - 9) * 60 + minute - 15;
    if (minutes_from_open < 0) return 0;
    int idx = minutes_from_open / 30;
    return std::min(idx, 12); // Brackets A (0) through M (12)
}

void StrategyEngine::update_tpo_profile(double high, double low, int bracket_idx) {
    if (bracket_idx < 0 || bracket_idx > 12 || high <= 0.0 || low <= 0.0) return;

    int min_bin = static_cast<int>(std::floor((low - TPO_BASE_PRICE) / TPO_BIN_SIZE));
    int max_bin = static_cast<int>(std::floor((high - TPO_BASE_PRICE) / TPO_BIN_SIZE));

    min_bin = std::max(0, std::min(min_bin, static_cast<int>(TPO_NUM_BINS - 1)));
    max_bin = std::max(0, std::min(max_bin, static_cast<int>(TPO_NUM_BINS - 1)));

    uint16_t bracket_bit = static_cast<uint16_t>(1 << bracket_idx);

    for (int b = min_bin; b <= max_bin; ++b) {
        m_tpo_bracket_masks[b] |= bracket_bit;
    }

    // Resolve mode across all bins (sub-nanosecond bit-mask iteration)
    int max_count = 0;
    double best_poc = 0.0;
    for (int b = 0; b < TPO_NUM_BINS; ++b) {
        if (m_tpo_bracket_masks[b] > 0) {
            int count = __builtin_popcount(m_tpo_bracket_masks[b]);
            if (count > max_count) {
                max_count = count;
                best_poc = TPO_BASE_PRICE + (b + 0.5) * TPO_BIN_SIZE;
            }
        }
    }
    if (max_count > 0) {
        m_current_tpo_max_count = max_count;
        m_current_tpo_poc = best_poc;
    }
}

// -----------------------------------------------------------------------------
// Model Spatial Box with AVWAP Arm Gate (09:20 - 15:00 IST)
// -----------------------------------------------------------------------------
double StrategyEngine::get_box_avwap(double current_tp) const {
    double vol_delta = m_cum_vol - m_box_anchor_vol;
    double pv_delta = m_cum_pv - m_box_anchor_pv;
    if (vol_delta > 0.0) {
        return pv_delta / vol_delta;
    }
    return current_tp;
}

void StrategyEngine::evaluate_model_spatial_box(
    redisContext* redis,
    const Candle1M& bar,
    const MicrostructureMetrics& m,
    const std::string& time_str
) {
    double tp = (bar.high + bar.low + bar.close) / 3.0;
    m_cum_pv += tp * bar.volume;
    m_cum_vol += bar.volume;

    if (!m_box_initialized) {
        m_box_initialized = true;
        m_box_high = bar.high;
        m_box_low = bar.low;
        m_box_anchor_cvd = m.cum_cvd;
        m_box_anchor_pv = m_cum_pv;
        m_box_anchor_vol = m_cum_vol;
        m_box_armed = false;
        m_box_armed_dir = 0;
        return;
    }

    if (m_pool.has_active_position_for_model("Model Spatial Box")) {
        return;
    }

    m_box_high = std::max(m_box_high, bar.high);
    m_box_low = std::min(m_box_low, bar.low);
    double box_range = m_box_high - m_box_low;
    double box_avwap = get_box_avwap(tp);

    // 50-pt Compression & AVWAP Overshoot Invalidation Checks (aligned with Python backtest)
    bool should_reset = (box_range > 50.0);
    if (!should_reset && m_box_armed) {
        if (m_box_armed_dir == 1 && (bar.close - box_avwap) > 15.0) {
            should_reset = true;
        } else if (m_box_armed_dir == -1 && (box_avwap - bar.close) > 15.0) {
            should_reset = true;
        }
    }

    if (should_reset) {
        m_box_high = bar.high;
        m_box_low = bar.low;
        m_box_anchor_cvd = m.cum_cvd;
        m_box_anchor_pv = m_cum_pv;
        m_box_anchor_vol = m_cum_vol;
        m_box_armed = false;
        m_box_armed_dir = 0;
    } else {
        double delta_cvd = m.cum_cvd - m_box_anchor_cvd;
        // CVD Threshold: 55,000 shares (aligned with ModelSpatialBoxConfig in backtest)
        if (delta_cvd >= 55000.0) {
            // AVWAP Arm Gate: price <= box_avwap + 15.0 pts
            if (bar.close <= box_avwap + 15.0) {
                m_box_armed = true;
                m_box_armed_dir = 1;
            }
        } else if (delta_cvd <= -55000.0) {
            // AVWAP Arm Gate: price >= box_avwap - 15.0 pts
            if (bar.close >= box_avwap - 15.0) {
                m_box_armed = true;
                m_box_armed_dir = -1;
            }
        }
    }

    // Breakout Entry Execution (15-min cooldown from last exit)
    int64_t cur_minute = bar.minute_ts / 60;
    if (m_box_armed && (cur_minute - m_box_last_exit_minute >= 15)) {
        OptionType chosen_type = OptionType::CE;
        bool triggered = false;
        std::string regime = "";

        // Replicate Strict Zero-Wick Conviction Close (Institutional Marubozu Impulse from Backtest)
        if (m_box_armed_dir == 1 && bar.close >= m_box_high) {
            chosen_type = OptionType::CE;
            triggered = true;
            regime = "50-pt Box Breakout + AVWAP Arm Gate (CE)";
        } else if (m_box_armed_dir == -1 && bar.close <= m_box_low) {
            chosen_type = OptionType::PE;
            triggered = true;
            regime = "50-pt Box Breakdown + AVWAP Arm Gate (PE)";
        }

        if (triggered) {
            // Uniform strike selection: closest to and <= 155.0 INR premium
            TargetStrikeResult target = resolve_target_strike(redis, bar.close, chosen_type, 155.0);
            double entry_ask = (target.ask > 0.0) ? target.ask : 155.0;

            // Execution timestamp at bar close / next bar open
            std::string exec_time_str = format_ist_time((bar.minute_ts + 60) * 1000);

            TradeSignal sig;
            sig.model_name = "Model Spatial Box";
            sig.option_type = chosen_type;
            sig.strike = target.strike;
            sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen_type);
            sig.option_ask = entry_ask;
            sig.fut_price = bar.close;
            sig.sl_fut = (chosen_type == OptionType::CE) ? (bar.close - 20.0) : (bar.close + 20.0);
            sig.tp_fut = (chosen_type == OptionType::CE) ? (bar.close + 45.0) : (bar.close - 45.0);
            sig.timestamp_str = exec_time_str;

            UnifiedPosition out_pos;
            std::string reason;
            bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);

            if (allocated) {
                out_pos.target_opt_price = entry_ask + 45.0;
                out_pos.sl_opt_price = entry_ask - 15.0;
                out_pos.active_box_avwap = box_avwap;
                out_pos.bars_held = 0;

                for (auto& p : m_pool.active_positions) {
                    if (p.position_id == out_pos.position_id) {
                        p.target_opt_price = entry_ask + 45.0;
                        p.sl_opt_price = entry_ask - 15.0;
                        p.active_box_avwap = box_avwap;
                        p.bars_held = 0;
                        break;
                    }
                }

                // Reset box state on entry
                m_box_armed = false;
                m_box_armed_dir = 0;
                m_box_high = bar.high;
                m_box_low = bar.low;
                m_box_anchor_cvd = m.cum_cvd;
                m_box_anchor_pv = m_cum_pv;
                m_box_anchor_vol = m_cum_vol;

                publish_trade_event(redis, "POSITION_OPENED", out_pos, regime + " (" + exec_time_str + ")");
                if (m_trade_callback) {
                    m_trade_callback("POSITION_OPENED", out_pos, regime);
                }
                publish_portfolio_state(redis, exec_time_str);
                std::cout << "🚀 [Model Spatial Box] Allocated " << out_pos.position_id 
                          << " (" << out_pos.lots << " lots) | Margin: Rs " << out_pos.margin_locked
                          << " | " << regime << " @ " << exec_time_str << std::endl;
            } else {
                std::cout << "⚠️ [Model Spatial Box] Allocation rejected @ " << exec_time_str << " | Reason: " << reason << std::endl;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Model TPO Market Profile POC Reversion (10:30 - 14:45 IST)
// -----------------------------------------------------------------------------
void StrategyEngine::evaluate_model_tpo_poc(
    redisContext* redis,
    const Candle1M& bar,
    const MicrostructureMetrics& m,
    const std::string& time_str
) {
    if (!enable_model_tpo_poc) {
        return;
    }

    if (m_pool.has_active_position_for_model("TPO POC Reversion")) {
        return;
    }

    // Cap at max 1 trade per day for TPO POC Reversion (matching backtest break)
    int trades_count = 0;
    for (const auto& p : m_pool.active_positions) {
        if (p.model_name == "TPO POC Reversion") trades_count++;
    }
    for (const auto& p : m_pool.closed_positions) {
        if (p.model_name == "TPO POC Reversion") trades_count++;
    }
    if (trades_count >= 1) {
        return;
    }

    if (m_current_tpo_poc <= 0.0) {
        return;
    }

    double dist = bar.close - m_current_tpo_poc;
    OptionType chosen_type = OptionType::CE;
    bool triggered = false;
    std::string regime = "";

    // Oversold: Price deviates below POC by >= 25 pts, green candle or positive CVD
    if (dist <= -25.0 && (bar.close > bar.open || m.cvd_15m > 0.0)) {
        chosen_type = OptionType::CE;
        triggered = true;
        regime = "TPO POC Oversold Fade (CE)";
    }
    // Overbought: Price deviates above POC by >= 25 pts, red candle or negative CVD
    else if (dist >= 25.0 && (bar.close < bar.open || m.cvd_15m < 0.0)) {
        chosen_type = OptionType::PE;
        triggered = true;
        regime = "TPO POC Overbought Fade (PE)";
    }

    if (triggered) {
        // Uniform strike selection: closest to and <= 155.0 INR premium
        TargetStrikeResult target = resolve_target_strike(redis, bar.close, chosen_type, 155.0);
        double entry_ask = (target.ask > 0.0) ? target.ask : 155.0;

        TradeSignal sig;
        sig.model_name = "TPO POC Reversion";
        sig.option_type = chosen_type;
        sig.strike = target.strike;
        sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen_type);
        sig.option_ask = entry_ask;
        sig.fut_price = bar.close;
        sig.sl_fut = (chosen_type == OptionType::CE) ? (bar.close - 20.0) : (bar.close + 20.0);
        sig.tp_fut = m_current_tpo_poc;
        sig.timestamp_str = time_str;

        UnifiedPosition out_pos;
        std::string reason;
        bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);

        if (allocated) {
            out_pos.tpo_poc_at_entry = m_current_tpo_poc;
            out_pos.tpo_target_futures = m_current_tpo_poc;
            out_pos.sl_futures_price = sig.sl_fut;
            out_pos.bars_held = 0;

            for (auto& p : m_pool.active_positions) {
                if (p.position_id == out_pos.position_id) {
                    p.tpo_poc_at_entry = m_current_tpo_poc;
                    p.tpo_target_futures = m_current_tpo_poc;
                    p.sl_futures_price = sig.sl_fut;
                    p.bars_held = 0;
                    break;
                }
            }

            publish_trade_event(redis, "POSITION_OPENED", out_pos, regime + " (" + time_str + ")");
            if (m_trade_callback) {
                m_trade_callback("POSITION_OPENED", out_pos, regime);
            }
            publish_portfolio_state(redis, time_str);
            std::cout << "🚀 [TPO POC Reversion] Allocated " << out_pos.position_id 
                      << " (" << out_pos.lots << " lots) | Margin: Rs " << out_pos.margin_locked
                      << " | " << regime << " @ " << time_str << std::endl;
        } else {
            std::cout << "⚠️ [TPO POC Reversion] Allocation rejected @ " << time_str << " | Reason: " << reason << std::endl;
        }
    }
}

// -----------------------------------------------------------------------------
// Horizon 3: Causal Dalton Value Area Engine (10:15 - 13:30 IST)
// -----------------------------------------------------------------------------
void StrategyEngine::lock_initial_balance_value_area() {
    if (m_ib_locked) return;
    m_ib_locked = true;

    // Periods A & B: bracket bits 0 and 1 -> mask 0x03
    int tot_ib_tpos = 0;
    int max_ib_tpos = 0;
    int ib_poc_bin = -1;
    std::vector<int> active_bins;

    for (size_t b = 0; b < TPO_NUM_BINS; ++b) {
        int count = __builtin_popcount(m_tpo_bracket_masks[b] & 0x03);
        if (count > 0) {
            tot_ib_tpos += count;
            active_bins.push_back(static_cast<int>(b));
            if (count > max_ib_tpos) {
                max_ib_tpos = count;
                ib_poc_bin = static_cast<int>(b);
            }
        }
    }

    if (tot_ib_tpos > 0 && ib_poc_bin >= 0) {
        m_ib_poc = TPO_BASE_PRICE + (ib_poc_bin + 0.5) * TPO_BIN_SIZE;
        double target_tpos = tot_ib_tpos * 0.70;
        int cur_tpos = __builtin_popcount(m_tpo_bracket_masks[ib_poc_bin] & 0x03);
        int min_va_bin = ib_poc_bin;
        int max_va_bin = ib_poc_bin;

        auto it = std::find(active_bins.begin(), active_bins.end(), ib_poc_bin);
        int poc_idx = static_cast<int>(std::distance(active_bins.begin(), it));
        int u = poc_idx + 1;
        int d = poc_idx - 1;

        while (cur_tpos < target_tpos && (u < static_cast<int>(active_bins.size()) || d >= 0)) {
            int u_val = (u < static_cast<int>(active_bins.size())) ? __builtin_popcount(m_tpo_bracket_masks[active_bins[u]] & 0x03) : 0;
            int d_val = (d >= 0) ? __builtin_popcount(m_tpo_bracket_masks[active_bins[d]] & 0x03) : 0;

            if (u_val >= d_val && u_val > 0) {
                cur_tpos += u_val;
                max_va_bin = std::max(max_va_bin, active_bins[u]);
                u++;
            } else if (d_val > 0) {
                cur_tpos += d_val;
                min_va_bin = std::min(min_va_bin, active_bins[d]);
                d--;
            } else {
                break;
            }
        }

        // Standard Steidlmayer Value Area outer boundaries (aligned with Python backtest)
        m_ib_vah = TPO_BASE_PRICE + (max_va_bin + 1.0) * TPO_BIN_SIZE;  // Upper bound of top VA bin (e.g. 23140)
        m_ib_val = TPO_BASE_PRICE + min_va_bin * TPO_BIN_SIZE;          // Lower bound of bottom VA bin (e.g. 23080)
    } else {
        m_ib_poc = (m_ib_high > 0.0 && m_ib_low < 1e8) ? (m_ib_high + m_ib_low) / 2.0 : 0.0;
        m_ib_vah = m_ib_high;
        m_ib_val = (m_ib_low < 1e8) ? m_ib_low : 0.0;
    }

    std::cout << "🔒 [Causal Dalton VA] Initial Balance Locked at 10:15:00 IST -> High: " 
              << m_ib_high << ", Low: " << m_ib_low << ", POC: " << m_ib_poc 
              << ", VAH: " << m_ib_vah << ", VAL: " << m_ib_val << std::endl;
}

double StrategyEngine::calculate_chain_pcr(redisContext* redis) {
    if (!redis) return 1.0;

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Cache PCR for 15 seconds for hot-path sub-microsecond latency
    if (m_cached_pcr_time_ms > 0 && (now_ms - m_cached_pcr_time_ms) < 15000) {
        return m_cached_pcr;
    }

    if (m_cached_front_expiry.empty()) {
        init_front_expiry(redis);
    }

    if (m_cached_front_expiry.empty()) {
        return 1.0;
    }

    redisReply* r_all = (redisReply*)redisCommand(redis, "HGETALL %s", m_cached_front_expiry.c_str());
    if (!r_all || r_all->type != REDIS_REPLY_ARRAY) {
        if (r_all) freeReplyObject(r_all);
        return 1.0;
    }

    int64_t tot_pe_oi = 0;
    int64_t tot_ce_oi = 0;
    std::vector<std::pair<std::string, bool>> tokens_with_type;

    for (size_t i = 0; i + 1 < r_all->elements; i += 2) {
        if (!r_all->element[i]->str || !r_all->element[i + 1]->str) continue;
        std::string field = r_all->element[i]->str;
        std::string token = r_all->element[i + 1]->str;

        bool is_pe = (field.find(":PE") != std::string::npos);
        bool is_ce = (field.find(":CE") != std::string::npos);
        if (is_pe || is_ce) {
            tokens_with_type.push_back({token, is_pe});
            redisAppendCommand(redis, "HGET md:quote:%s oi", token.c_str());
        }
    }
    freeReplyObject(r_all);

    for (const auto& item : tokens_with_type) {
        redisReply* r_oi = nullptr;
        if (redisGetReply(redis, (void**)&r_oi) == REDIS_OK && r_oi) {
            if (r_oi->str) {
                int64_t oi = std::strtoll(r_oi->str, nullptr, 10);
                if (oi > 0) {
                    if (item.second) tot_pe_oi += oi;
                    else tot_ce_oi += oi;
                }
            }
            freeReplyObject(r_oi);
        }
    }

    double pcr = 1.0;
    if (tot_ce_oi > 0) {
        pcr = static_cast<double>(tot_pe_oi) / static_cast<double>(tot_ce_oi);
    }

    m_cached_pcr = pcr;
    m_cached_pcr_time_ms = now_ms;
    return pcr;
}

void StrategyEngine::evaluate_model_dalton_va(
    redisContext* redis,
    const Candle1M& bar,
    const MicrostructureMetrics& m,
    const std::string& time_str
) {
    if (!enable_model_dalton_va) {
        return;
    }

    // Ensure IB is locked
    if (!m_ib_locked) {
        if (time_str >= "10:15") {
            lock_initial_balance_value_area();
        } else {
            return;
        }
    }

    // Check trading window: 10:15 to 13:30 IST
    if (time_str < "10:15" || time_str > "13:30") {
        return;
    }

    // Check if position already open for Dalton VA
    if (m_pool.has_active_position_for_model("Causal Dalton VA")) {
        return;
    }

    // Track probe outside Value Area
    if (bar.low <= m_ib_val - 5.0) {
        m_was_below_val = true;
    }
    if (bar.high >= m_ib_vah + 5.0) {
        m_was_above_vah = true;
    }

    OptionType chosen_type = OptionType::CE;
    bool triggered = false;
    std::string regime = "";
    double target_fut = 0.0;
    double sl_fut = 0.0;

    // Dalton 80% Rule Trigger 1: Long CE (probed below VAL, re-entered above VAL with positive CVD)
    if (m_was_below_val && bar.close >= m_ib_val && m.cvd_15m > 0.0) {
        double pcr = calculate_chain_pcr(redis);
        if (pcr < 0.70) {
            std::cout << "⚠️ [Causal Dalton VA] REJECTED LONG CE: PCR " << pcr 
                      << " < 0.70 (Call-heavy regime / weak put floor) @ " << time_str << std::endl;
            return;
        }

        chosen_type = OptionType::CE;
        triggered = true;
        target_fut = m_ib_vah;
        sl_fut = m_ib_val - 15.0;
        regime = "Dalton 80% Bullish VA Traverse (Buy CE)";
    }
    // Dalton 80% Rule Trigger 2: Short PE (probed above VAH, re-entered below VAH with negative CVD)
    else if (m_was_above_vah && bar.close <= m_ib_vah && m.cvd_15m < 0.0) {
        double pcr = calculate_chain_pcr(redis);
        if (pcr > 1.35) {
            std::cout << "⚠️ [Causal Dalton VA] REJECTED SHORT PE: PCR " << pcr 
                      << " > 1.35 (Put-heavy regime / oversold into put wall) @ " << time_str << std::endl;
            return;
        }

        chosen_type = OptionType::PE;
        triggered = true;
        target_fut = m_ib_val;
        sl_fut = m_ib_vah + 15.0;
        regime = "Dalton 80% Bearish VA Traverse (Buy PE)";
    }

    if (triggered) {
        // Uniform strike selection targeting <= 155.0 INR premium
        TargetStrikeResult target = resolve_target_strike(redis, bar.close, chosen_type, 155.0);
        double entry_ask = (target.ask > 0.0) ? target.ask : 155.0;

        // Execution timestamp at bar close / next bar open
        std::string exec_time_str = format_ist_time((bar.minute_ts + 60) * 1000);

        TradeSignal sig;
        sig.model_name = "Causal Dalton VA";
        sig.option_type = chosen_type;
        sig.strike = target.strike;
        sig.symbol = "NIFTY_" + std::to_string(target.strike) + "_" + option_type_to_string(chosen_type);
        sig.option_ask = entry_ask;
        sig.fut_price = bar.close;
        sig.sl_fut = sl_fut;
        sig.tp_fut = target_fut;
        sig.timestamp_str = exec_time_str;

        UnifiedPosition out_pos;
        std::string reason;
        bool allocated = m_pool.evaluate_and_allocate(sig, out_pos, reason);

        if (allocated) {
            out_pos.dalton_ib_vah = m_ib_vah;
            out_pos.dalton_ib_val = m_ib_val;
            out_pos.dalton_ib_poc = m_ib_poc;
            out_pos.dalton_pcr = m_cached_pcr;
            out_pos.tpo_target_futures = target_fut;
            out_pos.sl_futures_price = sl_fut;
            out_pos.bars_held = 0;

            for (auto& p : m_pool.active_positions) {
                if (p.position_id == out_pos.position_id) {
                    p.dalton_ib_vah = m_ib_vah;
                    p.dalton_ib_val = m_ib_val;
                    p.dalton_ib_poc = m_ib_poc;
                    p.dalton_pcr = m_cached_pcr;
                    p.tpo_target_futures = target_fut;
                    p.sl_futures_price = sl_fut;
                    p.bars_held = 0;
                    break;
                }
            }

            if (chosen_type == OptionType::CE) m_was_below_val = false;
            else m_was_above_vah = false;

            m_dalton_trades_today++;

            publish_trade_event(redis, "POSITION_OPENED", out_pos, regime + " (" + exec_time_str + ")");
            if (m_trade_callback) {
                m_trade_callback("POSITION_OPENED", out_pos, regime);
            }
            publish_portfolio_state(redis, exec_time_str);
            std::cout << "🚀 [Causal Dalton VA] Allocated " << out_pos.position_id 
                      << " (" << out_pos.lots << " lots) | Margin: Rs " << out_pos.margin_locked
                      << " | " << regime << " @ " << exec_time_str 
                      << " | Target: " << target_fut << " | SL: " << sl_fut 
                      << " | PCR: " << m_cached_pcr << std::endl;
        } else {
            std::cout << "⚠️ [Causal Dalton VA] Allocation rejected @ " << exec_time_str 
                      << " | Reason: " << reason << std::endl;
        }
    }
}
