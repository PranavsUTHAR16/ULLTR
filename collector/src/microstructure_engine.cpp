#include "microstructure_engine.hpp"
#include "redis_utils.hpp"

#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>

MicrostructureEngine::MicrostructureEngine() {
}

void MicrostructureEngine::update_minute_boundary(
    SymbolState& state,
    int64_t minute_ts,
    double current_oi,
    double ltp
) {
    if (state.current_minute_ts == 0) {
        state.current_minute_ts = minute_ts;
        size_t idx = (minute_ts / 60) % VWAP_WINDOW_MINS;
        state.buckets_90m[idx].minute_timestamp = minute_ts;
        state.buckets_90m[idx].close_oi = current_oi;
        state.buckets_90m[idx].close_price = ltp;
        state.buckets_90m[idx].qb_delta = 0.0;
        size_t oi_idx = (minute_ts / 60) % CVD_WINDOW_MINS;
        state.oi_ring[oi_idx] = current_oi;
        state.oi_ts_ring[oi_idx] = minute_ts;
        state.price_ring[oi_idx] = ltp;
        return;
    }

    if (minute_ts <= state.current_minute_ts) {
        return;
    }

    // Advance minute: evict any bucket in ring buffer that is >= 90 minutes old
    int64_t prev_ts = state.current_minute_ts;
    state.current_minute_ts = minute_ts;

    // Zero out old bucket at new index and subtract its contents from running 90m accumulators
    size_t new_idx = (minute_ts / 60) % VWAP_WINDOW_MINS;
    MinuteBucket& old_b = state.buckets_90m[new_idx];
    if (old_b.minute_timestamp > 0 && old_b.minute_timestamp <= minute_ts - (VWAP_WINDOW_MINS * 60)) {
        state.roll_buy_vol_90m = std::max(0.0, state.roll_buy_vol_90m - old_b.buy_vol);
        state.roll_sell_vol_90m = std::max(0.0, state.roll_sell_vol_90m - old_b.sell_vol);
        state.roll_buy_dollar_90m = std::max(0.0, state.roll_buy_dollar_90m - old_b.buy_dollar);
        state.roll_sell_dollar_90m = std::max(0.0, state.roll_sell_dollar_90m - old_b.sell_dollar);
    }

    // Initialize fresh bucket
    old_b.minute_timestamp = minute_ts;
    old_b.buy_vol = 0.0;
    old_b.sell_vol = 0.0;
    old_b.buy_dollar = 0.0;
    old_b.sell_dollar = 0.0;
    old_b.qb_delta = 0.0;
    old_b.close_oi = current_oi;
    old_b.close_price = ltp;

    // Record in 15-minute OI & Price ring buffers
    size_t oi_idx = (minute_ts / 60) % CVD_WINDOW_MINS;
    state.oi_ring[oi_idx] = current_oi;
    state.oi_ts_ring[oi_idx] = minute_ts;
    state.price_ring[oi_idx] = ltp;
}

void MicrostructureEngine::process_tick(
    redisContext* redis,
    const std::string& symbol,
    double ltp,
    double bid,
    double ask,
    int64_t cum_vol,
    double oi,
    int64_t ts_exchange_ms
) {
    if (ltp <= 0.0) return;

    int64_t now_ms = (ts_exchange_ms > 0) ? ts_exchange_ms : std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    int64_t ts_sec = now_ms / 1000;
    int64_t minute_ts = (ts_sec / 60) * 60;

    std::string today_date = redis_utils::now_ist_date();

    SymbolState& state = m_states[symbol];
    if (state.symbol.empty()) {
        state.symbol = symbol;
        state.session_date = today_date;
        state.last_ltp = ltp;
        state.last_bid = bid;
        state.last_ask = ask;
        state.last_cum_vol = cum_vol;
        state.last_oi = oi;
        state.initial_oi = oi;
        state.initial_oi_set = true;
        restore_session_from_redis(redis, symbol, state);
    } else if (state.session_date != today_date) {
        // Daily rollover reset: clear previous day accumulators
        state.session_date = today_date;
        state.cum_cvd = 0.0;
        state.cum_session_dollar = 0.0;
        state.cum_session_vol = 0.0;
        state.poc_bins.clear();
        state.max_poc_bin = 0;
        state.max_poc_vol = 0.0;
        state.initial_oi_set = false;
        state.initial_price_set = false;
        for (size_t i = 0; i < VWAP_WINDOW_MINS; ++i) state.buckets_90m[i] = MinuteBucket();
        state.roll_buy_vol_90m = 0.0;
        state.roll_sell_vol_90m = 0.0;
        state.roll_buy_dollar_90m = 0.0;
        state.roll_sell_dollar_90m = 0.0;
        restore_session_from_redis(redis, symbol, state);
    }

    if (!state.initial_oi_set && oi > 0.0) {
        state.initial_oi = oi;
        state.initial_oi_set = true;
    }

    // 1. Calculate incremental trade volume
    int64_t inc_vol = 0;
    if (cum_vol > 0) {
        if (state.last_cum_vol > 0 && cum_vol >= state.last_cum_vol) {
            inc_vol = cum_vol - state.last_cum_vol;
        }
        state.last_cum_vol = cum_vol;
    }

    // 2. Lee-Ready (1991) Trade Flow Classification (for Dual-VWAP volumes)
    double sign = 0.0;
    if (bid > 0.0 && ask > 0.0 && ask > bid) {
        double mid = (bid + ask) * 0.5;
        if (ltp > mid) sign = 1.0;
        else if (ltp < mid) sign = -1.0;
        else sign = 0.0;
    } else if (state.last_ltp > 0.0) {
        double diff = ltp - state.last_ltp;
        if (diff > 0.0) sign = 1.0;
        else if (diff < 0.0) sign = -1.0;
        else sign = 0.0;
    } else {
        sign = 0.0;
    }
    state.last_sign = sign;

    double buy_v = 0.0;
    double sell_v = 0.0;
    if (sign > 0.0) {
        buy_v = static_cast<double>(inc_vol);
    } else if (sign < 0.0) {
        sell_v = static_cast<double>(inc_vol);
    } else {
        // 50/50 split on ties / midpoint matches
        buy_v = static_cast<double>(inc_vol) * 0.5;
        sell_v = static_cast<double>(inc_vol) * 0.5;
    }
    double buy_d = buy_v * ltp;
    double sell_d = sell_v * ltp;

    // 2b. Quote Boundary Rule (100% Exact Match for ModelPOC CVD)
    double qb_sign = 0.0;
    if (bid > 0.0 && ask > 0.0) {
        if (ltp >= ask) qb_sign = 1.0;
        else if (ltp <= bid) qb_sign = -1.0;
        else qb_sign = 0.0;
    }
    double qb_delta_tick = static_cast<double>(inc_vol) * qb_sign;
    state.cum_cvd += qb_delta_tick;

    // 3. Minute boundary management
    update_minute_boundary(state, minute_ts, oi, ltp);

    // 4. Add to current 1-minute bucket and 90-minute running accumulators
    size_t b_idx = (minute_ts / 60) % VWAP_WINDOW_MINS;
    MinuteBucket& curr_b = state.buckets_90m[b_idx];
    curr_b.buy_vol += buy_v;
    curr_b.sell_vol += sell_v;
    curr_b.buy_dollar += buy_d;
    curr_b.sell_dollar += sell_d;
    curr_b.qb_delta += qb_delta_tick;
    curr_b.close_oi = oi;
    curr_b.close_price = ltp;

    state.roll_buy_vol_90m += buy_v;
    state.roll_sell_vol_90m += sell_v;
    state.roll_buy_dollar_90m += buy_d;
    state.roll_sell_dollar_90m += sell_d;

    // 5. Developing Point of Control (dPOC 5.0 pt bins from 09:20 IST) & Session VWAP
    std::string tick_date = redis_utils::format_ist_date(now_ms);
    if (tick_date == today_date) {
        if (inc_vol > 0) {
            state.cum_session_dollar += (static_cast<double>(inc_vol) * ltp);
            state.cum_session_vol += static_cast<double>(inc_vol);
        }

        // 09:15 IST = 33,300 seconds from midnight IST (+19800s offset from UTC)
        // Matches ModelPOCV2 backtest profile accumulation starting at 09:15:00 open
        int64_t ist_sec_of_day = (ts_sec + 19800) % 86400;
        if (ist_sec_of_day >= 33300 && inc_vol > 0) {
            int64_t bin_idx = static_cast<int64_t>(std::round(ltp / 5.0));
            state.poc_bins[bin_idx] += inc_vol;
            if (state.poc_bins[bin_idx] > state.max_poc_vol) {
                state.max_poc_vol = state.poc_bins[bin_idx];
                state.max_poc_bin = bin_idx;
            }
        }
    }

    state.last_ltp = ltp;
    state.last_bid = (bid > 0.0) ? bid : state.last_bid;
    state.last_ask = (ask > 0.0) ? ask : state.last_ask;
    state.last_oi = (oi > 0.0) ? oi : state.last_oi;

    // 6. Compute instantaneous metrics
    MicrostructureMetrics m;
    m.symbol = symbol;
    m.ltp = ltp;
    m.bid = state.last_bid;
    m.ask = state.last_ask;
    m.roll_buy_vol = state.roll_buy_vol_90m;
    m.roll_sell_vol = state.roll_sell_vol_90m;
    m.vwap_buy = (state.roll_buy_vol_90m > 0.0) ? (state.roll_buy_dollar_90m / state.roll_buy_vol_90m) : ltp;
    m.vwap_sell = (state.roll_sell_vol_90m > 0.0) ? (state.roll_sell_dollar_90m / state.roll_sell_vol_90m) : ltp;
    m.session_vwap = (state.cum_session_vol > 0.0) ? (state.cum_session_dollar / state.cum_session_vol) : ltp;
    m.cum_cvd = state.cum_cvd;
    m.dpoc = (state.max_poc_bin > 0) ? (state.max_poc_bin * 5.0) : (std::round(ltp / 5.0) * 5.0);
    m.updated_at_ms = now_ms;

    // Calculate 15-minute Quote Boundary CVD (Matches ModelPOC backtest)
    double cvd_15m = 0.0;
    int64_t cutoff_15m = minute_ts - (CVD_WINDOW_MINS * 60);
    for (size_t i = 0; i < VWAP_WINDOW_MINS; ++i) {
        const MinuteBucket& b = state.buckets_90m[i];
        if (b.minute_timestamp > cutoff_15m && b.minute_timestamp <= minute_ts) {
            cvd_15m += b.qb_delta;
        }
    }
    m.cvd_15m = cvd_15m;

    // Calculate 15-minute Delta OI and Delta Price from historical buckets
    if (!state.initial_price_set && ltp > 0.0) {
        state.initial_price = ltp;
        state.initial_price_set = true;
    }

    double ref_oi = 0.0;
    double ref_price = 0.0;
    int64_t best_diff = 1000000;

    for (size_t i = 0; i < VWAP_WINDOW_MINS; ++i) {
        const MinuteBucket& b = state.buckets_90m[i];
        if (b.minute_timestamp > 0 && b.minute_timestamp <= cutoff_15m && b.close_oi > 0.0) {
            int64_t diff = cutoff_15m - b.minute_timestamp;
            if (diff < best_diff) {
                best_diff = diff;
                ref_oi = b.close_oi;
                ref_price = b.close_price;
            }
        }
    }

    if (ref_oi <= 0.0) {
        ref_oi = state.initial_oi;
    }
    if (ref_price <= 0.0) {
        ref_price = state.initial_price;
    }

    m.delta_oi_15m = (ref_oi > 0.0 && oi > 0.0) ? (oi - ref_oi) : 0.0;
    m.delta_price_15m = (ref_price > 0.0 && ltp > 0.0) ? (ltp - ref_price) : 0.0;

    // Cache latest computed metrics for StrategyEngine consumption
    state.last_metrics = m;

    // 7. Write to Redis (throttled to at most once per 50ms per symbol unless volume printed)
    if (now_ms - state.last_redis_write_ms >= 50 || inc_vol > 0) {
        write_to_redis(redis, state, m);
        state.last_redis_write_ms = now_ms;
    }
}

void MicrostructureEngine::write_to_redis(
    redisContext* redis,
    SymbolState& state,
    const MicrostructureMetrics& m
) {
    if (!redis) return;

    std::string key = "md:microstructure:" + m.symbol;
    std::string lead = "NEUTRAL";
    if (m.roll_buy_vol > m.roll_sell_vol * 1.08) {
        lead = "BUYERS_DOMINANT";
    } else if (m.roll_sell_vol > m.roll_buy_vol * 1.08) {
        lead = "SELLERS_DOMINANT";
    }

    // Construct fast Redis HSET command
    redisCommand(
        redis,
        "HSET %s ltp %.2f bid %.2f ask %.2f vwap_buy %.2f vwap_sell %.2f session_vwap %.2f "
        "roll_buy_vol %.0f roll_sell_vol %.0f cvd_15m %.0f delta_oi_15m %.0f delta_price_15m %.2f "
        "dpoc %.1f cum_cvd %.0f lead %s updated_at_ms %lld",
        key.c_str(),
        m.ltp, m.bid, m.ask, m.vwap_buy, m.vwap_sell, m.session_vwap,
        m.roll_buy_vol, m.roll_sell_vol, m.cvd_15m, m.delta_oi_15m, m.delta_price_15m,
        m.dpoc, m.cum_cvd, lead.c_str(), m.updated_at_ms
    );
}

bool MicrostructureEngine::get_metrics(const std::string& symbol, MicrostructureMetrics& out) const {
    auto it = m_states.find(symbol);
    if (it == m_states.end()) return false;
    out = it->second.last_metrics;
    return true;
}

void MicrostructureEngine::restore_session_from_redis(
    redisContext* redis,
    const std::string& symbol,
    SymbolState& state
) {
    if (!redis) return;

    int64_t now_wall_sec = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    std::string today_date = redis_utils::now_ist_date();
    int64_t ist_wall_sec = (now_wall_sec + 19800) % 86400;
    int64_t today_0915_sec = now_wall_sec - ist_wall_sec + 33300;
    int64_t today_1530_sec = now_wall_sec - ist_wall_sec + 55800;

    // If starting before 09:15 AM IST, there are no session candles to restore
    if (now_wall_sec < today_0915_sec) {
        return;
    }

    std::string pattern = "md:candle:" + symbol + ":1m:*";
    redisReply* r_keys = (redisReply*)redisCommand(redis, "KEYS %s", pattern.c_str());
    if (!r_keys) return;

    if (r_keys->type == REDIS_REPLY_ARRAY && r_keys->elements > 0) {
        double sum_dollar = 0.0;
        double sum_vol = 0.0;
        for (size_t i = 0; i < r_keys->elements; ++i) {
            if (!r_keys->element[i]->str) continue;
            std::string k_str = r_keys->element[i]->str;
            size_t last_colon = k_str.rfind(':');
            if (last_colon == std::string::npos) continue;
            try {
                int64_t c_ts = std::stoll(k_str.substr(last_colon + 1));
                // Strictly require candle belongs to TODAY's date and session window
                std::string bar_date = redis_utils::format_ist_date(c_ts * 1000);
                if (bar_date != today_date) continue;
                if (c_ts >= today_0915_sec && c_ts < now_wall_sec && c_ts <= today_1530_sec) {
                    redisReply* r_c = (redisReply*)redisCommand(redis, "HMGET %s close volume", k_str.c_str());
                    if (r_c && r_c->type == REDIS_REPLY_ARRAY && r_c->elements >= 2) {
                        double c = (r_c->element[0]->str) ? std::atof(r_c->element[0]->str) : 0.0;
                        double v = (r_c->element[1]->str) ? std::atof(r_c->element[1]->str) : 0.0;
                        if (c > 0.0 && v > 0.0) {
                            sum_dollar += (c * v);
                            sum_vol += v;
                            int64_t bin_idx = static_cast<int64_t>(std::round(c / 5.0));
                            state.poc_bins[bin_idx] += v;
                            if (state.poc_bins[bin_idx] > state.max_poc_vol) {
                                state.max_poc_vol = state.poc_bins[bin_idx];
                                state.max_poc_bin = bin_idx;
                            }
                        }
                    }
                    if (r_c) freeReplyObject(r_c);
                }
            } catch (...) {}
        }
        if (sum_vol > 0.0) {
            state.cum_session_dollar = sum_dollar;
            state.cum_session_vol = sum_vol;
            // NOTE: Do NOT overwrite state.last_cum_vol with sum_vol!
            // state.last_cum_vol must track Upstox exchange-reported cumulative volume.
            std::cout << "✅ [MicrostructureEngine] Restored session VWAP from Redis for " << symbol
                      << ": VWAP = " << (sum_dollar / sum_vol) << " | Volume = " << sum_vol
                      << " | dPOC = " << (state.max_poc_bin * 5.0) << std::endl;
        }
    }
    freeReplyObject(r_keys);
}

