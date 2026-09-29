#include "strike_resolver.hpp"
#include <iostream>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// init_front_expiry
// ---------------------------------------------------------------------------
void StrikeResolver::init_front_expiry() {
    if (!m_redis) return;

    std::string today_date = redis_utils::now_ist_date();
    std::string min_chain_key = "chain:NIFTY:" + today_date;

    redisReply* r_keys = (redisReply*)redisCommand(m_redis, "KEYS chain:NIFTY:*");
    if (!r_keys) return;

    if (r_keys->type == REDIS_REPLY_ARRAY && r_keys->elements > 0) {
        std::vector<std::string> chains;

        // Prefer chains on or after today
        for (size_t i = 0; i < r_keys->elements; ++i) {
            if (!r_keys->element[i]->str) continue;
            std::string k = r_keys->element[i]->str;
            if (k.find(":meta") == std::string::npos && k >= min_chain_key) {
                chains.push_back(k);
            }
        }

        // Fallback: any chain without :meta
        if (chains.empty()) {
            for (size_t i = 0; i < r_keys->elements; ++i) {
                if (!r_keys->element[i]->str) continue;
                std::string k = r_keys->element[i]->str;
                if (k.find(":meta") == std::string::npos) {
                    chains.push_back(k);
                }
            }
        }

        std::sort(chains.begin(), chains.end());
        if (!chains.empty()) {
            m_cached_front_expiry = chains.front();
            std::cout << "🎯 [StrikeResolver] Front Expiry: " << m_cached_front_expiry << std::endl;
        }
    }
    freeReplyObject(r_keys);
}

// ---------------------------------------------------------------------------
// resolve_instrument_key
// ---------------------------------------------------------------------------
std::string StrikeResolver::resolve_instrument_key(int64_t strike, OptionType type) const {
    if (m_cached_front_expiry.empty() || !m_redis) return "";

    std::string field = std::to_string(strike) + ":" + option_type_to_string(type);
    redisReply* r = (redisReply*)redisCommand(m_redis, "HGET %s %s",
                                               m_cached_front_expiry.c_str(), field.c_str());
    std::string token;
    if (r) {
        if (r->type == REDIS_REPLY_STRING && r->str) token = r->str;
        freeReplyObject(r);
    }
    return token;
}

// ---------------------------------------------------------------------------
// resolve_price — STRICT: no fallback to historical close or delta estimate.
//   Returns 0.0 on DATA_STALE. Callers MUST check.
// ---------------------------------------------------------------------------
double StrikeResolver::resolve_price(int64_t strike, OptionType type, bool is_ask) const {
    if (!m_redis) return 0.0;

    // Step 1: try via chain token
    std::string token = resolve_instrument_key(strike, type);
    if (!token.empty()) {
        // Try ask or bid first
        const char* primary_field = is_ask ? "ask" : "bid";
        redisReply* r = (redisReply*)redisCommand(m_redis, "HGET md:quote:%s %s",
                                                   token.c_str(), primary_field);
        double price = 0.0;
        if (r) {
            if (r->type == REDIS_REPLY_STRING && r->str) price = std::atof(r->str);
            freeReplyObject(r);
        }

        // If bid/ask is stale, try ltp
        if (price <= 0.0) {
            redisReply* r2 = (redisReply*)redisCommand(m_redis, "HGET md:quote:%s ltp", token.c_str());
            if (r2) {
                if (r2->type == REDIS_REPLY_STRING && r2->str) price = std::atof(r2->str);
                freeReplyObject(r2);
            }
        }

        if (price > 0.0) return price;
    }

    // DATA_STALE — return 0.0 explicitly. Callers must NOT use this for SL/entry decisions.
    return 0.0;
}

// ---------------------------------------------------------------------------
// resolve_target_strike
// ---------------------------------------------------------------------------
TargetStrikeResult StrikeResolver::resolve_target_strike(
    double fut_price, OptionType type, double target_premium) const {

    TargetStrikeResult res;
    if (!m_redis || m_cached_front_expiry.empty()) return res;

    const std::string type_suffix = (type == OptionType::CE) ? ":CE" : ":PE";

    redisReply* r_all = (redisReply*)redisCommand(m_redis, "HGETALL %s",
                                                   m_cached_front_expiry.c_str());
    if (!r_all || r_all->type != REDIS_REPLY_ARRAY) {
        if (r_all) freeReplyObject(r_all);
        return res; // DATA_STALE
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
                double ask = 0.0, bid = 0.0;

                redisReply* rq = (redisReply*)redisCommand(m_redis, "HMGET md:quote:%s ask bid ltp",
                                                           token.c_str());
                if (rq && rq->type == REDIS_REPLY_ARRAY && rq->elements >= 3) {
                    if (rq->element[0]->str) ask = std::atof(rq->element[0]->str);
                    if (rq->element[1]->str) bid = std::atof(rq->element[1]->str);
                    if (ask <= 0.0 && rq->element[2]->str) ask = std::atof(rq->element[2]->str);
                    if (bid <= 0.0) bid = ask;
                }
                if (rq) freeReplyObject(rq);

                // Only include if live quote exists (>= 15 INR minimum liquidity threshold)
                if (ask >= 15.0) {
                    candidates.push_back({strike, token, bid, ask});
                }
            } catch (...) {
                continue;
            }
        }
    }
    freeReplyObject(r_all);

    if (candidates.empty()) return res; // DATA_STALE

    // Prefer highest premium that is still ≤ target_premium
    std::vector<Candidate> under;
    for (const auto& c : candidates) {
        if (c.ask <= target_premium) under.push_back(c);
    }

    Candidate chosen;
    if (!under.empty()) {
        chosen = *std::max_element(under.begin(), under.end(),
                                   [](const Candidate& a, const Candidate& b) {
                                       return a.ask < b.ask;
                                   });
    } else {
        // All over target: take cheapest
        chosen = *std::min_element(candidates.begin(), candidates.end(),
                                   [](const Candidate& a, const Candidate& b) {
                                       return a.ask < b.ask;
                                   });
    }

    res.strike = chosen.strike;
    res.instrument_key = chosen.token;
    res.ask = chosen.ask;
    res.bid = chosen.bid;
    return res;
}

// ---------------------------------------------------------------------------
// get_current_dte
// ---------------------------------------------------------------------------
int StrikeResolver::get_current_dte() const {
    if (m_cached_front_expiry.empty()) return 2;
    size_t last_colon = m_cached_front_expiry.rfind(':');
    if (last_colon == std::string::npos) return 2;
    std::string exp_date = m_cached_front_expiry.substr(last_colon + 1, 10);

    std::string today = redis_utils::now_ist_date();
    if (exp_date == today) return 0;

    int y1, m1, d1, y2, m2, d2;
    if (sscanf(today.c_str(),    "%d-%d-%d", &y1, &m1, &d1) != 3) return 2;
    if (sscanf(exp_date.c_str(), "%d-%d-%d", &y2, &m2, &d2) != 3) return 2;

    std::tm tm1{}; tm1.tm_year = y1 - 1900; tm1.tm_mon = m1 - 1; tm1.tm_mday = d1;
    std::tm tm2{}; tm2.tm_year = y2 - 1900; tm2.tm_mon = m2 - 1; tm2.tm_mday = d2;

    std::time_t t1 = std::mktime(&tm1);
    std::time_t t2 = std::mktime(&tm2);
    if (t1 == -1 || t2 == -1) return 2;

    double diff = std::difftime(t2, t1) / 86400.0;
    return std::max(0, static_cast<int>(std::round(diff)));
}
