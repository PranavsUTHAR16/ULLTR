#pragma once

#include "strategy_types.hpp"
#include "redis_utils.hpp"
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <cmath>
#include <algorithm>

/**
 * StrikeResolver
 * --------------
 * Single-responsibility: resolves option prices and strike selection from Redis.
 * No strategy logic, no state mutated beyond expiry cache.
 *
 * All price lookups are strictly live bid/ask/ltp from md:quote:{instrument_key}.
 * There is ZERO fallback to any historical close or estimated delta price.
 * If a live price cannot be found, 0.0 is returned and callers must treat that
 * as DATA_STALE — entries are blocked, exits are deferred.
 */
class StrikeResolver {
public:
    explicit StrikeResolver(redisContext* redis) : m_redis(redis) {}

    /// Refresh the front-expiry chain key from Redis. Call once at startup.
    void init_front_expiry();

    /// Return the cached chain key (e.g. "chain:NIFTY:2026-09-29").
    const std::string& front_expiry() const { return m_cached_front_expiry; }

    /// Live bid or ask for a known strike. Returns 0.0 on DATA_STALE.
    double resolve_price(int64_t strike, OptionType type, bool is_ask) const;

    /**
     * Resolve the best candidate strike whose ASK is closest to (and ≤) target_premium.
     * If no strike fits under the cap, returns the cheapest available strike.
     * Returns a zeroed TargetStrikeResult if live data is completely unavailable.
     */
    TargetStrikeResult resolve_target_strike(double fut_price,
                                             OptionType type,
                                             double target_premium = 155.0) const;

    /// Days-to-expiry based on front expiry date vs today. Returns 2 as a safe default.
    int get_current_dte() const;

    /// Set the redis context (e.g., after reconnect).
    void set_redis(redisContext* redis) { m_redis = redis; }

private:
    redisContext* m_redis = nullptr;
    std::string m_cached_front_expiry;

    std::string resolve_instrument_key(int64_t strike, OptionType type) const;
};
