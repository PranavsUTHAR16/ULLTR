#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>
#include <hiredis/hiredis.h>

struct Candle {
    int64_t timestamp = 0; // Unix epoch in seconds
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    int64_t volume = 0;
    std::string status;    // "historical", "live", "reconciled"
};

class CandleManager {
public:
    CandleManager(
        const std::string& redis_host,
        int redis_port,
        const std::string& redis_unix_socket,
        const std::string& token,
        const std::vector<std::string>& instruments,
        const std::string& config_path
    );

    ~CandleManager();

    // 1. Core historical seeding
    void catch_up_historical_candles(redisContext* sync_redis);

    // 2. Active candles mapping
    void init_active_candles(redisContext* sync_redis);

    // 3. Real-time tick aggregation (runs on hot ingestion thread)
    void process_tick_candle(
        redisContext* sync_redis,
        const std::string& symbol,
        double price,
        int64_t tick_volume,
        int64_t ts_exchange
    );

    // Reload token upon dynamic authentication refresh
    void reload_token();

    // Bar Close Notification Callback
    using BarCloseCallback = std::function<void(const std::string& symbol, const std::string& tf, const Candle& closed_candle)>;
    void set_bar_close_callback(BarCloseCallback cb) { m_bar_close_callback = cb; }
    void add_instrument(const std::string& sym) {
        if (std::find(m_instruments.begin(), m_instruments.end(), sym) == m_instruments.end()) {
            m_instruments.push_back(sym);
        }
    }

private:
    BarCloseCallback m_bar_close_callback;
    // Dynamic HTTPS client using Boost.Beast (used only for startup catch-up)
    std::string https_get(const std::string& target);

    // Configuration properties
    std::string m_redis_host;
    int m_redis_port;
    std::string m_redis_unix_socket;
    std::string m_token;
    std::vector<std::string> m_instruments;
    std::string m_config_path;

    // Active tick tracking states (hot thread storage)
    std::unordered_map<std::string, int64_t> m_last_volumes;
    // Map of symbol -> Map of timeframe ("1m", "3m", "5m", "15m", "30m") -> Candle
    std::unordered_map<std::string, std::unordered_map<std::string, Candle>> m_active_candles;
    std::unordered_map<std::string, int> m_timeframes;
};

