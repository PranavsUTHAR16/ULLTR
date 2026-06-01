#include "candle_manager.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <stdexcept>
#include <cstdio>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;

// --- TIMEZONE & STRING UTILS ---

inline bool is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

inline int days_in_month(int y, int m) {
    static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m == 2 && is_leap(y)) return 29;
    return days[m - 1];
}

int64_t get_utc_epoch(int year, int month, int day, int hour, int minute, int second) {
    int64_t days = 0;
    for (int y = 1970; y < year; ++y) {
        days += is_leap(y) ? 366 : 365;
    }
    for (int m = 1; m < month; ++m) {
        days += days_in_month(year, m);
    }
    days += (day - 1);
    
    int64_t seconds = days * 86400 + hour * 3600 + minute * 60 + second;
    return seconds;
}

int64_t parse_iso_timestamp(const std::string& ts_str) {
    // Format: "2026-05-29T15:29:00+05:30"
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    int tz_hour = 5, tz_min = 30;
    char sign = '+';
    
    int parsed = std::sscanf(ts_str.c_str(), "%d-%d-%dT%d:%d:%d%c%d:%d", 
                            &year, &month, &day, &hour, &minute, &second, &sign, &tz_hour, &tz_min);
    
    int64_t epoch = get_utc_epoch(year, month, day, hour, minute, second);
    int64_t offset_sec = tz_hour * 3600 + tz_min * 60;
    
    if (sign == '+') {
        epoch -= offset_sec;
    } else if (sign == '-') {
        epoch += offset_sec;
    }
    return epoch;
}

std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped << std::hex;
    for (char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << std::setfill('0') << std::uppercase << int(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}

// --- CONSTRUCTOR & DESTRUCTOR ---

CandleManager::CandleManager(
    const std::string& redis_host,
    int redis_port,
    const std::string& redis_unix_socket,
    const std::string& token,
    const std::vector<std::string>& instruments,
    const std::string& config_path
) : m_redis_host(redis_host),
    m_redis_port(redis_port),
    m_redis_unix_socket(redis_unix_socket),
    m_token(token),
    m_instruments(instruments),
    m_config_path(config_path),
    m_stop_reco(false)
{
    m_timeframes = {
        {"1m", 60},
        {"3m", 180},
        {"5m", 300},
        {"15m", 900},
        {"30m", 1800}
    };
}

CandleManager::~CandleManager() {
    stop_reconciliation();
}

// --- TOKEN REFRESH FLOW ---

void CandleManager::reload_token() {
    try {
        std::ifstream f(m_config_path);
        if (f.is_open()) {
            json cfg;
            f >> cfg;
            std::string token_file = cfg.value("access_token_file", "");
            if (!token_file.empty()) {
                std::ifstream tf(token_file);
                if (tf.is_open()) {
                    json tk;
                    tf >> tk;
                    m_token = tk.at("access_token").get<std::string>();
                    std::cout << "[CandleManager] Successfully reloaded access token from: " << token_file << std::endl;
                    return;
                }
            }
        }
    } catch (...) {}
}

// --- BOOST BEAST HTTPS CLIENT ---

std::string CandleManager::https_get(const std::string& target) {
    std::string host = "api.upstox.com";
    std::string port = "443";
    
    int attempt = 0;
    while (attempt < 2) {
        try {
            net::io_context ioc;
            ssl::context ctx{ssl::context::tls_client};
            ctx.set_verify_mode(ssl::verify_none);
            
            tcp::resolver resolver{ioc};
            ssl::stream<tcp::socket> stream{ioc, ctx};
            
            auto const results = resolver.resolve(host, port);
            net::connect(stream.next_layer(), results);
            
            SSL_set_tlsext_host_name(stream.native_handle(), host.c_str());
            stream.handshake(ssl::stream_base::client);
            
            http::request<http::string_body> req{http::verb::get, target, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "upstox-cpp-collector/1.0");
            req.set(http::field::accept, "application/json");
            req.set(http::field::authorization, "Bearer " + m_token);
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);
            
            beast::error_code ec;
            stream.shutdown(ec);
            
            if (res.result() == http::status::unauthorized) {
                std::cout << "⚠️ CandleManager API Unauthorized (401). Triggering token refresh..." << std::endl;
                int ret = std::system("python /Users/prana/Desktop/open_source/web/login/auth.py");
                if (ret != 0) {
                    std::system("python3 /Users/prana/Desktop/open_source/web/login/auth.py");
                }
                reload_token();
                attempt++;
                continue;
            }
            
            if (res.result() != http::status::ok) {
                std::cerr << "[HTTPS GET] API returned error status: " << res.result() << " for target: " << target << std::endl;
                return "";
            }
            
            return res.body();
        } catch (const std::exception& e) {
            std::cerr << "[HTTPS GET] Exception: " << e.what() << std::endl;
            attempt++;
        }
    }
    return "";
}

// --- SEEDING & AGGREGATION UTILS ---

std::vector<Candle> aggregate_candles(const std::vector<Candle>& candles_1m, int interval_sec) {
    std::vector<Candle> candles_agg;
    if (candles_1m.empty()) return candles_agg;
    
    std::unordered_map<int64_t, std::vector<Candle>> groups;
    std::vector<int64_t> group_keys;
    
    for (const auto& c : candles_1m) {
        int64_t ts_parent = (c.timestamp / interval_sec) * interval_sec;
        if (groups.find(ts_parent) == groups.end()) {
            group_keys.push_back(ts_parent);
        }
        groups[ts_parent].push_back(c);
    }
    
    std::sort(group_keys.begin(), group_keys.end());
    
    for (int64_t ts : group_keys) {
        const auto& group = groups[ts];
        if (group.empty()) continue;
        
        Candle agg;
        agg.timestamp = ts;
        agg.open = group.front().open;
        agg.close = group.back().close;
        
        double high = group.front().high;
        double low = group.front().low;
        int64_t volume = 0;
        
        for (const auto& c : group) {
            high = std::max(high, c.high);
            low = std::min(low, c.low);
            volume += c.volume;
        }
        
        agg.high = high;
        agg.low = low;
        agg.volume = volume;
        agg.status = "historical";
        
        candles_agg.push_back(agg);
    }
    
    return candles_agg;
}

void cleanup_old_redis_candles(redisContext* sync_redis, const std::string& symbol, const std::string& timeframe) {
    std::string zset_key = "md:candles:" + symbol + ":" + timeframe;
    redisReply* range_reply = (redisReply*)redisCommand(sync_redis, "ZRANGE %s 0 -1", zset_key.c_str());
    if (range_reply && range_reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> old_keys;
        for (size_t i = 0; i < range_reply->elements; ++i) {
            std::string ts = range_reply->element[i]->str;
            old_keys.push_back("md:candle:" + symbol + ":" + timeframe + ":" + ts);
        }
        if (!old_keys.empty()) {
            for (const auto& key : old_keys) {
                redisAppendCommand(sync_redis, "DEL %s", key.c_str());
            }
            for (size_t i = 0; i < old_keys.size(); ++i) {
                redisReply* r;
                redisGetReply(sync_redis, (void**)&r);
                if (r) freeReplyObject(r);
            }
        }
    }
    if (range_reply) freeReplyObject(range_reply);
}

void seed_candles_to_redis(redisContext* sync_redis, const std::string& symbol, const std::string& timeframe, const std::vector<Candle>& candles) {
    if (candles.empty()) return;
    
    cleanup_old_redis_candles(sync_redis, symbol, timeframe);
    
    std::string zset_key = "md:candles:" + symbol + ":" + timeframe;
    redisReply* del_reply = (redisReply*)redisCommand(sync_redis, "DEL %s", zset_key.c_str());
    if (del_reply) freeReplyObject(del_reply);
    
    for (const auto& c : candles) {
        std::string candle_key = "md:candle:" + symbol + ":" + timeframe + ":" + std::to_string(c.timestamp);
        redisAppendCommand(sync_redis, "HSET %s open %f high %f low %f close %f volume %lld status %s",
                           candle_key.c_str(), c.open, c.high, c.low, c.close, c.volume, c.status.c_str());
        redisAppendCommand(sync_redis, "ZADD %s %lld %lld", zset_key.c_str(), c.timestamp, c.timestamp);
    }
    
    for (size_t i = 0; i < candles.size() * 2; ++i) {
        redisReply* r;
        redisGetReply(sync_redis, (void**)&r);
        if (r) freeReplyObject(r);
    }
}

// --- HISTORICAL SEEDING CORE ---

void CandleManager::catch_up_historical_candles(redisContext* sync_redis) {
    std::cout << "⏳ [CandleManager] Catching up last 5 trading days via HTTPS..." << std::endl;
    
    std::time_t t = std::time(nullptr);
    std::tm* now = std::localtime(&t);
    char today_str[20];
    std::strftime(today_str, sizeof(today_str), "%Y-%m-%d", now);
    
    std::time_t from_time = t - 8 * 86400; // 8 calendar days
    std::tm* from = std::localtime(&from_time);
    char from_str[20];
    std::strftime(from_str, sizeof(from_str), "%Y-%m-%d", from);
    
    std::cout << "[CandleManager] Active Query Range: " << from_str << " to " << today_str << std::endl;
    
    for (size_t idx = 0; idx < m_instruments.size(); ++idx) {
        const std::string& symbol = m_instruments[idx];
        std::cout << "   [" << (idx + 1) << "/" << m_instruments.size() << "] Catching up history for " << symbol << "..." << std::endl;
        
        std::string encoded_symbol = url_encode(symbol);
        
        // 1. Intraday Minute Candles
        std::string target_intra = "/v3/historical-candle/intraday/" + encoded_symbol + "/minutes/1";
        std::string resp_intra = https_get(target_intra);
        
        // 2. Historical Minute Candles
        std::string target_hist = "/v3/historical-candle/" + encoded_symbol + "/minutes/1/" + today_str + "/" + from_str;
        std::string resp_hist = https_get(target_hist);
        
        std::vector<Candle> merged_candles;
        
        // Parse historical
        if (!resp_hist.empty()) {
            try {
                auto res = json::parse(resp_hist);
                if (res.value("status", "") == "success") {
                    auto candles = res["data"]["candles"];
                    for (const auto& c : candles) {
                        Candle cand;
                        cand.timestamp = parse_iso_timestamp(c[0].get<std::string>());
                        cand.open = c[1].get<double>();
                        cand.high = c[2].get<double>();
                        cand.low = c[3].get<double>();
                        cand.close = c[4].get<double>();
                        cand.volume = c[5].get<int64_t>();
                        cand.status = "historical";
                        merged_candles.push_back(cand);
                    }
                }
            } catch (...) {}
        }
        
        // Parse intraday
        if (!resp_intra.empty()) {
            try {
                auto res = json::parse(resp_intra);
                if (res.value("status", "") == "success") {
                    auto candles = res["data"]["candles"];
                    for (const auto& c : candles) {
                        Candle cand;
                        cand.timestamp = parse_iso_timestamp(c[0].get<std::string>());
                        cand.open = c[1].get<double>();
                        cand.high = c[2].get<double>();
                        cand.low = c[3].get<double>();
                        cand.close = c[4].get<double>();
                        cand.volume = c[5].get<int64_t>();
                        cand.status = "historical";
                        merged_candles.push_back(cand);
                    }
                }
            } catch (...) {}
        }
        
        if (merged_candles.empty()) {
            std::cout << "   ⚠️ [CandleManager] No historical candles returned for: " << symbol << std::endl;
            continue;
        }
        
        // Sort and drop duplicates
        std::sort(merged_candles.begin(), merged_candles.end(), [](const Candle& a, const Candle& b) {
            return a.timestamp < b.timestamp;
        });
        
        std::vector<Candle> unique_candles;
        for (const auto& c : merged_candles) {
            if (unique_candles.empty() || unique_candles.back().timestamp != c.timestamp) {
                unique_candles.push_back(c);
            }
        }
        
        // Group unique IST calendar days to get exactly 5 trading days
        std::unordered_map<int64_t, bool> unique_days;
        std::vector<int64_t> sorted_days;
        for (const auto& c : unique_candles) {
            int64_t day_ist = (c.timestamp + 19800) / 86400;
            if (unique_days.find(day_ist) == unique_days.end()) {
                unique_days[day_ist] = true;
                sorted_days.push_back(day_ist);
            }
        }
        
        std::sort(sorted_days.begin(), sorted_days.end());
        std::vector<Candle> final_1m;
        
        if (sorted_days.size() > 5) {
            std::unordered_map<int64_t, bool> target_days;
            for (size_t i = sorted_days.size() - 5; i < sorted_days.size(); ++i) {
                target_days[sorted_days[i]] = true;
            }
            for (const auto& c : unique_candles) {
                int64_t day_ist = (c.timestamp + 19800) / 86400;
                if (target_days.find(day_ist) != target_days.end()) {
                    final_1m.push_back(c);
                }
            }
        } else {
            final_1m = unique_candles;
        }
        
        // Seed 1m candles
        seed_candles_to_redis(sync_redis, symbol, "1m", final_1m);
        
        // Aggregate higher timeframes and seed them
        std::vector<std::pair<std::string, int>> tfs = {
            {"3m", 180},
            {"5m", 300},
            {"15m", 900},
            {"30m", 1800}
        };
        for (const auto& tf_pair : tfs) {
            std::vector<Candle> agg = aggregate_candles(final_1m, tf_pair.second);
            seed_candles_to_redis(sync_redis, symbol, tf_pair.first, agg);
        }
        
        std::cout << "   ✅ Seeding and aggregation successfully complete." << std::endl;
    }
}

// --- ACTIVE STATE LOADER ---

void CandleManager::init_active_candles(redisContext* sync_redis) {
    for (const auto& symbol : m_instruments) {
        // Load initial cumulative volumes
        std::string quote_key = "md:quote:" + symbol;
        redisReply* vol_reply = (redisReply*)redisCommand(sync_redis, "HGET %s volume", quote_key.c_str());
        if (vol_reply && vol_reply->type == REDIS_REPLY_STRING) {
            try {
                m_last_volumes[symbol] = std::stoll(vol_reply->str);
            } catch (...) {
                m_last_volumes[symbol] = 0;
            }
        } else {
            m_last_volumes[symbol] = 0;
        }
        if (vol_reply) freeReplyObject(vol_reply);
        
        // Load active candles for all timeframes
        for (const auto& tf_pair : m_timeframes) {
            std::string tf = tf_pair.first;
            std::string zset_key = "md:candles:" + symbol + ":" + tf;
            redisReply* z_reply = (redisReply*)redisCommand(sync_redis, "ZRANGE %s -1 -1", zset_key.c_str());
            if (z_reply && z_reply->type == REDIS_REPLY_ARRAY && z_reply->elements > 0) {
                std::string ts_str = z_reply->element[0]->str;
                std::string candle_key = "md:candle:" + symbol + ":" + tf + ":" + ts_str;
                
                redisReply* h_reply = (redisReply*)redisCommand(sync_redis, "HGETALL %s", candle_key.c_str());
                if (h_reply && h_reply->type == REDIS_REPLY_ARRAY && h_reply->elements > 0) {
                    Candle c;
                    c.timestamp = std::stoll(ts_str);
                    
                    for (size_t i = 0; i < h_reply->elements; i += 2) {
                        std::string field = h_reply->element[i]->str;
                        std::string val = h_reply->element[i+1]->str;
                        
                        if (field == "open") c.open = std::stod(val);
                        else if (field == "high") c.high = std::stod(val);
                        else if (field == "low") c.low = std::stod(val);
                        else if (field == "close") c.close = std::stod(val);
                        else if (field == "volume") c.volume = std::stoll(val);
                        else if (field == "status") c.status = val;
                    }
                    
                    m_active_candles[symbol][tf] = c;
                }
                if (h_reply) freeReplyObject(h_reply);
            }
            if (z_reply) freeReplyObject(z_reply);
        }
    }
}

// --- REAL-TIME TICK AGGREGATOR ---

void CandleManager::process_tick_candle(
    redisContext* sync_redis,
    const std::string& symbol,
    double price,
    int64_t tick_volume,
    int64_t ts_exchange
) {
    if (std::find(m_instruments.begin(), m_instruments.end(), symbol) == m_instruments.end()) {
        return;
    }
    
    if (m_last_volumes.find(symbol) == m_last_volumes.end()) {
        m_last_volumes[symbol] = tick_volume;
        return;
    }
    
    int64_t inc_vol = 0;
    if (tick_volume > 0) {
        int64_t last_vol = m_last_volumes[symbol];
        if (last_vol > 0) {
            inc_vol = std::max(static_cast<int64_t>(0), tick_volume - last_vol);
        }
        m_last_volumes[symbol] = tick_volume;
    }
    
    int64_t ts_sec = ts_exchange > 0 ? (ts_exchange / 1000) : (std::time(nullptr));
    
    for (const auto& tf_pair : m_timeframes) {
        const std::string& tf = tf_pair.first;
        int duration = tf_pair.second;
        
        int64_t ts_candle = (ts_sec / duration) * duration;
        
        auto& symbol_candles = m_active_candles[symbol];
        bool has_active = symbol_candles.find(tf) != symbol_candles.end();
        
        if (!has_active || symbol_candles[tf].timestamp < ts_candle) {
            // A candle interval has officially closed!
            if (has_active && tf == "1m") {
                // Add the closed 1m candle to background queue
                std::lock_guard<std::mutex> lock(m_reco_mutex);
                m_reco_queue.push_back({symbol, symbol_candles[tf].timestamp});
            }
            
            Candle c;
            c.timestamp = ts_candle;
            c.open = price;
            c.high = price;
            c.low = price;
            c.close = price;
            c.volume = inc_vol;
            c.status = "live";
            
            symbol_candles[tf] = c;
            
            std::string candle_key = "md:candle:" + symbol + ":" + tf + ":" + std::to_string(ts_candle);
            std::string zset_key = "md:candles:" + symbol + ":" + tf;
            
            redisAppendCommand(sync_redis, "HSET %s open %f high %f low %f close %f volume %lld status %s",
                               candle_key.c_str(), price, price, price, price, inc_vol, "live");
            redisAppendCommand(sync_redis, "ZADD %s %lld %lld", zset_key.c_str(), ts_candle, ts_candle);
            
            redisReply* r1; redisGetReply(sync_redis, (void**)&r1); if (r1) freeReplyObject(r1);
            redisReply* r2; redisGetReply(sync_redis, (void**)&r2); if (r2) freeReplyObject(r2);
        } else {
            auto& c = symbol_candles[tf];
            c.high = std::max(c.high, price);
            c.low = std::min(c.low, price);
            c.close = price;
            c.volume += inc_vol;
            c.status = "live";
            
            std::string candle_key = "md:candle:" + symbol + ":" + tf + ":" + std::to_string(ts_candle);
            redisReply* up_reply = (redisReply*)redisCommand(sync_redis, "HSET %s high %f low %f close %f volume %lld status %s",
                                                           candle_key.c_str(), c.high, c.low, c.close, c.volume, "live");
            if (up_reply) freeReplyObject(up_reply);
        }
    }
}

// --- SELF-HEALING RECONCILIATION THREAD ---

void CandleManager::start_reconciliation() {
    m_stop_reco = false;
    m_reco_thread = std::thread(&CandleManager::reconciliation_loop, this);
}

void CandleManager::stop_reconciliation() {
    m_stop_reco = true;
    if (m_reco_thread.joinable()) {
        m_reco_thread.join();
    }
}

void CandleManager::reconciliation_loop() {
    std::cout << "🩺 [CandleManager] Native self-healing C++ reconciliation thread started." << std::endl;
    
    // Background thread establishes its own distinct, thread-safe Redis connection
    redisContext* reco_redis = nullptr;
    if (!m_redis_unix_socket.empty()) {
        reco_redis = redisConnectUnix(m_redis_unix_socket.c_str());
    } else {
        reco_redis = redisConnect(m_redis_host.c_str(), m_redis_port);
    }
    
    if (!reco_redis || reco_redis->err) {
        std::cerr << "❌ [CandleManager] Background reconciliation Redis connection failed!" << std::endl;
        if (reco_redis) redisFree(reco_redis);
        return;
    }
    
    while (!m_stop_reco) {
        std::pair<std::string, int64_t> item;
        bool has_item = false;
        
        {
            std::lock_guard<std::mutex> lock(m_reco_mutex);
            if (!m_reco_queue.empty()) {
                item = m_reco_queue.front();
                m_reco_queue.erase(m_reco_queue.begin());
                has_item = true;
            }
        }
        
        if (!has_item) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        
        // Wait 5 seconds for broker API synchronization
        for (int i = 0; i < 10 && !m_stop_reco; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        
        if (m_stop_reco) break;
        
        std::string symbol = item.first;
        int64_t ts_closed = item.second;
        
        std::string encoded_symbol = url_encode(symbol);
        std::string target = "/v3/historical-candle/intraday/" + encoded_symbol + "/minutes/1";
        
        std::string response = https_get(target);
        if (response.empty()) continue;
        
        try {
            auto res = json::parse(response);
            if (res.value("status", "") == "success") {
                auto candles = res["data"]["candles"];
                bool matched = false;
                
                for (const auto& c : candles) {
                    std::string ts_str = c[0].get<std::string>();
                    int64_t ts_api = parse_iso_timestamp(ts_str);
                    
                    if (ts_api == ts_closed) {
                        double api_o = c[1].get<double>();
                        double api_h = c[2].get<double>();
                        double api_l = c[3].get<double>();
                        double api_c = c[4].get<double>();
                        int64_t api_v = c[5].get<int64_t>();
                        
                        // Cross-check
                        std::string candle_key = "md:candle:" + symbol + ":1m:" + std::to_string(ts_closed);
                        redisReply* r_check = (redisReply*)redisCommand(reco_redis, "HGETALL %s", candle_key.c_str());
                        
                        bool discrepancy = false;
                        if (!r_check || r_check->type != REDIS_REPLY_ARRAY || r_check->elements == 0) {
                            discrepancy = true;
                        } else {
                            double cur_o = 0, cur_h = 0, cur_l = 0, cur_c = 0;
                            int64_t cur_v = 0;
                            for (size_t i = 0; i < r_check->elements; i += 2) {
                                std::string field = r_check->element[i]->str;
                                std::string val = r_check->element[i+1]->str;
                                if (field == "open") cur_o = std::stod(val);
                                else if (field == "high") cur_h = std::stod(val);
                                else if (field == "low") cur_l = std::stod(val);
                                else if (field == "close") cur_c = std::stod(val);
                                else if (field == "volume") cur_v = std::stoll(val);
                            }
                            
                            if (std::abs(cur_o - api_o) > 0.01 ||
                                std::abs(cur_h - api_h) > 0.01 ||
                                std::abs(cur_l - api_l) > 0.01 ||
                                std::abs(cur_c - api_c) > 0.01 ||
                                std::abs(cur_v - api_v) > 0) {
                                discrepancy = true;
                            }
                        }
                        if (r_check) freeReplyObject(r_check);
                        
                        if (discrepancy) {
                            std::cout << "   ⚠️ [CandleManager] Discrepancy detected for " << symbol << " 1m at " << ts_closed << "! Self-healing..." << std::endl;
                            redisReply* set_reply = (redisReply*)redisCommand(reco_redis, 
                                "HSET %s open %f high %f low %f close %f volume %lld status %s",
                                candle_key.c_str(), api_o, api_h, api_l, api_c, api_v, "reconciled");
                            if (set_reply) freeReplyObject(set_reply);
                            
                            // Re-aggregate parents
                            propagate_parent_recalculations(reco_redis, symbol, ts_closed);
                        } else {
                            redisReply* set_reply = (redisReply*)redisCommand(reco_redis, "HSET %s status %s", candle_key.c_str(), "reconciled");
                            if (set_reply) freeReplyObject(set_reply);
                        }
                        matched = true;
                        break;
                    }
                }
                
                if (!matched) {
                    // Check if candle is just absent in API (sometimes brokers take slightly longer)
                    // We can re-queue it one time if it is extremely fresh
                    std::time_t now_time = std::time(nullptr);
                    if (now_time - ts_closed < 180) {
                        std::lock_guard<std::mutex> lock(m_reco_mutex);
                        m_reco_queue.push_back({symbol, ts_closed});
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[CandleManager] Reconciliation thread parsing error: " << e.what() << std::endl;
        }
    }
    
    redisFree(reco_redis);
    std::cout << "🩺 [CandleManager] Background reconciliation thread cleanly terminated." << std::endl;
}

void CandleManager::propagate_parent_recalculations(
    redisContext* reco_redis,
    const std::string& symbol,
    int64_t ts_candle
) {
    std::vector<std::pair<std::string, int>> parent_timeframes = {
        {"3m", 180},
        {"5m", 300},
        {"15m", 900},
        {"30m", 1800}
    };
    
    for (const auto& p : parent_timeframes) {
        const std::string& tf = p.first;
        int duration = p.second;
        
        int64_t ts_parent = (ts_candle / duration) * duration;
        
        std::vector<Candle> valid_candles;
        for (int64_t t = ts_parent; t < ts_parent + duration; t += 60) {
            std::string candle_key = "md:candle:" + symbol + ":1m:" + std::to_string(t);
            redisReply* r = (redisReply*)redisCommand(reco_redis, "HGETALL %s", candle_key.c_str());
            if (r && r->type == REDIS_REPLY_ARRAY && r->elements > 0) {
                Candle c;
                c.timestamp = t;
                for (size_t i = 0; i < r->elements; i += 2) {
                    std::string field = r->element[i]->str;
                    std::string val = r->element[i+1]->str;
                    if (field == "open") c.open = std::stod(val);
                    else if (field == "high") c.high = std::stod(val);
                    else if (field == "low") c.low = std::stod(val);
                    else if (field == "close") c.close = std::stod(val);
                    else if (field == "volume") c.volume = std::stoll(val);
                }
                valid_candles.push_back(c);
            }
            if (r) freeReplyObject(r);
        }
        
        if (valid_candles.empty()) continue;
        
        try {
            double o = valid_candles.front().open;
            double c = valid_candles.back().close;
            double h = valid_candles.front().high;
            double l = valid_candles.front().low;
            int64_t v = 0;
            
            for (const auto& candle : valid_candles) {
                h = std::max(h, candle.high);
                l = std::min(l, candle.low);
                v += candle.volume;
            }
            
            std::string parent_key = "md:candle:" + symbol + ":" + tf + ":" + std::to_string(ts_parent);
            std::string zset_key = "md:candles:" + symbol + ":" + tf;
            
            redisReply* r_hset = (redisReply*)redisCommand(reco_redis, 
                "HSET %s open %f high %f low %f close %f volume %lld status %s",
                parent_key.c_str(), o, h, l, c, v, "reconciled");
            if (r_hset) freeReplyObject(r_hset);
            
            redisReply* r_zadd = (redisReply*)redisCommand(reco_redis, "ZADD %s %lld %lld", zset_key.c_str(), ts_parent, ts_parent);
            if (r_zadd) freeReplyObject(r_zadd);
            
            std::cout << "   🔄 [CandleManager] Propagated aggregation for " << symbol << " " << tf << " at " << ts_parent << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "[CandleManager] Parent re-aggregation error: " << e.what() << std::endl;
        }
    }
}
