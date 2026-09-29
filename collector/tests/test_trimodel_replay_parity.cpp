#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <cstdint>
#include <chrono>
#include <ctime>
#include <map>
#include <cassert>
#include <hiredis/hiredis.h>

#include "../src/strategy_types.hpp"
#include "../src/fifo_pool.hpp"
#include "../src/strategy_engine.hpp"
#include "../src/microstructure_engine.hpp"

struct BarRecord {
    std::string bar_1m;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    int64_t volume = 0;
    double delta = 0.0;
    double oi = 0.0;
    double dpoc = 0.0;
    double cum_cvd = 0.0;
    std::string time_str;
    double vwap = 0.0;
    double delta_cvd_15m = 0.0;
    double delta_oi_15m = 0.0;
    double delta_price_15m = 0.0;
};

struct OptionQuoteRecord {
    std::string bar_1m;
    std::string symbol;
    int64_t strike = 0;
    std::string option_type;
    double ltp = 0.0;
    double bid = 0.0;
    uint32_t bid_qty = 0;
    double ask = 0.0;
    uint32_t ask_qty = 0;
    double delta = 0.0;
    double theta = 0.0;
    double gamma = 0.0;
    double vega = 0.0;
    double iv = 0.0;
    uint64_t oi = 0;
    uint64_t volume = 0;
    double close = 0.0;
    std::string time_str;
};

static int64_t parse_epoch(const std::string& ts_str) {
    struct tm t = {0};
    int y = 0, m = 0, d = 0, hh = 0, mm = 0, ss = 0;
    if (std::sscanf(ts_str.c_str(), "%d-%d-%d %d:%d:%d", &y, &m, &d, &hh, &mm, &ss) == 6) {
        t.tm_year = y - 1900;
        t.tm_mon = m - 1;
        t.tm_mday = d;
        t.tm_hour = hh;
        t.tm_min = mm;
        t.tm_sec = ss;
        t.tm_isdst = -1;
        time_t local_sec = timegm(&t);
        return (local_sec - 19800); // Convert from IST to UTC epoch seconds
    }
    return 0;
}

static std::vector<BarRecord> load_bars(const std::string& csv_path) {
    std::vector<BarRecord> bars;
    std::ifstream file(csv_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open CSV: " << csv_path << std::endl;
        return bars;
    }
    std::string line;
    std::getline(file, line); // Header
    
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> row;
        while (std::getline(ss, token, ',')) {
            row.push_back(token);
        }
        if (row.size() >= 15) {
            BarRecord r;
            r.bar_1m = row[0];
            r.open = std::stod(row[1]);
            r.high = std::stod(row[2]);
            r.low = std::stod(row[3]);
            r.close = std::stod(row[4]);
            r.volume = std::stoll(row[5]);
            r.delta = std::stod(row[6]);
            r.oi = std::stod(row[7]);
            r.dpoc = std::stod(row[8]);
            r.cum_cvd = std::stod(row[9]);
            r.time_str = row[10];
            r.vwap = std::stod(row[11]);
            r.delta_cvd_15m = std::stod(row[12]);
            r.delta_oi_15m = std::stod(row[13]);
            r.delta_price_15m = std::stod(row[14]);
            bars.push_back(r);
        }
    }
    return bars;
}

static std::unordered_map<std::string, std::vector<OptionQuoteRecord>> load_option_quotes(
    const std::string& csv_path,
    std::unordered_map<std::string, std::string>& chain_meta
) {
    std::unordered_map<std::string, std::vector<OptionQuoteRecord>> quotes_by_time;
    std::ifstream file(csv_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open options CSV: " << csv_path << std::endl;
        return quotes_by_time;
    }
    std::string line;
    std::getline(file, line); // Header
    
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> row;
        while (std::getline(ss, token, ',')) {
            row.push_back(token);
        }
        if (row.size() >= 18) {
            OptionQuoteRecord q;
            q.bar_1m = row[0];
            q.symbol = row[1];
            q.strike = std::stoll(row[2]);
            q.option_type = row[3];
            q.ltp = std::stod(row[4]);
            q.bid = std::stod(row[5]);
            q.bid_qty = std::stoul(row[6]);
            q.ask = std::stod(row[7]);
            q.ask_qty = std::stoul(row[8]);
            q.delta = std::stod(row[9]);
            q.theta = std::stod(row[10]);
            q.gamma = std::stod(row[11]);
            q.vega = std::stod(row[12]);
            q.iv = std::stod(row[13]);
            q.oi = std::stoull(row[14]);
            q.volume = std::stoull(row[15]);
            q.close = std::stod(row[16]);
            q.time_str = row[17];

            if (q.strike > 0 && (q.option_type == "CE" || q.option_type == "PE")) {
                std::string field = std::to_string(q.strike) + ":" + q.option_type;
                chain_meta[field] = q.symbol;
            }

            quotes_by_time[q.time_str].push_back(q);
        }
    }
    return quotes_by_time;
}

void run_session_replay(
    redisContext* redis,
    const std::string& target_date,
    const std::string& bars_csv,
    const std::string& opts_csv
) {
    std::cout << "\n=================================================================================" << std::endl;
    std::cout << "🏛️ C++ STRATEGY ENGINE REPLAY PARITY VERIFICATION" << std::endl;
    std::cout << "   Target Session : " << target_date << std::endl;
    std::cout << "   Bars Source    : " << bars_csv << std::endl;
    std::cout << "   Options Source : " << opts_csv << std::endl;
    std::cout << "=================================================================================" << std::endl;

    // 1. Load bars and option quotes
    std::vector<BarRecord> bars = load_bars(bars_csv);
    std::cout << "📥 Loaded " << bars.size() << " 1-minute futures bars." << std::endl;
    if (bars.empty()) {
        std::cerr << "❌ No bars loaded. Aborting." << std::endl;
        return;
    }

    std::unordered_map<std::string, std::string> chain_meta;
    auto quotes_by_time = load_option_quotes(opts_csv, chain_meta);
    std::cout << "📥 Loaded " << quotes_by_time.size() << " distinct option minutes ("
              << chain_meta.size() << " contracts)." << std::endl;

    // 2. Clean stale chain keys from Redis
    redisReply* old_chains = (redisReply*)redisCommand(redis, "KEYS chain:NIFTY:*");
    if (old_chains) {
        if (old_chains->type == REDIS_REPLY_ARRAY) {
            for (size_t i = 0; i < old_chains->elements; ++i) {
                if (old_chains->element[i]->str) {
                    redisCommand(redis, "DEL %s", old_chains->element[i]->str);
                }
            }
        }
        freeReplyObject(old_chains);
    }

    // 3. Seed Option Chain Metadata into Redis
    std::string chain_key = "chain:NIFTY:" + target_date;
    for (const auto& kv : chain_meta) {
        redisAppendCommand(redis, "HSET %s %s %s", chain_key.c_str(), kv.first.c_str(), kv.second.c_str());
    }
    for (size_t i = 0; i < chain_meta.size(); ++i) {
        redisReply* r = nullptr;
        if (redisGetReply(redis, (void**)&r) == REDIS_OK && r) freeReplyObject(r);
    }
    std::cout << "✅ Seeded " << chain_meta.size() << " contracts into Redis " << chain_key << std::endl;

    // 4. Initialize FIFO Pool and Tri-Model Strategy Engine
    FIFOPool pool(20000.0);
    StrategyEngine engine(pool);
    engine.set_front_expiry(chain_key);
    engine.enable_model_poc_v2 = true;
    engine.enable_model_spatial_box = true;
    engine.enable_model_dalton_va = true;
    engine.enable_model_tpo_poc = false; // Blocked per user request

    std::vector<std::string> trade_events;
    engine.set_trade_callback([&trade_events](const std::string& evt, const UnifiedPosition& pos, const std::string& msg) {
        std::ostringstream ss;
        ss << "📢 [" << evt << "] " << pos.position_id << " (" << pos.model_name 
           << ") " << pos.lots << " lots " << pos.strike << " " 
           << option_type_to_string(pos.option_type) 
           << " | Ask/Bid: " << pos.entry_option_price << " / " << pos.exit_option_price
           << " | Pts: " << pos.get_points()
           << " | " << msg;
        trade_events.push_back(ss.str());
        std::cout << ss.str() << std::endl;
    });

    Candle1M prev_bar;
    auto t_start = std::chrono::high_resolution_clock::now();

    // 5. Step through each 1-minute bar
    for (size_t i = 0; i < bars.size(); ++i) {
        const auto& r = bars[i];
        
        // A. Inject dynamic 1m option quotes into Redis for this minute
        auto it_opts = quotes_by_time.find(r.time_str);
        if (it_opts != quotes_by_time.end()) {
            for (const auto& q : it_opts->second) {
                redisAppendCommand(
                    redis, 
                    "HSET md:quote:%s symbol %s ltp %f close %f bid %f bid_qty %u ask %f ask_qty %u delta %f theta %f gamma %f vega %f iv %f oi %llu volume %llu",
                    q.symbol.c_str(), q.symbol.c_str(), q.ltp, q.close, q.bid, q.bid_qty, q.ask, q.ask_qty,
                    q.delta, q.theta, q.gamma, q.vega, q.iv, (unsigned long long)q.oi, (unsigned long long)q.volume
                );
            }
            for (size_t k = 0; k < it_opts->second.size(); ++k) {
                redisReply* reply = nullptr;
                if (redisGetReply(redis, (void**)&reply) == REDIS_OK && reply) freeReplyObject(reply);
            }
        }

        // B. Update Index Spot Quote in Redis
        redisReply* r_hset = (redisReply*)redisCommand(redis, "HSET %s symbol %s ltp %f close %f volume %lld",
                     "md:quote:NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty 50", r.close, r.close, (long long)r.volume);
        if (r_hset) freeReplyObject(r_hset);
        redisReply* r_spot_ptr = (redisReply*)redisCommand(redis, "SET spot:NIFTY %s", "NSE_INDEX|Nifty 50");
        if (r_spot_ptr) freeReplyObject(r_spot_ptr);

        // C. Construct Bar & Microstructure Metrics
        Candle1M bar;
        bar.minute_ts = parse_epoch(r.bar_1m);
        bar.open = r.open;
        bar.high = r.high;
        bar.low = r.low;
        bar.close = r.close;
        bar.volume = r.volume;

        MicrostructureMetrics m;
        m.ltp = r.close;
        m.dpoc = r.dpoc;
        m.session_vwap = r.vwap;
        m.cum_cvd = r.cum_cvd;
        m.cvd_15m = r.delta_cvd_15m;
        m.delta_oi_15m = r.delta_oi_15m;
        m.delta_price_15m = r.delta_price_15m;

        // D. Trigger on_1m_bar on C++ StrategyEngine
        engine.on_1m_bar(redis, "NSE_INDEX|Nifty 50", bar, (i > 0 ? prev_bar : bar), m);
        prev_bar = bar;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    // 6. Scorecard Summary
    std::cout << "\n=================================================================================" << std::endl;
    std::cout << "📊 C++ REPLAY PERFORMANCE SCORECARD (SESSION: " << target_date << ")" << std::endl;
    std::cout << "=================================================================================" << std::endl;
    std::cout << "Total 1m Bars Evaluated     : " << bars.size() << " bars in " << elapsed_ms << " ms (" 
              << (bars.size() / (elapsed_ms / 1000.0)) << " bars/sec)" << std::endl;
    std::cout << "Active Positions Remaining : " << pool.active_positions.size() << std::endl;
    std::cout << "Total Closed Trades        : " << pool.closed_positions.size() << std::endl;

    double total_pnl = 0.0;
    int wins = 0;
    int losses = 0;

    for (const auto& pos : pool.closed_positions) {
        double pts = pos.get_points();
        double pnl = pts * (pos.lots * 65);
        total_pnl += pnl;
        if (pts > 0) wins++; else losses++;

        std::cout << "  • [" << std::setw(18) << pos.model_name << "] " 
                  << std::setw(5) << pos.entry_time << " -> " << std::setw(5) << pos.exit_time << " | "
                  << pos.strike << " " << option_type_to_string(pos.option_type) 
                  << " (" << pos.lots << " lots) | Entry Ask: Rs " << std::fixed << std::setprecision(2) << pos.entry_option_price
                  << " -> Exit Bid: Rs " << pos.exit_option_price
                  << " | Net: " << std::showpos << pts << std::noshowpos << " pts (Rs " << std::showpos << pnl << std::noshowpos << ")"
                  << " | Reason: " << pos.exit_reason << std::endl;
    }

    std::cout << "\n__JSON_START__\n[";
    for (size_t i = 0; i < pool.closed_positions.size(); ++i) {
        const auto& pos = pool.closed_positions[i];
        double pts = pos.get_points();
        double pnl = pos.realized_pnl;
        if (i > 0) std::cout << ",";
        std::cout << "{"
                  << "\"trade_id\":" << (i + 1) << ","
                  << "\"position_id\":\"" << pos.position_id << "\","
                  << "\"model_name\":\"" << pos.model_name << "\","
                  << "\"symbol\":\"" << pos.symbol << "\","
                  << "\"strike\":" << pos.strike << ","
                  << "\"option_type\":\"" << option_type_to_string(pos.option_type) << "\","
                  << "\"lots\":" << pos.lots << ","
                  << "\"entry_time\":\"" << pos.entry_time << "\","
                  << "\"exit_time\":\"" << pos.exit_time << "\","
                  << "\"entry_ask\":" << pos.entry_option_price << ","
                  << "\"exit_bid\":" << pos.exit_option_price << ","
                  << "\"net_points\":" << pts << ","
                  << "\"realized_pnl\":" << pnl << ","
                  << "\"exit_reason\":\"" << pos.exit_reason << "\""
                  << "}";
    }
    std::cout << "]\n__JSON_END__\n";

    std::cout << "---------------------------------------------------------------------------------" << std::endl;
    std::cout << "Win / Loss Record           : " << wins << " Wins / " << losses << " Losses (" 
              << (pool.closed_positions.empty() ? 0.0 : (wins * 100.0 / pool.closed_positions.size())) << "%)" << std::endl;
    std::cout << "Realized Equity PnL         : Rs " << std::showpos << total_pnl << std::noshowpos << std::endl;
    std::cout << "Pool Free Cash              : Rs " << pool.get_free_cash() << std::endl;
    std::cout << "=================================================================================" << std::endl;
}

int main(int argc, char* argv[]) {
    // 1. Connect to Redis via Unix socket or TCP
    redisContext* redis = redisConnectUnix("/Users/prana/Desktop/open_source/web/redis.sock");
    if (!redis || redis->err) {
        std::cout << "[WARN] Redis Unix socket unavailable, falling back to 127.0.0.1:6379..." << std::endl;
        redis = redisConnect("127.0.0.1", 6379);
    }
    if (!redis || redis->err) {
        std::cerr << "❌ Redis connection failed: " << (redis ? redis->errstr : "null") << std::endl;
        return 1;
    }
    std::cout << "✅ Connected to Redis successfully." << std::endl;

    std::string target_date = "2026-09-23";
    if (argc > 1) {
        target_date = argv[1];
    }

    if (target_date == "all" || target_date == "both") {
        // Replay 2026-09-22 first
        run_session_replay(
            redis,
            "2026-09-22",
            "/Users/prana/Desktop/open_source/web/collector/session_bars_20260922.csv",
            "/Users/prana/Desktop/open_source/web/collector/session_options_20260922.csv"
        );
        // Replay 2026-09-23 second
        run_session_replay(
            redis,
            "2026-09-23",
            "/Users/prana/Desktop/open_source/web/collector/session_bars_20260923.csv",
            "/Users/prana/Desktop/open_source/web/collector/session_options_20260923.csv"
        );
    } else {
        std::string date_clean = target_date;
        date_clean.erase(std::remove(date_clean.begin(), date_clean.end(), '-'), date_clean.end());
        std::string bars_csv = "/Users/prana/Desktop/open_source/web/collector/session_bars_" + date_clean + ".csv";
        std::string opts_csv = "/Users/prana/Desktop/open_source/web/collector/session_options_" + date_clean + ".csv";

        run_session_replay(redis, target_date, bars_csv, opts_csv);
    }

    redisFree(redis);
    return 0;
}
