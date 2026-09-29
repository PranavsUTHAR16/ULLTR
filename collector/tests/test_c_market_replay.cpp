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
#include <hiredis/hiredis.h>

#include "../src/strategy_types.hpp"
#include "../src/fifo_pool.hpp"
#include "../src/strategy_engine.hpp"
#include "../src/microstructure_engine.hpp"

struct MendProgressionRecord {
    std::string time_str;
    double spot = 0.0;
    double s_star = 0.0;
    double maker_net_delta = 0.0;
};

std::map<std::string, MendProgressionRecord> load_mend_progression(const std::string& csv_path) {
    std::map<std::string, MendProgressionRecord> progression;
    if (csv_path.empty()) return progression;
    std::ifstream file(csv_path);
    if (!file.is_open()) {
        std::cout << "[INFO] Dynamic MEND progression file not found: " << csv_path << " (MEND will rely on live chain metrics)" << std::endl;
        return progression;
    }
    std::string line;
    std::getline(file, line); // Header
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> row;
        while (std::getline(ss, token, ',')) row.push_back(token);
        if (row.size() >= 9) {
            std::string bar_time = row[0];
            std::string t_str = (bar_time.size() >= 16) ? bar_time.substr(11, 5) : bar_time;
            MendProgressionRecord rec;
            rec.time_str = t_str;
            rec.spot = std::stod(row[2]);
            if (!row[5].empty()) rec.s_star = std::stod(row[5]);
            if (!row[8].empty()) rec.maker_net_delta = std::stod(row[8]);
            progression[t_str] = rec;
        }
    }
    return progression;
}

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

int64_t parse_epoch(const std::string& ts_str) {
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
        return (local_sec - 19800);
    }
    return 0;
}

std::vector<BarRecord> load_bars(const std::string& csv_path) {
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

std::unordered_map<std::string, std::vector<OptionQuoteRecord>> load_option_quotes(
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

            std::string field = std::to_string(q.strike) + ":" + q.option_type;
            chain_meta[field] = q.symbol;

            quotes_by_time[q.time_str].push_back(q);
        }
    }
    return quotes_by_time;
}

int main(int argc, char* argv[]) {
    std::string bars_csv = "/Users/prana/Desktop/open_source/web/collector/session_bars_20260908.csv";
    std::string opts_csv = "/Users/prana/Desktop/open_source/web/collector/session_options_20260908.csv";
    std::string mend_csv = "";
    std::string model_filter = "all";
    std::string target_date = "";

    for (int a = 1; a < argc; ++a) {
        std::string arg = argv[a];
        if (arg == "--model" && a + 1 < argc) {
            model_filter = argv[++a];
        } else if (arg == "--bars" && a + 1 < argc) {
            bars_csv = argv[++a];
        } else if (arg == "--options" && a + 1 < argc) {
            opts_csv = argv[++a];
        } else if (arg == "--mend" && a + 1 < argc) {
            mend_csv = argv[++a];
        } else if (arg == "--date" && a + 1 < argc) {
            target_date = argv[++a];
        } else if (arg.rfind("--", 0) != 0) {
            if (bars_csv == "/Users/prana/Desktop/open_source/web/collector/session_bars_20260908.csv") {
                bars_csv = arg;
            } else {
                opts_csv = arg;
            }
        }
    }

    std::cout << "=================================================================================" << std::endl;
    std::cout << "🏛️ C++ STRATEGY ENGINE REAL-TIME REPLAY & VALIDATION BENCHMARK" << std::endl;
    std::cout << "   Testing Native C++ Dual M.E.N.D. Portfolio (Series + Intraday)" << std::endl;
    std::cout << "=================================================================================" << std::endl;

    // 1. Connect to Redis via Unix socket
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

    // 2. Load historical 1m bars
    std::vector<BarRecord> bars = load_bars(bars_csv);
    std::cout << "📥 Loaded " << bars.size() << " 1-minute futures bars from " << bars_csv << std::endl;
    if (bars.empty()) return 1;

    if (target_date.empty() && !bars.empty()) {
        target_date = bars[0].bar_1m.substr(0, 10);
    }
    std::cout << "📅 Target Replay Date Identified: " << target_date << std::endl;

    // 3. Load historical 1m option quotes
    std::unordered_map<std::string, std::string> chain_meta;
    auto quotes_by_time = load_option_quotes(opts_csv, chain_meta);
    std::cout << "📥 Loaded " << quotes_by_time.size() << " distinct option timesteps (" 
              << chain_meta.size() << " contracts) from " << opts_csv << std::endl;

    // 4. Seed Option Chain Metadata into Redis (cleaning any stale chains first)
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
    std::string chain_key = "chain:NIFTY:" + target_date;
    for (const auto& kv : chain_meta) {
        redisAppendCommand(redis, "HSET %s %s %s", chain_key.c_str(), kv.first.c_str(), kv.second.c_str());
    }
    for (size_t i = 0; i < chain_meta.size(); ++i) {
        redisReply* r = nullptr;
        if (redisGetReply(redis, (void**)&r) == REDIS_OK && r) freeReplyObject(r);
    }
    std::cout << "✅ Seeded " << chain_meta.size() << " contracts into Redis " << chain_key << std::endl;

    // 4b. Load MEND 15m True Black-Scholes Progression (On-the-fly dynamic session file)
    if (mend_csv.empty() && !bars_csv.empty()) {
        std::string d_clean = target_date;
        d_clean.erase(std::remove(d_clean.begin(), d_clean.end(), '-'), d_clean.end());
        size_t last_slash = bars_csv.find_last_of("/\\");
        std::string dir = (last_slash != std::string::npos) ? bars_csv.substr(0, last_slash + 1) : "";
        std::string candidate = dir + "session_mend_" + d_clean + ".csv";
        std::ifstream f_test(candidate);
        if (f_test.is_open()) {
            mend_csv = candidate;
        }
    }
    auto mend_progression = load_mend_progression(mend_csv);
    if (!mend_progression.empty()) {
        std::cout << "📥 Loaded " << mend_progression.size() << " dynamic on-the-fly MEND progression intervals from " << mend_csv << std::endl;
    } else {
        std::cout << "ℹ️ Running MEND on live real-time chain metrics (zero pre-computed files)." << std::endl;
    }

    // 5. Initialize FIFO Pool and C++ Strategy Engine
    FIFOPool pool(20000.0);
    StrategyEngine engine(pool);
    if (model_filter == "intra" || model_filter == "mend_intra" || model_filter == "intraday") {
        engine.enable_model_mend_series = false;
        engine.enable_model_mend_intraday = true;
        engine.enable_spatial_box = false;
        std::cout << "🎯 Model Filter Active: Running ONLY Model M.E.N.D. (Intraday)" << std::endl;
    } else if (model_filter == "series" || model_filter == "mend_series") {
        engine.enable_model_mend_series = true;
        engine.enable_model_mend_intraday = false;
        engine.enable_spatial_box = false;
        std::cout << "🎯 Model Filter Active: Running ONLY Model M.E.N.D. (Series)" << std::endl;
    } else {
        engine.enable_model_mend_series = true;
        engine.enable_model_mend_intraday = true;
        engine.enable_spatial_box = false;
        std::cout << "🎯 Model Filter Active: Running Dual M.E.N.D. Portfolio (Series + Intraday)" << std::endl;
    }

    engine.set_trade_callback([](const std::string& evt, const UnifiedPosition& pos, const std::string& msg) {
        std::cout << "📢 [EVENT: " << evt << "] " << pos.position_id << " (" << pos.model_name 
                  << ") " << pos.lots << " lots " << pos.strike << " " 
                  << option_type_to_string(pos.option_type) 
                  << " | " << msg << std::endl;
    });

    Candle1M prev_bar;
    auto t_start = std::chrono::high_resolution_clock::now();

    // 6. Step through each 1-minute bar with dynamic Redis quote feeder
    for (size_t i = 0; i < bars.size(); ++i) {
        const auto& r = bars[i];
        
        // A. Inject dynamic 1m option quotes into Redis for this exact minute
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

        // B. Update MEND True Black-Scholes S* & Index Spot Quote in Redis
        double current_spot = r.close;
        if (!mend_progression.empty()) {
            auto it_prog = mend_progression.upper_bound(r.time_str);
            if (it_prog != mend_progression.begin()) {
                --it_prog;
                redisCommand(redis, "SET mend:s_star %f", it_prog->second.s_star);
                redisCommand(redis, "SET mend:maker_delta %f", it_prog->second.maker_net_delta);
                current_spot = it_prog->second.spot;
            }
        }

        redisReply* r_hset = (redisReply*)redisCommand(redis, "HSET %s symbol %s ltp %f close %f volume %lld",
                     "md:quote:NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty 50", current_spot, current_spot, (long long)r.volume);
        if (r_hset) freeReplyObject(r_hset);
        redisReply* r_spot_ptr = (redisReply*)redisCommand(redis, "SET spot:NIFTY %s", "NSE_INDEX|Nifty 50");
        if (r_spot_ptr) freeReplyObject(r_spot_ptr);

        // C. Construct Bar & Metrics
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

        // D. Trigger on_1m_bar
        engine.on_1m_bar(redis, "NSE_INDEX|Nifty 50", bar, (i > 0 ? prev_bar : bar), m);
        prev_bar = bar;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    // 7. Print Scorecard
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

        std::cout << "  • [" << std::setw(12) << pos.model_name << "] " 
                  << std::setw(5) << pos.entry_time << " -> " << std::setw(5) << pos.exit_time << " | "
                  << pos.strike << " " << option_type_to_string(pos.option_type) 
                  << " (" << pos.lots << " lots) | Entry Ask: Rs " << std::fixed << std::setprecision(2) << pos.entry_option_price
                  << " -> Exit Bid: Rs " << pos.exit_option_price
                  << " | Net: " << std::showpos << pts << std::noshowpos << " pts (Rs " << std::showpos << pnl << std::noshowpos << ")"
                  << " | Reason: " << pos.exit_reason << std::endl;
    }

    std::cout << "---------------------------------------------------------------------------------" << std::endl;
    std::cout << "Win / Loss Record           : " << wins << " Wins / " << losses << " Losses (" 
              << (pool.closed_positions.empty() ? 0.0 : (wins * 100.0 / pool.closed_positions.size())) << "%)" << std::endl;
    std::cout << "Realized Equity PnL         : Rs " << std::showpos << total_pnl << std::noshowpos << std::endl;
    std::cout << "Pool Free Cash              : Rs " << pool.get_free_cash() << std::endl;
    std::cout << "=================================================================================" << std::endl;

    redisFree(redis);
    return 0;
}
