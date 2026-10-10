#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#include <direct.h>
#define MKDIR(d) _mkdir(d)
#else
#define MKDIR(d) mkdir(d, 0755)
#endif

// High-performance Market Tick Record (Stack-allocated, zero-allocation parsing)
struct MarketTickRecord {
    std::string symbol;
    int64_t ts_recv_ns{0};
    int64_t ts_recv_ms{0};
    int64_t ts_exchange{0};
    double ltp{0.0};
    double close{0.0};
    double volume{0.0};
    double oi{0.0};
    double iv{0.0};
    double atp{0.0};
    
    // Depth (levels 1..5)
    double bid[5]{0.0};
    int64_t bid_qty[5]{0};
    double ask[5]{0.0};
    int64_t ask_qty[5]{0};
    int64_t tbq{0};
    int64_t tsq{0};
    
    // Greeks
    double delta{0.0};
    double theta{0.0};
    double gamma{0.0};
    double vega{0.0};
    double rho{0.0};
    bool has_greeks{false};
    
    // CAS fields
    double iep{0.0};
    double reference_price{0.0};
    int64_t ieq{0};
    int64_t iiq_total{0};
    int64_t iiq_market{0};
    bool cas_eligible{false};
    
    bool is_valid{true};
};

// P4: Feed Sanity Filter & Microsecond Quality Check
inline bool validate_tick_sanity(MarketTickRecord& t) {
    // 1. Numerical sanity check
    if (t.ltp < 0.0 || std::isnan(t.ltp) || std::isinf(t.ltp)) {
        t.is_valid = false;
        return false;
    }
    // 2. Order book inverted spread check (bid > ask)
    if (t.bid[0] > 0.0 && t.ask[0] > 0.0 && t.bid[0] > t.ask[0]) {
        t.is_valid = false;
        return false;
    }
    // 3. Timestamp sanity check
    if (t.ts_exchange <= 0 && t.ts_recv_ms <= 0) {
        t.is_valid = false;
        return false;
    }
    return true;
}

// P0 (Option B) & P1: Local Write-Ahead Log (WAL) Spooler
class TickWalWriter {
public:
    TickWalWriter() 
        : m_current_hour_tag(""), m_ticks_written(0), m_bytes_buffered(0) {}

    ~TickWalWriter() {
        close();
    }

    bool init(const std::string& wal_dir) {
        m_wal_dir = wal_dir;
        ensure_directory(m_wal_dir);
        return rotate_if_needed();
    }

    void write_tick(const MarketTickRecord& t) {
        if (!t.is_valid) return;

        rotate_if_needed();

        if (!m_file.is_open()) return;

        // Fast formatting directly into pre-allocated string buffer
        m_buf.clear();
        m_buf.append("{\"sym\":\"").append(t.symbol).append("\"");
        m_buf.append(",\"ltp\":").append(std::to_string(t.ltp));
        m_buf.append(",\"close\":").append(std::to_string(t.close));
        m_buf.append(",\"vol\":").append(std::to_string(static_cast<int64_t>(t.volume)));
        m_buf.append(",\"oi\":").append(std::to_string(static_cast<int64_t>(t.oi)));
        m_buf.append(",\"iv\":").append(std::to_string(t.iv));
        m_buf.append(",\"ts_recv\":").append(std::to_string(t.ts_recv_ms));
        m_buf.append(",\"ts_recv_ns\":").append(std::to_string(t.ts_recv_ns));
        m_buf.append(",\"ts_exch\":").append(std::to_string(t.ts_exchange));
        
        // Best Bid / Ask
        m_buf.append(",\"bid\":").append(std::to_string(t.bid[0]));
        m_buf.append(",\"bid_qty\":").append(std::to_string(t.bid_qty[0]));
        m_buf.append(",\"ask\":").append(std::to_string(t.ask[0]));
        m_buf.append(",\"ask_qty\":").append(std::to_string(t.ask_qty[0]));
        
        // Depth 2..5
        for (int i = 1; i < 5; ++i) {
            std::string sfx = std::to_string(i + 1);
            if (t.bid[i] > 0.0 || t.ask[i] > 0.0) {
                m_buf.append(",\"bid").append(sfx).append("\":").append(std::to_string(t.bid[i]));
                m_buf.append(",\"bid_qty").append(sfx).append("\":").append(std::to_string(t.bid_qty[i]));
                m_buf.append(",\"ask").append(sfx).append("\":").append(std::to_string(t.ask[i]));
                m_buf.append(",\"ask_qty").append(sfx).append("\":").append(std::to_string(t.ask_qty[i]));
            }
        }
        
        m_buf.append(",\"tbq\":").append(std::to_string(t.tbq));
        m_buf.append(",\"tsq\":").append(std::to_string(t.tsq));

        if (t.has_greeks) {
            m_buf.append(",\"delta\":").append(std::to_string(t.delta));
            m_buf.append(",\"theta\":").append(std::to_string(t.theta));
            m_buf.append(",\"gamma\":").append(std::to_string(t.gamma));
            m_buf.append(",\"vega\":").append(std::to_string(t.vega));
            m_buf.append(",\"rho\":").append(std::to_string(t.rho));
        }

        if (t.iep > 0.0) {
            m_buf.append(",\"iep\":").append(std::to_string(t.iep));
            m_buf.append(",\"ieq\":").append(std::to_string(t.ieq));
            m_buf.append(",\"iiq_total\":").append(std::to_string(t.iiq_total));
            m_buf.append(",\"iiq_market\":").append(std::to_string(t.iiq_market));
            m_buf.append(",\"ref_px\":").append(std::to_string(t.reference_price));
            m_buf.append(",\"cas_el\":").append(t.cas_eligible ? "1" : "0");
        }

        m_buf.append("}\n");

        m_file.write(m_buf.data(), m_buf.size());
        m_ticks_written++;
        m_bytes_buffered += m_buf.size();

        // Flush buffer every 1,000 ticks or 64 KB
        if (m_ticks_written % 1000 == 0 || m_bytes_buffered >= 65536) {
            m_file.flush();
            m_bytes_buffered = 0;
        }
    }

    void flush() {
        if (m_file.is_open()) {
            m_file.flush();
            m_bytes_buffered = 0;
        }
    }

    void close() {
        if (m_file.is_open()) {
            m_file.flush();
            m_file.close();
        }
    }

    uint64_t get_ticks_written() const { return m_ticks_written; }

private:
    std::string m_wal_dir;
    std::string m_current_hour_tag;
    std::ofstream m_file;
    std::string m_buf;
    uint64_t m_ticks_written;
    size_t m_bytes_buffered;

    static void ensure_directory(const std::string& path) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            MKDIR(path.c_str());
        }
    }

    static std::string get_hour_tag() {
        auto now = std::chrono::system_clock::now();
        std::time_t tt = std::chrono::system_clock::to_time_t(now);
        std::tm tm_buf;
#ifdef _WIN32
        localtime_s(&tm_buf, &tt);
#else
        localtime_r(&tt, &tm_buf);
#endif
        std::ostringstream ss;
        ss << std::put_time(&tm_buf, "%Y%m%d_%H");
        return ss.str();
    }

    bool rotate_if_needed() {
        std::string tag = get_hour_tag();
        if (tag != m_current_hour_tag) {
            if (m_file.is_open()) {
                m_file.flush();
                m_file.close();
            }
            m_current_hour_tag = tag;
            std::string filename = m_wal_dir + "/wal_ticks_" + tag + ".jsonl";
            m_file.open(filename, std::ios::out | std::ios::app);
            if (!m_file.is_open()) {
                std::cerr << "❌ [TickWalWriter] Error opening WAL file: " << filename << std::endl;
                return false;
            }
            std::cout << "📝 [TickWalWriter] Active WAL rotated to: " << filename << std::endl;
        }
        return true;
    }
};
