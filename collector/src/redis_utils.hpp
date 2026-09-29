#pragma once

#include <string>
#include <ctime>
#include <chrono>
#include <cstdio>

namespace redis_utils {

/// Format epoch_ms as HH:MM in IST (UTC+5:30).
inline std::string format_ist_time(int64_t epoch_ms) {
    int64_t ist_sec = (epoch_ms / 1000) + 19800; // UTC + 5:30
    int sec_of_day = static_cast<int>(ist_sec % 86400);
    if (sec_of_day < 0) sec_of_day += 86400;
    int hour = sec_of_day / 3600;
    int min  = (sec_of_day % 3600) / 60;
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%02d:%02d", hour, min);
    return std::string(buf);
}

/// Format epoch_ms as YYYY-MM-DD in IST.
inline std::string format_ist_date(int64_t epoch_ms) {
    std::time_t raw = static_cast<std::time_t>((epoch_ms / 1000) + 19800);
    std::tm tm_buf{};
#if defined(_WIN32)
    gmtime_s(&tm_buf, &raw);
#else
    gmtime_r(&raw, &tm_buf);
#endif
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
                  tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday);
    return std::string(buf);
}

/// Return current IST time string as HH:MM.
inline std::string now_ist_time() {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return format_ist_time(ms);
}

/// Return current IST date string as YYYY-MM-DD.
inline std::string now_ist_date() {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return format_ist_date(ms);
}

} // namespace redis_utils
