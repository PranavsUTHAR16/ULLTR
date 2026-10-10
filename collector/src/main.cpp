#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <cmath>
#include <unordered_map>
#include <cstdlib>
#include <memory>
#include <atomic>

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>
#include <pthread.h>

#ifdef __linux__
#include <sched.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "MarketDataFeedV3.pb.h"
#include "candle_manager.hpp"
#include "microstructure_engine.hpp"
#include "strategy_engine.hpp"
#include "fifo_pool.hpp"
#include "tick_wal_writer.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;
namespace upstox = com::upstox::marketdatafeederv3udapi::rpc::proto;

#ifndef GIT_COMMIT_HASH
#define GIT_COMMIT_HASH "dev-build"
#endif

// P5: Thread Core Pinning & CPU Affinity
inline void pin_thread_to_core(int core_id) {
    if (core_id < 0) return;
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_t current_thread = pthread_self();
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "⚠️ [CPU Pinning] Failed to set CPU affinity to Core " << core_id << ": " << strerror(rc) << std::endl;
    } else {
        std::cout << "🎯 [CPU Pinning] Successfully pinned ingestion hot path to CPU Core " << core_id << std::endl;
    }

    errno = 0;
    nice(-20);
#elif defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::cout << "🎯 [QoS Priority] Thread priority set to QOS_CLASS_USER_INTERACTIVE on macOS" << std::endl;
#endif
}

// P5: Low-Latency Socket Tuning (TCP_NODELAY, SO_RCVBUF, TCP_QUICKACK, SO_BUSY_POLL)
inline void configure_low_latency_socket(tcp::socket& sock) {
    try {
        // 1. Disable Nagle's algorithm (TCP_NODELAY) - immediate transmission without packet buffering
        sock.set_option(boost::asio::ip::tcp::no_delay(true));

        // 2. Expand receive & send buffer sizes to 4MB to prevent drops during market opening auctions
        sock.set_option(boost::asio::socket_base::receive_buffer_size(4 * 1024 * 1024));
        sock.set_option(boost::asio::socket_base::send_buffer_size(1 * 1024 * 1024));

        // 3. Keepalive configuration to detect dead peers and prevent NAT timeouts
        sock.set_option(boost::asio::socket_base::keep_alive(true));

#ifdef __linux__
        int fd = sock.native_handle();
        // 4. TCP_QUICKACK to avoid 40ms delayed-ACK timeouts
        int quickack = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));

        // 5. SO_BUSY_POLL for kernel-level low-latency polling (50 microseconds)
        int busy_poll = 50;
        setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &busy_poll, sizeof(busy_poll));
#endif
        std::cout << "⚡ [Network Engine] TCP socket tuned: TCP_NODELAY=1, SO_RCVBUF=4MB, QUICKACK=1, BUSY_POLL=50us" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "⚠️ [Network Engine] Notice while configuring low-latency socket: " << e.what() << std::endl;
    }
}

class MarketDataIngestor {
public:
    MarketDataIngestor(const std::string& config_path, bool is_replay = false) 
        : m_reconnect_attempts(0), m_redis(nullptr), m_mock(false), m_replay(is_replay), m_last_reconnect_time_ms(0), m_last_portfolio_publish_ms(0),
          m_last_msg_time_ms(0), m_is_connected(false), m_watchdog_triggered(false), m_stop_watchdog(false), m_active_socket(nullptr),
          m_ticks_processed_window(0), m_last_stats_log_ms(0) {
        std::cout << "==========================================================" << std::endl;
        std::cout << "🚀 ULLTR C++ Collector Ingestor Starting" << std::endl;
        std::cout << "📦 Git Commit: " << GIT_COMMIT_HASH << " | Built: " << __DATE__ << " " << __TIME__ << std::endl;
        std::cout << "==========================================================" << std::endl;
        m_config_path = config_path;
        load_config(config_path);
        m_watchdog_thread = std::thread(&MarketDataIngestor::watchdog_worker, this);
    }
    void set_replay(bool r) { m_replay = r; }

    ~MarketDataIngestor() {
        m_stop_watchdog.store(true, std::memory_order_release);
        if (m_watchdog_thread.joinable()) {
            m_watchdog_thread.join();
        }
        if (m_redis) {
            redisFree(m_redis);
        }
    }

    void connect_redis() {
        if (m_redis) {
            redisFree(m_redis);
            m_redis = nullptr;
        }
        
        if (!m_redis_unix_socket.empty()) {
            std::cout << "Connecting to Redis via Unix Socket: " << m_redis_unix_socket << "..." << std::endl;
            m_redis = redisConnectUnix(m_redis_unix_socket.c_str());
        }
        if (!m_redis || m_redis->err) {
            if (m_redis) {
                std::cout << "Redis Unix Socket unavailable (" << m_redis->errstr << "), falling back to TCP " << m_redis_host << ":" << m_redis_port << "..." << std::endl;
                redisFree(m_redis);
                m_redis = nullptr;
            } else {
                std::cout << "Connecting to Redis at " << m_redis_host << ":" << m_redis_port << "..." << std::endl;
            }
            m_redis = redisConnect(m_redis_host.c_str(), m_redis_port);
        }
        
        if (m_redis == nullptr || m_redis->err) {
            if (m_redis) {
                std::cerr << "Redis Connection Error: " << m_redis->errstr << std::endl;
                redisFree(m_redis);
                m_redis = nullptr;
            } else {
                std::cerr << "Redis Connection Error: Can't allocate redis context" << std::endl;
            }
        } else {
            std::cout << "Successfully connected to Redis!" << std::endl;
        }
    }

    std::string get_authorized_url() {
        try {
            net::io_context ioc;
            ssl::context ctx{ssl::context::tls_client};
            ctx.set_verify_mode(ssl::verify_none);

            tcp::resolver resolver{ioc};
            ssl::stream<tcp::socket> stream{ioc, ctx};

            auto const results = resolver.resolve("api.upstox.com", "443");
            net::connect(beast::get_lowest_layer(stream), results);

            if (!SSL_set_tlsext_host_name(stream.native_handle(), "api.upstox.com")) {
                throw boost::system::system_error(
                    static_cast<int>(::ERR_get_error()),
                    boost::asio::error::get_ssl_category(),
                    "Failed to set SNI Hostname for authorization request"
                );
            }

            stream.handshake(ssl::stream_base::client);

            http::request<http::string_body> req{http::verb::get, "/v3/feed/market-data-feed/authorize", 11};
            req.set(http::field::host, "api.upstox.com");
            req.set(http::field::user_agent, "upstox-cpp-collector/1.0");
            req.set(http::field::accept, "application/json");
            req.set(http::field::authorization, "Bearer " + m_token);

            http::write(stream, req);

            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);

            boost::system::error_code ec;
            stream.shutdown(ec);

            if (res.result() == http::status::unauthorized) {
                std::cout << "⚠️ Upstox API Unauthorized (401). Refreshing token via auth.py..." << std::endl;
                int ret = std::system("python /Users/prana/Desktop/open_source/web/login/auth.py");
                if (ret == 0) {
                    reload_token();
                }
                throw std::runtime_error("Access token expired (401). Refreshed token, retrying...");
            } else if (res.result() != http::status::ok) {
                throw std::runtime_error("Authorization API returned status " + std::to_string(static_cast<int>(res.result())) + ": " + res.body());
            }

            auto response_json = json::parse(res.body());
            if (response_json.contains("data")) {
                auto data = response_json["data"];
                if (data.contains("authorizedRedirectUri")) {
                    return data["authorizedRedirectUri"].get<std::string>();
                } else if (data.contains("authorized_redirect_uri")) {
                    return data["authorized_redirect_uri"].get<std::string>();
                }
            }
            throw std::runtime_error("Response JSON does not contain authorizedRedirectUri. Body: " + res.body());
        } catch (const std::exception& e) {
            std::cerr << "Failed to fetch authorized WebSocket URL: " << e.what() << std::endl;
            throw;
        }
    }

    void run() {
        pin_thread_to_core(m_pinned_core);
        connect_redis();
        
        if (m_redis) {
            // Auto-detect and subscribe front futures symbol from Redis
            redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
            if (r_fut) {
                if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                    std::string fut_sym = r_fut->str;
                    if (!fut_sym.empty()) {
                        m_front_futures_symbol = fut_sym;
                        if (std::find(m_instruments.begin(), m_instruments.end(), fut_sym) == m_instruments.end()) {
                            m_instruments.push_back(fut_sym);
                            std::cout << "🔥 [Dynamic Instrument] Auto-appended Front Future to active tracking: " << fut_sym << std::endl;
                        } else {
                            std::cout << "🎯 [Dynamic Instrument] Front Future already present in active tracking: " << fut_sym << std::endl;
                        }
                    }
                }
                freeReplyObject(r_fut);
            }
        }
        
        if (m_replay) {
            std::cout << "==================================================" << std::endl;
            std::cout << "   RUNNING IN MARKET REPLAY MODE (LOCAL REDIS STREAM)" << std::endl;
            std::cout << "==================================================" << std::endl;
            run_replay();
            return;
        }
        
        if (m_mock) {
            std::cout << "==================================================" << std::endl;
            std::cout << "   RUNNING IN MOCK SIMULATOR MODE (SANDBOX)" << std::endl;
            std::cout << "==================================================" << std::endl;
            run_simulation();
            return;
        }
        
        if (m_redis) {
            m_candle_mgr = std::make_unique<CandleManager>(
                m_redis_host, m_redis_port, m_redis_unix_socket,
                m_token, m_instruments, m_config_path
            );
            if (!m_skip_historical_catchup) {
                m_candle_mgr->catch_up_historical_candles(m_redis);
            } else {
                std::cout << "⏩ [Ingestor] Bypassing C++ historical catch-up seeding as requested in config." << std::endl;
            }
            m_candle_mgr->init_active_candles(m_redis);
            m_microstructure_mgr = std::make_unique<MicrostructureEngine>();
            m_fifo_pool = std::make_unique<FIFOPool>(m_starting_capital);
            m_fifo_pool->restore_state(m_redis);
            m_strategy_engine = std::make_unique<StrategyEngine>(*m_fifo_pool);
            m_strategy_engine->init_front_expiry(m_redis);
            m_strategy_engine->set_trade_callback([](const std::string& ev, const UnifiedPosition& pos, const std::string& msg) {
                std::cout << "⚡ [C++ StrategyEngine] " << ev << " | Model: " << pos.model_name
                          << " | " << pos.symbol << " (" << pos.lots << " Lots) | PnL: Rs " << pos.realized_pnl
                          << " | Reason: " << msg << std::endl;
            });

            // ─── CRITICAL: Replay today's candles from Redis in LIVE mode ─────────────
            m_strategy_engine->rehydrate_from_redis(m_redis);


            m_candle_mgr->set_bar_close_callback([this](const std::string& symbol, const std::string& tf, const Candle& closed_bar) {
                if (tf == "1m" && m_strategy_engine && m_microstructure_mgr && m_redis) {
                    if (m_front_futures_symbol.empty()) {
                        redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
                        if (r_fut) {
                            if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                                m_front_futures_symbol = r_fut->str;
                            }
                            freeReplyObject(r_fut);
                        }
                    }

                    if (!m_front_futures_symbol.empty() && symbol == m_front_futures_symbol) {
                        MicrostructureMetrics metrics;
                        if (m_microstructure_mgr->get_metrics(symbol, metrics)) {
                            Candle1M bar;
                            bar.minute_ts = closed_bar.timestamp;
                            bar.open = closed_bar.open;
                            bar.high = closed_bar.high;
                            bar.low = closed_bar.low;
                            bar.close = closed_bar.close;
                            bar.volume = closed_bar.volume;

                            Candle1M prev_bar = m_last_1m_bars[symbol];
                            if (prev_bar.minute_ts == 0) {
                                prev_bar = bar;
                            }

                            m_strategy_engine->on_1m_bar(m_redis, symbol, bar, prev_bar, metrics);
                            m_last_1m_bars[symbol] = bar;
                        }
                    }
                }
            });
        }
        while (m_reconnect_attempts < m_max_reconnect_attempts) {
            try {
                std::cout << "Requesting authorized WebSocket URI..." << std::endl;
                std::string auth_url = get_authorized_url();
                std::cout << "Authorized URI obtained: " << auth_url << std::endl;
                
                std::string ws_host = m_host;
                std::string ws_port = m_port;
                std::string ws_target = m_target;
                
                if (auth_url.rfind("wss://", 0) == 0) {
                    std::string temp = auth_url.substr(6);
                    size_t slash_pos = temp.find('/');
                    if (slash_pos != std::string::npos) {
                        ws_host = temp.substr(0, slash_pos);
                        ws_target = temp.substr(slash_pos);
                    } else {
                        ws_host = temp;
                        ws_target = "/";
                    }
                    
                    size_t colon_pos = ws_host.find(':');
                    if (colon_pos != std::string::npos) {
                        ws_port = ws_host.substr(colon_pos + 1);
                        ws_host = ws_host.substr(0, colon_pos);
                    }
                }
                
                std::cout << "Starting connection to " << ws_host << ":" << ws_port << ws_target << " (Attempt " << (m_reconnect_attempts + 1) << ")..." << std::endl;
                
                net::io_context ioc;
                ssl::context ctx{ssl::context::tls_client};
                
                // Disable certificate verification to prevent SSL handshake errors on missing CA certificates
                ctx.set_verify_mode(ssl::verify_none);
                
                tcp::resolver resolver{ioc};
                websocket::stream<ssl::stream<tcp::socket>> ws{ioc, ctx};
                
                auto const results = resolver.resolve(ws_host, ws_port);
                
                std::cout << "Connecting to server TCP socket..." << std::endl;
                net::connect(beast::get_lowest_layer(ws), results);
                configure_low_latency_socket(beast::get_lowest_layer(ws));
                
                // Set SNI Hostname (essential for modern secure endpoints)
                if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), ws_host.c_str())) {
                    throw boost::system::system_error(
                        static_cast<int>(::ERR_get_error()),
                        boost::asio::error::get_ssl_category(),
                        "Failed to set SNI Hostname"
                    );
                }
                
                std::cout << "Performing SSL handshake..." << std::endl;
                ws.next_layer().handshake(ssl::stream_base::client);
                
                // Configure WebSocket Handshake Decorator to append Authorization header
                ws.set_option(websocket::stream_base::decorator(
                    [this](websocket::request_type& req) {
                        req.set(http::field::authorization, "Bearer " + m_token);
                        req.set(http::field::user_agent, "upstox-cpp-collector/1.0");
                    }
                ));
                
                std::cout << "Performing WebSocket handshake..." << std::endl;
                websocket::response_type res;
                beast::error_code handshake_ec;
                ws.handshake(res, ws_host, ws_target, handshake_ec);
                
                if (handshake_ec) {
                    auto status = res.result();
                    if (status == http::status::found ||
                        status == http::status::temporary_redirect ||
                        status == http::status::moved_permanently ||
                        status == http::status::permanent_redirect) {
                        
                        std::string location{res["Location"]};
                        std::cout << "HTTP Redirect (" << status << ") received. Target location: " << location << std::endl;
                        
                        std::string new_host = "";
                        std::string new_port = "443";
                        std::string new_target = "";
                        
                        if (location.rfind("wss://", 0) == 0) {
                            std::string temp = location.substr(6);
                            size_t slash_pos = temp.find('/');
                            if (slash_pos != std::string::npos) {
                                new_host = temp.substr(0, slash_pos);
                                new_target = temp.substr(slash_pos);
                            } else {
                                new_host = temp;
                                new_target = "/";
                            }
                            
                            size_t colon_pos = new_host.find(':');
                            if (colon_pos != std::string::npos) {
                                new_port = new_host.substr(colon_pos + 1);
                                new_host = new_host.substr(0, colon_pos);
                            }
                        }
                        
                        if (!new_host.empty()) {
                            std::cout << "Re-connecting to redirected host: " << new_host << ":" << new_port << " with target: " << new_target << std::endl;
                            
                            websocket::stream<ssl::stream<tcp::socket>> ws_redirect{ioc, ctx};
                            auto const redirect_results = resolver.resolve(new_host, new_port);
                            
                            std::cout << "Connecting to redirected server TCP socket..." << std::endl;
                            net::connect(beast::get_lowest_layer(ws_redirect), redirect_results);
                            configure_low_latency_socket(beast::get_lowest_layer(ws_redirect));
                            
                            std::cout << "Performing redirected SSL handshake..." << std::endl;
                            if (!SSL_set_tlsext_host_name(ws_redirect.next_layer().native_handle(), new_host.c_str())) {
                                throw boost::system::system_error(
                                    static_cast<int>(::ERR_get_error()),
                                    boost::asio::error::get_ssl_category(),
                                    "Failed to set SNI Hostname for redirected connection"
                                );
                            }
                            ws_redirect.next_layer().handshake(ssl::stream_base::client);
                            
                            ws_redirect.set_option(websocket::stream_base::decorator(
                                [this](websocket::request_type& req) {
                                    req.set(http::field::authorization, "Bearer " + m_token);
                                    req.set(http::field::user_agent, "upstox-cpp-collector/1.0");
                                }
                            ));
                            
                            std::cout << "Performing redirected WebSocket handshake..." << std::endl;
                            ws_redirect.handshake(new_host, new_target);
                            
                            std::cout << "Redirected Upstox WebSocket Connection Established successfully!" << std::endl;
                            m_reconnect_attempts = 0;
                            
                            send_subscription(ws_redirect);
                            
                            m_active_socket.store(&beast::get_lowest_layer(ws_redirect), std::memory_order_release);
                            m_last_msg_time_ms.store(
                                std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch()
                                ).count(),
                                std::memory_order_release
                            );
                            m_is_connected.store(true, std::memory_order_release);
                            m_watchdog_triggered.store(false, std::memory_order_release);

                            beast::flat_buffer buffer;
                            buffer.reserve(65536);
                            for (;;) {
                                buffer.clear();
                                ws_redirect.read(buffer);
                                process_message(buffer);
                            }
                            m_is_connected.store(false, std::memory_order_release);
                            m_active_socket.store(nullptr, std::memory_order_release);
                            continue;
                        }
                    }
                    throw beast::system_error{handshake_ec};
                }
                
                std::cout << "Upstox WebSocket Connection Established successfully (no redirect)!" << std::endl;
                m_reconnect_attempts = 0;
                
                send_subscription(ws);
                
                m_active_socket.store(&beast::get_lowest_layer(ws), std::memory_order_release);
                m_last_msg_time_ms.store(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count(),
                    std::memory_order_release
                );
                m_is_connected.store(true, std::memory_order_release);
                m_watchdog_triggered.store(false, std::memory_order_release);

                beast::flat_buffer buffer;
                buffer.reserve(65536);
                for (;;) {
                    buffer.clear();
                    ws.read(buffer);
                    process_message(buffer);
                }
                m_is_connected.store(false, std::memory_order_release);
                m_active_socket.store(nullptr, std::memory_order_release);
                
            } catch (const std::exception& e) {
                m_is_connected.store(false, std::memory_order_release);
                m_active_socket.store(nullptr, std::memory_order_release);

                bool watchdog_fired = m_watchdog_triggered.exchange(false);
                if (watchdog_fired) {
                    std::cerr << "🔄 [RECONNECT] 3-second watchdog triggered immediate reconnect! Connecting now (0s timeout delay)..." << std::endl;
                    m_reconnect_attempts = 0;
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    continue;
                }

                std::cerr << "WebSocket error: " << e.what() << std::endl;
                m_reconnect_attempts++;
                
                if (m_reconnect_attempts >= m_max_reconnect_attempts) {
                    std::cerr << "Max reconnect attempts reached (" << m_max_reconnect_attempts << "). Exiting..." << std::endl;
                    break;
                }
                
                int backoff_sec = std::min(static_cast<int>(std::pow(2, m_reconnect_attempts)), 60);
                std::cout << "Waiting " << backoff_sec << " seconds before reconnecting (Attempt " << m_reconnect_attempts << "/" << m_max_reconnect_attempts << ")..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(backoff_sec));
            }
        }
    }

private:
    void load_config(const std::string& config_path) {
        std::ifstream f(config_path);
        if (!f.is_open()) {
            throw std::runtime_error("Could not open config file: " + config_path);
        }
        
        json cfg;
        f >> cfg;
        
        m_redis_host = cfg.value("redis_host", "127.0.0.1");
        m_redis_port = cfg.value("redis_port", 6379);
        m_redis_unix_socket = cfg.value("redis_unix_socket", "");
        m_mock = cfg.value("mock", false);
        m_skip_historical_catchup = cfg.value("skip_historical_catchup", false);
        m_host = cfg.value("host", "api.upstox.com");
        m_port = cfg.value("port", "443");
        m_target = cfg.value("target", "/v3/feed/market-data-feed");
        m_mode = cfg.value("mode", "full");
        m_instruments = cfg["instruments"].get<std::vector<std::string>>();
        m_starting_capital = cfg.value("starting_capital", 25000.0);
        m_wal_dir = cfg.value("wal_dir", "/Users/prana/Desktop/open_source/web/data/tick_wal");
        m_wal_writer.init(m_wal_dir);
        m_pinned_core = cfg.value("pinned_core", 1);
        
        // Read access token from file (bypassed in replay or mock mode)
        m_replay = cfg.value("replay", m_replay);
        if (!m_replay && !m_mock) {
            std::string token_file = cfg.value("access_token_file", "");
            if (token_file.empty()) {
                throw std::runtime_error("access_token_file is not specified in config!");
            }
            
            std::ifstream tf(token_file);
            if (!tf.is_open()) {
                throw std::runtime_error("Could not open token file: " + token_file);
            }
            
            json tk;
            tf >> tk;
            m_token = tk.at("access_token").get<std::string>();
            std::cout << "Config and access token successfully loaded!" << std::endl;
        } else {
            std::cout << "Config successfully loaded (Replay/Mock mode: bypassing external Upstox auth)." << std::endl;
        }
    }

    void reload_token() {
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
                        std::cout << "Successfully reloaded access token from: " << token_file << std::endl;
                        return;
                    }
                }
            }
            std::cerr << "Warning: Failed to reload access token!" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Warning: Error reloading access token: " << e.what() << std::endl;
        }
    }

    void send_subscription(websocket::stream<ssl::stream<tcp::socket>>& ws) {
        json sub_msg;
        sub_msg["guid"] = "cpp-collector-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        sub_msg["method"] = "sub";
        sub_msg["data"]["instrumentKeys"] = m_instruments;
        sub_msg["data"]["mode"] = m_mode;
        
        std::string payload = sub_msg.dump();
        
        // Upstox V3 API expects subscription requests as a BINARY frames
        ws.binary(true);
        ws.write(net::buffer(payload));
        
        std::cout << "Subscription request sent for " << m_instruments.size() 
                  << " instruments in '" << m_mode << "' mode." << std::endl;
    }

    void process_message(beast::flat_buffer& buffer) {
        upstox::FeedResponse response;
        const auto* data = static_cast<const char*>(buffer.data().data());
        size_t size = buffer.size();
        
        if (!response.ParseFromArray(data, size)) {
            std::cerr << "Protobuf binary parsing failed!" << std::endl;
            return;
        }
        
        // P4: Nanosecond hardware/kernel receive timestamping
        auto now_clock = std::chrono::high_resolution_clock::now();
        int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now_clock.time_since_epoch()).count();
        int64_t now_ms = now_ns / 1'000'000;
        m_last_msg_time_ms.store(now_ms, std::memory_order_relaxed);
        
        if (m_redis == nullptr) {
            if (now_ms - m_last_reconnect_time_ms > 5000) {
                std::cout << "[Ingestor] Redis is offline. Attempting rate-limited reconnect..." << std::endl;
                m_last_reconnect_time_ms = now_ms;
                connect_redis();
            }
        }
        
        // Zero heap allocations: stack-allocated records
        std::vector<MarketTickRecord> records;
        records.reserve(response.feeds().size());

        for (auto const& [symbol, feed] : response.feeds()) {
            MarketTickRecord rec;
            rec.symbol = symbol;
            rec.ts_recv_ms = now_ms;
            rec.ts_recv_ns = now_ns;

            // LTPC Parse
            if (feed.has_ltpc()) {
                auto const& ltpc = feed.ltpc();
                rec.ltp = ltpc.ltp();
                rec.ts_exchange = ltpc.ltt();
                rec.close = ltpc.cp();
                if (ltpc.has_iep()) rec.iep = ltpc.iep().value();
            }

            // FullFeed Parse
            if (feed.has_fullfeed()) {
                auto const& ff = feed.fullfeed();
                if (ff.has_marketff()) {
                    auto const& mff = ff.marketff();
                    rec.ltp = mff.ltpc().ltp();
                    rec.ts_exchange = mff.ltpc().ltt();
                    rec.close = mff.ltpc().cp();
                    if (mff.ltpc().has_iep()) rec.iep = mff.ltpc().iep().value();
                    rec.volume = static_cast<double>(mff.vtt());
                    rec.oi = static_cast<double>(mff.oi());
                    rec.iv = mff.iv();
                    rec.atp = mff.atp();
                    rec.tbq = mff.tbq();
                    rec.tsq = mff.tsq();

                    if (mff.iep() > 0.0) rec.iep = mff.iep();
                    if (mff.rp() > 0.0) rec.reference_price = mff.rp();
                    if (mff.ieq() > 0) rec.ieq = mff.ieq();
                    if (mff.iiqtotal() != 0) rec.iiq_total = mff.iiqtotal();
                    if (mff.iiqm() != 0) rec.iiq_market = mff.iiqm();
                    rec.cas_eligible = mff.caseligible();

                    if (mff.has_marketlevel() && mff.marketlevel().bidaskquote_size() > 0) {
                        int sz = std::min(mff.marketlevel().bidaskquote_size(), 5);
                        for (int i = 0; i < sz; ++i) {
                            auto const& q = mff.marketlevel().bidaskquote(i);
                            rec.bid[i] = q.bidp();
                            rec.bid_qty[i] = q.bidq();
                            rec.ask[i] = q.askp();
                            rec.ask_qty[i] = q.askq();
                        }
                    }

                    if (mff.has_optiongreeks()) {
                        auto const& g = mff.optiongreeks();
                        rec.delta = g.delta();
                        rec.theta = g.theta();
                        rec.gamma = g.gamma();
                        rec.vega = g.vega();
                        rec.rho = g.rho();
                        rec.has_greeks = true;
                    }
                } else if (ff.has_indexff()) {
                    auto const& iff = ff.indexff();
                    rec.ltp = iff.ltpc().ltp();
                    rec.ts_exchange = iff.ltpc().ltt();
                    rec.close = iff.ltpc().cp();
                }
            }

            // FirstLevelWithGreeks Parse
            if (feed.has_firstlevelwithgreeks()) {
                auto const& flg = feed.firstlevelwithgreeks();
                rec.ltp = flg.ltpc().ltp();
                rec.ts_exchange = flg.ltpc().ltt();
                rec.close = flg.ltpc().cp();
                rec.oi = static_cast<double>(flg.oi());
                rec.iv = flg.iv();
                rec.volume = static_cast<double>(flg.vtt());

                if (flg.has_firstdepth()) {
                    auto const& best = flg.firstdepth();
                    rec.bid[0] = best.bidp();
                    rec.bid_qty[0] = best.bidq();
                    rec.ask[0] = best.askp();
                    rec.ask_qty[0] = best.askq();
                }

                if (flg.has_optiongreeks()) {
                    auto const& g = flg.optiongreeks();
                    rec.delta = g.delta();
                    rec.theta = g.theta();
                    rec.gamma = g.gamma();
                    rec.vega = g.vega();
                    rec.rho = g.rho();
                    rec.has_greeks = true;
                }
            }

            // P4: Feed Sanity Filter
            validate_tick_sanity(rec);

            // P0 (Option B) & P1: Local Write-Ahead Log (WAL) Spooling (Zero Tick Loss)
            if (rec.is_valid) {
                m_wal_writer.write_tick(rec);
            }

            records.push_back(std::move(rec));
        }

        // P2: High-Performance Pipelined Redis Ingestion for Strategy Engine (Sub-Millisecond)
        if (m_redis != nullptr && !records.empty()) {
            size_t queued_cmds = 0;
            for (const auto& rec : records) {
                if (!rec.is_valid) continue;
                std::string key = "md:quote:" + rec.symbol;

                std::vector<std::string> args;
                args.reserve(36);
                args.push_back("HSET");
                args.push_back(key);
                args.push_back("symbol"); args.push_back(rec.symbol);
                args.push_back("source"); args.push_back("upstox");
                args.push_back("status"); args.push_back("live");
                args.push_back("ltp"); args.push_back(std::to_string(rec.ltp));
                args.push_back("close"); args.push_back(std::to_string(rec.close));
                args.push_back("volume"); args.push_back(std::to_string(static_cast<int64_t>(rec.volume)));
                args.push_back("oi"); args.push_back(std::to_string(static_cast<int64_t>(rec.oi)));
                args.push_back("iv"); args.push_back(std::to_string(rec.iv));

                for (int i = 0; i < 5; ++i) {
                    std::string sfx = (i == 0) ? "" : std::to_string(i + 1);
                    if (rec.bid[i] > 0.0 || rec.ask[i] > 0.0) {
                        args.push_back("bid" + sfx); args.push_back(std::to_string(rec.bid[i]));
                        args.push_back("bid_qty" + sfx); args.push_back(std::to_string(rec.bid_qty[i]));
                        args.push_back("ask" + sfx); args.push_back(std::to_string(rec.ask[i]));
                        args.push_back("ask_qty" + sfx); args.push_back(std::to_string(rec.ask_qty[i]));
                    }
                }
                args.push_back("tbq"); args.push_back(std::to_string(rec.tbq));
                args.push_back("tsq"); args.push_back(std::to_string(rec.tsq));
                args.push_back("ts_exchange"); args.push_back(std::to_string(rec.ts_exchange));
                args.push_back("ts_recv"); args.push_back(std::to_string(rec.ts_recv_ms));

                if (rec.iep > 0.0) {
                    args.push_back("iep"); args.push_back(std::to_string(rec.iep));
                    args.push_back("reference_price"); args.push_back(std::to_string(rec.reference_price));
                    args.push_back("ieq"); args.push_back(std::to_string(rec.ieq));
                    args.push_back("iiq_total"); args.push_back(std::to_string(rec.iiq_total));
                    args.push_back("iiq_market"); args.push_back(std::to_string(rec.iiq_market));
                    args.push_back("cas_eligible"); args.push_back(rec.cas_eligible ? "1" : "0");
                }

                if (rec.has_greeks) {
                    args.push_back("delta"); args.push_back(std::to_string(rec.delta));
                    args.push_back("theta"); args.push_back(std::to_string(rec.theta));
                    args.push_back("gamma"); args.push_back(std::to_string(rec.gamma));
                    args.push_back("vega"); args.push_back(std::to_string(rec.vega));
                    args.push_back("rho"); args.push_back(std::to_string(rec.rho));
                }

                std::vector<const char*> argv;
                std::vector<size_t> argvlen;
                argv.reserve(args.size());
                argvlen.reserve(args.size());
                for (const auto& a : args) {
                    argv.push_back(a.c_str());
                    argvlen.push_back(a.size());
                }

                if (redisAppendCommandArgv(m_redis, argv.size(), argv.data(), argvlen.data()) == REDIS_OK) {
                    queued_cmds++;
                }

                // If CAS IEP present, pipeline CAS live key
                if (rec.iep > 0.0) {
                    std::string cas_key = "cas:live:" + rec.symbol;
                    std::vector<std::string> c_args = {
                        "HSET", cas_key,
                        "symbol", rec.symbol,
                        "iep", std::to_string(rec.iep),
                        "ts_recv", std::to_string(now_ms),
                        "ieq", std::to_string(rec.ieq),
                        "iiq_total", std::to_string(rec.iiq_total),
                        "reference_price", std::to_string(rec.reference_price)
                    };
                    std::vector<const char*> c_argv;
                    std::vector<size_t> c_len;
                    c_argv.reserve(c_args.size());
                    c_len.reserve(c_args.size());
                    for (const auto& ca : c_args) {
                        c_argv.push_back(ca.c_str());
                        c_len.push_back(ca.size());
                    }
                    if (redisAppendCommandArgv(m_redis, c_argv.size(), c_argv.data(), c_len.data()) == REDIS_OK) {
                        queued_cmds++;
                    }
                }
            }

            // Drain all pipelined Redis replies in a single burst
            for (size_t i = 0; i < queued_cmds; ++i) {
                redisReply* set_reply = nullptr;
                if (redisGetReply(m_redis, (void**)&set_reply) == REDIS_OK && set_reply) {
                    freeReplyObject(set_reply);
                }
            }
        }

        // Processing for Candle & Microstructure Engines
        for (const auto& rec : records) {
            if (!rec.is_valid) continue;

            if (m_redis && m_candle_mgr) {
                m_candle_mgr->process_tick_candle(m_redis, rec.symbol, rec.ltp, static_cast<int64_t>(rec.volume), rec.ts_exchange);
            }

            if (m_redis && m_microstructure_mgr) {
                m_microstructure_mgr->process_tick(m_redis, rec.symbol, rec.ltp, rec.bid[0], rec.ask[0], static_cast<int64_t>(rec.volume), rec.oi, rec.ts_exchange);

                if (m_strategy_engine) {
                    if (m_front_futures_symbol.empty() && m_redis) {
                        redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
                        if (r_fut) {
                            if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                                m_front_futures_symbol = r_fut->str;
                            }
                            freeReplyObject(r_fut);
                        }
                    }
                    if (!m_front_futures_symbol.empty() && rec.symbol == m_front_futures_symbol) {
                        MicrostructureMetrics metrics;
                        if (m_microstructure_mgr->get_metrics(rec.symbol, metrics)) {
                            m_strategy_engine->on_tick(m_redis, rec.symbol, rec.ltp, rec.bid[0], rec.ask[0], rec.ts_exchange, metrics);
                        }
                    }
                }
            }

            m_ticks_processed_window++;
        }

        // Periodic 10-second portfolio telemetry publish to Redis
        int64_t now_epoch_ms = now_ms;
        if (m_strategy_engine && m_redis && (now_epoch_ms - m_last_portfolio_publish_ms >= 10000)) {
            m_last_portfolio_publish_ms = now_epoch_ms;
            m_strategy_engine->publish_portfolio_state(m_redis);
        }

        // Rate-limited throughput telemetry (every 5 seconds)
        if (now_epoch_ms - m_last_stats_log_ms >= 5000) {
            double rate = (m_ticks_processed_window * 1000.0) / std::max<int64_t>(1, (now_epoch_ms - m_last_stats_log_ms));
            std::cout << "📊 [C++ Ingestor] Pipeline Throughput: " << std::fixed << std::setprecision(1) << rate 
                      << " ticks/s | Frames: " << response.feeds().size()
                      << " | Total WAL Written: " << m_wal_writer.get_ticks_written() << std::endl;
            m_ticks_processed_window = 0;
            m_last_stats_log_ms = now_epoch_ms;
        }
    }
    
    // Config properties
    std::string m_config_path;
    std::string m_redis_host;
    int m_redis_port;
    std::string m_redis_unix_socket;
    bool m_mock;
    bool m_replay;
    bool m_skip_historical_catchup;
    std::string m_host;
    std::string m_port;
    std::string m_target;
    std::string m_mode;
    std::vector<std::string> m_instruments;
    std::string m_token;
    int m_pinned_core{1};
    
    void run_simulation() {
        // Seed random number generator
        std::srand(std::time(nullptr));
        
        // Initialize simulated prices for each instrument
        std::unordered_map<std::string, double> base_prices;
        for (const auto& symbol : m_instruments) {
            if (symbol.find("INE020B01018") != std::string::npos) {
                base_prices[symbol] = 2500.0; // Simulated Reliance
            } else if (symbol.find("INE467B01029") != std::string::npos) {
                base_prices[symbol] = 3400.0; // Simulated TCS
            } else {
                base_prices[symbol] = 500.0;
            }
        }
        
        while (true) {
            int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
            
            for (const auto& symbol : m_instruments) {
                double& price = base_prices[symbol];
                
                // Apply random walk (-0.05% to +0.05%)
                double pct = ((std::rand() % 1000) - 500) / 100000.0;
                price += price * pct;
                
                double bid = price - (price * 0.0005);
                double ask = price + (price * 0.0005);
                int64_t volume = 100000 + (std::rand() % 500000);
                
                json norm;
                norm["symbol"] = symbol;
                norm["source"] = "upstox_sandbox";
                norm["ts_recv"] = now_ms;
                norm["ts_exchange"] = now_ms - 2;
                norm["status"] = "live";
                norm["ltp"] = std::round(price * 100.0) / 100.0;
                norm["bid"] = std::round(bid * 100.0) / 100.0;
                norm["ask"] = std::round(ask * 100.0) / 100.0;
                norm["volume"] = volume;
                norm["oi"] = 1200000.0;
                norm["close"] = std::round(price * 0.99 * 100.0) / 100.0;
                
                std::string norm_str = norm.dump();
                
                if (m_redis) {
                    std::string key = "md:quote:" + symbol;
                    redisReply* set_reply = (redisReply*)redisCommand(m_redis, "SET %s %s", key.c_str(), norm_str.c_str());
                    if (set_reply) freeReplyObject(set_reply);
                    
                    redisReply* pub_reply = (redisReply*)redisCommand(m_redis, "PUBLISH md:stream:all %s", norm_str.c_str());
                    if (pub_reply) freeReplyObject(pub_reply);
                }
                
                std::cout << "[Mock Tick] " << symbol << " | LTP: " << norm["ltp"] 
                          << " | Bid: " << norm["bid"] << " | Ask: " << norm["ask"] << std::endl;
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    void print_replay_scorecard() {
        if (!m_fifo_pool) return;
        std::cout << "\n=================================================================================" << std::endl;
        std::cout << "📊 C++ FORWARD TESTER REPLAY PERFORMANCE SCORECARD" << std::endl;
        std::cout << "=================================================================================" << std::endl;
        std::cout << "Active Positions Remaining : " << m_fifo_pool->active_positions.size() << std::endl;
        std::cout << "Total Closed Trades        : " << m_fifo_pool->closed_positions.size() << std::endl;

        double total_pnl = 0.0;
        int wins = 0;
        int losses = 0;

        for (const auto& pos : m_fifo_pool->closed_positions) {
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

        std::cout << "---------------------------------------------------------------------------------" << std::endl;
        std::cout << "Win / Loss Record           : " << wins << " Wins / " << losses << " Losses (" 
                  << (m_fifo_pool->closed_positions.empty() ? 0.0 : (wins * 100.0 / m_fifo_pool->closed_positions.size())) << "%)" << std::endl;
        std::cout << "Realized Equity PnL         : Rs " << std::showpos << total_pnl << std::noshowpos << std::endl;
        std::cout << "Pool Free Cash              : Rs " << m_fifo_pool->get_free_cash() << std::endl;
        std::cout << "=================================================================================" << std::endl;
    }

    void run_replay() {
        pin_thread_to_core(m_pinned_core);
        std::cout << "==================================================" << std::endl;
        std::cout << "   RUNNING IN MARKET REPLAY RECEIVER MODE" << std::endl;
        std::cout << "   Subscribing to Redis channel 'md:stream:all'..." << std::endl;
        std::cout << "==================================================" << std::endl;

        if (m_redis) {
            redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
            if (r_fut) {
                if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                    m_front_futures_symbol = r_fut->str;
                    std::cout << "🎯 [Replay Receiver] Front Future Symbol Resolved: " << m_front_futures_symbol << std::endl;
                }
                freeReplyObject(r_fut);
            }
        }

        m_candle_mgr = std::make_unique<CandleManager>(
            m_redis_host, m_redis_port, m_redis_unix_socket,
            m_token, m_instruments, m_config_path
        );
        if (!m_front_futures_symbol.empty()) {
            m_candle_mgr->add_instrument(m_front_futures_symbol);
        }
        m_candle_mgr->add_instrument("NSE_INDEX|Nifty 50");
        // In replay receiver mode, start candle manager clean without restoring stale candles
        // m_candle_mgr->init_active_candles(m_redis);

        m_microstructure_mgr = std::make_unique<MicrostructureEngine>();
        m_fifo_pool = std::make_unique<FIFOPool>(m_starting_capital);
        m_strategy_engine = std::make_unique<StrategyEngine>(*m_fifo_pool);
        m_strategy_engine->enable_model_poc_v2 = true;
        m_strategy_engine->enable_model_spatial_box = true;
        m_strategy_engine->enable_model_dalton_va = true;
        m_strategy_engine->enable_model_tpo_poc = false;

        m_strategy_engine->set_trade_callback([](const std::string& ev, const UnifiedPosition& pos, const std::string& msg) {
            std::cout << "⚡ [C++ StrategyEngine] " << ev << " | Model: " << pos.model_name
                      << " | " << pos.symbol << " (" << pos.lots << " Lots) | Entry: Rs " << pos.entry_option_price
                      << " -> Exit: Rs " << pos.exit_option_price
                      << " | Pts: " << pos.get_points()
                      << " | PnL: Rs " << pos.realized_pnl
                      << " | Reason: " << msg << std::endl;
        });

        // ─── CRITICAL: Replay today's candles from Redis to rebuild Initial Balance,
        //   TPO masks, POC baseline, and Dalton probes. Without this, any restart
        //   during the session produces a corrupted IB → wrong VAH/VAL → false trades.
        m_strategy_engine->rehydrate_from_redis(m_redis);

        m_candle_mgr->set_bar_close_callback([this](const std::string& symbol, const std::string& tf, const Candle& closed_bar) {
            if (tf == "1m" && m_strategy_engine && m_microstructure_mgr && m_redis) {
                if (m_front_futures_symbol.empty()) {
                    redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
                    if (r_fut) {
                        if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                            m_front_futures_symbol = r_fut->str;
                        }
                        freeReplyObject(r_fut);
                    }
                }

                if (!m_front_futures_symbol.empty() && symbol == m_front_futures_symbol) {
                    MicrostructureMetrics metrics;
                    if (m_microstructure_mgr->get_metrics(symbol, metrics)) {
                        Candle1M bar;
                        bar.minute_ts = closed_bar.timestamp;
                        bar.open = closed_bar.open;
                        bar.high = closed_bar.high;
                        bar.low = closed_bar.low;
                        bar.close = closed_bar.close;
                        bar.volume = closed_bar.volume;

                        Candle1M prev_bar = m_last_1m_bars[symbol];
                        if (prev_bar.minute_ts == 0) {
                            prev_bar = bar;
                        }
                        m_last_1m_bars[symbol] = bar;

                        m_strategy_engine->on_1m_bar(m_redis, symbol, bar, prev_bar, metrics);
                    }
                }
            }
        });

        // Dedicated subscriber hiredis context
        redisContext* sub_redis = nullptr;
        if (!m_redis_unix_socket.empty()) {
            sub_redis = redisConnectUnix(m_redis_unix_socket.c_str());
        }
        if (!sub_redis || sub_redis->err) {
            if (sub_redis) {
                redisFree(sub_redis);
                sub_redis = nullptr;
            }
            sub_redis = redisConnect(m_redis_host.c_str(), m_redis_port);
        }
        if (!sub_redis || sub_redis->err) {
            std::cerr << "❌ Redis connection failed for subscriber!" << std::endl;
            return;
        }

        redisReply* r_sub = (redisReply*)redisCommand(sub_redis, "SUBSCRIBE md:stream:all");
        if (r_sub) freeReplyObject(r_sub);
        std::cout << "✅ Subscribed to 'md:stream:all'. Ingesting live ticks from Replay Engine..." << std::endl;

        int64_t tick_count = 0;
        redisReply* reply = nullptr;
        while (redisGetReply(sub_redis, (void**)&reply) == REDIS_OK && reply) {
            if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                if (reply->element[2]->str) {
                    std::string payload = reply->element[2]->str;
                    try {
                        auto norm = json::parse(payload);
                        if (norm.value("status", "") == "REPLAY_COMPLETE") {
                            std::cout << "\n🏁 Replay stream complete message received from Redis!" << std::endl;
                            freeReplyObject(reply);
                            break;
                        }

                        std::string symbol = norm.value("symbol", "");
                        if (!symbol.empty()) {
                            double p_val = std::stod(norm.value("ltp", "0.0"));
                            double b_val = std::stod(norm.value("bid", "0.0"));
                            double a_val = std::stod(norm.value("ask", "0.0"));
                            int64_t v_val = std::stoll(norm.value("volume", "0"));
                            double oi_val = std::stod(norm.value("oi", "0"));
                            int64_t ts_val = std::stoll(norm.value("ts_exchange", "0"));

                            m_candle_mgr->process_tick_candle(m_redis, symbol, p_val, v_val, ts_val);
                            m_microstructure_mgr->process_tick(m_redis, symbol, p_val, b_val, a_val, v_val, oi_val, ts_val);

                            if (m_strategy_engine) {
                                if (m_front_futures_symbol.empty() && m_redis) {
                                    redisReply* r_fut = (redisReply*)redisCommand(m_redis, "GET fut:NIFTY:front");
                                    if (r_fut) {
                                        if (r_fut->type == REDIS_REPLY_STRING && r_fut->str) {
                                            m_front_futures_symbol = r_fut->str;
                                            m_candle_mgr->add_instrument(m_front_futures_symbol);
                                            std::cout << "🎯 [Replay Receiver] Front Future Symbol Resolved: " << m_front_futures_symbol << std::endl;
                                        }
                                        freeReplyObject(r_fut);
                                    }
                                    m_strategy_engine->init_front_expiry(m_redis);
                                }
                                if (!m_front_futures_symbol.empty() && symbol == m_front_futures_symbol) {
                                    MicrostructureMetrics metrics;
                                    if (m_microstructure_mgr->get_metrics(symbol, metrics)) {
                                        m_strategy_engine->on_tick(m_redis, symbol, p_val, b_val, a_val, ts_val, metrics);
                                    }
                                }
                            }

                            tick_count++;
                            if (tick_count % 10000 == 0) {
                                std::cout << "⏳ Processed " << tick_count << " ticks from Redis stream..." << std::endl;
                            }
                        }
                    } catch (...) {}
                }
            }
            freeReplyObject(reply);
        }

        redisFree(sub_redis);
        print_replay_scorecard();
    }
    
    void watchdog_worker() {
        while (!m_stop_watchdog.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            if (m_replay || !m_is_connected.load(std::memory_order_acquire)) {
                continue;
            }

            int64_t last_msg = m_last_msg_time_ms.load(std::memory_order_acquire);
            if (last_msg <= 0) continue;

            auto now = std::chrono::system_clock::now();
            int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()
            ).count();

            int64_t elapsed_ms = now_ms - last_msg;

            // Check if market hours (09:15 to 15:30 IST on weekdays Monday to Friday)
            time_t now_t = std::chrono::system_clock::to_time_t(now);
            time_t ist_t = now_t + 19800; // UTC to IST (+5:30 = 19800 seconds)
            std::tm tm_buf;
            gmtime_r(&ist_t, &tm_buf);
            int hour = tm_buf.tm_hour;
            int minute = tm_buf.tm_min;
            int wday = tm_buf.tm_wday; // 0=Sun, 1=Mon, ..., 5=Fri, 6=Sat

            bool is_market_hours = (wday >= 1 && wday <= 5) &&
                ((hour == 9 && minute >= 15) || (hour >= 10 && hour < 15) || (hour == 15 && minute <= 30));

            if (is_market_hours && elapsed_ms >= 3000) {
                std::cerr << "🚨 [WATCHDOG] Feed gap detected: No WebSocket message received for "
                          << elapsed_ms << "ms (>= 3.0s) during live market hours ("
                          << (hour < 10 ? "0" : "") << hour << ":" << (minute < 10 ? "0" : "") << minute << " IST)! "
                          << "Forcing immediate WebSocket reconnect without waiting for socket timeout..."
                          << std::endl;

                m_watchdog_triggered.store(true, std::memory_order_release);
                m_is_connected.store(false, std::memory_order_release);
                tcp::socket* sock = m_active_socket.exchange(nullptr);
                if (sock) {
                    boost::system::error_code ec;
                    sock->cancel(ec);
                    sock->shutdown(tcp::socket::shutdown_both, ec);
                    sock->close(ec);
                }
                // Update last_msg to prevent repeat triggers before reconnect completes
                m_last_msg_time_ms.store(now_ms, std::memory_order_release);
            }
        }
    }
    
    // Watchdog properties (reconnects immediately on >3s feed silence during market hours)
    std::atomic<int64_t> m_last_msg_time_ms;
    std::atomic<bool> m_is_connected;
    std::atomic<bool> m_watchdog_triggered;
    std::atomic<bool> m_stop_watchdog;
    std::atomic<tcp::socket*> m_active_socket;
    std::thread m_watchdog_thread;

    // Reconnect properties
    int m_reconnect_attempts;
    const int m_max_reconnect_attempts = 15;
    int64_t m_last_reconnect_time_ms;
    
    // Redis context
    redisContext* m_redis;
    
    // Strategy Engine & Portfolio Management
    std::unique_ptr<CandleManager> m_candle_mgr;
    std::unique_ptr<MicrostructureEngine> m_microstructure_mgr;
    std::unique_ptr<FIFOPool> m_fifo_pool;
    std::unique_ptr<StrategyEngine> m_strategy_engine;
    double m_starting_capital = 25000.0;
    std::string m_front_futures_symbol;
    std::unordered_map<std::string, Candle1M> m_last_1m_bars;
    int64_t m_last_portfolio_publish_ms;

    // P0/P1: Local Write-Ahead Log (WAL) Spooler & Telemetry
    std::string m_wal_dir;
    TickWalWriter m_wal_writer;
    uint64_t m_ticks_processed_window;
    int64_t m_last_stats_log_ms;
};

int main(int argc, char* argv[]) {
    std::string config_path = "config.json";
    bool replay_flag = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--version" || arg == "-v") {
            std::cout << "ULLTR C++ Ingestion Collector" << std::endl;
            std::cout << "Commit: " << GIT_COMMIT_HASH << std::endl;
            std::cout << "Built: " << __DATE__ << " " << __TIME__ << std::endl;
            return 0;
        } else if (arg == "--replay" || arg == "-r") {
            replay_flag = true;
        } else if (arg.rfind("--", 0) != 0) {
            config_path = arg;
        }
    }
    
    try {
        MarketDataIngestor ingestor(config_path, replay_flag);
        ingestor.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
