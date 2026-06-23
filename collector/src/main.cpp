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

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>

#include "MarketDataFeedV3.pb.h"
#include "candle_manager.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;

class MarketDataIngestor {
public:
    MarketDataIngestor(const std::string& config_path) 
        : m_reconnect_attempts(0), m_redis(nullptr), m_mock(false), m_last_reconnect_time_ms(0) {
        m_config_path = config_path;
        load_config(config_path);
    }

    ~MarketDataIngestor() {
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
        } else {
            std::cout << "Connecting to Redis at " << m_redis_host << ":" << m_redis_port << "..." << std::endl;
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

            if (res.result() != http::status::ok) {
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
        connect_redis();
        
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
                            
                            beast::flat_buffer buffer;
                            for (;;) {
                                buffer.clear();
                                ws_redirect.read(buffer);
                                process_message(buffer);
                            }
                            continue;
                        }
                    }
                    throw beast::system_error{handshake_ec};
                }
                
                std::cout << "Upstox WebSocket Connection Established successfully (no redirect)!" << std::endl;
                m_reconnect_attempts = 0;
                
                send_subscription(ws);
                
                beast::flat_buffer buffer;
                for (;;) {
                    buffer.clear();
                    ws.read(buffer);
                    process_message(buffer);
                }
                
            } catch (const std::exception& e) {
                std::cerr << "WebSocket error: " << e.what() << std::endl;
                
                // If handshake failed or declined, trigger Playwright token refresh
                std::cout << "⚠️ WebSocket connection failed. Deleting stale token and triggering automated token refresh using auth.py..." << std::endl;
                std::remove("/Users/prana/Desktop/open_source/web/login/access_token.json");
                int ret = std::system("python /Users/prana/Desktop/open_source/web/login/auth.py");
                if (ret == 0) {
                    reload_token();
                } else {
                    std::cerr << "❌ Automated token refresh failed!" << std::endl;
                }
                
                m_reconnect_attempts++;
                
                if (m_reconnect_attempts >= m_max_reconnect_attempts) {
                    std::cerr << "Max reconnect attempts reached. Exiting..." << std::endl;
                    break;
                }
                
                int backoff_sec = std::min(static_cast<int>(std::pow(2, m_reconnect_attempts)), 60);
                std::cout << "Waiting " << backoff_sec << " seconds before reconnecting..." << std::endl;
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
        
        // Read access token from file
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
        
        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        if (m_redis == nullptr) {
            if (now_ms - m_last_reconnect_time_ms > 5000) {
                std::cout << "[Ingestor] Redis is offline. Attempting rate-limited reconnect..." << std::endl;
                m_last_reconnect_time_ms = now_ms;
                connect_redis();
            }
        }
        
        // Loop through all instruments in the feeds map
        for (auto const& [symbol, feed] : response.feeds()) {
            json norm;
            norm["symbol"] = symbol;
            norm["source"] = "upstox";
            norm["ts_recv"] = now_ms;
            norm["status"] = "live";
            
            // LTPC Parse
            if (feed.has_ltpc()) {
                auto const& ltpc = feed.ltpc();
                norm["ltp"] = ltpc.ltp();
                norm["ts_exchange"] = ltpc.ltt();
                norm["close"] = ltpc.cp();
            }
            
            // FullFeed Parse
            if (feed.has_fullfeed()) {
                auto const& ff = feed.fullfeed();
                if (ff.has_marketff()) {
                    auto const& mff = ff.marketff();
                    norm["ltp"] = mff.ltpc().ltp();
                    norm["ts_exchange"] = mff.ltpc().ltt();
                    norm["close"] = mff.ltpc().cp();
                    norm["volume"] = mff.vtt();
                    norm["oi"] = mff.oi();
                    norm["iv"] = mff.iv();
                    norm["atp"] = mff.atp();
                    norm["tbq"] = mff.tbq();
                    norm["tsq"] = mff.tsq();
                    
                    if (mff.has_marketlevel() && mff.marketlevel().bidaskquote_size() > 0) {
                        auto const& best = mff.marketlevel().bidaskquote(0);
                        norm["bid"] = best.bidp();
                        norm["bid_qty"] = best.bidq();
                        norm["ask"] = best.askp();
                        norm["ask_qty"] = best.askq();
                    }

                    if (mff.has_optiongreeks()) {
                        auto const& greeks = mff.optiongreeks();
                        norm["option_greeks"]["delta"] = greeks.delta();
                        norm["option_greeks"]["theta"] = greeks.theta();
                        norm["option_greeks"]["gamma"] = greeks.gamma();
                        norm["option_greeks"]["vega"] = greeks.vega();
                        norm["option_greeks"]["rho"] = greeks.rho();
                    }
                } else if (ff.has_indexff()) {
                    auto const& iff = ff.indexff();
                    norm["ltp"] = iff.ltpc().ltp();
                    norm["ts_exchange"] = iff.ltpc().ltt();
                    norm["close"] = iff.ltpc().cp();
                }
            }
            
            // FirstLevelWithGreeks Parse
            if (feed.has_firstlevelwithgreeks()) {
                auto const& flg = feed.firstlevelwithgreeks();
                norm["ltp"] = flg.ltpc().ltp();
                norm["ts_exchange"] = flg.ltpc().ltt();
                norm["close"] = flg.ltpc().cp();
                norm["oi"] = flg.oi();
                norm["iv"] = flg.iv();
                norm["volume"] = flg.vtt();
                
                if (flg.has_firstdepth()) {
                    auto const& best = flg.firstdepth();
                    norm["bid"] = best.bidp();
                    norm["bid_qty"] = best.bidq();
                    norm["ask"] = best.askp();
                    norm["ask_qty"] = best.askq();
                }

                if (flg.has_optiongreeks()) {
                    auto const& greeks = flg.optiongreeks();
                    norm["option_greeks"]["delta"] = greeks.delta();
                    norm["option_greeks"]["theta"] = greeks.theta();
                    norm["option_greeks"]["gamma"] = greeks.gamma();
                    norm["option_greeks"]["vega"] = greeks.vega();
                    norm["option_greeks"]["rho"] = greeks.rho();
                }
            }
            
            std::string norm_str = norm.dump();
            
            // Update & Publish to Redis
            if (m_redis) {
                std::string key = "md:quote:" + symbol;
                
                // Construct fields vector for HSET
                std::vector<std::string> args;
                args.push_back("HSET");
                args.push_back(key);
                
                // Add fields dynamically from norm
                args.push_back("symbol"); args.push_back(symbol);
                args.push_back("source"); args.push_back("upstox");
                args.push_back("status"); args.push_back(norm.value("status", "offline"));
                
                if (norm.contains("ltp")) { args.push_back("ltp"); args.push_back(std::to_string(norm["ltp"].get<double>())); }
                if (norm.contains("close")) { args.push_back("close"); args.push_back(std::to_string(norm["close"].get<double>())); }
                if (norm.contains("volume")) { args.push_back("volume"); args.push_back(std::to_string(norm["volume"].get<int64_t>())); }
                if (norm.contains("oi")) { args.push_back("oi"); args.push_back(std::to_string(norm["oi"].get<double>())); }
                if (norm.contains("iv")) { args.push_back("iv"); args.push_back(std::to_string(norm["iv"].get<double>())); }
                
                if (norm.contains("bid")) { args.push_back("bid"); args.push_back(std::to_string(norm["bid"].get<double>())); }
                if (norm.contains("bid_qty")) { args.push_back("bid_qty"); args.push_back(std::to_string(norm["bid_qty"].get<int64_t>())); }
                if (norm.contains("ask")) { args.push_back("ask"); args.push_back(std::to_string(norm["ask"].get<double>())); }
                if (norm.contains("ask_qty")) { args.push_back("ask_qty"); args.push_back(std::to_string(norm["ask_qty"].get<int64_t>())); }
                
                if (norm.contains("ts_exchange")) { args.push_back("ts_exchange"); args.push_back(std::to_string(norm["ts_exchange"].get<int64_t>())); }
                if (norm.contains("ts_recv")) { args.push_back("ts_recv"); args.push_back(std::to_string(norm["ts_recv"].get<int64_t>())); }
                
                // Add nested Option Greeks if available
                if (norm.contains("option_greeks")) {
                    auto const& g = norm["option_greeks"];
                    if (g.contains("delta")) { args.push_back("delta"); args.push_back(std::to_string(g["delta"].get<double>())); }
                    if (g.contains("theta")) { args.push_back("theta"); args.push_back(std::to_string(g["theta"].get<double>())); }
                    if (g.contains("gamma")) { args.push_back("gamma"); args.push_back(std::to_string(g["gamma"].get<double>())); }
                    if (g.contains("vega")) { args.push_back("vega"); args.push_back(std::to_string(g["vega"].get<double>())); }
                    if (g.contains("rho")) { args.push_back("rho"); args.push_back(std::to_string(g["rho"].get<double>())); }
                }
                
                // Invoke dynamic redisCommandArgv
                std::vector<const char*> argv;
                std::vector<size_t> argvlen;
                for (auto const& arg : args) {
                    argv.push_back(arg.c_str());
                    argvlen.push_back(arg.size());
                }
                
                redisReply* set_reply = (redisReply*)redisCommandArgv(m_redis, argv.size(), argv.data(), argvlen.data());
                if (set_reply) {
                    if (set_reply->type == REDIS_REPLY_ERROR) {
                        std::cerr << "Redis HSET Error: " << set_reply->str << " | Key: " << key << std::endl;
                    }
                    freeReplyObject(set_reply);
                } else {
                    std::cerr << "Redis HSET command failed (null reply)! Reconnecting to Redis..." << std::endl;
                    connect_redis();
                    m_last_reconnect_time_ms = now_ms;
                }
                
                // Publish normalized tick to subscriber channel for optional websocket streaming
                if (m_redis) {
                    redisReply* pub_reply = (redisReply*)redisCommand(m_redis, "PUBLISH md:stream:all %s", norm_str.c_str());
                    if (pub_reply) {
                        freeReplyObject(pub_reply);
                    }
                }
            }
            
            if (m_redis && m_candle_mgr) {
                double p_val = norm.value("ltp", 0.0);
                int64_t v_val = norm.value("volume", static_cast<int64_t>(0));
                int64_t ts_val = norm.value("ts_exchange", static_cast<int64_t>(0));
                m_candle_mgr->process_tick_candle(m_redis, symbol, p_val, v_val, ts_val);
            }
            
            std::cout << "[Tick] " << symbol << " | LTP: " << norm.value("ltp", 0.0) 
                      << " | Bid: " << norm.value("bid", 0.0) << " | Ask: " << norm.value("ask", 0.0);
            if (norm.contains("option_greeks")) {
                auto const& g = norm["option_greeks"];
                std::cout << " | Delta: " << g.value("delta", 0.0) << " | Theta: " << g.value("theta", 0.0);
            }
            std::cout << std::endl;
        }
    }
    
    // Config properties
    std::string m_config_path;
    std::string m_redis_host;
    int m_redis_port;
    std::string m_redis_unix_socket;
    bool m_mock;
    bool m_skip_historical_catchup;
    std::string m_host;
    std::string m_port;
    std::string m_target;
    std::string m_mode;
    std::vector<std::string> m_instruments;
    std::string m_token;
    
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
    
    // Reconnect properties
    int m_reconnect_attempts;
    const int m_max_reconnect_attempts = 15;
    int64_t m_last_reconnect_time_ms;
    
    // Redis context
    redisContext* m_redis;
    
    // Candle manager
    std::unique_ptr<CandleManager> m_candle_mgr;
};

int main(int argc, char* argv[]) {
    std::string config_path = "config.json";
    if (argc > 1) {
        config_path = argv[1];
    }
    
    try {
        MarketDataIngestor ingestor(config_path);
        ingestor.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
