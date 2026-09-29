#include <iostream>
#include <vector>
#include <cassert>
#include <iomanip>

#include "../src/strategy_types.hpp"
#include "../src/fifo_pool.hpp"
#include "../src/strategy_engine.hpp"

// Mock Redis C API for deterministic exit verification
extern "C" {
    void freeReplyObject(void* reply) {
        if (reply) free(reply);
    }
    void* redisCommand(redisContext* c, const char* format, ...) {
        return nullptr;
    }
    int redisAppendCommand(redisContext* c, const char* format, ...) {
        return 0;
    }
    int redisGetReply(redisContext* c, void** reply) {
        if (reply) *reply = nullptr;
        return 0;
    }
}

void test_poc_v2_vwap_trail_exit_pe() {
    std::cout << "[RUN] Test A: Model POC V2 PE Runner Trailing Session VWAP Exit..." << std::endl;
    FIFOPool pool(25000.0);
    StrategyEngine engine(pool);

    // Seed Today's POC V2 23600 PE entered at 10:06 @ 152.65
    UnifiedPosition pos;
    pos.position_id = "UP_1";
    pos.model_name = "Model POC V2";
    pos.symbol = "NIFTY_23600_PE";
    pos.strike = 23600;
    pos.option_type = OptionType::PE;
    pos.lots = 2;
    pos.remaining_lots = 1;
    pos.quantity = 65;
    pos.entry_time = "10:06";
    pos.entry_option_price = 152.65;
    pos.entry_futures_price = 23448.00;
    pos.current_option_price = 260.00;
    pos.current_futures_price = 23350.00;
    pos.margin_locked = 9922.25;
    pos.is_active = true;
    pos.t1_hit = true;
    pos.lot1_exit_time = "10:30";
    pos.lot1_exit_opt = 175.50;
    pos.lot1_exit_fut = 23418.00;
    pos.lot1_realized_pnl = (175.50 - 152.65) * 65; // +1,485.25
    pos.lot2_sl_futures_price = 23446.00; // BE floor

    pool.active_positions.push_back(pos);
    pool.realized_profits_today = pos.lot1_realized_pnl;

    MicrostructureMetrics m;
    m.session_vwap = 23374.00; // Current live VWAP
    // Active SL for PE runner: min(23446.00, VWAP + 5.0) = min(23446.00, 23379.00) = 23379.00

    // Tick 1: Price is 23360.00 (below active SL 23379.00) -> Position remains alive
    engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23360.00, 23355.00, 23362.00, 1790060000000, m);
    assert(pool.active_positions.size() == 1);
    std::cout << "      Tick 1 (LTP 23360.00 < SL 23379.00): Position active, trailing VWAP." << std::endl;

    // Tick 2: Price spikes up to 23380.00 (high = 23381.00 >= active SL 23379.00) -> Exits!
    engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23380.00, 23375.00, 23381.00, 1790060060000, m);
    assert(pool.active_positions.empty());
    assert(pool.closed_positions.size() == 1);
    const auto& cp = pool.closed_positions[0];
    assert(cp.exit_reason == "VWAP Trail Exit");
    assert(cp.exit_futures_price == 23379.00);
    assert(cp.realized_pnl > cp.lot1_realized_pnl);
    std::cout << "      Tick 2 (High 23381.00 >= SL 23379.00): VWAP Trail Exit triggered cleanly!" << std::endl;
    std::cout << "      Total PnL (Lot 1 Banked + Lot 2 Runner): Rs " << cp.realized_pnl << std::endl;
    std::cout << "  [PASS] Test A: Model POC V2 PE VWAP Trail Exit Verified\n" << std::endl;
}

void test_spatial_box_time_exit() {
    std::cout << "[RUN] Test B: Model Spatial Box Time Exit (45m)..." << std::endl;
    FIFOPool pool(25000.0);
    StrategyEngine engine(pool);

    // Seed Today's Spatial Box 23350 PE entered at 10:52 @ 43.35 with 44 bars held
    UnifiedPosition pos;
    pos.position_id = "UP_2";
    pos.model_name = "Model Spatial Box";
    pos.symbol = "NIFTY_23350_PE";
    pos.strike = 23350;
    pos.option_type = OptionType::PE;
    pos.lots = 1;
    pos.remaining_lots = 1;
    pos.quantity = 65;
    pos.entry_time = "10:52";
    pos.entry_option_price = 43.35;
    pos.entry_futures_price = 23410.00;
    pos.current_option_price = 51.55;
    pos.current_futures_price = 23350.00;
    pos.margin_locked = 2817.75;
    pos.is_active = true;
    pos.target_opt_price = 88.35;
    pos.sl_opt_price = 28.35;
    pos.bars_held = 44; // 1 bar away from 45m time exit

    pool.active_positions.push_back(pos);

    MicrostructureMetrics m;
    m.session_vwap = 23374.00;

    // Simulate 1-minute bar close -> increments bars_held to 45
    Candle1M bar;
    bar.minute_ts = 1790060100;
    bar.open = 23350.0; bar.high = 23355.0; bar.low = 23348.0; bar.close = 23352.0; bar.volume = 1000.0;
    engine.on_1m_bar(nullptr, "NSE_INDEX|Nifty 50", bar, bar, m);

    // Bars held reaches 45 -> Time Exit triggered!
    assert(pool.active_positions.empty());
    assert(pool.closed_positions.size() == 1);
    const auto& cp = pool.closed_positions[0];
    assert(cp.exit_reason == "Time Exit (45m)");
    std::cout << "      Bars held reached 45 -> Time Exit (45m) triggered cleanly!" << std::endl;
    std::cout << "      Realized PnL: Rs " << cp.realized_pnl << " @ Rs " << cp.exit_option_price << std::endl;
    std::cout << "  [PASS] Test B: Model Spatial Box Time Exit (45m) Verified\n" << std::endl;
}

void test_spatial_box_target_and_sl_exit() {
    std::cout << "[RUN] Test C: Model Spatial Box Target (+45pt) & SL (-15pt) Exits..." << std::endl;
    
    // Sub-case 1: Target Hit (+45pt)
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "UP_2";
        pos.model_name = "Model Spatial Box";
        pos.symbol = "NIFTY_23350_PE";
        pos.strike = 23350;
        pos.option_type = OptionType::PE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:52";
        pos.entry_option_price = 43.35;
        pos.current_option_price = 88.50; // Bid crossed target 88.35!
        pos.current_futures_price = 23310.00;
        pos.margin_locked = 2817.75;
        pos.is_active = true;
        pos.target_opt_price = 88.35;
        pos.sl_opt_price = 28.35;
        pos.bars_held = 10;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23310.0, 23305.0, 23312.0, 1790060000000, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Target Reached (+45pt)");
        std::cout << "      Sub-case 1: Target Reached (+45pt) triggered cleanly!" << std::endl;
    }

    // Sub-case 2: Stop Loss Hit (-15pt)
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "UP_3";
        pos.model_name = "Model Spatial Box";
        pos.symbol = "NIFTY_23350_PE";
        pos.strike = 23350;
        pos.option_type = OptionType::PE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:52";
        pos.entry_option_price = 43.35;
        pos.current_option_price = 28.10; // Bid hit 28.10 <= SL 28.35!
        pos.current_futures_price = 23440.00;
        pos.margin_locked = 2817.75;
        pos.is_active = true;
        pos.target_opt_price = 88.35;
        pos.sl_opt_price = 28.35;
        pos.bars_held = 15;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23440.0, 23438.0, 23442.0, 1790060000000, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Stop Loss Hit (-15pt)");
        std::cout << "      Sub-case 2: Stop Loss Hit (-15pt) triggered cleanly!" << std::endl;
    }

    std::cout << "  [PASS] Test C: Model Spatial Box Target & SL Exits Verified\n" << std::endl;
}

void test_dalton_va_exits() {
    std::cout << "[RUN] Test D: Horizon 3 Causal Dalton Value Area Exits (Target, SL, Window Close)..." << std::endl;

    // Sub-case 1: Bullish Dalton VA (Long CE) -> Target VAH hit
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "DVA_1";
        pos.model_name = "Causal Dalton VA";
        pos.symbol = "NIFTY_23400_CE";
        pos.strike = 23400;
        pos.option_type = OptionType::CE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:20";
        pos.entry_option_price = 150.00;
        pos.current_option_price = 190.00;
        pos.entry_futures_price = 23380.00;
        pos.tpo_target_futures = 23420.00; // IB VAH
        pos.sl_futures_price = 23365.00;    // IB VAL - 15
        pos.is_active = true;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        // LTP crosses VAH (23420.00)
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23422.0, 23420.0, 23423.0, 1790060000000, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Target Reached (VAH)");
        std::cout << "      Sub-case 1: CE Target Reached (VAH) triggered cleanly!" << std::endl;
    }

    // Sub-case 2: Bullish Dalton VA (Long CE) -> Stop Loss hit (VAL - 15)
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "DVA_2";
        pos.model_name = "Causal Dalton VA";
        pos.symbol = "NIFTY_23400_CE";
        pos.strike = 23400;
        pos.option_type = OptionType::CE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:20";
        pos.entry_option_price = 150.00;
        pos.current_option_price = 135.00;
        pos.entry_futures_price = 23380.00;
        pos.tpo_target_futures = 23420.00; // IB VAH
        pos.sl_futures_price = 23365.00;    // IB VAL - 15
        pos.is_active = true;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        // LTP drops below SL (23365.00)
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23363.0, 23362.0, 23364.0, 1790060000000, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Stop Loss Hit (VAL - 15pt)");
        std::cout << "      Sub-case 2: CE Stop Loss Hit (VAL - 15pt) triggered cleanly!" << std::endl;
    }

    // Sub-case 3: Bearish Dalton VA (Short PE) -> Target VAL hit
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "DVA_3";
        pos.model_name = "Causal Dalton VA";
        pos.symbol = "NIFTY_23400_PE";
        pos.strike = 23400;
        pos.option_type = OptionType::PE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:35";
        pos.entry_option_price = 150.00;
        pos.current_option_price = 190.00;
        pos.entry_futures_price = 23420.00;
        pos.tpo_target_futures = 23380.00; // IB VAL
        pos.sl_futures_price = 23435.00;    // IB VAH + 15
        pos.is_active = true;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        // LTP drops below VAL (23380.00)
        engine.on_tick(nullptr, "NSE_INDEX|Nifty 50", 23378.0, 23377.0, 23379.0, 1790060000000, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Target Reached (VAL)");
        std::cout << "      Sub-case 3: PE Target Reached (VAL) triggered cleanly!" << std::endl;
    }

    // Sub-case 4: Window Close Exit at 13:30 IST
    {
        FIFOPool pool(25000.0);
        StrategyEngine engine(pool);

        UnifiedPosition pos;
        pos.position_id = "DVA_4";
        pos.model_name = "Causal Dalton VA";
        pos.symbol = "NIFTY_23400_CE";
        pos.strike = 23400;
        pos.option_type = OptionType::CE;
        pos.lots = 1;
        pos.remaining_lots = 1;
        pos.quantity = 65;
        pos.entry_time = "10:20";
        pos.entry_option_price = 150.00;
        pos.current_option_price = 160.00;
        pos.entry_futures_price = 23380.00;
        pos.tpo_target_futures = 23420.00;
        pos.sl_futures_price = 23365.00;
        pos.is_active = true;

        pool.active_positions.push_back(pos);

        MicrostructureMetrics m;
        Candle1M bar;
        // 13:30:00 IST -> epoch 1790150400 (minute 13:30)
        bar.minute_ts = 1790150400;
        bar.open = 23400.0; bar.high = 23405.0; bar.low = 23395.0; bar.close = 23400.0; bar.volume = 1000;
        engine.on_1m_bar(nullptr, "NSE_INDEX|Nifty 50", bar, bar, m);

        assert(pool.active_positions.empty());
        assert(pool.closed_positions.size() == 1);
        assert(pool.closed_positions[0].exit_reason == "Window Close (13:30 IST)");
        std::cout << "      Sub-case 4: Window Close (13:30 IST) triggered cleanly!" << std::endl;
    }

    std::cout << "  [PASS] Test D: Horizon 3 Causal Dalton Value Area Exits Verified\n" << std::endl;
}

int main() {
    std::cout << "==========================================================================" << std::endl;
    std::cout << "   ULLTR LIVE PRODUCTION EXIT LOGIC MATHEMATICAL VERIFICATION SUITE       " << std::endl;
    std::cout << "   (Model POC V2 + Spatial Box + Causal Dalton VA)                        " << std::endl;
    std::cout << "==========================================================================" << std::endl << std::endl;

    test_poc_v2_vwap_trail_exit_pe();
    test_spatial_box_time_exit();
    test_spatial_box_target_and_sl_exit();
    test_dalton_va_exits();

    std::cout << "==========================================================================" << std::endl;
    std::cout << "   🎉 ALL MODEL EXIT LOGIC CONFIRMED 100% OPERATIONAL & ACCURATE!         " << std::endl;
    std::cout << "==========================================================================" << std::endl;
    return 0;
}
