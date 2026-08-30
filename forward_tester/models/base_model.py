# forward_tester/models/base_model.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import date
from forward_tester.position import ForwardTestPosition

class BaseTradingModel(ABC):
    """
    Abstract Base Class for all autonomous live forward testing strategies.
    Defines the contract for daily initialization, intraday tick/candle monitoring,
    position management, and EOD square-off.
    """
    def __init__(self, model_id: str, name: str, data_client: Any, config: Any):
        self.model_id = model_id
        self.name = name
        self.data_client = data_client
        self.config = config
        self.active_positions: List[ForwardTestPosition] = []
        self.closed_positions: List[ForwardTestPosition] = []
        self.daily_pnl: float = 0.0

    @abstractmethod
    def init_trading_day(self, trade_date: str) -> None:
        """Initialize parameters, volatility anchors, DTE routing, and day state."""
        pass

    @abstractmethod
    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        """
        Called strictly on 5-minute candle boundaries (XX:00, XX:05, XX:10, ...).
        Evaluates signals on completed candles. Returns trade action dict if triggered.
        """
        pass

    @abstractmethod
    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        """
        Monitors active positions against live spot/option price feeds.
        Evaluates stop losses, take profits, break-even locks, and partial scalings.
        Returns list of newly closed positions.
        """
        pass

    @abstractmethod
    def execute_eod_squareoff(self, exit_time_str: str = "15:00") -> List[ForwardTestPosition]:
        """Closes all active positions at 15:00 hard market cutoff."""
        pass

    def get_realized_and_unrealized_pnl(self) -> Tuple[float, float, float]:
        """Returns (realized_pnl, unrealized_pnl, total_pnl)."""
        realized = sum(p.pnl for p in self.closed_positions)
        unrealized = sum(p.pnl for p in self.active_positions if p.status == "OPEN")
        return realized, unrealized, realized + unrealized
