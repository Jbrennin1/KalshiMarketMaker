"""
Test utilities for WebSocket integration tests
Provides mock WebSocket helpers and message generators
"""
import json
import time
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock


class MockWebSocketApp:
    """Mock websocket client for unit tests"""
    def __init__(self):
        self.sent_messages = []
        self.on_open_callback = None
        self.on_message_callback = None
        self.on_error_callback = None
        self.on_close_callback = None
        self.is_running = False
        
    def send(self, message: str):
        """Capture sent messages"""
        self.sent_messages.append(json.loads(message))
        
    def run_forever(self):
        """Simulate running forever"""
        self.is_running = True
        if self.on_open_callback:
            self.on_open_callback(self)
            
    def close(self):
        """Simulate close"""
        self.is_running = False
        if self.on_close_callback:
            self.on_close_callback(self, 1000, "Normal closure")
            
    def simulate_message(self, message: Dict):
        """Simulate receiving a message"""
        if self.on_message_callback:
            message_str = json.dumps(message)
            self.on_message_callback(self, message_str)
            
    def simulate_error(self, error: Exception):
        """Simulate an error"""
        if self.on_error_callback:
            self.on_error_callback(self, error)


def create_mock_ws_message(msg_type: str, sid: int, msg: Dict) -> Dict:
    """Create a mock WebSocket message"""
    return {
        "type": msg_type,
        "sid": sid,
        "msg": msg
    }


def create_mock_ticker_message(ticker: str, yes_bid: float = 0.45, yes_ask: float = 0.46, 
                                price: float = 0.455, ts: Optional[Any] = None) -> Dict:
    """Create a mock ticker message
    
    CRITICAL FIX 2.2: Support both string and numeric formats
    """
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    # Support both string and numeric formats
    return {
        "type": "ticker",
        "sid": 1,
        "msg": {
            "market_ticker": ticker,
            "price": str(int(price * 100)),  # String format
            "yes_bid": str(int(yes_bid * 100)),  # String format
            "yes_ask": str(int(yes_ask * 100)),  # String format
            "no_bid": str(int((1 - yes_ask) * 100)),
            "no_ask": str(int((1 - yes_bid) * 100)),
            "yes_bid_dollars": str(yes_bid),  # String dollars
            "yes_ask_dollars": str(yes_ask),
            "volume": "10000",
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def create_mock_orderbook_message(ticker: str, bids: list = None, asks: list = None,
                                  ts: Optional[Any] = None) -> Dict:
    """Create a mock orderbook message
    
    bids/asks format: [[price_cents, size], ...]
    """
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    if bids is None:
        bids = [[45, 100], [44, 200], [43, 300]]  # Default bids
    if asks is None:
        asks = [[46, 100], [47, 200], [48, 300]]  # Default asks
    
    return {
        "type": "orderbook",
        "sid": 2,
        "msg": {
            "market_ticker": ticker,
            "bids": bids,
            "asks": asks,
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def create_mock_fill_message(ticker: str, order_id: str, fill_id: str, 
                             price: float = 0.45, count: int = 5,
                             action: str = "buy", side: str = "yes",
                             ts: Optional[Any] = None) -> Dict:
    """Create a mock fill message"""
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    return {
        "type": "fill",
        "sid": 3,
        "msg": {
            "ticker": ticker,
            "order_id": order_id,
            "fill_id": fill_id,
            "price": str(int(price * 100)),  # String format
            "count": str(count),  # String format
            "action": action,
            "side": side,
            "remaining_count": "0",
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def create_mock_position_message(ticker: str, position: int = 10,
                                 ts: Optional[Any] = None) -> Dict:
    """Create a mock position message"""
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    return {
        "type": "position",
        "sid": 4,
        "msg": {
            "ticker": ticker,
            "position": str(position),  # String format
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def create_mock_lifecycle_message(ticker: str, event_type: str = "MARKET_HALTED",
                                 ts: Optional[Any] = None) -> Dict:
    """Create a mock lifecycle message"""
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    return {
        "type": "lifecycle",
        "sid": 5,
        "msg": {
            "ticker": ticker,
            "event_type": event_type,
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def create_mock_trade_message(ticker: str, price: float = 0.45, size: int = 10,
                              ts: Optional[Any] = None) -> Dict:
    """Create a mock trade message"""
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    return {
        "type": "trade",
        "sid": 6,
        "msg": {
            "market_ticker": ticker,
            "price": str(int(price * 100)),  # String format
            "size": str(size),  # String format
            "ts": ts if isinstance(ts, str) else str(int(ts.timestamp()))
        }
    }


def wait_for_condition(condition: Callable[[], bool], timeout: float = 5.0, 
                      interval: float = 0.1) -> bool:
    """Wait for a condition to become true
    
    Useful for async tests where we need to wait for callbacks
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False

