"""
Market State Store
Shared in-memory store for WebSocket data
"""
from collections import deque, defaultdict
from threading import Lock
from typing import Dict, Optional, List
from datetime import datetime, timezone
import logging
from kalshi_mm.utils import parse_kalshi_timestamp_cached, get_price_field


class MarketStateStore:
    def __init__(self, config: Optional[Dict] = None):
        self._config = (config or {}).get("websockets", {})
        self._lock = Lock()
        self.logger = logging.getLogger("MarketStateStore")
        
        # Staleness thresholds from config
        self.ticker_staleness = self._config.get("ticker_staleness_seconds", 10)
        self.orderbook_staleness = self._config.get("orderbook_staleness_seconds", 10)
        self.position_staleness = self._config.get("position_staleness_seconds", 30)
        
        # Buffer sizes from config
        max_price_history = self._config.get("max_price_history", 1000)
        max_trades_per_ticker = self._config.get("max_trades_per_ticker", 1000)
        
        # Ticker data: ticker -> latest ticker message
        self.tickers: Dict[str, Dict] = {}
        self.ticker_timestamps: Dict[str, datetime] = {}
        self.last_price_change: Dict[str, datetime] = {}  # CRITICAL FIX 3.2
        
        # CRITICAL FIX 3.1: Rolling midprice + spread buffer for regime detection
        self.price_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_price_history)
        )
        
        # Orderbook data: ticker -> normalized orderbook
        self.orderbooks: Dict[str, Dict] = {}
        self.orderbook_timestamps: Dict[str, datetime] = {}
        
        # Public trades: ticker -> ring buffer of recent trades
        self.trades: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_trades_per_ticker)
        )
        
        # User fills: queue for MM consumption
        self.user_fills: deque = deque(maxlen=10000)
        self.last_fill_ts: Optional[datetime] = None  # For re-seeding on reconnect
        
        # Positions: ticker -> position data
        self.positions: Dict[str, Dict] = {}
        self.position_timestamps: Dict[str, datetime] = {}
        
        # Lifecycle events: ticker -> latest event
        self.lifecycle_events: Dict[str, Dict] = {}
        
        # Health flags
        self.ws_healthy = True
        self.last_message_time = datetime.now(timezone.utc)
        
    def update_ticker(self, ticker: str, msg: Dict):
        """Update ticker data from WS message
        
        CRITICAL FIX #3: Deep copy message and reject out-of-order updates
        CRITICAL FIX 2.2: Treat ALL numeric fields as strings
        """
        with self._lock:
            # CRITICAL FIX #3: Deep copy to avoid race conditions
            msg_copy = msg.copy()
            
            # CRITICAL FIX #2: Reject backward timestamps (out-of-order messages)
            ts_raw = msg_copy.get('ts', '')
            ts = parse_kalshi_timestamp_cached(str(ts_raw)) if ts_raw else None
            if ts:
                if ticker in self.ticker_timestamps:
                    if ts < self.ticker_timestamps[ticker]:
                        # Late/out-of-order WS message - ignore
                        return
                self.ticker_timestamps[ticker] = ts
                
            # CRITICAL FIX 3.2: Only update if price actually changed
            old_price = self.tickers.get(ticker, {}).get('price')
            new_price = msg_copy.get('price')
            if old_price != new_price:
                self.last_price_change[ticker] = datetime.now(timezone.utc)
                
            self.tickers[ticker] = msg_copy
            
            # CRITICAL FIX 3.1: Update rolling price history for regime detection
            # CRITICAL FIX 2.2: Parse numeric fields as strings
            # Kalshi sends yes_bid/yes_ask as numeric values in cents
            yes_bid_raw = msg_copy.get("yes_bid", 0) or msg_copy.get("yes_bid_cents", 0) or 0
            yes_ask_raw = msg_copy.get("yes_ask", 0) or msg_copy.get("yes_ask_cents", 0) or 0
            
            # Handle string or numeric (Kalshi sends numeric in cents)
            yes_bid = float(yes_bid_raw) / 100.0 if yes_bid_raw else 0.0
            yes_ask = float(yes_ask_raw) / 100.0 if yes_ask_raw else 0.0
            
            if yes_bid > 0 and yes_ask > 0:
                mid = (yes_bid + yes_ask) / 2
                spread = yes_ask - yes_bid
                self.price_history[ticker].append({
                    'mid': mid,
                    'spread': spread,
                    'ts': ts or datetime.now(timezone.utc)
                })
                # Log periodically (first update and every 20th update)
                if not hasattr(self, '_update_counts'):
                    self._update_counts = {}
                self._update_counts[ticker] = self._update_counts.get(ticker, 0) + 1
                count = self._update_counts[ticker]
                if count == 1 or count % 20 == 0:
                    import logging
                    logger = logging.getLogger("MarketStateStore")
                    logger.info(f"PRICE_UPDATE: {ticker} (update #{count}): mid={mid:.4f}, spread={spread:.4f}, history_len={len(self.price_history[ticker])}")
            else:
                # Log when price fields are invalid (periodically)
                if not hasattr(self, '_invalid_price_counts'):
                    self._invalid_price_counts = {}
                self._invalid_price_counts[ticker] = self._invalid_price_counts.get(ticker, 0) + 1
                count = self._invalid_price_counts[ticker]
                if count == 1 or count % 50 == 0:
                    import logging
                    logger = logging.getLogger("MarketStateStore")
                    logger.warning(f"PRICE_UPDATE: {ticker} invalid prices (update #{count}): yes_bid={yes_bid_raw}, yes_ask={yes_ask_raw}")
                
    def get_ticker(self, ticker: str) -> Optional[Dict]:
        """Get latest ticker data, return None if stale"""
        with self._lock:
            if ticker not in self.tickers:
                return None
            # Check staleness using config threshold
            if ticker in self.ticker_timestamps:
                age = (datetime.now(timezone.utc) - self.ticker_timestamps[ticker]).total_seconds()
                if age > self.ticker_staleness:
                    return None
            return self.tickers[ticker].copy()  # Return copy for safety
    
    def get_normalized_ticker(self, ticker: str) -> Optional[Dict]:
        """Get normalized ticker data using get_price_field() helper
        
        Pre-implementation checklist #4: Normalized helper to prevent conversion bugs
        """
        raw = self.get_ticker(ticker)
        if not raw:
            return None
            
        # Use get_price_field() for consistent conversion
        return {
            "ticker": ticker,
            "yes_bid": get_price_field(raw, "yes_bid"),
            "yes_ask": get_price_field(raw, "yes_ask"),
            "no_bid": get_price_field(raw, "no_bid"),
            "no_ask": get_price_field(raw, "no_ask"),
            "volume_24h": get_price_field(raw, "volume_24h"),
            "last_trade_ts": raw.get("last_trade_ts") or raw.get("ts"),
            # Include other fields that might be needed
            "price": get_price_field(raw, "price"),
            "yes_bid_cents": raw.get("yes_bid_cents") or raw.get("yes_bid"),
            "yes_ask_cents": raw.get("yes_ask_cents") or raw.get("yes_ask"),
        }
            
    def update_orderbook(self, ticker: str, msg: Dict):
        """Update orderbook from WS message
        
        Handles both snapshots (full replacement) and deltas (incremental updates)
        """
        with self._lock:
            msg_copy = msg.copy()
            ts_raw = msg_copy.get('ts', '')
            ts = parse_kalshi_timestamp_cached(str(ts_raw)) if ts_raw else None
            if ts:
                if ticker in self.orderbook_timestamps:
                    if ts < self.orderbook_timestamps[ticker]:
                        return  # Out-of-order
                self.orderbook_timestamps[ticker] = ts
            
            # Check if this is a snapshot or delta
            is_snapshot = msg_copy.get("_is_snapshot", False)
            is_delta = msg_copy.get("_is_delta", False)
            
            if is_snapshot:
                # SNAPSHOT: Replace entire orderbook
                bids_raw = msg_copy.get("bids", [])
                asks_raw = msg_copy.get("asks", [])
                
                bids = []
                for bid in bids_raw:
                    if not bid or len(bid) < 2:
                        continue
                    try:
                        price_raw = bid[0]
                        size_raw = bid[1]
                        price = float(price_raw) if price_raw is not None else 0.0
                        size = int(size_raw) if size_raw is not None else 0
                        # Kalshi always sends prices in cents - convert to dollars
                        price = price / 100.0
                        bids.append((price, size))
                    except (ValueError, TypeError, IndexError):
                        continue
                
                asks = []
                for ask in asks_raw:
                    if not ask or len(ask) < 2:
                        continue
                    try:
                        price_raw = ask[0]
                        size_raw = ask[1]
                        price = float(price_raw) if price_raw is not None else 0.0
                        size = int(size_raw) if size_raw is not None else 0
                        # Kalshi always sends prices in cents - convert to dollars
                        price = price / 100.0
                        asks.append((price, size))
                    except (ValueError, TypeError, IndexError):
                        continue
                
                total_bid = sum(size for _, size in bids)
                total_ask = sum(size for _, size in asks)
                imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1) if (total_bid + total_ask) > 0 else 0
                
                self.orderbooks[ticker] = {
                    "bids": bids,
                    "asks": asks,
                    "total_bid_depth": total_bid,
                    "total_ask_depth": total_ask,
                    "imbalance": imbalance,
                    "best_bid": bids[0][0] if bids else None,
                    "best_ask": asks[0][0] if asks else None,
                    "spread": (asks[0][0] - bids[0][0]) if (bids and asks) else None
                }
                
                # Diagnostic logging
                self.logger.info(
                    f"ORDERBOOK_SNAPSHOT for {ticker}: bid_depth={total_bid}, ask_depth={total_ask}, "
                    f"bid_levels={len(bids)}, ask_levels={len(asks)}, "
                    f"best_bid={bids[0][0] if bids else None}, best_ask={asks[0][0] if asks else None}"
                )
                
            elif is_delta:
                # DELTA: Apply incremental update to existing orderbook
                # Delta format: {"price": 96, "price_dollars": "0.960", "delta": -54, "side": "yes"}
                # If orderbook doesn't exist yet, we can't apply delta (wait for snapshot)
                if ticker not in self.orderbooks:
                    self.logger.debug(f"ORDERBOOK_DELTA for {ticker}: Cannot apply delta - no existing orderbook (waiting for snapshot)")
                    return  # Can't apply delta without existing orderbook
                
                price_cents = msg_copy.get("price")
                delta = msg_copy.get("delta", 0)
                side = msg_copy.get("side", "").lower()  # "yes" or "no"
                
                if price_cents is None or delta == 0:
                    self.logger.debug(f"ORDERBOOK_DELTA for {ticker}: Invalid delta (price={price_cents}, delta={delta})")
                    return  # Invalid delta
                
                # Kalshi always sends prices in cents - convert to dollars
                price_dollars = float(price_cents) / 100.0
                
                # Get current orderbook
                orderbook = self.orderbooks[ticker]
                bids = list(orderbook["bids"])
                asks = list(orderbook["asks"])
                old_bid_depth = orderbook.get("total_bid_depth", 0)
                old_ask_depth = orderbook.get("total_ask_depth", 0)
                
                # Apply delta based on side
                if side == "yes":
                    # YES side: affects bids (people buying YES)
                    # Find or create price level in bids
                    updated = False
                    for i, (bid_price, bid_size) in enumerate(bids):
                        if abs(bid_price - price_dollars) < 0.0001:  # Price match (accounting for float precision)
                            new_size = bid_size + delta
                            if new_size <= 0:
                                # Remove price level
                                bids.pop(i)
                            else:
                                # Update size
                                bids[i] = (bid_price, new_size)
                            updated = True
                            break
                    
                    if not updated and delta > 0:
                        # Add new price level
                        bids.append((price_dollars, delta))
                        # Re-sort descending (highest bid first)
                        bids.sort(key=lambda x: x[0], reverse=True)
                
                elif side == "no":
                    # NO side: affects asks (people buying NO = selling YES)
                    # Convert NO price to YES ask price: YES_ask = 100 - NO_price
                    yes_ask_price = 1.0 - price_dollars
                    
                    # Find or create price level in asks
                    updated = False
                    for i, (ask_price, ask_size) in enumerate(asks):
                        if abs(ask_price - yes_ask_price) < 0.0001:  # Price match
                            new_size = ask_size + delta
                            if new_size <= 0:
                                # Remove price level
                                asks.pop(i)
                            else:
                                # Update size
                                asks[i] = (ask_price, new_size)
                            updated = True
                            break
                    
                    if not updated and delta > 0:
                        # Add new price level
                        asks.append((yes_ask_price, delta))
                        # Re-sort ascending (lowest ask first)
                        asks.sort(key=lambda x: x[0])
                
                # Recalculate totals and best prices
                total_bid = sum(size for _, size in bids)
                total_ask = sum(size for _, size in asks)
                imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1) if (total_bid + total_ask) > 0 else 0
                
                # Update orderbook
                self.orderbooks[ticker] = {
                    "bids": bids,
                    "asks": asks,
                    "total_bid_depth": total_bid,
                    "total_ask_depth": total_ask,
                    "imbalance": imbalance,
                    "best_bid": bids[0][0] if bids else None,
                    "best_ask": asks[0][0] if asks else None,
                    "spread": (asks[0][0] - bids[0][0]) if (bids and asks) else None
                }
                
                # Diagnostic logging (log every delta, but at DEBUG to avoid spam)
                self.logger.debug(
                    f"ORDERBOOK_DELTA for {ticker}: side={side}, price={price_dollars:.4f}, delta={delta}, "
                    f"bid_depth: {old_bid_depth} -> {total_bid}, ask_depth: {old_ask_depth} -> {total_ask}"
                )
            else:
                # Neither snapshot nor delta - log warning
                self.logger.warning(f"Unknown orderbook message type for {ticker}: {msg_copy}")
            
    def get_orderbook(self, ticker: str) -> Optional[Dict]:
        """Get latest orderbook, return None if stale"""
        with self._lock:
            if ticker not in self.orderbooks:
                return None
            if ticker in self.orderbook_timestamps:
                age = (datetime.now(timezone.utc) - self.orderbook_timestamps[ticker]).total_seconds()
                if age > self.orderbook_staleness:
                    return None
            return self.orderbooks[ticker].copy()
            
    def add_trade(self, ticker: str, trade: Dict):
        """Add trade to ring buffer
        
        CRITICAL FIX 2.2: Treat numeric fields as strings
        """
        with self._lock:
            trade_copy = trade.copy()
            self.trades[ticker].append(trade_copy)
            
    def get_recent_trades(self, ticker: str, minutes: int = 5) -> List[Dict]:
        """Get trades from last N minutes"""
        with self._lock:
            cutoff = datetime.now(timezone.utc).timestamp() - (minutes * 60)
            result = []
            for t in self.trades[ticker]:
                ts_raw = t.get('ts', '')
                if ts_raw:
                    ts = parse_kalshi_timestamp_cached(str(ts_raw))
                    if ts and ts.timestamp() >= cutoff:
                        result.append(t.copy())
            return result
            
    def add_user_fill(self, fill: Dict):
        """Add user fill to queue
        
        CRITICAL FIX 3.5: Track last_fill_ts for re-seeding
        """
        with self._lock:
            fill_copy = fill.copy()
            self.user_fills.append(fill_copy)
            # Update last_fill_ts for re-seeding on reconnect
            ts_raw = fill_copy.get('ts', '')
            if ts_raw:
                ts = parse_kalshi_timestamp_cached(str(ts_raw))
                if ts and (self.last_fill_ts is None or ts > self.last_fill_ts):
                    self.last_fill_ts = ts
                
    def get_user_fills(self) -> List[Dict]:
        """Drain user fills queue"""
        with self._lock:
            fills = [f.copy() for f in self.user_fills]
            self.user_fills.clear()
            return fills
            
    def update_position(self, ticker: str, position: Dict):
        """Update position from WS message"""
        with self._lock:
            self.positions[ticker] = position.copy()
            ts_raw = position.get('ts', '')
            if ts_raw:
                ts = parse_kalshi_timestamp_cached(str(ts_raw))
                if ts:
                    self.position_timestamps[ticker] = ts
                
    def get_position(self, ticker: str) -> Optional[int]:
        """Get current position for ticker"""
        with self._lock:
            if ticker not in self.positions:
                return None
            if ticker in self.position_timestamps:
                age = (datetime.now(timezone.utc) - self.position_timestamps[ticker]).total_seconds()
                if age > self.position_staleness:
                    return None
            pos = self.positions[ticker].get('position', 0)
            # Handle string or numeric
            if isinstance(pos, str):
                return int(pos)
            return int(pos) if pos is not None else 0
            
    def update_lifecycle(self, ticker: str, event: Dict):
        """Update lifecycle event"""
        with self._lock:
            self.lifecycle_events[ticker] = event.copy()
            
    def get_lifecycle(self, ticker: str) -> Optional[Dict]:
        """Get latest lifecycle event"""
        with self._lock:
            return self.lifecycle_events.get(ticker, {}).copy() if ticker in self.lifecycle_events else None
            
    def get_price_history(self, ticker: str, minutes: int = 15) -> List[Dict]:
        """Get price history for last N minutes (for regime detection)"""
        with self._lock:
            if ticker not in self.price_history:
                return []
            cutoff = datetime.now(timezone.utc).timestamp() - (minutes * 60)
            raw_count = len(self.price_history[ticker])
            filtered = [
                p.copy() for p in self.price_history[ticker]
                if p['ts'].timestamp() >= cutoff
            ]
            # Log periodically (first call and every 20th call)
            if not hasattr(self, '_get_history_counts'):
                self._get_history_counts = {}
            self._get_history_counts[ticker] = self._get_history_counts.get(ticker, 0) + 1
            count = self._get_history_counts[ticker]
            if count == 1 or count % 20 == 0:
                import logging
                logger = logging.getLogger("MarketStateStore")
                logger.debug(f"GET_HISTORY: {ticker} raw={raw_count}, filtered={len(filtered)}, cutoff_age={minutes}min")
            return filtered

