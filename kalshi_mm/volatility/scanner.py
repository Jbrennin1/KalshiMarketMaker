"""
Volatility Scanner - Continuously monitors all markets for volatility spikes
"""
import json
import logging
import time
import threading
from typing import Dict, Optional, Callable, List
from datetime import datetime, timezone
import statistics
import math

from kalshi_mm.discovery import MarketDiscovery
from kalshi_mm.api import KalshiTradingAPI
from kalshi_mm.utils import get_price_field, parse_kalshi_timestamp_cached
from kalshi_mm.volatility.models import MarketState, VolatilityEvent, VolatilityEndedEvent


class VolatilityScanner:
    """Continuously scans markets for volatility events"""
    
    def __init__(
        self,
        api: KalshiTradingAPI,
        discovery: MarketDiscovery,
        config: Dict,
        event_callback: Optional[Callable[[VolatilityEvent], None]] = None,
        ended_callback: Optional[Callable[[VolatilityEndedEvent], None]] = None,
        ws_client: Optional[object] = None,  # KalshiWebsocketClient
        state_store: Optional[object] = None,  # MarketStateStore
    ):
        self.api = api
        self.discovery = discovery
        self.config = config
        self.event_callback = event_callback
        self.ended_callback = ended_callback
        self.logger = logging.getLogger("VolatilityScanner")
        self.ws_client = ws_client
        self.state_store = state_store
        self.ws_enabled = config.get('websockets', {}).get('enabled', False)
        self.ws_fallback = config.get('websockets', {}).get('ws_fallback_on_unhealthy', True)
        self._subscription_ids = {}  # ticker -> list of subscription IDs
        
        # Track which markets we're subscribed to
        self._subscribed_markets = set()
        
        # Set up detailed file logger for sweep cycles
        self.detail_logger = logging.getLogger("VolatilityScanner.Details")
        self.detail_logger.setLevel(logging.INFO)
        # Create log file with date in name in logs/ directory
        log_dir = config.get('logging', {}).get('log_dir', 'logs')
        import os
        os.makedirs(log_dir, exist_ok=True)
        log_filename = os.path.join(log_dir, f"volatility_sweep_details_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.FileHandler(log_filename, mode='a')
        file_handler.setLevel(logging.INFO)
        detail_formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(detail_formatter)
        self.detail_logger.addHandler(file_handler)
        self.detail_logger.propagate = False  # Don't propagate to root logger
        
        # Set up pipeline analytics logger for filtering reports
        self.pipeline_logger = logging.getLogger("VolatilityScanner.Pipeline")
        self.pipeline_logger.setLevel(logging.INFO)
        pipeline_log_filename = os.path.join(log_dir, f"filtering_pipeline_{datetime.now().strftime('%Y%m%d')}.log")
        pipeline_file_handler = logging.FileHandler(pipeline_log_filename, mode='a')
        pipeline_file_handler.setLevel(logging.INFO)
        pipeline_formatter = logging.Formatter('%(asctime)s - %(message)s')
        pipeline_file_handler.setFormatter(pipeline_formatter)
        self.pipeline_logger.addHandler(pipeline_file_handler)
        self.pipeline_logger.propagate = False  # Don't propagate to root logger
        
        # Sweep cycle tracking
        self.markets_checked_this_cycle: set = set()
        self.current_cycle_start_time: Optional[datetime] = None
        self.cycle_number = 0
        self.cycle_market_details: List[Dict] = []  # Store details for current cycle
        
        # Scanner configuration
        scanner_config = config.get('volatility_scanner', {})
        self.refresh_interval = scanner_config.get('refresh_interval_seconds', 60)  # Metadata validation interval
        self.rolling_window_minutes = scanner_config.get('rolling_window_minutes', 15)
        self.volatility_window_minutes = scanner_config.get('volatility_window_minutes', 10)
        
        # Volatility thresholds
        self.price_jump_threshold_cents = scanner_config.get('price_jump_threshold_cents', 12)
        self.price_jump_window_minutes = scanner_config.get('price_jump_window_minutes', 5)
        self.sigma_threshold = scanner_config.get('sigma_threshold', 0.02)
        
        # Category exclusions
        self.exclude_categories = set(scanner_config.get('exclude_categories', []))
        
        # Volume: Simple absolute threshold check (reuses scoring.min_volume_24h from config)
        # Note: volume_24h from Kalshi API is only updated the next day (stale data),
        # so we use simple absolute threshold instead of tracking changes
        
        # Liquidity requirements
        self.max_time_to_expiry_hours = scanner_config.get('max_time_to_expiry_hours', 24)
        self.min_liquidity_spread = scanner_config.get('min_liquidity_spread', 0.10)
        
        # In-memory state: ticker -> MarketState
        self.market_states: Dict[str, MarketState] = {}
        
        # Previous volume deltas for velocity calculation
        self.prev_volume_deltas: Dict[str, float] = {}
        
        # Running flag
        self.running = False
        self.scanner_thread: Optional[threading.Thread] = None
        
        # Track active volatility events (tickers with active sessions)
        self.active_volatility_events: set = set()
        
        # Two-phase scanning state
        self.initial_scan_complete = False
        self.scan_count = 0
        self.tracked_tickers = set()  # Markets we're actively tracking (filtered)
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Rediscovery thread
        self.rediscovery_thread: Optional[threading.Thread] = None
        
        # Shared state for regime (Refinement 9: Backpressure Against Scanner Lag)
        # MM can poll this with the freshest regime timestamp
        self.regime_by_ticker: Dict[str, Dict] = {}  # ticker -> {'regime': str, 'timestamp': datetime}
        self.regime_lock = threading.Lock()
    
    # Phase 2.4: Dynamic Subscription Management
    def _subscribe_to_market(self, ticker: str):
        """Subscribe to WS channels for a market"""
        if not self.ws_enabled or not self.ws_client or not self.state_store:
            return
        
        if ticker in self._subscribed_markets:
            return  # Already subscribed
        
        try:
            sids = []
            # Subscribe ticker (always) - use event-driven callback
            sid = self.ws_client.subscribe_ticker(ticker, lambda msg_type, msg: 
                self._on_ticker_update(ticker, msg_type, msg))
            sids.append(sid)
            
            # Subscribe orderbook (if enabled)
            if self.config.get('websockets', {}).get('subscribe_orderbook', True):
                sid = self.ws_client.subscribe_orderbook(ticker, lambda msg_type, msg:
                    self.state_store.update_orderbook(ticker, msg))
                sids.append(sid)
                
            # Subscribe trades (if enabled)
            if self.config.get('websockets', {}).get('subscribe_trades', True):
                sid = self.ws_client.subscribe_trades(ticker, lambda msg_type, msg:
                    self.state_store.add_trade(ticker, msg))
                sids.append(sid)
            
            self._subscription_ids[ticker] = sids
            self._subscribed_markets.add(ticker)
            self.logger.debug(f"Subscribed to WS channels for {ticker}: {len(sids)} subscriptions")
        except Exception as e:
            self.logger.error(f"Error subscribing to {ticker}: {e}", exc_info=True)
            
    def _unsubscribe_from_market(self, ticker: str):
        """Unsubscribe when market is removed from tracking"""
        if not self.ws_enabled or not self.ws_client:
            return
        
        if ticker not in self._subscribed_markets:
            return
        
        try:
            sids = self._subscription_ids.get(ticker, [])
            for sid in sids:
                self.ws_client.unsubscribe(sid)
            self._subscription_ids.pop(ticker, None)
            self._subscribed_markets.discard(ticker)
            
            # Remove market assignment from connection pool to free up the connection slot
            # Check if ws_client is a WebSocketConnectionPool (has remove_market_assignment method)
            if hasattr(self.ws_client, 'remove_market_assignment'):
                try:
                    self.ws_client.remove_market_assignment(ticker)
                    self.logger.debug(f"Removed market assignment for {ticker}, connection slot freed")
                except Exception as e:
                    self.logger.warning(f"Error removing market assignment for {ticker}: {e}")
            
            self.logger.debug(f"Unsubscribed from WS channels for {ticker}")
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {ticker}: {e}", exc_info=True)
    
    def _on_ticker_update(self, ticker: str, msg_type: str, msg: Dict):
        """
        Event-driven handler for WebSocket ticker updates.
        Updates MarketState.mid_prices directly from WebSocket messages.
        """
        try:
            # Track ticker update counts for periodic logging
            if not hasattr(self, '_ticker_update_counts'):
                self._ticker_update_counts = {}
            self._ticker_update_counts[ticker] = self._ticker_update_counts.get(ticker, 0) + 1
            
            # Log first update and every 10th update
            count = self._ticker_update_counts[ticker]
            if count == 1 or count % 10 == 0:
                self.logger.info(f"TICKER_UPDATE: {ticker} (update #{count}), yes_bid={msg.get('yes_bid')}, yes_ask={msg.get('yes_ask')}")
            
            # Update state_store for latest ticker snapshot (used by other components)
            if self.state_store:
                self.state_store.update_ticker(ticker, msg)
            
            # Only update if market is tracked and WebSocket is healthy
            if not self.state_store or not self.state_store.ws_healthy:
                if count == 1 or count % 50 == 0:
                    self.logger.warning(f"TICKER_UPDATE: {ticker} skipped update (ws_healthy={self.state_store.ws_healthy if self.state_store else False})")
                return
            
            # Check if market is tracked
            with self.lock:
                if ticker not in self.market_states:
                    if count == 1 or count % 50 == 0:
                        self.logger.warning(f"TICKER_UPDATE: {ticker} not tracked yet (update #{count})")
                    return  # Market not tracked yet
                
                # Get market state (while holding lock)
                state = self.market_states[ticker]
            
            # Extract price data from message (Kalshi sends yes_bid/yes_ask as numeric cents)
            yes_bid_raw = msg.get("yes_bid", 0) or msg.get("yes_bid_cents", 0) or 0
            yes_ask_raw = msg.get("yes_ask", 0) or msg.get("yes_ask_cents", 0) or 0
            
            # Convert from cents to dollars
            yes_bid = float(yes_bid_raw) / 100.0 if yes_bid_raw else 0.0
            yes_ask = float(yes_ask_raw) / 100.0 if yes_ask_raw else 0.0
            
            # Skip if invalid prices
            if yes_bid <= 0 or yes_ask <= 0:
                if count == 1 or count % 50 == 0:
                    self.logger.warning(f"TICKER_UPDATE: {ticker} invalid prices (yes_bid={yes_bid_raw}, yes_ask={yes_ask_raw})")
                return
            
            # Calculate mid and spread
            mid = (yes_bid + yes_ask) / 2
            spread = yes_ask - yes_bid
            
            # Parse timestamp
            ts_raw = msg.get('ts', '')
            ts = parse_kalshi_timestamp_cached(str(ts_raw)) if ts_raw else datetime.now(timezone.utc)
            if not ts:
                ts = datetime.now(timezone.utc)
            
            # Check if market was in warmup phase before update
            points_before = len(state.mid_prices)
            was_in_warmup = points_before < 3
            
            # Duplicate detection: check if timestamp already exists (within 1 second tolerance)
            ts_int = int(ts.timestamp())
            existing_timestamps = {int(existing_ts.timestamp()) for existing_ts in state.timestamps}
            
            if ts_int in existing_timestamps:
                # Duplicate timestamp - skip
                if count == 1 or count % 100 == 0:
                    self.logger.debug(f"TICKER_UPDATE: {ticker} duplicate timestamp {ts_int}, skipping")
                return
            
            # Get last known liquidity value (WebSocket ticker doesn't include liquidity)
            last_liquidity = state.liquidity_values[-1] if state.liquidity_values else 0.0
            
            # Append directly to MarketState (single source of truth)
            with self.lock:
                state.mid_prices.append(mid)
                state.spreads.append(spread)
                state.timestamps.append(ts)
                state.liquidity_values.append(last_liquidity)
                state.last_activity_time = ts
            
            # Trim to rolling window
            state.trim_to_window(self.rolling_window_minutes)
            
            # Check if market exited warmup phase
            points_after = len(state.mid_prices)
            warmup_exit = was_in_warmup and points_after >= 3
            
            # Log update activity (first time and periodically)
            if count == 1 or (count % 10 == 0) or (points_before < 3 and count % 20 == 0):
                self.logger.info(f"PRICE_UPDATE: {ticker} added 1 point ({points_before} -> {points_after} total), mid={mid:.4f}, spread={spread:.4f}")
            
            # Log warmup exit immediately (important event)
            if warmup_exit:
                self.logger.info(
                    f"WARMUP_EXIT: {ticker} exited warmup phase ({points_before} -> {points_after} price points)"
                )
        
        except Exception as e:
            self.logger.error(f"{ticker}: Error in WebSocket ticker update: {e}", exc_info=True)
    
    def start(self):
        """Start the scanner in a background thread"""
        if self.running:
            self.logger.warning("Scanner already running")
            return
        
        self.running = True
        self.scanner_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.scanner_thread.start()
        self.logger.info(f"Volatility scanner started (metadata validation interval: {self.refresh_interval}s)")
        
        # Start rediscovery thread
        self.rediscovery_thread = threading.Thread(target=self._rediscovery_loop, daemon=True)
        self.rediscovery_thread.start()
        
        if self.ws_enabled and self.state_store:
            self.logger.info("WebSocket event-driven price updates enabled")
    
    def stop(self):
        """Stop the scanner"""
        self.running = False
        
        if self.scanner_thread:
            self.scanner_thread.join(timeout=10)
        self.logger.info("Volatility scanner stopped")
    
    def get_regime(self, ticker: str) -> Optional[Dict]:
        """
        Get current regime for a ticker (for MM access).
        Returns: {'regime': str, 'timestamp': datetime, 'stable': bool} or None
        """
        with self.regime_lock:
            return self.regime_by_ticker.get(ticker)
    
    def _scan_loop(self):
        """
        Main scanning loop for metadata validation and market filtering.
        Note: Price monitoring is handled directly by _on_ticker_update() WebSocket callback when WebSocket is enabled.
        This loop focuses on:
        - Fetching market metadata (volume_24h, status, time_to_expiry, close_time, category)
        - Validating markets are still worth tracking
        - Removing markets that no longer meet criteria
        """
        while self.running:
            scan_start = time.time()
            try:
                if not self.initial_scan_complete:
                    self._initial_full_scan()
                    self.initial_scan_complete = True
                else:
                    self._incremental_sweep()
                self.scan_count += 1
            except Exception as e:
                self.logger.error(f"Error in scan loop: {e}", exc_info=True)
            
            scan_duration = time.time() - scan_start
            sleep_time = max(0, self.refresh_interval - scan_duration)
            
            if scan_duration > self.refresh_interval:
                self.logger.warning(
                    f"Scan {self.scan_count} took {scan_duration:.1f}s "
                    f"(exceeded {self.refresh_interval}s interval)"
                )
            
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    def _filter_active_markets(self, markets: List[Dict], verbose_logging: bool = False) -> tuple:
        """
        Filter to markets worth tracking (status, age, volume, spread, liquidity, price, freshness)
        Returns: (filtered_markets, rejection_details) if verbose_logging, else (filtered_markets, None)
        """
        filtered = []
        now = datetime.now(timezone.utc)
        
        # Track rejection reasons for diagnostics
        rejection_counts = {
            'category': 0,
            'status': 0,
            'age': 0,
            'volume': 0,
            'freshness': 0,
            'spread': 0,
            'two_sided': 0,
            'liquidity': 0,
            'price': 0
        }
        
        # Track detailed rejection data for ALL rejected markets
        rejection_details = {
            'category': [],
            'status': [],
            'age': [],
            'volume': [],
            'freshness': [],
            'spread': [],
            'two_sided': [],
            'liquidity': [],
            'price': []
        }
        
        # Track first rejected market for diagnostic logging
        first_rejected_market = None
        first_rejection_reason = None
        first_rejection_status_value = None
        
        # Get thresholds for detailed logging
        min_volume_24h = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
        
        for market in markets:
            # Get market metrics for detailed rejection tracking
            ticker = market.get('ticker', 'unknown')
            
            # Check category exclusion FIRST (before calculating expensive metrics)
            category = self.discovery.get_category(market)
            if category in self.exclude_categories:
                rejection_counts['category'] += 1
                rejection_details['category'].append({
                    'ticker': ticker,
                    'category': category,
                    'excluded_categories': list(self.exclude_categories)
                })
                self.logger.debug(f"Market {ticker} rejected by category filter: category='{category}' (excluded: {self.exclude_categories})")
                continue
            
            spread = self.discovery.calculate_spread(market)
            volume_24h = get_price_field(market, 'volume_24h') or 0.0
            liquidity = self.discovery.get_book_depth(market)
            mid_price = self.discovery.get_mid_price(market, 'yes')
            
            # Check status (must be active/open - ignore paused, closed, expired, settlement)
            # Note: API returns status='active' for open markets, not 'open'
            status = market.get('status', '')
            if status:  # Only check if status field exists
                if status.lower() not in ('active', 'open'):  # Accept both 'active' and 'open'
                    rejection_counts['status'] += 1
                    rejection_details['status'].append({
                        'ticker': ticker,
                        'status': status,
                        'spread': spread,
                        'volume_24h': volume_24h,
                        'liquidity': liquidity,
                        'mid_price': mid_price
                    })
                    # Track first rejection for diagnostics
                    if first_rejected_market is None:
                        first_rejected_market = market
                        first_rejection_reason = 'status'
                        first_rejection_status_value = status
                    self.logger.debug(f"Market {ticker} rejected by status filter: status='{status}' (expected 'active' or 'open')")
                    continue
            # If status is missing, trust API filter and allow market
            else:
                # Log when status field is missing (for diagnostics)
                self.logger.debug(f"Market {market.get('ticker', 'unknown')} has no status field, trusting API filter")
            
            # Check market age (must be at least 60 seconds old to avoid newborn markets with garbage books)
            # Use open_time instead of created_time (API doesn't have created_time field)
            created_time = market.get('open_time') or market.get('created_time')
            if created_time:
                try:
                    if isinstance(created_time, str):
                        # Handle ISO format strings
                        if created_time.endswith('Z'):
                            created_time = created_time[:-1] + '+00:00'
                        created_time = datetime.fromisoformat(created_time)
                    if isinstance(created_time, datetime):
                        if created_time.tzinfo is None:
                            created_time = created_time.replace(tzinfo=timezone.utc)
                        elif created_time.tzinfo != timezone.utc:
                            created_time = created_time.astimezone(timezone.utc)
                        age_seconds = (now - created_time).total_seconds()
                        if age_seconds < 60:
                            rejection_counts['age'] += 1
                            rejection_details['age'].append({
                                'ticker': ticker,
                                'age_seconds': age_seconds,
                                'spread': spread,
                                'volume_24h': volume_24h,
                                'liquidity': liquidity,
                                'mid_price': mid_price
                            })
                            continue
                except Exception as e:
                    self.logger.debug(f"Error parsing created_time for {market.get('ticker', 'unknown')}: {e}")
                    # If we can't parse, allow the market (better to include than exclude)
            
            # Check volume (must meet minimum threshold - same as discovery/scoring)
            if volume_24h < min_volume_24h:
                rejection_counts['volume'] += 1
                rejection_details['volume'].append({
                    'ticker': ticker,
                    'volume_24h': volume_24h,
                    'threshold': min_volume_24h,
                    'spread': spread,
                    'liquidity': liquidity,
                    'mid_price': mid_price
                })
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'volume'
                continue
            
            # Check freshness (must have recent activity)
            # Use time_of_last_market_fetch as proxy for last_trade_timestamp
            last_activity = market.get('time_of_last_market_fetch')
            if last_activity:
                try:
                    if isinstance(last_activity, str):
                        if last_activity.endswith('Z'):
                            last_activity = last_activity[:-1] + '+00:00'
                        last_activity = datetime.fromisoformat(last_activity)
                    if isinstance(last_activity, datetime):
                        if last_activity.tzinfo is None:
                            last_activity = last_activity.replace(tzinfo=timezone.utc)
                        elif last_activity.tzinfo != timezone.utc:
                            last_activity = last_activity.astimezone(timezone.utc)
                        freshness_seconds = (now - last_activity).total_seconds()
                        if freshness_seconds > 600:  # 10 minutes
                            rejection_counts['freshness'] += 1
                            rejection_details['freshness'].append({
                                'ticker': ticker,
                                'freshness_seconds': freshness_seconds,
                                'spread': spread,
                                'volume_24h': volume_24h,
                                'liquidity': liquidity,
                                'mid_price': mid_price
                            })
                            if first_rejected_market is None:
                                first_rejected_market = market
                                first_rejection_reason = 'freshness'
                            continue
                except Exception as e:
                    self.logger.debug(f"Error parsing last_activity for {market.get('ticker', 'unknown')}: {e}")
                    # If we can't parse, allow the market (better to include than exclude)
            
            # Check spread (must be tight with real two-sided orderbook)
            if spread > 0.20:  # 20 cent max spread (tighter than 35c)
                rejection_counts['spread'] += 1
                rejection_details['spread'].append({
                    'ticker': ticker,
                    'spread': spread,
                    'threshold': 0.20,
                    'volume_24h': volume_24h,
                    'liquidity': liquidity,
                    'mid_price': mid_price
                })
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'spread'
                continue
            
            # Check for real two-sided orderbook
            # Kalshi uses yes_bid/yes_ask (in cents)
            yes_bid = market.get('yes_bid', 0)
            yes_ask = market.get('yes_ask', 0)
            # Convert from cents to dollars
            yes_bid_normalized = yes_bid / 100 if yes_bid > 0 else 0
            yes_ask_normalized = yes_ask / 100 if yes_ask > 0 else 1
            
            if yes_bid_normalized <= 0 or yes_ask_normalized >= 1:
                rejection_counts['two_sided'] += 1
                rejection_details['two_sided'].append({
                    'ticker': ticker,
                    'yes_bid': yes_bid,
                    'yes_ask': yes_ask,
                    'yes_bid_normalized': yes_bid_normalized,
                    'yes_ask_normalized': yes_ask_normalized,
                    'spread': spread,
                    'volume_24h': volume_24h,
                    'liquidity': liquidity,
                    'mid_price': mid_price
                })
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'two_sided'
                continue
            
            # Check liquidity
            if liquidity == 0:
                rejection_counts['liquidity'] += 1
                rejection_details['liquidity'].append({
                    'ticker': ticker,
                    'liquidity': liquidity,
                    'spread': spread,
                    'volume_24h': volume_24h,
                    'mid_price': mid_price
                })
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'liquidity'
                continue
            
            # Check price (avoid extremes)
            if mid_price < 0.05 or mid_price > 0.95:
                rejection_counts['price'] += 1
                rejection_details['price'].append({
                    'ticker': ticker,
                    'mid_price': mid_price,
                    'threshold_min': 0.05,
                    'threshold_max': 0.95,
                    'spread': spread,
                    'volume_24h': volume_24h,
                    'liquidity': liquidity
                })
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'price'
                continue
            
            filtered.append(market)
        
        # Log rejection statistics
        if len(markets) > 0:
            total_rejected = sum(rejection_counts.values())
            self.logger.info(
                f"Filter stats: {len(filtered)}/{len(markets)} passed. "
                f"Rejections: status={rejection_counts['status']}, "
                f"age={rejection_counts['age']}, volume={rejection_counts['volume']}, "
                f"freshness={rejection_counts['freshness']}, spread={rejection_counts['spread']}, "
                f"two_sided={rejection_counts['two_sided']}, liquidity={rejection_counts['liquidity']}, "
                f"price={rejection_counts['price']}"
            )
            
            # Diagnostic logging for first rejected market
            if verbose_logging and first_rejected_market is not None:
                self.logger.info("=" * 80)
                self.logger.info("DIAGNOSTIC: First Rejected Market Details")
                self.logger.info("=" * 80)
                self.logger.info(f"Rejection reason: {first_rejection_reason}")
                if first_rejection_reason == 'status':
                    self.logger.info(f"Status value found: '{first_rejection_status_value}' (type: {type(first_rejection_status_value).__name__})")
                self.logger.info(f"Market ticker: {first_rejected_market.get('ticker', 'N/A')}")
                self.logger.info("All field names in market object:")
                for field_name in sorted(first_rejected_market.keys()):
                    value = first_rejected_market[field_name]
                    # Truncate long values
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    self.logger.info(f"  - {field_name}: {value_str}")
                self.logger.info("")
                self.logger.info("Complete market object (JSON):")
                try:
                    market_json = json.dumps(first_rejected_market, indent=2, default=str)
                    self.logger.info(market_json)
                except Exception as e:
                    self.logger.warning(f"Could not serialize market to JSON: {e}")
                self.logger.info("=" * 80)
            
            # Log detailed rejection summary if verbose logging enabled
            if verbose_logging:
                self._log_detailed_rejection_summary(rejection_details, rejection_counts)
                return filtered, rejection_details
        
        return filtered, None
    
    def _log_detailed_rejection_summary(self, rejection_details: Dict, rejection_counts: Dict):
        """Log detailed summary of rejected markets with metrics and distributions"""
        self.pipeline_logger.info("")
        self.pipeline_logger.info("=" * 100)
        self.pipeline_logger.info("DETAILED REJECTION ANALYSIS")
        self.pipeline_logger.info("=" * 100)
        
        for reason, details_list in rejection_details.items():
            if not details_list:
                continue
            
            count = rejection_counts.get(reason, 0)
            self.pipeline_logger.info(f"\n{reason.upper()} REJECTIONS: {count} markets")
            self.pipeline_logger.info("-" * 100)
            
            # Calculate distributions
            if reason == 'volume':
                volumes = [d['volume_24h'] for d in details_list]
                if volumes:
                    volumes.sort()
                    self.pipeline_logger.info(f"  Volume distribution: min=${volumes[0]:.2f}, "
                                   f"max=${volumes[-1]:.2f}, median=${volumes[len(volumes)//2]:.2f}, "
                                   f"p95=${volumes[int(len(volumes)*0.95)]:.2f}")
                    threshold = details_list[0].get('threshold', 1000.0)
                    self.pipeline_logger.info(f"  Threshold: ${threshold:.2f}")
                    # Show markets closest to threshold (top 10)
                    close_to_threshold = sorted(details_list, key=lambda x: abs(x['volume_24h'] - threshold))[:10]
                    self.pipeline_logger.info(f"  Top 10 closest to threshold:")
                    for d in close_to_threshold:
                        gap = threshold - d['volume_24h']
                        self.pipeline_logger.info(f"    {d['ticker']}: ${d['volume_24h']:.2f} (${gap:.2f} below threshold, "
                                       f"spread={d['spread']:.3f}, liquidity={d['liquidity']:.2f})")
            
            elif reason == 'spread':
                spreads = [d['spread'] for d in details_list]
                if spreads:
                    spreads.sort()
                    self.pipeline_logger.info(f"  Spread distribution: min={spreads[0]:.3f}, "
                                   f"max={spreads[-1]:.3f}, median={spreads[len(spreads)//2]:.3f}, "
                                   f"p95={spreads[int(len(spreads)*0.95)]:.3f}")
                    threshold = details_list[0].get('threshold', 0.20)
                    self.pipeline_logger.info(f"  Threshold: {threshold:.3f}")
                    # Show markets closest to threshold
                    close_to_threshold = sorted(details_list, key=lambda x: abs(x['spread'] - threshold))[:10]
                    self.pipeline_logger.info(f"  Top 10 closest to threshold:")
                    for d in close_to_threshold:
                        gap = d['spread'] - threshold
                        self.pipeline_logger.info(f"    {d['ticker']}: {d['spread']:.3f} ({gap:.3f} above threshold, "
                                       f"volume=${d['volume_24h']:.2f}, liquidity={d['liquidity']:.2f})")
            
            elif reason == 'age':
                ages = [d['age_seconds'] for d in details_list]
                if ages:
                    ages.sort()
                    self.pipeline_logger.info(f"  Age distribution: min={ages[0]:.1f}s, "
                                   f"max={ages[-1]:.1f}s, median={ages[len(ages)//2]:.1f}s")
                    self.pipeline_logger.info(f"  Threshold: 60 seconds")
                    # Show oldest markets (closest to threshold)
                    close_to_threshold = sorted(details_list, key=lambda x: x['age_seconds'], reverse=True)[:10]
                    self.pipeline_logger.info(f"  Top 10 closest to threshold (oldest):")
                    for d in close_to_threshold:
                        gap = 60 - d['age_seconds']
                        self.pipeline_logger.info(f"    {d['ticker']}: {d['age_seconds']:.1f}s ({gap:.1f}s below threshold, "
                                       f"volume=${d['volume_24h']:.2f}, spread={d['spread']:.3f})")
            
            elif reason == 'freshness':
                freshnesses = [d['freshness_seconds'] for d in details_list]
                if freshnesses:
                    freshnesses.sort()
                    self.pipeline_logger.info(f"  Freshness distribution: min={freshnesses[0]:.1f}s, "
                                   f"max={freshnesses[-1]:.1f}s, median={freshnesses[len(freshnesses)//2]:.1f}s")
                    self.pipeline_logger.info(f"  Threshold: 600 seconds (10 minutes)")
                    # Show freshest markets (closest to threshold)
                    close_to_threshold = sorted(details_list, key=lambda x: x['freshness_seconds'])[:10]
                    self.pipeline_logger.info(f"  Top 10 closest to threshold (freshest):")
                    for d in close_to_threshold:
                        gap = d['freshness_seconds'] - 600
                        self.pipeline_logger.info(f"    {d['ticker']}: {d['freshness_seconds']:.1f}s ({gap:.1f}s above threshold, "
                                       f"volume=${d['volume_24h']:.2f}, spread={d['spread']:.3f})")
            
            elif reason == 'price':
                prices = [d['mid_price'] for d in details_list]
                if prices:
                    prices.sort()
                    self.pipeline_logger.info(f"  Price distribution: min={prices[0]:.4f}, "
                                   f"max={prices[-1]:.4f}, median={prices[len(prices)//2]:.4f}")
                    self.pipeline_logger.info(f"  Threshold: 0.05 - 0.95")
                    # Show sample markets
                    self.pipeline_logger.info(f"  Sample rejected markets (first 10):")
                    for d in details_list[:10]:
                        self.pipeline_logger.info(f"    {d['ticker']}: price={d['mid_price']:.4f}, "
                                       f"volume=${d['volume_24h']:.2f}, spread={d['spread']:.3f}")
            
            else:
                # For other rejection types, show sample markets
                self.pipeline_logger.info(f"  Sample rejected markets (first 10):")
                for d in details_list[:10]:
                    self.pipeline_logger.info(f"    {d['ticker']}: volume=${d.get('volume_24h', 0):.2f}, "
                                   f"spread={d.get('spread', 0):.3f}, liquidity={d.get('liquidity', 0):.2f}")
        
        self.pipeline_logger.info("")
        self.pipeline_logger.info("=" * 100)
    
    def _log_filtering_pipeline_report(self, all_markets: List[Dict], binary_markets: List[Dict], 
                                      active_markets: List[Dict], scored_markets: List[tuple],
                                      top_markets: List[tuple], rejection_details: Optional[Dict]):
        """Log complete filtering pipeline analysis with recommendations"""
        self.pipeline_logger.info("")
        self.pipeline_logger.info("=" * 100)
        self.pipeline_logger.info("COMPLETE FILTERING PIPELINE REPORT")
        self.pipeline_logger.info("=" * 100)
        
        # Stage 1: API Fetch
        total_fetched = len(all_markets)
        self.pipeline_logger.info(f"\nStage 1 - API Fetch: {total_fetched:,} markets")
        
        # Stage 2: Binary Filter
        non_binary = total_fetched - len(binary_markets)
        binary_pct = (len(binary_markets) / total_fetched * 100) if total_fetched > 0 else 0
        self.pipeline_logger.info(f"Stage 2 - Binary Filter: {len(binary_markets):,} passed, "
                        f"{non_binary:,} removed ({non_binary/total_fetched*100:.1f}%)")
        self.pipeline_logger.info(f"  Retention: {binary_pct:.1f}% of total markets")
        
        # Stage 3: Active Market Filter
        rejected = len(binary_markets) - len(active_markets)
        active_pct = (len(active_markets) / len(binary_markets) * 100) if len(binary_markets) > 0 else 0
        self.pipeline_logger.info(f"Stage 3 - Active Market Filter: {len(active_markets):,} passed, "
                        f"{rejected:,} removed ({rejected/len(binary_markets)*100:.1f}% of binary)")
        self.pipeline_logger.info(f"  Retention: {active_pct:.1f}% of binary markets, "
                        f"{len(active_markets)/total_fetched*100:.2f}% of total markets")
        
        if rejection_details:
            self.pipeline_logger.info(f"\n  Rejection Breakdown:")
            total_rejected = sum(len(details) for details in rejection_details.values())
            for reason, details_list in rejection_details.items():
                if details_list:
                    count = len(details_list)
                    pct = (count / len(binary_markets) * 100) if len(binary_markets) > 0 else 0
                    self.pipeline_logger.info(f"    {reason}: {count:,} markets ({pct:.1f}% of binary)")
        
        # Stage 4: Scoring
        if scored_markets:
            scores = [score for score, _ in scored_markets]
            scores_sorted = sorted(scores, reverse=True)
            self.pipeline_logger.info(f"\nStage 4 - Scoring: {len(scored_markets):,} markets scored")
            self.pipeline_logger.info(f"  Score range: {scores_sorted[-1]:.2f} to {scores_sorted[0]:.2f}")
            self.pipeline_logger.info(f"  Score median: {scores_sorted[len(scores_sorted)//2]:.2f}")
            self.pipeline_logger.info(f"  Score p95: {scores_sorted[int(len(scores_sorted)*0.05)]:.2f}")
            self.pipeline_logger.info(f"  Score p99: {scores_sorted[int(len(scores_sorted)*0.01)]:.2f}")
        
        # Stage 5: Top N Selection
        top_n = len(top_markets)
        if len(scored_markets) > top_n:
            cutoff_score = scored_markets[top_n - 1][0]
            excluded = len(scored_markets) - top_n
            excluded_pct = (excluded / len(scored_markets) * 100) if len(scored_markets) > 0 else 0
            self.pipeline_logger.info(f"\nStage 5 - Top {top_n} Selection: {top_n:,} selected, "
                            f"{excluded:,} excluded ({excluded_pct:.1f}%)")
            self.pipeline_logger.info(f"  Cutoff score: {cutoff_score:.2f}")
            self.pipeline_logger.info(f"  Final retention: {top_n/total_fetched*100:.3f}% of total markets")
        else:
            self.pipeline_logger.info(f"\nStage 5 - Top {top_n} Selection: {top_n:,} selected "
                            f"(all {len(scored_markets)} scored markets)")
            self.pipeline_logger.info(f"  Final retention: {top_n/total_fetched*100:.3f}% of total markets")
        
        # Recommendations
        self.pipeline_logger.info(f"\nRECOMMENDATIONS:")
        if rejection_details:
            volume_rejections = len(rejection_details.get('volume', []))
            if volume_rejections > 10000:
                min_volume = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
                self.pipeline_logger.info(f"  - Volume Filter: {volume_rejections:,} markets rejected")
                self.pipeline_logger.info(f"    Current threshold: ${min_volume:,.0f}")
                self.pipeline_logger.info(f"    Consider: Reviewing volume data quality or lowering threshold")
            
            spread_rejections = len(rejection_details.get('spread', []))
            if spread_rejections > 1000:
                self.pipeline_logger.info(f"  - Spread Filter: {spread_rejections:,} markets rejected")
                self.pipeline_logger.info(f"    Current threshold: 0.20 (20 cents)")
                self.pipeline_logger.info(f"    Consider: Reviewing if threshold is too tight")
            
            if len(active_markets) < 50:
                self.pipeline_logger.info(f"  - Low Active Markets: Only {len(active_markets)} markets passed all filters")
                self.pipeline_logger.info(f"    Consider: Relaxing filters to capture more opportunities")
        
        self.pipeline_logger.info("")
        self.pipeline_logger.info("=" * 100)
        self.pipeline_logger.info("")
    
    def _score_market_activity(self, market: Dict) -> float:
        """Score market by activity (volume, liquidity, recency, spread)"""
        volume_24h = market.get('volume_24h', 0) / 100
        liquidity = self.discovery.get_book_depth(market)
        spread = self.discovery.calculate_spread(market)
        
        # Recency bonus (0-1) within the last 10 minutes
        recency_score = 0.0
        last_activity = market.get('time_of_last_market_fetch')
        if last_activity:
            try:
                if isinstance(last_activity, str):
                    if last_activity.endswith('Z'):
                        last_activity = last_activity[:-1] + '+00:00'
                    ts = datetime.fromisoformat(last_activity)
                else:
                    ts = last_activity
                
                if isinstance(ts, datetime):
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    elif ts.tzinfo != timezone.utc:
                        ts = ts.astimezone(timezone.utc)
                    age = (datetime.now(timezone.utc) - ts).total_seconds()
                    if age < 600:  # Within 10 minutes
                        recency_score = 1 - (age / 600.0)
            except Exception:
                pass  # If we can't parse, just use 0
        
        # Activity score: volume + liquidity + recency, penalize wide spreads
        score = (volume_24h * 0.5) + (liquidity * 0.3) + (recency_score * 10) - (spread * 100)
        return score
    
    def _initial_full_scan(self):
        """Full discovery scan: filter, score, track top 200"""
        start_time = time.time()
        self.logger.info("Starting initial full market scan...")
        
        # Fetch all open markets
        all_markets = self.discovery.get_all_open_markets()
        self.logger.info(f"Fetched {len(all_markets)} total markets")
        
        # Filter to binary markets
        binary_markets = self.discovery.filter_binary_markets(all_markets)
        self.logger.info(f"Filtered to {len(binary_markets)} binary markets")
        
        # Filter to active markets only (enable verbose logging for diagnostics)
        active_markets, rejection_details = self._filter_active_markets(binary_markets, verbose_logging=True)
        self.logger.info(f"Filtered to {len(active_markets)} active markets (from {len(binary_markets)} binary)")
        
        # Score and sort by activity
        scored_markets = []
        for market in active_markets:
            score = self._score_market_activity(market)
            scored_markets.append((score, market))
        
        scored_markets.sort(key=lambda x: x[0], reverse=True)
        
        # Score distribution analysis
        if scored_markets:
            scores = [score for score, _ in scored_markets]
            scores_sorted = sorted(scores, reverse=True)
            self.pipeline_logger.info(f"Score distribution: min={scores_sorted[-1]:.2f}, "
                           f"max={scores_sorted[0]:.2f}, median={scores_sorted[len(scores_sorted)//2]:.2f}, "
                           f"p95={scores_sorted[int(len(scores_sorted)*0.05)]:.2f}, "
                           f"p99={scores_sorted[int(len(scores_sorted)*0.01)]:.2f}")
            
            # Top N cutoff analysis
            top_n = 200
            if len(scored_markets) > top_n:
                cutoff_score = scored_markets[top_n - 1][0]
                self.pipeline_logger.info(f"Top {top_n} cutoff score: {cutoff_score:.2f}")
                # Show markets just below cutoff
                near_miss_count = min(20, len(scored_markets) - top_n)
                if near_miss_count > 0:
                    self.pipeline_logger.info(f"Markets just below cutoff (rank {top_n + 1}-{top_n + near_miss_count}):")
                    for i, (score, market) in enumerate(scored_markets[top_n:top_n + near_miss_count]):
                        ticker = market.get('ticker', 'unknown')
                        volume_24h = get_price_field(market, 'volume_24h') or 0.0
                        spread = self.discovery.calculate_spread(market)
                        liquidity = self.discovery.get_book_depth(market)
                        gap = cutoff_score - score
                        self.pipeline_logger.info(f"  {i + top_n + 1}. {ticker}: score={score:.2f} "
                                       f"({gap:.2f} below cutoff, volume=${volume_24h:.2f}, "
                                       f"spread={spread:.3f}, liquidity={liquidity:.2f})")
        
        # Take top 200
        top_markets = scored_markets[:200]
        with self.lock:
            self.tracked_tickers = {m.get('ticker') for _, m in top_markets}
        
        # Initialize state for top markets
        current_time = datetime.now(timezone.utc)
        for _, market in top_markets:
            ticker = market.get('ticker')
            if ticker:
                # CRITICAL: Create MarketState BEFORE subscribing to WebSocket
                # This ensures market exists when WebSocket updates arrive
                try:
                    result = self._update_market_state(market, current_time)
                    
                    # Verify market state was created
                    with self.lock:
                        if ticker not in self.market_states:
                            # Log detailed diagnostic info
                            category = None
                            try:
                                category = self.discovery.get_category(market)
                            except Exception:
                                pass
                            
                            excluded = category in self.exclude_categories if category else False
                            self.logger.error(
                                f"Failed to create MarketState for {ticker} before subscription. "
                                f"Category: {category}, "
                                f"Excluded categories: {self.exclude_categories}, "
                                f"Is excluded: {excluded}, "
                                f"_update_market_state returned: {result}"
                            )
                            continue
                except Exception as e:
                    self.logger.error(
                        f"Exception creating MarketState for {ticker}: {e}",
                        exc_info=True
                    )
                    continue
                
                # Phase 2.4: Subscribe to WS channels for new markets
                if ticker not in self._subscribed_markets:
                    self._subscribe_to_market(ticker)
        
        scan_duration = time.time() - start_time
        
        # Log complete pipeline report
        self._log_filtering_pipeline_report(
            all_markets, binary_markets, active_markets, scored_markets, top_markets, rejection_details
        )
        
        self.logger.info(
            f"Initial scan complete: tracking top {len(self.tracked_tickers)} markets "
            f"(from {len(active_markets)} active), {scan_duration:.2f}s"
        )
    
    def _incremental_sweep(self):
        """
        Periodic metadata validation sweep.
        Fetches market metadata (volume_24h, status, time_to_expiry, close_time, category) via REST API
        and validates markets are still worth tracking. Price monitoring is handled separately by
        _on_ticker_update() WebSocket callback when WebSocket is enabled.
        """
        start_time = time.time()
        
        # Thread-safe snapshot of tracked tickers
        with self.lock:
            if not self.tracked_tickers:
                self.logger.warning("No tracked markets to sweep")
                return
            
            tickers_list = list(self.tracked_tickers)
        
        # Calculate chunk size to target ~30s cycle time
        target_cycle_seconds = 30
        sweeps_per_cycle = max(1, target_cycle_seconds // self.refresh_interval)
        chunk_size = max(1, len(tickers_list) // sweeps_per_cycle)
        
        # Get current chunk of tickers to process (round-robin)
        start_idx = (self.scan_count * chunk_size) % len(tickers_list)
        end_idx = start_idx + chunk_size
        if end_idx <= len(tickers_list):
            chunk_tickers = tickers_list[start_idx:end_idx]
        else:
            # Wrap around
            chunk_tickers = tickers_list[start_idx:] + tickers_list[:end_idx - len(tickers_list)]
        
        # Rate limit sanity check
        effective_rps = chunk_size / self.refresh_interval
        max_scanner_rps = 10  # Conservative limit
        if effective_rps > max_scanner_rps:
            self.logger.warning(
                f"Scanner RPS ({effective_rps:.1f}) exceeds max ({max_scanner_rps}), "
                f"consider adjusting chunk_size or refresh_interval"
            )
        
        self.logger.debug(f"Processing chunk: {len(chunk_tickers)} markets (sweep {self.scan_count})")
        
        # Fetch market data for each ticker in chunk
        markets = []
        rate_limit_delay = 1.0 / 20  # 20 requests per second
        
        for i, ticker in enumerate(chunk_tickers):
            try:
                response = self.discovery.api.make_request('GET', f'/markets/{ticker}')
                market = response.get('market')
                if market:
                    markets.append(market)
                
                # Rate limiting
                if i < len(chunk_tickers) - 1:  # Don't sleep after last request
                    time.sleep(rate_limit_delay)
                    
            except Exception as e:
                self.logger.warning(f"Error fetching market {ticker}: {e}")
                continue
        
        # Filter to binary and remove non-binary markets from tracking
        binary_markets = self.discovery.filter_binary_markets(markets)
        binary_tickers = {m.get('ticker') for m in binary_markets if m.get('ticker')}
        
        # Remove any non-binary markets from tracked_tickers
        with self.lock:
            tracked_snapshot = set(self.tracked_tickers)
            for ticker in chunk_tickers:
                if ticker in tracked_snapshot and ticker not in binary_tickers:
                    # Market is no longer binary (or was never binary), remove it
                    self.tracked_tickers.discard(ticker)
                    self.logger.debug(f"Removed non-binary market {ticker} from tracking")
        
        # Update tracked states (only binary markets)
        self._update_tracked_states(binary_markets)
        
        scan_duration = time.time() - start_time
        self.logger.debug(
            f"Incremental sweep complete: processed {len(binary_markets)}/{len(chunk_tickers)} markets, "
            f"{scan_duration:.3f}s"
        )
        
        # Check if full cycle is complete (all tracked markets checked)
        with self.lock:
            tracked_count = len(self.tracked_tickers)
            checked_count = len(self.markets_checked_this_cycle)
            # Get set of currently tracked tickers
            current_tracked = set(self.tracked_tickers)
        
        # Check if we've checked all currently tracked markets
        # Use intersection to only count markets that are both checked AND still tracked
        checked_tracked = self.markets_checked_this_cycle & current_tracked
        
        # Debug logging for cycle progress
        self.logger.debug(
            f"Cycle progress: {len(checked_tracked)}/{tracked_count} tracked markets checked "
            f"({len(self.markets_checked_this_cycle)} total checked, {len(self.markets_checked_this_cycle - current_tracked)} removed)"
        )
        
        if tracked_count > 0 and len(checked_tracked) >= tracked_count:
            # Full cycle complete - log summary
            with self.lock:
                details_count = len(self.cycle_market_details)
            self.logger.info(
                f"Cycle #{self.cycle_number} complete: checked {len(checked_tracked)}/{tracked_count} tracked markets "
                f"({details_count} with detailed metrics)"
            )
            self._log_full_cycle_summary()
            # Reset for next cycle
            with self.lock:
                self.markets_checked_this_cycle.clear()
                self.current_cycle_start_time = None
                self.cycle_market_details.clear()
                self.cycle_number += 1
        
        # Log diagnostic summary every 10 sweeps (only after initial scan completes)
        if self.initial_scan_complete and self.scan_count % 10 == 0:
            self._log_diagnostic_summary()
    
    def _update_tracked_states(self, markets: List[Dict]):
        """Update tracked markets (only updates existing tracked markets)"""
        current_time = datetime.now(timezone.utc)
        events_emitted = 0
        removed_count = 0
        
        # Thread-safe check of tracked tickers
        with self.lock:
            tracked_snapshot = set(self.tracked_tickers)
        
        # FIRST: Add all tracked markets in this batch to cycle tracking
        # This ensures cycles complete even if markets are removed
        for market in markets:
            ticker = market.get('ticker')
            if ticker and ticker in tracked_snapshot:
                with self.lock:
                    self.markets_checked_this_cycle.add(ticker)
                    if self.current_cycle_start_time is None:
                        self.current_cycle_start_time = datetime.now(timezone.utc)
        
        # NOW filter to active markets
        active_markets, _ = self._filter_active_markets(markets, verbose_logging=False)
        active_tickers = {m.get('ticker') for m in active_markets if m.get('ticker')}
        
        # Create a map of active markets by ticker for quick lookup
        active_markets_by_ticker = {m.get('ticker'): m for m in active_markets}
        
        # Process each fetched market
        for market in markets:
            ticker = market.get('ticker')
            if not ticker:
                continue
            
            try:
                is_active = ticker in active_tickers
                
                if ticker in tracked_snapshot:
                    if is_active:
                        # Update existing tracked market
                        event = self._update_market_state(active_markets_by_ticker[ticker], current_time)
                        if event:
                            events_emitted += 1
                            self.active_volatility_events.add(ticker)
                            if self.event_callback:
                                self.event_callback(event)
                    else:
                        # Market no longer active - log rejection details
                        rejection_reason = self._determine_rejection_reason(market)
                        self._log_market_rejection(ticker, market, rejection_reason, current_time)
                        
                        # Remove from tracking
                        with self.lock:
                            self.tracked_tickers.discard(ticker)
                            # Keep in cycle tracking (already added above)
                        removed_count += 1
                        # Cleanup state will happen in _cleanup_old_states()
                # Note: We don't add new markets here - that's handled by periodic rediscovery
            
            except Exception as e:
                self.logger.warning(f"Error processing market {ticker}: {e}")
        
        # Check for ended volatility events
        self._check_ended_events(markets, current_time)
        
        # Cleanup old state
        self._cleanup_old_states()
        
        if removed_count > 0 or events_emitted > 0:
            with self.lock:
                tracked_count = len(self.tracked_tickers)
            self.logger.info(
                f"Sweep: {tracked_count} tracked, "
                f"{removed_count} removed, {events_emitted} events"
            )
    
    def _determine_rejection_reason(self, market: Dict) -> str:
        """Determine why a market was rejected by filters"""
        # Check category exclusion FIRST
        category = self.discovery.get_category(market)
        if category in self.exclude_categories:
            return f"category:{category} excluded"
        
        # Check status
        status = market.get('status', '')
        if status and status.lower() not in ('active', 'open'):
            return f"status:{status}"
        
        # Check age (must be at least 60 seconds old)
        now = datetime.now(timezone.utc)
        created_time = market.get('open_time') or market.get('created_time')
        if created_time:
            try:
                if isinstance(created_time, str):
                    if created_time.endswith('Z'):
                        created_time = created_time[:-1] + '+00:00'
                    created_time = datetime.fromisoformat(created_time)
                if isinstance(created_time, datetime):
                    if created_time.tzinfo is None:
                        created_time = created_time.replace(tzinfo=timezone.utc)
                    elif created_time.tzinfo != timezone.utc:
                        created_time = created_time.astimezone(timezone.utc)
                    if (now - created_time).total_seconds() < 60:
                        return "age:<60s"
            except Exception:
                pass
        
        # Check volume
        volume_24h = get_price_field(market, 'volume_24h') or 0.0
        min_volume = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
        if volume_24h < min_volume:
            return f"volume:{volume_24h:.0f}<{min_volume:.0f}"
        
        # Check freshness
        last_activity = market.get('time_of_last_market_fetch')
        if last_activity:
            try:
                if isinstance(last_activity, str):
                    if last_activity.endswith('Z'):
                        last_activity = last_activity[:-1] + '+00:00'
                    last_activity = datetime.fromisoformat(last_activity)
                if isinstance(last_activity, datetime):
                    if last_activity.tzinfo is None:
                        last_activity = last_activity.replace(tzinfo=timezone.utc)
                    elif last_activity.tzinfo != timezone.utc:
                        last_activity = last_activity.astimezone(timezone.utc)
                    if (now - last_activity).total_seconds() > 600:  # 10 minutes
                        return "freshness:>10m"
            except Exception:
                pass
        
        # Check spread
        spread = self.discovery.calculate_spread(market)
        if spread > 0.20:  # 20 cent max spread
            return f"spread:{spread:.3f}>0.20"
        
        # Check two-sided orderbook
        yes_bid = market.get('yes_bid', 0)
        yes_ask = market.get('yes_ask', 0)
        yes_bid_normalized = yes_bid / 100 if yes_bid > 0 else 0
        yes_ask_normalized = yes_ask / 100 if yes_ask > 0 else 1
        if yes_bid_normalized <= 0 or yes_ask_normalized >= 1:
            return "two_sided:invalid"
        
        # Check liquidity
        liquidity = self.discovery.get_book_depth(market)
        if liquidity == 0:
            return "liquidity:0"
        
        # Check price
        mid_price = self.discovery.get_mid_price(market, 'yes')
        if mid_price < 0.05 or mid_price > 0.95:
            return f"price:{mid_price:.3f} out_of_range"
        
        return "unknown"
    
    def _log_market_rejection(self, ticker: str, market: Dict, rejection_reason: str, timestamp: datetime):
        """Log detailed rejection information for a market"""
        # Extract metrics
        mid_price = self.discovery.get_mid_price(market, 'yes')
        spread = self.discovery.calculate_spread(market)
        volume_24h = get_price_field(market, 'volume_24h') or 0.0
        time_to_expiry = self.discovery.get_time_to_expiry_days(market)
        status = market.get('status', 'unknown')
        liquidity = self.discovery.get_book_depth(market)
        
        # Get thresholds
        min_volume_24h = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
        
        rejection_detail = {
            'ticker': ticker,
            'timestamp': timestamp.isoformat(),
            'mid_price': mid_price,
            'spread': spread,
            'volume_24h': volume_24h,
            'liquidity': liquidity,
            'time_to_expiry_hours': time_to_expiry * 24 if time_to_expiry else None,
            'status': status,
            'failure_reason': 'rejected',
            'rejection_reason': rejection_reason,
            'thresholds': {
                'volume_24h': min_volume_24h,
                'spread': 0.20,  # 20 cent max spread from _filter_active_markets
                'min_liquidity_spread': self.min_liquidity_spread
            }
        }
        
        with self.lock:
            self.cycle_market_details.append(rejection_detail)
    
    def _update_market_state(self, market: Dict, timestamp: datetime) -> Optional[VolatilityEvent]:
        """Update state for a market and check for volatility events"""
        ticker = market.get('ticker')
        if not ticker:
            return None
        
        # Note: Category exclusion is now handled in _filter_active_markets() before scoring,
        # so markets reaching here should not be excluded. This check is kept as a safety net.
        category = self.discovery.get_category(market)
        if category in self.exclude_categories:
            self.logger.warning(
                f"{ticker}: Excluded category '{category}' reached _update_market_state() - "
                f"this should not happen (should be filtered earlier). Excluded categories: {self.exclude_categories}"
            )
            return None
        
        # Get current market metrics
        mid_price = self.discovery.get_mid_price(market, 'yes')
        spread = self.discovery.calculate_spread(market)
        # Phase 1.2: Use get_price_field() utility
        volume_24h = get_price_field(market, 'volume_24h') or 0.0
        liquidity = self.discovery.get_book_depth(market)
        time_to_expiry = self.discovery.get_time_to_expiry_days(market)
        
        # Get close time
        close_time = None
        expiry_str = self.discovery.get_expiry_iso_string(market)
        if expiry_str:
            try:
                if expiry_str.endswith('Z'):
                    expiry_str = expiry_str[:-1] + '+00:00'
                close_time = datetime.fromisoformat(expiry_str)
                if close_time.tzinfo != timezone.utc:
                    close_time = close_time.astimezone(timezone.utc)
            except Exception as e:
                self.logger.debug(f"Could not parse close_time for {ticker}: {e}")
        
        # Get or create market state
        if ticker not in self.market_states:
            self.market_states[ticker] = MarketState(
                ticker=ticker,
                mid_prices=[],
                volume_deltas=[],
                spreads=[],
                liquidity_values=[],
                timestamps=[],
                time_to_expiry=time_to_expiry,
                close_time=close_time,
                last_activity_time=timestamp
            )
            self.prev_volume_deltas[ticker] = 0.0
        
        state = self.market_states[ticker]
        
        # Note: Cycle tracking is now done in _update_tracked_states() before filtering
        # This ensures all markets are counted, even those that are removed
        
        # Note: WebSocket price updates are handled directly in _on_ticker_update() callback
        # This method focuses on metadata updates and adds REST API snapshot as fallback/verification
        
        # Update metadata (time_to_expiry, close_time) - these change slowly and come from REST API
        state.time_to_expiry = time_to_expiry
        state.close_time = close_time
        
        # Update state with price snapshot (volume_delta removed - volume_24h is stale, using absolute threshold instead)
        # This REST API snapshot acts as fallback/verification when WebSocket is unavailable
        # When WebSocket is enabled, price updates come continuously from _on_ticker_update()
        # but we still add REST snapshot occasionally to ensure we have data if WebSocket fails
        # Duplicate detection: check if timestamp already exists (within 1 second tolerance)
        ts_int = int(timestamp.timestamp())
        existing_timestamps = {int(existing_ts.timestamp()) for existing_ts in state.timestamps}
        if ts_int not in existing_timestamps:
            state.add_snapshot(mid_price, spread, liquidity, timestamp)
        
        # Trim to rolling window
        state.trim_to_window(self.rolling_window_minutes)
        
        # Check if we have enough data
        if len(state.mid_prices) < 3:
            # Still log basic info for warmup markets
            min_volume_24h = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
            with self.lock:
                warmup_detail = {
                    'ticker': ticker,
                    'timestamp': timestamp.isoformat(),
                    'mid_price': mid_price if state.mid_prices else 0.0,
                    'spread': spread,
                    'sigma': 0.0,
                    'price_jump': 0,
                    'volume_24h': volume_24h,
                    'time_to_expiry_hours': time_to_expiry * 24 if time_to_expiry else None,
                    'failure_reason': 'warmup',
                    'price_points': len(state.mid_prices),
                    'thresholds': {
                        'sigma': self.sigma_threshold,
                        'price_jump_cents': self.price_jump_threshold_cents,
                        'volume_24h': min_volume_24h,
                        'spread': self.min_liquidity_spread
                    }
                }
                self.cycle_market_details.append(warmup_detail)
            return None  # Early return, but already tracked in cycle and logged
        
        # Calculate regime features
        sigma = self._calculate_sigma(state)
        
        # Update returns (ring buffer)
        if len(state.mid_prices) >= 2:
            for i in range(1, len(state.mid_prices)):
                if state.mid_prices[i-1] > 0:
                    ret = (state.mid_prices[i] - state.mid_prices[i-1]) / state.mid_prices[i-1]
                    state.returns.append(ret)
        
        # Calculate features
        state.mean_reversion_score = self._calculate_mean_reversion_score(state)
        state.drift_score = self._calculate_drift_score(state, sigma)
        state.order_flow_imbalance = self._calculate_order_flow_imbalance(state)
        state.avg_spread = statistics.mean(state.spreads) if state.spreads else 0.0
        state.max_spread = max(state.spreads) if state.spreads else 0.0
        
        # Phase 2.3: Orderbook Integration for Regime Detection (use store instead of REST)
        try:
            scanner_config = self.config.get('volatility_scanner', {})
            regimes_config = self.config.get('regimes', {})
            orderbook_depth = scanner_config.get('orderbook_depth', 10)
            
            if self.ws_enabled and self.state_store and self.state_store.ws_healthy:
                orderbook = self.state_store.get_orderbook(ticker)
                # Note: price_history is no longer used - price data comes directly from state.mid_prices
            else:
                # Fallback to REST
                orderbook = self.discovery.api.get_orderbook(ticker, depth=orderbook_depth)
            
            # Handle empty books
            if not orderbook or not (orderbook.get("bids") or orderbook.get("asks")):
                state.avg_depth = 0
                state.avg_spread = 1.0  # Force CHAOTIC regime
                self.logger.debug(f"{ticker}: Empty orderbook, forcing CHAOTIC")
            else:
                # Phase 3.1: Add minimum book spread sanity check
                if orderbook.get("best_ask") and orderbook.get("best_bid"):
                    if orderbook["best_ask"] < orderbook["best_bid"]:
                        # Inverted bid/ask (API glitch)
                        state.avg_spread = 1.0
                        state.avg_depth = 0
                        self.logger.warning(f"{ticker}: Inverted bid/ask detected, forcing CHAOTIC")
                    else:
                        # Use real orderbook depth
                        state.avg_depth = (orderbook["total_bid_depth"] + orderbook["total_ask_depth"]) / 2
                        state.avg_spread = orderbook["spread"] or state.avg_spread
                else:
                    # Fallback to calculated values
                    state.avg_depth = statistics.mean(state.liquidity_values) if state.liquidity_values else 0.0
        except Exception as e:
            # Fallback to calculated values if orderbook fetch fails
            self.logger.debug(f"{ticker}: Could not fetch orderbook: {e}")
            state.avg_depth = statistics.mean(state.liquidity_values) if state.liquidity_values else 0.0
        
        # Count jumps
        price_jump = self._check_price_jump(state)
        if price_jump is not None and price_jump >= self.price_jump_threshold_cents:
            state.jump_count += 1
        
        # Classify regime
        new_regime = self._classify_regime(state, sigma)
        
        # Update regime history for stability check
        if new_regime != state.regime:
            state.regime = new_regime
            state.regime_timestamp = timestamp
        state.regime_history.append(new_regime)
        
        # Update shared state for MM access (Refinement 9)
        with self.regime_lock:
            self.regime_by_ticker[ticker] = {
                'regime': new_regime,
                'timestamp': timestamp,
                'stable': self._is_regime_stable(state, regimes_config.get('stability_required', 3))
            }
        
        # Log detailed metrics for this market
        self._log_market_details(ticker, state, volume_24h, market)
        
        # Check for volatility event
        return self._check_volatility_event(state, volume_24h, new_regime)
    
    def _log_market_details(self, ticker: str, state: MarketState, volume_24h: float, market: Dict):
        """Log detailed metrics for a market to file
        
        This function uses the EXACT same logic as _check_volatility_event to ensure
        "passed_all" in logs means the market would actually spawn an MM session.
        """
        # Note: Cycle tracking is now done in _update_market_state() before early return
        # This ensures all markets are counted, even those in warmup phase
        
        # Calculate all metrics
        price_jump = self._check_price_jump(state)
        # Get actual jump magnitude for logging (even if below threshold)
        actual_jump = self._calculate_price_jump_magnitude(state)
        sigma = self._calculate_sigma(state)
        current_spread = state.spreads[-1] if state.spreads else 999.0
        mid_price = state.mid_prices[-1] if state.mid_prices else 0.0
        
        # Get regime from state
        regime = state.regime if hasattr(state, 'regime') and state.regime else 'UNKNOWN'
        
        # Get volume threshold from config (reuses scoring.min_volume_24h - same threshold as discovery)
        min_volume_24h = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
        
        # Get regime filtering config
        scanner_config = self.config.get('volatility_scanner', {})
        regime_filters = scanner_config.get('regime_filters', {})
        allowed_regimes = set(regime_filters.get('allowed_for_events', ['MEAN_REVERTING', 'QUIET']))
        disallowed_regimes = set(regime_filters.get('disallowed_for_events', ['TRENDING', 'CHAOTIC', 'NOISE', 'UNKNOWN']))
        regimes_config = self.config.get('regimes', {})
        stability_required = regimes_config.get('stability_required', 3)
        
        # Get recent trades threshold
        discovery_config = self.config.get('discovery', {})
        recent_activity = discovery_config.get('recent_activity', {}).get('execution', {})
        min_recent_trades = recent_activity.get('min_recent_trades_for_event_markets', 0)
        
        # Calculate mean-reversion (logged for analysis but not required for volatility detection)
        has_mean_reversion = self._check_mean_reversion(state)
        has_price_jump = price_jump is not None and price_jump >= self.price_jump_threshold_cents
        has_sigma = sigma >= self.sigma_threshold
        
        # EXACT SAME LOGIC AS _check_volatility_event
        # Volatility passes if EITHER jump OR sigma threshold is met (OR logic, not AND)
        has_volatility = has_price_jump or has_sigma
        
        # Check regime stability
        is_regime_stable = self._is_regime_stable(state, stability_required)
        
        # Check recent trades
        recent_price_updates = len([ts for ts in state.timestamps 
                                    if (datetime.now(timezone.utc) - ts).total_seconds() < 300]) if min_recent_trades > 0 else 999
        
        # Determine which filter failed (if any) - matching _check_volatility_event exactly
        failure_reason = None
        failure_details = {}
        
        if len(state.mid_prices) < 3:
            failure_reason = "warmup"
            failure_details = {'price_points': len(state.mid_prices), 'required': 3}
        elif ticker in self.active_volatility_events:
            failure_reason = "already_active"
            failure_details = {}
        elif current_spread > self.min_liquidity_spread:
            failure_reason = "spread"
            failure_details = {'spread': current_spread, 'threshold': self.min_liquidity_spread}
        elif not has_volatility:
            failure_reason = "volatility"
            failure_details = {
                'price_jump': price_jump if price_jump is not None else 0,
                'price_jump_threshold': self.price_jump_threshold_cents,
                'sigma': sigma,
                'sigma_threshold': self.sigma_threshold,
                'has_mean_reversion': has_mean_reversion,
                'has_price_jump': has_price_jump,
                'has_sigma': has_sigma,
                'volatility_passed': False,
                'sigma_passed': has_sigma,
                'jump_passed': has_price_jump
            }
        elif not is_regime_stable:
            failure_reason = "regime_stability"
            failure_details = {
                'regime': regime,
                'regime_history': list(state.regime_history)[-stability_required:] if hasattr(state, 'regime_history') else [],
                'stability_required': stability_required
            }
        elif (regime in disallowed_regimes or regime not in allowed_regimes) and not (regime == "TRENDING" and has_sigma):
            # Special case: Allow TRENDING regime when sigma is above threshold
            # TRENDING markets with high volatility are tradeable
            # If regime is TRENDING and has_sigma, skip this elif and continue to next filter
            if regime in ["CHAOTIC", "NOISE", "UNKNOWN"]:
                # Always block these regimes regardless of sigma
                failure_reason = "regime_filter"
                failure_details = {
                    'regime': regime,
                    'allowed_regimes': list(allowed_regimes),
                    'disallowed_regimes': list(disallowed_regimes),
                    'trending_allowed_due_to_sigma': False
                }
            else:
                # Other disallowed regimes (shouldn't happen with current config, but handle gracefully)
                failure_reason = "regime_filter"
                failure_details = {
                    'regime': regime,
                    'allowed_regimes': list(allowed_regimes),
                    'disallowed_regimes': list(disallowed_regimes),
                    'trending_allowed_due_to_sigma': False
                }
        elif volume_24h < min_volume_24h:
            failure_reason = "volume"
            failure_details = {'volume_24h': volume_24h, 'threshold': min_volume_24h}
        elif min_recent_trades > 0 and recent_price_updates < min_recent_trades:
            failure_reason = "recent_trades"
            failure_details = {
                'recent_price_updates': recent_price_updates,
                'required': min_recent_trades
            }
        else:
            failure_reason = "passed_all"
            failure_details = {
                'volatility_passed': True,
                'sigma_passed': has_sigma,
                'jump_passed': has_price_jump
            }
        
        # Store details for cycle summary
        market_detail = {
            'ticker': ticker,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'mid_price': mid_price,
            'spread': current_spread,
            'sigma': sigma,
            'price_jump': actual_jump if actual_jump is not None else 0,  # Show actual jump magnitude for logging
            'volume_24h': volume_24h,  # Absolute volume in dollars
            'time_to_expiry_hours': state.time_to_expiry * 24 if state.time_to_expiry else None,
            'failure_reason': failure_reason,
            'failure_details': failure_details,
            'regime': regime,
            'has_mean_reversion': has_mean_reversion,
            'is_regime_stable': is_regime_stable,
            'recent_price_updates': recent_price_updates if min_recent_trades > 0 else None,
            'thresholds': {
                'sigma': self.sigma_threshold,
                'price_jump_cents': self.price_jump_threshold_cents,
                'volume_24h': min_volume_24h,
                'spread': self.min_liquidity_spread,
                'mean_reversion_required': True,
                'regime_stability_required': stability_required,
                'min_recent_trades': min_recent_trades
            }
        }
        
        with self.lock:
            self.cycle_market_details.append(market_detail)
    
    def _check_volatility_event(self, state: MarketState, volume_24h: float, regime: str) -> Optional[VolatilityEvent]:
        """Check if market meets volatility event criteria"""
        ticker = state.ticker
        
        # Warmup check: require at least 3 price points for sigma calculation
        if len(state.mid_prices) < 3:
            self.logger.debug(f"{ticker}: Warmup - only {len(state.mid_prices)} price points (need 3)")
            return None
        
        # Skip if already active
        if ticker in self.active_volatility_events:
            self.logger.debug(f"{ticker}: Already active, skipping")
            return None
        
        # Check spread filter
        current_spread = state.spreads[-1] if state.spreads else 999.0
        if current_spread > self.min_liquidity_spread:
            self.logger.debug(f"{ticker}: Failed spread filter (spread={current_spread:.3f})")
            return None
        
        # Check volatility (at least one must pass)
        price_jump = self._check_price_jump(state)
        actual_jump = self._calculate_price_jump_magnitude(state)
        sigma = self._calculate_sigma(state)
        
        # Calculate mean-reversion (logged for analysis but not required for volatility detection)
        has_mean_reversion = self._check_mean_reversion(state)
        has_price_jump = price_jump is not None and price_jump >= self.price_jump_threshold_cents
        has_sigma = sigma >= self.sigma_threshold
        
        # Volatility passes if EITHER jump OR sigma threshold is met (OR logic, not AND)
        has_volatility = has_price_jump or has_sigma
        
        if has_volatility:
            jump_str = f"{price_jump:.1f}c" if price_jump is not None else f"{actual_jump:.1f}c (below threshold)" if actual_jump is not None else "None"
            self.logger.debug(
                f"{ticker}: Volatility filter passed - "
                f"jump={jump_str} (threshold: {self.price_jump_threshold_cents}c, passed={has_price_jump}), "
                f"sigma={sigma:.4f} (threshold: {self.sigma_threshold}, passed={has_sigma}), "
                f"volatility_passed=True"
            )
        
        if not has_volatility:
            jump_str = f"{price_jump:.1f}c" if price_jump is not None else f"{actual_jump:.1f}c (below threshold)" if actual_jump is not None else "None"
            mean_rev_str = "YES" if has_mean_reversion else "NO"
            self.logger.info(
                f"{ticker}: Failed volatility filter - "
                f"jump={jump_str} (threshold: {self.price_jump_threshold_cents}c, passed={has_price_jump}), "
                f"sigma={sigma:.4f} (threshold: {self.sigma_threshold}, passed={has_sigma}), "
                f"mean_reversion={mean_rev_str}, "
                f"volatility_passed=False (need: jump OR sigma)"
            )
            return None
        
        # Regime filtering - only emit events in allowed regimes
        scanner_config = self.config.get('volatility_scanner', {})
        regime_filters = scanner_config.get('regime_filters', {})
        allowed_regimes = set(regime_filters.get('allowed_for_events', ['MEAN_REVERTING', 'QUIET']))
        disallowed_regimes = set(regime_filters.get('disallowed_for_events', ['TRENDING', 'CHAOTIC', 'NOISE', 'UNKNOWN']))
        
        # Check regime stability requirement
        regimes_config = self.config.get('regimes', {})
        stability_required = regimes_config.get('stability_required', 3)
        if not self._is_regime_stable(state, stability_required):
            regime_history = list(state.regime_history)[-stability_required:] if hasattr(state, 'regime_history') else []
            self.logger.info(
                f"{ticker}: Failed regime stability filter - "
                f"current={regime}, history={regime_history}, "
                f"need {stability_required} consecutive stable regimes"
            )
            return None
        
        # Filter by regime
        # Special case: Allow TRENDING regime when sigma is above threshold
        # TRENDING markets with high volatility are tradeable
        regime_blocked = regime in disallowed_regimes or regime not in allowed_regimes
        if regime_blocked:
            if regime == "TRENDING" and has_sigma:
                # TRENDING is allowed due to high sigma - skip regime filter
                self.logger.debug(
                    f"{ticker}: TRENDING regime allowed due to high sigma (sigma={sigma:.4f} >= {self.sigma_threshold})"
                )
            elif regime in ["CHAOTIC", "NOISE", "UNKNOWN"]:
                # Always block these regimes regardless of sigma
                self.logger.info(
                    f"{ticker}: Failed regime filter - "
                    f"regime='{regime}' not allowed for events "
                    f"(allowed={allowed_regimes}, disallowed={disallowed_regimes})"
                )
                return None
            else:
                self.logger.info(
                    f"{ticker}: Failed regime filter - "
                    f"regime='{regime}' not allowed for events "
                    f"(allowed={allowed_regimes}, disallowed={disallowed_regimes})"
                )
                return None
        
        # Simple liquidity check - reuse existing scoring.min_volume_24h threshold
        # (same threshold used in discovery, keeps system aligned: "this is what we consider liquid enough, period")
        min_volume_24h = self.config.get('scoring', {}).get('min_volume_24h', 1000.0)
        # volume_24h is already converted to dollars in _update_market_state
        if volume_24h < min_volume_24h:
            self.logger.info(
                f"{ticker}: Failed volume filter - "
                f"volume_24h=${volume_24h:.1f} < ${min_volume_24h:.1f} threshold"
            )
            return None
        
        # Check recent trades threshold for event markets
        discovery_config = self.config.get('discovery', {})
        recent_activity = discovery_config.get('recent_activity', {}).get('execution', {})
        min_recent_trades = recent_activity.get('min_recent_trades_for_event_markets', 0)
        
        if min_recent_trades > 0:
            # Approximate recent trades from price updates in last 5 minutes
            recent_price_updates = len([ts for ts in state.timestamps 
                                        if (datetime.now(timezone.utc) - ts).total_seconds() < 300])
            if recent_price_updates < min_recent_trades:
                self.logger.info(
                    f"{ticker}: Failed recent trades filter - "
                    f"recent_price_updates={recent_price_updates} < {min_recent_trades} required"
                )
                return None
        
        # Calculate direction (optional)
        direction = None
        if len(state.mid_prices) >= 2:
            price_change = state.mid_prices[-1] - state.mid_prices[0]
            if abs(price_change) > 0.05:  # 5 cent threshold
                direction = 'down' if price_change < 0 else 'up'
        
        # Calculate signal strength (0-1)
        signal_strength = self._calculate_signal_strength(price_jump, sigma)
        
        # All filters passed - create event
        jump_str = f"{price_jump:.1f}c" if price_jump is not None else "None"
        self.logger.info(
            f"{ticker}: Volatility event detected! "
            f"jump={jump_str}, sigma={sigma:.4f}, regime={regime}, "
            f"volume_24h={volume_24h:.1f}, strength={signal_strength:.2f}"
        )
        
        # Create event (volume fields set to 0.0 for backward compatibility)
        event = VolatilityEvent(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            jump_magnitude=price_jump,
            sigma=sigma,
            volume_multiplier=0.0,  # DEPRECATED: volume_24h is stale
            volume_delta=0.0,  # DEPRECATED: volume_24h is stale
            estimated_trades=0.0,  # DEPRECATED: derived from volume_delta
            volume_velocity=None,  # DEPRECATED: volume_24h is stale, disabled
            close_time=state.close_time,
            direction=direction,
            signal_strength=signal_strength,
            regime=regime
        )
        
        return event
    
    def _check_price_jump(self, state: MarketState) -> Optional[float]:
        """Check for price jump in the specified window
        
        Returns the jump magnitude in cents if >= threshold, None otherwise.
        For logging purposes, use _calculate_price_jump_magnitude() to get actual value.
        """
        jump_cents = self._calculate_price_jump_magnitude(state)
        if jump_cents is None:
            return None
        return jump_cents if jump_cents >= self.price_jump_threshold_cents else None
    
    def _calculate_price_jump_magnitude(self, state: MarketState) -> Optional[float]:
        """Calculate the actual price jump magnitude in cents (regardless of threshold)
        
        This is used for logging to show the actual jump even if below threshold.
        """
        if len(state.mid_prices) < 2:
            return None
        
        # Get prices within the jump window
        cutoff_time = datetime.now(timezone.utc).timestamp() - (self.price_jump_window_minutes * 60)
        window_prices = []
        window_times = []
        
        for i, ts in enumerate(state.timestamps):
            if ts.timestamp() >= cutoff_time:
                window_prices.append(state.mid_prices[i])
                window_times.append(ts)
        
        if len(window_prices) < 2:
            return None
        
        # Calculate jump magnitude in cents
        price_change = abs(window_prices[-1] - window_prices[0])
        jump_cents = price_change * 100
        
        return jump_cents
    
    def _calculate_sigma(self, state: MarketState) -> float:
        """Calculate realized volatility (sigma) over volatility window"""
        if len(state.mid_prices) < 2:
            return 0.0
        
        # Get prices within volatility window
        cutoff_time = datetime.now(timezone.utc).timestamp() - (self.volatility_window_minutes * 60)
        window_prices = []
        
        for i, ts in enumerate(state.timestamps):
            if ts.timestamp() >= cutoff_time:
                window_prices.append(state.mid_prices[i])
        
        if len(window_prices) < 2:
            return 0.0
        
        # Calculate returns
        returns = []
        for i in range(1, len(window_prices)):
            if window_prices[i-1] > 0:
                ret = (window_prices[i] - window_prices[i-1]) / window_prices[i-1]
                returns.append(ret)
        
        if len(returns) < 2:
            return 0.0
        
        # Calculate standard deviation of returns
        sigma = statistics.stdev(returns) if len(returns) > 1 else 0.0
        
        # Annualize (rough approximation)
        # Assuming ~252 trading days, but for short windows this is just a scaling factor
        # For our purposes, we'll use the raw sigma
        return sigma
    
    def _check_mean_reversion(self, state: MarketState) -> bool:
        """
        Check if price oscillates around a mean (mean-reversion) vs directional drift.
        Returns True if price is oscillating (good for MM), False if drifting (bad).
        
        Ensures we have at least 3 minutes of data (not just N points, as resolution may vary).
        """
        # Get prices from last 3 minutes (ensure we have full 3-minute window)
        cutoff_time = datetime.now(timezone.utc).timestamp() - (3 * 60)
        window_prices = []
        window_times = []
        
        for i, ts in enumerate(state.timestamps):
            if ts.timestamp() >= cutoff_time:
                window_prices.append(state.mid_prices[i])
                window_times.append(ts)
        
        # Need at least 3 points for mean calculation
        if len(window_prices) < 3:
            return False
        
        # Ensure we have at least 3 minutes of data (check time span, not just point count)
        if window_times:
            time_span = (window_times[-1].timestamp() - window_times[0].timestamp()) / 60
            if time_span < 2.5:  # Need at least 2.5 minutes of actual data
                return False
        
        # Calculate mean price over 3-minute window
        mean_price = statistics.mean(window_prices)
        current_price = window_prices[-1]
        
        # Check if current price is close to mean (oscillating, not drifting)
        price_deviation = abs(current_price - mean_price)
        tolerance_cents = self.config.get('volatility_scanner', {}).get('mean_reversion_tolerance_cents', 6)
        tolerance = tolerance_cents / 100  # Convert cents to dollars
        
        return price_deviation <= tolerance
    
    def _calculate_mean_reversion_score(self, state: MarketState) -> float:
        """
        Calculate mean reversion score: fraction of moves that reverted.
        Uses relative reversion (% of initial move), not absolute price.
        Returns 0-1, where 1.0 = all moves reverted, 0.0 = no moves reverted.
        """
        if len(state.mid_prices) < 4:
            return 0.0
        
        # Detect local peaks and troughs
        moves = []  # List of (start_price, end_price, start_idx, end_idx)
        
        # Simple peak/trough detection: look for local maxima/minima
        for i in range(1, len(state.mid_prices) - 1):
            prev_price = state.mid_prices[i-1]
            curr_price = state.mid_prices[i]
            next_price = state.mid_prices[i+1]
            
            # Local peak (trough before, peak, then down)
            if curr_price > prev_price and curr_price > next_price:
                # Find the preceding trough
                for j in range(i-1, -1, -1):
                    if j == 0 or (state.mid_prices[j] < state.mid_prices[j-1] and 
                                  state.mid_prices[j] < state.mid_prices[j+1]):
                        move_size = curr_price - state.mid_prices[j]
                        if abs(move_size) > 0.01:  # At least 1 cent move
                            moves.append((state.mid_prices[j], curr_price, j, i))
                        break
            
            # Local trough (peak before, trough, then up)
            elif curr_price < prev_price and curr_price < next_price:
                # Find the preceding peak
                for j in range(i-1, -1, -1):
                    if j == 0 or (state.mid_prices[j] > state.mid_prices[j-1] and 
                                  state.mid_prices[j] > state.mid_prices[j+1]):
                        move_size = state.mid_prices[j] - curr_price
                        if abs(move_size) > 0.01:  # At least 1 cent move
                            moves.append((state.mid_prices[j], curr_price, j, i))
                        break
        
        if not moves:
            return 0.0
        
        # For each move, check if it reverted at least 50% within reasonable time
        reverted_count = 0
        reversion_window_seconds = 60  # Check for reversion within 60 seconds
        
        for start_price, end_price, start_idx, end_idx in moves:
            move_size = abs(end_price - start_price)
            direction = 1 if end_price > start_price else -1
            
            # Look ahead for reversion
            reverted = False
            for k in range(end_idx + 1, len(state.mid_prices)):
                # Check time window
                if len(state.timestamps) > k and len(state.timestamps) > end_idx:
                    time_diff = (state.timestamps[k].timestamp() - 
                               state.timestamps[end_idx].timestamp())
                    if time_diff > reversion_window_seconds:
                        break
                
                # Check if price moved back at least 50% of the original move
                revert_amount = state.mid_prices[k] - end_price
                if direction > 0:  # Was an up move
                    if revert_amount < -0.5 * move_size:  # Reverted down by 50%+
                        reverted = True
                        break
                else:  # Was a down move
                    if revert_amount > 0.5 * move_size:  # Reverted up by 50%+
                        reverted = True
                        break
            
            if reverted:
                reverted_count += 1
        
        return reverted_count / len(moves) if moves else 0.0
    
    def _calculate_drift_score(self, state: MarketState, sigma: float) -> float:
        """
        Calculate drift score: cumulative return normalized by volatility.
        A 5¢ drift in high-vol market means nothing; in low-vol it's extreme.
        Returns normalized drift (unitless).
        """
        if len(state.mid_prices) < 2:
            return 0.0
        
        # Cumulative simple return
        drift = abs(state.mid_prices[-1] - state.mid_prices[0])
        
        # Normalize by volatility (standard in intraday regime detection)
        regimes_config = self.config.get('regimes', {})
        drift_normalization_min_sigma = regimes_config.get('drift_normalization_min_sigma', 0.001)
        normalized_drift = drift / max(sigma, drift_normalization_min_sigma)
        
        return normalized_drift
    
    def _calculate_order_flow_imbalance(self, state: MarketState) -> float:
        """
        Calculate order flow imbalance as a proxy.
        Simple approximation: (# ticks up - # ticks down) / total_ticks
        Returns -1 to 1, where 1 = all up, -1 = all down, 0 = balanced.
        """
        if len(state.mid_prices) < 2:
            return 0.0
        
        up_ticks = 0
        down_ticks = 0
        
        for i in range(1, len(state.mid_prices)):
            if state.mid_prices[i] > state.mid_prices[i-1]:
                up_ticks += 1
            elif state.mid_prices[i] < state.mid_prices[i-1]:
                down_ticks += 1
        
        total_ticks = up_ticks + down_ticks
        if total_ticks == 0:
            return 0.0
        
        imbalance = (up_ticks - down_ticks) / total_ticks
        return imbalance
    
    def _classify_regime(self, state: MarketState, sigma: float) -> str:
        """
        Classify market regime using rule-based logic.
        Returns: UNKNOWN, QUIET, MEAN_REVERTING, TRENDING, CHAOTIC, or NOISE
        """
        regimes_config = self.config.get('regimes', {})
        min_observations = regimes_config.get('min_observations', 5)
        drift_threshold = regimes_config.get('drift_threshold', 0.08)
        min_sigma_for_regime = regimes_config.get('min_sigma_for_regime', 0.01)
        mean_reversion_good = regimes_config.get('mean_reversion_good', 0.6)
        mean_reversion_bad = regimes_config.get('mean_reversion_bad', 0.4)
        depth_threshold = regimes_config.get('depth_threshold', 50.0)  # Minimum liquidity
        
        # Refinement 1: UNKNOWN regime for insufficient data
        if len(state.mid_prices) < min_observations:
            return "UNKNOWN"
        
        # Calculate features
        avg_spread = statistics.mean(state.spreads) if state.spreads else 999.0
        max_spread = max(state.spreads) if state.spreads else 999.0
        avg_depth = statistics.mean(state.liquidity_values) if state.liquidity_values else 0.0
        
        # Refinement 4: CHAOTIC overrides everything - check first
        if avg_spread > 0.20 or avg_depth < depth_threshold:
            return "CHAOTIC"
        
        # Refinement 6: NOISE regime (low sigma + mid spreads + no direction)
        noise_spread_min = regimes_config.get('noise_spread_min', 0.07)
        noise_spread_max = regimes_config.get('noise_spread_max', 0.12)
        if sigma < min_sigma_for_regime and noise_spread_min <= avg_spread <= noise_spread_max:
            return "NOISE"
        
        # QUIET regime: low volatility, tight spreads
        if sigma < min_sigma_for_regime and avg_spread <= 0.05:
            return "QUIET"
        
        # Calculate drift and mean reversion for remaining classifications
        drift_score = self._calculate_drift_score(state, sigma)
        mean_reversion_score = self._calculate_mean_reversion_score(state)
        
        # Refinement 5: Adaptive drift threshold (per-market based on spread)
        # Thin markets require smaller thresholds
        adaptive_drift_threshold = drift_threshold * (avg_spread / 0.05)  # Normalize to 5¢ spread
        
        # TRENDING: persistent drift + low mean reversion
        if drift_score > adaptive_drift_threshold and mean_reversion_score < mean_reversion_bad:
            return "TRENDING"
        
        # MEAN_REVERTING: good mean reversion
        if mean_reversion_score > mean_reversion_good:
            return "MEAN_REVERTING"
        
        # Default fallback to CHAOTIC if uncertain
        return "CHAOTIC"
    
    def _is_regime_stable(self, state: MarketState, required_stability: int) -> bool:
        """
        Check if regime has been stable across N consecutive windows.
        Returns True if last N regime classifications match.
        """
        if len(state.regime_history) < required_stability:
            return False
        
        # Check if last N regimes are all the same
        last_n = list(state.regime_history)[-required_stability:]
        return len(set(last_n)) == 1
    
    def _calculate_signal_strength(
        self, price_jump: Optional[float], sigma: float
    ) -> float:
        """Calculate combined signal strength (0-1)"""
        # Normalize components
        jump_score = min(1.0, (price_jump or 0) / (self.price_jump_threshold_cents * 2)) if price_jump else 0.0
        sigma_score = min(1.0, sigma / (self.sigma_threshold * 2))
        
        # Weighted average (equal weight for jump and sigma)
        signal_strength = (jump_score * 0.5 + sigma_score * 0.5)
        return min(1.0, max(0.0, signal_strength))
    
    def _log_diagnostic_summary(self):
        """Log periodic diagnostic summary of market states"""
        if not self.market_states:
            return
        
        # Collect stats for all markets
        stats = {
            'warmup': [],  # Not enough data yet
            'spread_filter': [],  # Spread too wide
            'volatility_filter': [],  # Low volatility
            'passed_all': [],  # Passed all filters
            'close_to_triggering': []  # Markets close to triggering
        }
        
        # Thread-safe access to market states
        with self.lock:
            market_states_snapshot = dict(self.market_states)
            tracked_tickers_snapshot = set(self.tracked_tickers)
        
        # Check ALL tracked markets
        markets_checked = 0
        for ticker, state in market_states_snapshot.items():
            # Skip if not in tracked set
            if ticker not in tracked_tickers_snapshot:
                continue
            
            markets_checked += 1
            
            # Warmup check: require at least 3 price points for sigma calculation
            if len(state.mid_prices) < 3:
                stats['warmup'].append({'ticker': ticker, 'price_points': len(state.mid_prices)})
                continue
            
            # Check spread filter
            current_spread = state.spreads[-1] if state.spreads else 999.0
            if current_spread > self.min_liquidity_spread:
                stats['spread_filter'].append({
                    'ticker': ticker,
                    'spread': current_spread,
                    'max_allowed': self.min_liquidity_spread
                })
                continue
            
            # Check volatility
            price_jump = self._check_price_jump(state)
            # Get actual jump magnitude for logging (even if below threshold)
            actual_jump = self._calculate_price_jump_magnitude(state)
            sigma = self._calculate_sigma(state)
            has_volatility = (price_jump is not None and price_jump >= self.price_jump_threshold_cents) or \
                            (sigma >= self.sigma_threshold)
            
            if not has_volatility:
                jump_val = actual_jump if actual_jump is not None else 0
                stats['volatility_filter'].append({
                    'ticker': ticker,
                    'sigma': sigma,
                    'jump': jump_val,
                    'sigma_threshold': self.sigma_threshold,
                    'jump_threshold': self.price_jump_threshold_cents
                })
                # Track markets close to volatility threshold (50%+ of required)
                if sigma > self.sigma_threshold * 0.5 or jump_val > self.price_jump_threshold_cents * 0.5:
                    stats['close_to_triggering'].append({
                        'ticker': ticker,
                        'sigma': sigma,
                        'jump': jump_val,
                        'reason': 'volatility'
                    })
                continue
            
            # Check volume (simple absolute threshold - volume_24h is stale, so we can't get it here)
            # Note: Volume check is done in _check_volatility_event with actual volume_24h from market data
            # For diagnostic purposes, we'll skip volume check here since we don't have volume_24h in state
            # (volume_24h comes from API response, not stored in MarketState)
            
            # If we get here, market passed all filters but didn't trigger (maybe already active?)
            stats['passed_all'].append({
                'ticker': ticker,
                'sigma': sigma,
                'jump': price_jump
            })
        
        # Log summary
        total_tracked = len(market_states_snapshot)
        self.logger.info("=" * 80)
        self.logger.info("DIAGNOSTIC SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total tracked markets: {total_tracked}")
        self.logger.info(f"Markets checked: {markets_checked}")
        self.logger.info("")
        self.logger.info("Filter Failures:")
        self.logger.info(f"  Warmup (need more data): {len(stats['warmup'])}")
        self.logger.info(f"  Failed spread filter: {len(stats['spread_filter'])}")
        self.logger.info(f"  Failed volatility filter: {len(stats['volatility_filter'])}")
        # Volume filter removed - using simple absolute volume_24h threshold
        self.logger.info(f"  Passed all filters: {len(stats['passed_all'])}")
        
        # Show sample markets from each category
        if stats['spread_filter']:
            self.logger.info("")
            self.logger.info("Sample markets (failed spread):")
            for m in stats['spread_filter'][:3]:
                self.logger.info(
                    f"  {m['ticker']}: spread={m['spread']:.3f} (max={m['max_allowed']:.3f})"
                )
        
        if stats['volatility_filter']:
            self.logger.info("")
            self.logger.info("Sample markets (failed volatility):")
            # Sort to prioritize markets with non-zero sigma, then by sigma descending
            sorted_volatility = sorted(
                stats['volatility_filter'],
                key=lambda m: (m['sigma'] == 0, -m['sigma']),  # Non-zero sigma first, then highest sigma
                reverse=False
            )
            for m in sorted_volatility[:15]:  # Show top 15 instead of 5
                jump_str = f"{m['jump']:.1f}c" if m['jump'] is not None and m['jump'] > 0 else "None"
                self.logger.info(
                    f"  {m['ticker']}: sigma={m['sigma']:.6f} (need>={m['sigma_threshold']:.6f}), "
                    f"jump={jump_str} (need>={m['jump_threshold']:.1f}c)"
                )
        
        # Volume filter removed - using simple absolute volume_24h threshold in _check_volatility_event
        
        if stats['close_to_triggering']:
            self.logger.info("")
            self.logger.info("Markets close to triggering (50%+ of threshold):")
            # Sort by how close they are to thresholds
            def closeness_score(m):
                if m['reason'] == 'volatility' and self.sigma_threshold > 0:
                    return m['sigma'] / self.sigma_threshold
                return 0
            
            sorted_close = sorted(
                stats['close_to_triggering'],
                key=closeness_score,
                reverse=True  # Closest first
            )
            for m in sorted_close[:10]:  # Show top 10 instead of 5
                if m['reason'] == 'volatility':
                    jump_str = f"{m['jump']:.1f}c" if m['jump'] is not None else "None"
                    self.logger.info(
                        f"  {m['ticker']}: sigma={m['sigma']:.4f}, jump={jump_str} "
                        f"(need: sigma>={self.sigma_threshold}, jump>={self.price_jump_threshold_cents}c)"
                    )
        
        if stats['passed_all']:
            self.logger.info("")
            self.logger.info("Markets that passed all filters (may be already active):")
            for m in stats['passed_all'][:5]:
                jump_str = f"{m['jump']:.1f}c" if m['jump'] is not None else "None"
                self.logger.info(
                    f"  {m['ticker']}: sigma={m['sigma']:.4f}, jump={jump_str}, "
                    f"volume_24h=${m.get('volume_24h', 0):.1f}, spread={m.get('spread', 0):.3f}"
                )
        
        self.logger.info("=" * 80)
    
    def _log_full_cycle_summary(self):
        """Log detailed summary when a full cycle through all markets is complete"""
        with self.lock:
            cycle_details = list(self.cycle_market_details)
            cycle_start = self.current_cycle_start_time
            cycle_num = self.cycle_number
            tracked_count = len(self.tracked_tickers)
        
        # Log cycle completion even if no details (all markets in warmup)
        if not cycle_details:
            cycle_end = datetime.now(timezone.utc)
            cycle_duration = (cycle_end - cycle_start).total_seconds() if cycle_start else 0
            
            self.detail_logger.info("=" * 100)
            self.detail_logger.info(f"FULL SWEEP CYCLE #{cycle_num} COMPLETE")
            self.detail_logger.info("=" * 100)
            self.detail_logger.info(f"Cycle started: {cycle_start.isoformat() if cycle_start else 'N/A'}")
            self.detail_logger.info(f"Cycle ended: {cycle_end.isoformat()}")
            self.detail_logger.info(f"Cycle duration: {cycle_duration:.1f} seconds ({cycle_duration/60:.1f} minutes)")
            self.detail_logger.info(f"Markets tracked: {tracked_count}")
            self.detail_logger.info(f"Markets checked: {tracked_count}")
            self.detail_logger.info("")
            self.detail_logger.info("All markets are in warmup phase (< 3 price points)")
            self.detail_logger.info("No detailed metrics available yet.")
            self.detail_logger.info("")
            self.detail_logger.info("=" * 100)
            self.detail_logger.info(f"END OF CYCLE #{cycle_num}")
            self.detail_logger.info("=" * 100)
            self.detail_logger.info("")
            return
        
        cycle_end = datetime.now(timezone.utc)
        cycle_duration = (cycle_end - cycle_start).total_seconds() if cycle_start else 0
        
        # Aggregate statistics
        failure_counts = {
            'warmup': 0,
            'already_active': 0,
            'spread': 0,
            'volatility': 0,
            'regime_stability': 0,
            'regime_filter': 0,
            'volume': 0,
            'recent_trades': 0,
            'passed_all': 0,
            'rejected': 0
        }
        
        # Track rejection reasons
        rejection_counts = {
            'status': 0,
            'age': 0,
            'volume': 0,
            'freshness': 0,
            'spread': 0,
            'two_sided': 0,
            'liquidity': 0,
            'price': 0,
            'unknown': 0
        }
        
        rejected_markets = []
        
        for detail in cycle_details:
            reason = detail.get('failure_reason', 'unknown')
            if reason in failure_counts:
                failure_counts[reason] += 1
            
            # Track rejected markets separately
            if reason == 'rejected':
                rejected_markets.append(detail)
                rejection_reason = detail.get('rejection_reason', 'unknown')
                # Parse rejection reason (format: "reason:value" or "reason")
                if ':' in rejection_reason:
                    rejection_type = rejection_reason.split(':')[0]
                    if rejection_type in rejection_counts:
                        rejection_counts[rejection_type] += 1
                    else:
                        rejection_counts['unknown'] += 1
                else:
                    if rejection_reason in rejection_counts:
                        rejection_counts[rejection_reason] += 1
                    else:
                        rejection_counts['unknown'] += 1
        
        # Log cycle summary header
        self.detail_logger.info("=" * 100)
        self.detail_logger.info(f"FULL SWEEP CYCLE #{cycle_num} COMPLETE")
        self.detail_logger.info("=" * 100)
        self.detail_logger.info(f"Cycle started: {cycle_start.isoformat() if cycle_start else 'N/A'}")
        self.detail_logger.info(f"Cycle ended: {cycle_end.isoformat()}")
        self.detail_logger.info(f"Cycle duration: {cycle_duration:.1f} seconds ({cycle_duration/60:.1f} minutes)")
        self.detail_logger.info(f"Markets tracked: {tracked_count}")
        self.detail_logger.info(f"Markets checked: {len(cycle_details)}")
        self.detail_logger.info("")
        self.detail_logger.info("Filter Failure Summary:")
        self.detail_logger.info(f"  Warmup (need more data): {failure_counts['warmup']}")
        self.detail_logger.info(f"  Already active (session exists): {failure_counts['already_active']}")
        self.detail_logger.info(f"  Failed spread filter: {failure_counts['spread']}")
        self.detail_logger.info(f"  Failed volatility filter: {failure_counts['volatility']}")
        # Add volatility breakdown
        volatility_failures = [d for d in cycle_details if d.get('failure_reason') == 'volatility']
        sigma_passed_count = sum(1 for d in volatility_failures if d.get('failure_details', {}).get('has_sigma', False))
        jump_passed_count = sum(1 for d in volatility_failures if d.get('failure_details', {}).get('has_price_jump', False))
        if volatility_failures:
            self.detail_logger.info(f"    Volatility breakdown: sigma_passed={sigma_passed_count}, jump_passed={jump_passed_count}")
        self.detail_logger.info(f"  Failed regime stability: {failure_counts['regime_stability']}")
        self.detail_logger.info(f"  Failed regime filter: {failure_counts['regime_filter']}")
        self.detail_logger.info(f"  Failed volume filter: {failure_counts['volume']}")
        self.detail_logger.info(f"  Failed recent trades filter: {failure_counts['recent_trades']}")
        self.detail_logger.info(f"  Passed all filters: {failure_counts['passed_all']}")
        self.detail_logger.info(f"  Rejected (removed from tracking): {failure_counts['rejected']}")
        self.detail_logger.info("")
        
        # Log rejection breakdown if any markets were rejected
        if failure_counts['rejected'] > 0:
            self.detail_logger.info("Rejection Breakdown (markets removed from tracking):")
            self.detail_logger.info(f"  Status filter: {rejection_counts['status']}")
            self.detail_logger.info(f"  Age filter: {rejection_counts['age']}")
            self.detail_logger.info(f"  Volume filter: {rejection_counts['volume']}")
            self.detail_logger.info(f"  Freshness filter: {rejection_counts['freshness']}")
            self.detail_logger.info(f"  Spread filter: {rejection_counts['spread']}")
            self.detail_logger.info(f"  Two-sided orderbook filter: {rejection_counts['two_sided']}")
            self.detail_logger.info(f"  Liquidity filter: {rejection_counts['liquidity']}")
            self.detail_logger.info(f"  Price filter: {rejection_counts['price']}")
            self.detail_logger.info(f"  Unknown reason: {rejection_counts['unknown']}")
            self.detail_logger.info("")
            
            # Log detailed metrics for rejected markets
            self.detail_logger.info("Rejected Markets (detailed metrics):")
            for detail in rejected_markets[:10]:  # Show top 10 rejected markets
                ticker = detail['ticker']
                rejection_reason = detail.get('rejection_reason', 'unknown')
                self.detail_logger.info(f"\n  Market: {ticker}")
                self.detail_logger.info(f"    Rejection reason: {rejection_reason}")
                self.detail_logger.info(f"    Status: {detail.get('status', 'unknown')}")
                self.detail_logger.info(f"    Mid Price: {detail.get('mid_price', 0):.4f}")
                self.detail_logger.info(f"    Spread: {detail.get('spread', 0):.4f} (threshold: {detail['thresholds'].get('spread', 0.20):.4f})")
                self.detail_logger.info(f"    Volume 24h: ${detail.get('volume_24h', 0):.2f} (threshold: ${detail['thresholds'].get('volume_24h', 1000):.2f})")
                self.detail_logger.info(f"    Liquidity: {detail.get('liquidity', 0):.2f}")
                if detail.get('time_to_expiry_hours') is not None:
                    self.detail_logger.info(f"    Time to Expiry: {detail.get('time_to_expiry_hours', 0):.1f} hours")
                self.detail_logger.info(f"    Timestamp: {detail.get('timestamp', 'unknown')}")
            if len(rejected_markets) > 10:
                self.detail_logger.info(f"\n  ... and {len(rejected_markets) - 10} more rejected markets")
            self.detail_logger.info("")
        self.detail_logger.info("=" * 100)
        self.detail_logger.info("DETAILED MARKET METRICS")
        self.detail_logger.info("=" * 100)
        
        # Filter out markets with zero sigma and no price jump (not interesting)
        interesting_markets = [
            detail for detail in cycle_details
            if detail.get('sigma', 0) > 0 or detail.get('price_jump', 0) > 0 or detail.get('failure_reason') == 'passed_all'
        ]
        
        self.detail_logger.info(f"Markets with activity (sigma>0 or jump>0 or passed_all): {len(interesting_markets)}/{len(cycle_details)}")
        self.detail_logger.info("")
        
        # Log each market with full details (only interesting ones)
        for detail in interesting_markets:
            ticker = detail['ticker']
            self.detail_logger.info(f"\nMarket: {ticker}")
            self.detail_logger.info(f"  Status: {detail['failure_reason']}")
            self.detail_logger.info(f"  Mid Price: {detail['mid_price']:.4f}")
            self.detail_logger.info(f"  Spread: {detail['spread']:.4f} (threshold: {detail['thresholds']['spread']:.4f})")
            self.detail_logger.info(f"  Sigma: {detail['sigma']:.6f} (threshold: {detail['thresholds']['sigma']:.6f})")
            self.detail_logger.info(f"  Price Jump: {detail['price_jump']:.2f}c (threshold: {detail['thresholds']['price_jump_cents']:.2f}c)")
            self.detail_logger.info(f"  Volume 24h: ${detail.get('volume_24h', 0):.2f} (threshold: ${detail['thresholds'].get('volume_24h', 1000):.2f})")
            
            # Log regime information
            if 'regime' in detail:
                self.detail_logger.info(f"  Regime: {detail['regime']}")
            if 'has_mean_reversion' in detail:
                mean_rev_str = "YES" if detail['has_mean_reversion'] else "NO"
                self.detail_logger.info(f"  Mean Reversion: {mean_rev_str} (required for all triggers)")
            if 'is_regime_stable' in detail:
                stable_str = "YES" if detail['is_regime_stable'] else "NO"
                stability_req = detail['thresholds'].get('regime_stability_required', 3)
                self.detail_logger.info(f"  Regime Stable: {stable_str} (need {stability_req} consecutive)")
            if 'recent_price_updates' in detail and detail['recent_price_updates'] is not None:
                min_trades = detail['thresholds'].get('min_recent_trades', 0)
                self.detail_logger.info(f"  Recent Price Updates: {detail['recent_price_updates']} (required: {min_trades})")
            
            # Log failure details if available
            if 'failure_details' in detail and detail['failure_details']:
                fd = detail['failure_details']
                if detail['failure_reason'] == 'volatility':
                    self.detail_logger.info(f"    Volatility Details:")
                    self.detail_logger.info(f"      - Price Jump: {fd.get('price_jump', 0):.2f}c (threshold: {fd.get('price_jump_threshold', 12):.2f}c, passed: {fd.get('has_price_jump', False)})")
                    self.detail_logger.info(f"      - Sigma: {fd.get('sigma', 0):.6f} (threshold: {fd.get('sigma_threshold', 0.02):.6f}, passed: {fd.get('has_sigma', False)})")
                    self.detail_logger.info(f"      - Mean Reversion: {'YES' if fd.get('has_mean_reversion', False) else 'NO'} (REQUIRED for all triggers)")
                elif detail['failure_reason'] == 'regime_stability':
                    self.detail_logger.info(f"    Regime Stability Details:")
                    self.detail_logger.info(f"      - Current Regime: {fd.get('regime', 'UNKNOWN')}")
                    self.detail_logger.info(f"      - Regime History: {fd.get('regime_history', [])}")
                    self.detail_logger.info(f"      - Required: {fd.get('stability_required', 3)} consecutive")
                elif detail['failure_reason'] == 'regime_filter':
                    self.detail_logger.info(f"    Regime Filter Details:")
                    self.detail_logger.info(f"      - Current Regime: {fd.get('regime', 'UNKNOWN')}")
                    self.detail_logger.info(f"      - Allowed: {fd.get('allowed_regimes', [])}")
                    self.detail_logger.info(f"      - Disallowed: {fd.get('disallowed_regimes', [])}")
            
            if detail.get('time_to_expiry_hours') is not None:
                self.detail_logger.info(f"  Time to Expiry: {detail['time_to_expiry_hours']:.1f} hours")
            self.detail_logger.info(f"  Timestamp: {detail['timestamp']}")
        
        self.detail_logger.info("")
        self.detail_logger.info("=" * 100)
        self.detail_logger.info(f"END OF CYCLE #{cycle_num}")
        self.detail_logger.info("=" * 100)
        self.detail_logger.info("")
    
    def _check_ended_events(self, markets: List[Dict], current_time: datetime):
        """Check for markets where volatility has ended"""
        if not self.active_volatility_events:
            return
        
        ended_tickers = []
        
        for ticker in list(self.active_volatility_events):
            if ticker not in self.market_states:
                ended_tickers.append(ticker)
                continue
            
            state = self.market_states[ticker]
            
            # Check if volatility has dropped
            if len(state.mid_prices) < 2:
                continue
            
            sigma = self._calculate_sigma(state)
            price_jump = self._check_price_jump(state)
            
            # Check if volatility dropped below threshold
            has_volatility = (price_jump is not None and price_jump >= self.price_jump_threshold_cents) or \
                            (sigma >= self.sigma_threshold)
            
            if not has_volatility:
                ended_tickers.append(ticker)
                if self.ended_callback:
                    event = VolatilityEndedEvent(
                        ticker=ticker,
                        timestamp=current_time,
                        reason="volatility_collapse"
                    )
                    self.ended_callback(event)
        
        # Remove ended events
        for ticker in ended_tickers:
            self.active_volatility_events.discard(ticker)
    
    def _cleanup_old_states(self):
        """Remove state for markets that haven't been seen recently"""
        cutoff_time = datetime.now(timezone.utc).timestamp() - (self.rolling_window_minutes * 60 * 2)
        
        to_remove = []
        for ticker, state in self.market_states.items():
            if state.last_activity_time.timestamp() < cutoff_time:
                to_remove.append(ticker)
        
        for ticker in to_remove:
            del self.market_states[ticker]
            self.prev_volume_deltas.pop(ticker, None)
            self.active_volatility_events.discard(ticker)
    
    def _rediscovery_loop(self):
        """Background thread: full rediscovery every 10 minutes"""
        rediscovery_interval = 600  # 10 minutes
        
        while self.running:
            time.sleep(rediscovery_interval)
            
            if not self.running:
                break
            
            try:
                self.logger.info("Starting periodic full rediscovery...")
                
                # Re-run full discovery
                all_markets = self.discovery.get_all_open_markets()
                binary_markets = self.discovery.filter_binary_markets(all_markets)
                active_markets, _ = self._filter_active_markets(binary_markets, verbose_logging=False)
                
                # Score and sort
                scored_markets = []
                for market in active_markets:
                    score = self._score_market_activity(market)
                    scored_markets.append((score, market))
                
                scored_markets.sort(key=lambda x: x[0], reverse=True)
                
                # Get new top 200
                top_markets = scored_markets[:200]
                new_tracked = {m.get('ticker') for _, m in top_markets}
                
                # Thread-safe update of tracked set and state
                with self.lock:
                    # Calculate changes BEFORE reassigning
                    old_tracked = self.tracked_tickers
                    removed_tickers = old_tracked - new_tracked
                    added_tickers = new_tracked - old_tracked
                    
                    # Update tracked set
                    self.tracked_tickers = new_tracked
                    
                    # Reset cycle tracking since tracked set changed
                    self.markets_checked_this_cycle.clear()
                    self.current_cycle_start_time = None
                    self.cycle_market_details.clear()
                    
                    # Cleanup state for removed tickers
                    for ticker in removed_tickers:
                        self.market_states.pop(ticker, None)
                        self.prev_volume_deltas.pop(ticker, None)
                        self.active_volatility_events.discard(ticker)
                        # Phase 2.4: Unsubscribe from removed markets
                        self._unsubscribe_from_market(ticker)
                
                # Initialize state for new markets (outside lock to avoid long lock hold)
                current_time = datetime.now(timezone.utc)
                for _, market in top_markets:
                    ticker = market.get('ticker')
                    if ticker and ticker in added_tickers:
                        self._update_market_state(market, current_time)
                        # Phase 2.4: Subscribe to WS channels for new markets
                        self._subscribe_to_market(ticker)
                
                self.logger.info(
                    f"Rediscovery complete: tracking {len(new_tracked)} markets "
                    f"({len(removed_tickers)} removed, {len(added_tickers)} added)"
                )
                
            except Exception as e:
                self.logger.error(f"Error in rediscovery: {e}")
    
    def mark_event_ended(self, ticker: str):
        """Mark a volatility event as ended (called by manager when session terminates)"""
        self.active_volatility_events.discard(ticker)

