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

from market_discovery import MarketDiscovery
from mm import KalshiTradingAPI
from volatility_models import MarketState, VolatilityEvent, VolatilityEndedEvent


class VolatilityScanner:
    """Continuously scans markets for volatility events"""
    
    def __init__(
        self,
        api: KalshiTradingAPI,
        discovery: MarketDiscovery,
        config: Dict,
        event_callback: Optional[Callable[[VolatilityEvent], None]] = None,
        ended_callback: Optional[Callable[[VolatilityEndedEvent], None]] = None
    ):
        self.api = api
        self.discovery = discovery
        self.config = config
        self.event_callback = event_callback
        self.ended_callback = ended_callback
        self.logger = logging.getLogger("VolatilityScanner")
        
        # Set up detailed file logger for sweep cycles
        self.detail_logger = logging.getLogger("VolatilityScanner.Details")
        self.detail_logger.setLevel(logging.INFO)
        # Create log file with date in name
        log_filename = f"volatility_sweep_details_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_filename, mode='a')
        file_handler.setLevel(logging.INFO)
        detail_formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(detail_formatter)
        self.detail_logger.addHandler(file_handler)
        self.detail_logger.propagate = False  # Don't propagate to root logger
        
        # Sweep cycle tracking
        self.markets_checked_this_cycle: set = set()
        self.current_cycle_start_time: Optional[datetime] = None
        self.cycle_number = 0
        self.cycle_market_details: List[Dict] = []  # Store details for current cycle
        
        # Scanner configuration
        scanner_config = config.get('volatility_scanner', {})
        self.refresh_interval = scanner_config.get('refresh_interval_seconds', 45)
        self.rolling_window_minutes = scanner_config.get('rolling_window_minutes', 15)
        self.volatility_window_minutes = scanner_config.get('volatility_window_minutes', 10)
        
        # Volatility thresholds
        self.price_jump_threshold_cents = scanner_config.get('price_jump_threshold_cents', 12)
        self.price_jump_window_minutes = scanner_config.get('price_jump_window_minutes', 5)
        self.sigma_threshold = scanner_config.get('sigma_threshold', 0.02)
        
        # Volume requirements
        self.volume_burst_multiplier = scanner_config.get('volume_burst_multiplier', 2.5)
        self.volume_baseline_scans = scanner_config.get('volume_baseline_scans', 15)
        self.min_absolute_volume_per_5m = scanner_config.get('min_absolute_volume_per_5m', 30)
        self.estimated_trade_size = scanner_config.get('estimated_trade_size', 3)
        self.min_estimated_trades = scanner_config.get('min_estimated_trades', 3)
        
        # Optional volume velocity
        self.volume_velocity_enabled = scanner_config.get('volume_velocity_enabled', False)
        self.volume_velocity_threshold = scanner_config.get('volume_velocity_threshold', 200)
        
        # Liquidity requirements
        self.max_time_to_expiry_hours = scanner_config.get('max_time_to_expiry_hours', 24)
        self.min_liquidity_spread = scanner_config.get('min_liquidity_spread', 0.10)
        
        # In-memory state: ticker -> MarketState
        self.market_states: Dict[str, MarketState] = {}
        
        # Previous volume deltas for velocity calculation
        self.prev_volume_deltas: Dict[str, float] = {}
        
        # Previous absolute volume_24h for delta calculation
        self.prev_volume_24h: Dict[str, float] = {}
        
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
    
    def start(self):
        """Start the scanner in a background thread"""
        if self.running:
            self.logger.warning("Scanner already running")
            return
        
        self.running = True
        self.scanner_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.scanner_thread.start()
        self.logger.info(f"Volatility scanner started (refresh interval: {self.refresh_interval}s)")
        
        # Start rediscovery thread
        self.rediscovery_thread = threading.Thread(target=self._rediscovery_loop, daemon=True)
        self.rediscovery_thread.start()
    
    def stop(self):
        """Stop the scanner"""
        self.running = False
        if self.scanner_thread:
            self.scanner_thread.join(timeout=10)
        self.logger.info("Volatility scanner stopped")
    
    def _scan_loop(self):
        """Main scanning loop"""
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
    
    def _filter_active_markets(self, markets: List[Dict], verbose_logging: bool = False) -> List[Dict]:
        """Filter to markets worth tracking (status, age, volume, spread, liquidity, price, freshness)"""
        filtered = []
        now = datetime.now(timezone.utc)
        
        # Track rejection reasons for diagnostics
        rejection_counts = {
            'status': 0,
            'age': 0,
            'volume': 0,
            'freshness': 0,
            'spread': 0,
            'two_sided': 0,
            'liquidity': 0,
            'price': 0
        }
        
        # Track first rejected market for diagnostic logging
        first_rejected_market = None
        first_rejection_reason = None
        first_rejection_status_value = None
        
        for market in markets:
            # Check status (must be active/open - ignore paused, closed, expired, settlement)
            # Note: API returns status='active' for open markets, not 'open'
            status = market.get('status', '')
            if status:  # Only check if status field exists
                if status.lower() not in ('active', 'open'):  # Accept both 'active' and 'open'
                    rejection_counts['status'] += 1
                    # Track first rejection for diagnostics
                    if first_rejected_market is None:
                        first_rejected_market = market
                        first_rejection_reason = 'status'
                        first_rejection_status_value = status
                    self.logger.debug(f"Market {market.get('ticker', 'unknown')} rejected by status filter: status='{status}' (expected 'active' or 'open')")
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
                        if (now - created_time).total_seconds() < 60:
                            rejection_counts['age'] += 1
                            continue
                except Exception as e:
                    self.logger.debug(f"Error parsing created_time for {market.get('ticker', 'unknown')}: {e}")
                    # If we can't parse, allow the market (better to include than exclude)
            
            # Check volume (must have some activity)
            volume_24h = market.get('volume_24h', 0) / 100
            if volume_24h == 0:
                rejection_counts['volume'] += 1
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
                        if (now - last_activity).total_seconds() > 600:  # 10 minutes
                            rejection_counts['freshness'] += 1
                            if first_rejected_market is None:
                                first_rejected_market = market
                                first_rejection_reason = 'freshness'
                            continue
                except Exception as e:
                    self.logger.debug(f"Error parsing last_activity for {market.get('ticker', 'unknown')}: {e}")
                    # If we can't parse, allow the market (better to include than exclude)
            
            # Check spread (must be tight with real two-sided orderbook)
            spread = self.discovery.calculate_spread(market)
            if spread > 0.20:  # 20 cent max spread (tighter than 35c)
                rejection_counts['spread'] += 1
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
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'two_sided'
                continue
            
            # Check liquidity
            liquidity = self.discovery.get_book_depth(market)
            if liquidity == 0:
                rejection_counts['liquidity'] += 1
                if first_rejected_market is None:
                    first_rejected_market = market
                    first_rejection_reason = 'liquidity'
                continue
            
            # Check price (avoid extremes)
            mid_price = self.discovery.get_mid_price(market, 'yes')
            if mid_price < 0.05 or mid_price > 0.95:
                rejection_counts['price'] += 1
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
        
        return filtered
    
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
        active_markets = self._filter_active_markets(binary_markets, verbose_logging=True)
        self.logger.info(f"Filtered to {len(active_markets)} active markets (from {len(binary_markets)} binary)")
        
        # Score and sort by activity
        scored_markets = []
        for market in active_markets:
            score = self._score_market_activity(market)
            scored_markets.append((score, market))
        
        scored_markets.sort(key=lambda x: x[0], reverse=True)
        
        # Take top 200
        top_markets = scored_markets[:200]
        with self.lock:
            self.tracked_tickers = {m.get('ticker') for _, m in top_markets}
        
        # Initialize state for top markets
        current_time = datetime.now(timezone.utc)
        for _, market in top_markets:
            ticker = market.get('ticker')
            if ticker:
                self._update_market_state(market, current_time)
        
        scan_duration = time.time() - start_time
        self.logger.info(
            f"Initial scan complete: tracking top {len(self.tracked_tickers)} markets "
            f"(from {len(active_markets)} active), {scan_duration:.2f}s"
        )
    
    def _incremental_sweep(self):
        """Round-robin through tracked markets, fetching each directly"""
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
        
        # Filter to binary (should all be binary)
        binary_markets = self.discovery.filter_binary_markets(markets)
        
        # Update tracked states
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
        
        if tracked_count > 0 and checked_count >= tracked_count:
            # Full cycle complete - log summary
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
        
        # Filter all markets in one batch (fixes excessive logging)
        active_markets = self._filter_active_markets(markets, verbose_logging=False)
        active_tickers = {m.get('ticker') for m in active_markets if m.get('ticker')}
        
        # Create a map of active markets by ticker for quick lookup
        active_markets_by_ticker = {m.get('ticker'): m for m in active_markets}
        
        # Thread-safe check of tracked tickers
        with self.lock:
            tracked_snapshot = set(self.tracked_tickers)
        
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
                        # Market no longer active, remove from tracking
                        with self.lock:
                            self.tracked_tickers.discard(ticker)
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
    
    def _update_market_state(self, market: Dict, timestamp: datetime) -> Optional[VolatilityEvent]:
        """Update state for a market and check for volatility events"""
        ticker = market.get('ticker')
        if not ticker:
            return None
        
        # Get current market metrics
        mid_price = self.discovery.get_mid_price(market, 'yes')
        spread = self.discovery.calculate_spread(market)
        volume_24h = market.get('volume_24h', 0) / 100  # Convert cents to dollars
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
            self.prev_volume_24h[ticker] = volume_24h
        
        state = self.market_states[ticker]
        
        # Calculate volume delta (difference in cumulative volume_24h)
        prev_volume_24h = self.prev_volume_24h.get(ticker, volume_24h)
        volume_delta = max(0, volume_24h - prev_volume_24h)
        
        # Update previous volume_24h for next scan
        self.prev_volume_24h[ticker] = volume_24h
        
        # Update state
        state.add_snapshot(mid_price, volume_delta, spread, liquidity, timestamp)
        state.time_to_expiry = time_to_expiry
        state.close_time = close_time
        
        # Trim to rolling window
        state.trim_to_window(self.rolling_window_minutes)
        
        # Check if we have enough data
        if len(state.mid_prices) < 3:
            return None
        
        # Log detailed metrics for this market
        self._log_market_details(ticker, state, volume_delta, market)
        
        # Check for volatility event
        return self._check_volatility_event(state, volume_delta)
    
    def _log_market_details(self, ticker: str, state: MarketState, volume_delta: float, market: Dict):
        """Log detailed metrics for a market to file"""
        # Track this market in current cycle
        with self.lock:
            self.markets_checked_this_cycle.add(ticker)
            if self.current_cycle_start_time is None:
                self.current_cycle_start_time = datetime.now(timezone.utc)
        
        # Calculate all metrics
        price_jump = self._check_price_jump(state)
        sigma = self._calculate_sigma(state)
        volume_burst_multiplier = self._calculate_volume_burst_multiplier(state)
        estimated_trades = volume_delta / self.estimated_trade_size if self.estimated_trade_size > 0 else 0
        current_spread = state.spreads[-1] if state.spreads else 999.0
        mid_price = state.mid_prices[-1] if state.mid_prices else 0.0
        
        # Determine which filter failed (if any)
        failure_reason = None
        if len(state.volume_deltas) < 2:
            failure_reason = "warmup"
        elif current_spread > self.min_liquidity_spread:
            failure_reason = "spread"
        else:
            has_volatility = (price_jump is not None and price_jump >= self.price_jump_threshold_cents) or \
                            (sigma >= self.sigma_threshold)
            if not has_volatility:
                failure_reason = "volatility"
            else:
                volume_ok = (
                    volume_burst_multiplier >= self.volume_burst_multiplier and
                    volume_delta >= self.min_absolute_volume_per_5m and
                    estimated_trades >= self.min_estimated_trades
                )
                if not volume_ok:
                    failure_reason = "volume"
                else:
                    failure_reason = "passed_all"
        
        # Store details for cycle summary
        market_detail = {
            'ticker': ticker,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'mid_price': mid_price,
            'spread': current_spread,
            'sigma': sigma,
            'price_jump': price_jump if price_jump is not None else 0,
            'volume_multiplier': volume_burst_multiplier,
            'volume_delta': volume_delta,
            'estimated_trades': estimated_trades,
            'time_to_expiry_hours': state.time_to_expiry * 24 if state.time_to_expiry else None,
            'volume_deltas_count': len(state.volume_deltas),
            'failure_reason': failure_reason,
            'thresholds': {
                'sigma': self.sigma_threshold,
                'price_jump_cents': self.price_jump_threshold_cents,
                'volume_multiplier': self.volume_burst_multiplier,
                'volume_delta': self.min_absolute_volume_per_5m,
                'estimated_trades': self.min_estimated_trades,
                'spread': self.min_liquidity_spread
            }
        }
        
        with self.lock:
            self.cycle_market_details.append(market_detail)
    
    def _check_volatility_event(self, state: MarketState, volume_delta: float) -> Optional[VolatilityEvent]:
        """Check if market meets volatility event criteria"""
        ticker = state.ticker
        
        # Warmup check: require at least 2 volume deltas before checking events
        if len(state.volume_deltas) < 2:
            self.logger.debug(f"{ticker}: Warmup - only {len(state.volume_deltas)} volume deltas (need 2)")
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
        sigma = self._calculate_sigma(state)
        has_volatility = (price_jump is not None and price_jump >= self.price_jump_threshold_cents) or \
                        (sigma >= self.sigma_threshold)
        
        if not has_volatility:
            jump_str = f"{price_jump:.1f}c" if price_jump is not None else "None"
            self.logger.debug(
                f"{ticker}: Failed volatility filter "
                f"(jump={jump_str}, sigma={sigma:.4f}, "
                f"thresholds: jump>={self.price_jump_threshold_cents}c, sigma>={self.sigma_threshold})"
            )
            return None
        
        # Check volume requirements (ALL must pass)
        volume_burst_multiplier = self._calculate_volume_burst_multiplier(state)
        estimated_trades = volume_delta / self.estimated_trade_size if self.estimated_trade_size > 0 else 0
        
        volume_ok = (
            volume_burst_multiplier >= self.volume_burst_multiplier and
            volume_delta >= self.min_absolute_volume_per_5m and
            estimated_trades >= self.min_estimated_trades
        )
        
        if not volume_ok:
            self.logger.debug(
                f"{ticker}: Failed volume filter "
                f"(mult={volume_burst_multiplier:.2f}, delta={volume_delta:.1f}, "
                f"trades={estimated_trades:.1f}, "
                f"thresholds: mult>={self.volume_burst_multiplier}, "
                f"delta>={self.min_absolute_volume_per_5m}, trades>={self.min_estimated_trades})"
            )
            return None
        
        # Optional: Check volume velocity
        volume_velocity = None
        if self.volume_velocity_enabled:
            prev_delta = self.prev_volume_deltas.get(ticker, 0.0)
            if len(state.volume_deltas) >= 2:
                volume_velocity = state.volume_deltas[-1] - prev_delta
                if volume_velocity < self.volume_velocity_threshold:
                    self.logger.debug(
                        f"{ticker}: Failed volume velocity filter "
                        f"(velocity={volume_velocity:.1f}, threshold={self.volume_velocity_threshold})"
                    )
                    return None
        
        # Calculate direction (optional)
        direction = None
        if len(state.mid_prices) >= 2:
            price_change = state.mid_prices[-1] - state.mid_prices[0]
            if abs(price_change) > 0.05:  # 5 cent threshold
                direction = 'down' if price_change < 0 else 'up'
        
        # Calculate signal strength (0-1)
        signal_strength = self._calculate_signal_strength(
            price_jump, sigma, volume_burst_multiplier, volume_delta
        )
        
        # All filters passed - create event
        jump_str = f"{price_jump:.1f}c" if price_jump is not None else "None"
        self.logger.info(
            f"{ticker}: ✓ Volatility event detected! "
            f"jump={jump_str}, sigma={sigma:.4f}, "
            f"vol_mult={volume_burst_multiplier:.2f}, vol_delta={volume_delta:.1f}, "
            f"strength={signal_strength:.2f}"
        )
        
        # Create event
        event = VolatilityEvent(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            jump_magnitude=price_jump,
            sigma=sigma,
            volume_multiplier=volume_burst_multiplier,
            volume_delta=volume_delta,
            estimated_trades=estimated_trades,
            volume_velocity=volume_velocity,
            close_time=state.close_time,
            direction=direction,
            signal_strength=signal_strength
        )
        
        # Update previous volume delta for velocity calculation
        self.prev_volume_deltas[ticker] = volume_delta
        
        return event
    
    def _check_price_jump(self, state: MarketState) -> Optional[float]:
        """Check for price jump in the specified window"""
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
        
        return jump_cents if jump_cents >= self.price_jump_threshold_cents else None
    
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
    
    def _calculate_volume_burst_multiplier(self, state: MarketState) -> float:
        """Calculate volume burst multiplier (current / baseline)"""
        if len(state.volume_deltas) < 2:
            return 0.0
        
        # Get baseline from last N scans
        baseline_window = min(self.volume_baseline_scans, len(state.volume_deltas) - 1)
        if baseline_window < 2:
            return 0.0
        
        baseline_volumes = state.volume_deltas[:-1][-baseline_window:]
        baseline = statistics.mean(baseline_volumes) if baseline_volumes else 0.0
        
        if baseline <= 0:
            return 0.0
        
        current_volume = state.volume_deltas[-1]
        return current_volume / baseline
    
    def _calculate_signal_strength(
        self, price_jump: Optional[float], sigma: float,
        volume_multiplier: float, volume_delta: float
    ) -> float:
        """Calculate combined signal strength (0-1)"""
        # Normalize components
        jump_score = min(1.0, (price_jump or 0) / (self.price_jump_threshold_cents * 2)) if price_jump else 0.0
        sigma_score = min(1.0, sigma / (self.sigma_threshold * 2))
        volume_score = min(1.0, volume_multiplier / (self.volume_burst_multiplier * 2))
        
        # Weighted average
        signal_strength = (jump_score * 0.3 + sigma_score * 0.3 + volume_score * 0.4)
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
            'volume_filter': [],  # Low volume
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
            
            # Warmup check
            if len(state.volume_deltas) < 2:
                stats['warmup'].append({'ticker': ticker, 'volume_deltas': len(state.volume_deltas)})
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
            sigma = self._calculate_sigma(state)
            has_volatility = (price_jump is not None and price_jump >= self.price_jump_threshold_cents) or \
                            (sigma >= self.sigma_threshold)
            
            if not has_volatility:
                jump_val = price_jump if price_jump is not None else 0
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
            
            # Check volume
            volume_burst_multiplier = self._calculate_volume_burst_multiplier(state)
            volume_delta = state.volume_deltas[-1] if state.volume_deltas else 0
            estimated_trades = volume_delta / self.estimated_trade_size if self.estimated_trade_size > 0 else 0
            
            volume_ok = (
                volume_burst_multiplier >= self.volume_burst_multiplier and
                volume_delta >= self.min_absolute_volume_per_5m and
                estimated_trades >= self.min_estimated_trades
            )
            
            if not volume_ok:
                stats['volume_filter'].append({
                    'ticker': ticker,
                    'vol_mult': volume_burst_multiplier,
                    'vol_delta': volume_delta,
                    'trades': estimated_trades,
                    'vol_mult_threshold': self.volume_burst_multiplier,
                    'vol_delta_threshold': self.min_absolute_volume_per_5m,
                    'trades_threshold': self.min_estimated_trades
                })
                # Track markets close to volume threshold (70%+ of required)
                if volume_burst_multiplier > self.volume_burst_multiplier * 0.7 or \
                   volume_delta > self.min_absolute_volume_per_5m * 0.7:
                    stats['close_to_triggering'].append({
                        'ticker': ticker,
                        'vol_mult': volume_burst_multiplier,
                        'vol_delta': volume_delta,
                        'trades': estimated_trades,
                        'reason': 'volume'
                    })
                continue
            
            # If we get here, market passed all filters but didn't trigger (maybe already active?)
            stats['passed_all'].append({
                'ticker': ticker,
                'sigma': sigma,
                'jump': price_jump,
                'vol_mult': volume_burst_multiplier,
                'vol_delta': volume_delta,
                'trades': estimated_trades
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
        self.logger.info(f"  Failed volume filter: {len(stats['volume_filter'])}")
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
        
        if stats['volume_filter']:
            self.logger.info("")
            self.logger.info("Sample markets (failed volume):")
            # Sort to prioritize markets closest to thresholds
            sorted_volume = sorted(
                stats['volume_filter'],
                key=lambda m: max(
                    m['vol_mult'] / m['vol_mult_threshold'] if m['vol_mult_threshold'] > 0 else 0,
                    m['vol_delta'] / m['vol_delta_threshold'] if m['vol_delta_threshold'] > 0 else 0,
                    m['trades'] / m['trades_threshold'] if m['trades_threshold'] > 0 else 0
                ),
                reverse=True  # Highest ratios first (closest to thresholds)
            )
            for m in sorted_volume[:15]:  # Show top 15 instead of 5
                self.logger.info(
                    f"  {m['ticker']}: vol_mult={m['vol_mult']:.2f} (need>={m['vol_mult_threshold']:.2f}), "
                    f"vol_delta={m['vol_delta']:.1f} (need>={m['vol_delta_threshold']:.1f}), "
                    f"trades={m['trades']:.1f} (need>={m['trades_threshold']:.1f})"
                )
        
        if stats['close_to_triggering']:
            self.logger.info("")
            self.logger.info("Markets close to triggering:")
            # Sort by how close they are to thresholds
            def closeness_score(m):
                if m['reason'] == 'volatility' and self.sigma_threshold > 0:
                    return m['sigma'] / self.sigma_threshold
                elif m['reason'] == 'volume' and self.volume_burst_multiplier > 0:
                    return m.get('vol_mult', 0) / self.volume_burst_multiplier
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
                else:
                    self.logger.info(
                        f"  {m['ticker']}: vol_mult={m['vol_mult']:.2f}, vol_delta={m['vol_delta']:.1f}, "
                        f"trades={m['trades']:.1f} "
                        f"(need: mult>={self.volume_burst_multiplier}, delta>={self.min_absolute_volume_per_5m}, "
                        f"trades>={self.min_estimated_trades})"
                    )
        
        if stats['passed_all']:
            self.logger.info("")
            self.logger.info("Markets that passed all filters (may be already active):")
            for m in stats['passed_all'][:5]:
                jump_str = f"{m['jump']:.1f}c" if m['jump'] is not None else "None"
                self.logger.info(
                    f"  {m['ticker']}: sigma={m['sigma']:.4f}, jump={jump_str}, "
                    f"vol_mult={m['vol_mult']:.2f}, vol_delta={m['vol_delta']:.1f}, trades={m['trades']:.1f}"
                )
        
        self.logger.info("=" * 80)
    
    def _log_full_cycle_summary(self):
        """Log detailed summary when a full cycle through all markets is complete"""
        with self.lock:
            cycle_details = list(self.cycle_market_details)
            cycle_start = self.current_cycle_start_time
            cycle_num = self.cycle_number
            tracked_count = len(self.tracked_tickers)
        
        if not cycle_details:
            return
        
        cycle_end = datetime.now(timezone.utc)
        cycle_duration = (cycle_end - cycle_start).total_seconds() if cycle_start else 0
        
        # Aggregate statistics
        failure_counts = {
            'warmup': 0,
            'spread': 0,
            'volatility': 0,
            'volume': 0,
            'passed_all': 0
        }
        
        for detail in cycle_details:
            reason = detail.get('failure_reason', 'unknown')
            if reason in failure_counts:
                failure_counts[reason] += 1
        
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
        self.detail_logger.info(f"  Failed spread filter: {failure_counts['spread']}")
        self.detail_logger.info(f"  Failed volatility filter: {failure_counts['volatility']}")
        self.detail_logger.info(f"  Failed volume filter: {failure_counts['volume']}")
        self.detail_logger.info(f"  Passed all filters: {failure_counts['passed_all']}")
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
            self.detail_logger.info(f"  Volume Multiplier: {detail['volume_multiplier']:.2f}x (threshold: {detail['thresholds']['volume_multiplier']:.2f}x)")
            self.detail_logger.info(f"  Volume Delta: {detail['volume_delta']:.1f} (threshold: {detail['thresholds']['volume_delta']:.1f})")
            self.detail_logger.info(f"  Estimated Trades: {detail['estimated_trades']:.1f} (threshold: {detail['thresholds']['estimated_trades']:.1f})")
            if detail['time_to_expiry_hours'] is not None:
                self.detail_logger.info(f"  Time to Expiry: {detail['time_to_expiry_hours']:.1f} hours")
            self.detail_logger.info(f"  Volume Deltas Count: {detail['volume_deltas_count']}")
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
            self.prev_volume_24h.pop(ticker, None)
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
                active_markets = self._filter_active_markets(binary_markets, verbose_logging=False)
                
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
                        self.prev_volume_24h.pop(ticker, None)
                        self.prev_volume_deltas.pop(ticker, None)
                        self.active_volatility_events.discard(ticker)
                
                # Initialize state for new markets (outside lock to avoid long lock hold)
                current_time = datetime.now(timezone.utc)
                for _, market in top_markets:
                    ticker = market.get('ticker')
                    if ticker and ticker in added_tickers:
                        self._update_market_state(market, current_time)
                
                self.logger.info(
                    f"Rediscovery complete: tracking {len(new_tracked)} markets "
                    f"({len(removed_tickers)} removed, {len(added_tickers)} added)"
                )
                
            except Exception as e:
                self.logger.error(f"Error in rediscovery: {e}")
    
    def mark_event_ended(self, ticker: str):
        """Mark a volatility event as ended (called by manager when session terminates)"""
        self.active_volatility_events.discard(ticker)

