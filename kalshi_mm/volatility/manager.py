"""
Volatility MM Manager - Spawns, tracks, and kills short-lived MM sessions
"""
import logging
import logging.handlers
import time
import threading
from typing import Dict, Optional, Callable
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path

from kalshi_mm.api import KalshiTradingAPI, AvellanedaMarketMaker
from kalshi_mm.analytics import AnalyticsDB
from kalshi_mm.discovery import MarketDiscovery
from kalshi_mm.config import generate_config_for_market
from kalshi_mm.volatility.models import VolatilityEvent, VolatilityEndedEvent
from runner import create_api, create_market_maker, run_strategy


class VolatilityMMManager:
    """Manages ephemeral MM sessions spawned from volatility events"""
    
    def __init__(
        self,
        api: KalshiTradingAPI,
        discovery: MarketDiscovery,
        analytics_db: AnalyticsDB,
        config: Dict,
        scanner: Optional[object] = None,  # VolatilityScanner instance
        ws_client: Optional[object] = None,  # KalshiWebsocketClient
        state_store: Optional[object] = None,  # MarketStateStore
    ):
        self.api = api
        self.discovery = discovery
        self.analytics_db = analytics_db
        self.config = config
        self.scanner = scanner  # For regime access
        self.ws_client = ws_client  # Phase 4.2
        self.state_store = state_store  # Phase 4.2
        self.logger = logging.getLogger("VolatilityMMManager")
        
        # Manager configuration
        mm_config = config.get('volatility_mm', {})
        self.max_concurrent_sessions = mm_config.get('max_concurrent_sessions', 4)
        self.session_ttl_minutes = mm_config.get('session_ttl_minutes', 20)
        self.guardrails = mm_config.get('guardrails', {})
        
        # Active sessions: ticker -> (future, start_time, event, run_id)
        self.active_sessions: Dict[str, tuple] = {}
        
        # Thread pool for running sessions
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_sessions)
        
        # Lock for thread-safe operations
        self.lock = threading.Lock()
        
        # Running flag
        self.running = False
    
    def start(self):
        """Start the manager"""
        self.running = True
        self.logger.info(f"Volatility MM Manager started (max sessions: {self.max_concurrent_sessions})")
    
    def stop(self):
        """Stop the manager and terminate all sessions"""
        self.running = False
        
        with self.lock:
            # Cancel all active sessions
            for ticker, (future, start_time, event, run_id) in list(self.active_sessions.items()):
                self.logger.info(f"Terminating session for {ticker}")
                future.cancel()
                self._cleanup_session(ticker, "manager_stopped")
        
        self.executor.shutdown(wait=True)
        self.logger.info("Volatility MM Manager stopped")
    
    def handle_volatility_event(self, event: VolatilityEvent):
        """Handle a volatility event - spawn session if conditions met"""
        ticker = event.ticker
        
        with self.lock:
            # Check if already active
            if ticker in self.active_sessions:
                self.logger.debug(f"Session already active for {ticker}, skipping")
                return
            
            # Phase 3.2: Balance-Based Session Gating with Hysteresis
            mm_config = self.config.get('volatility_mm', {})
            try:
                balance_data = self.api.get_balance()
                available = balance_data.get('available_balance_dollars', 0)
                min_balance = mm_config.get('min_available_balance', 100)
                balance_hysteresis_ratio = mm_config.get('balance_hysteresis_ratio', 0.85)
                
                if available < min_balance:
                    if not self._balance_blocked.get(ticker, False):
                        self.logger.warning(f"Insufficient balance: ${available} < ${min_balance}")
                        self._balance_blocked[ticker] = True
                    
                    # If passive exit enabled, allow session but don't open new positions
                    if mm_config.get('passive_exit', False):
                        self.logger.info("Balance low but passive_exit enabled - allowing session")
                        # Continue to spawn session
                    else:
                        return  # Block session entirely
                elif self._balance_blocked.get(ticker, False) and available >= (min_balance * balance_hysteresis_ratio):
                    self.logger.info(f"Balance recovered: ${available} >= ${min_balance * balance_hysteresis_ratio}")
                    self._balance_blocked[ticker] = False
            except Exception as e:
                self.logger.warning(f"Could not check balance: {e}")
            
            # Phase 3.3: Resting Order Value Check with Hysteresis
            try:
                resting_data = self.api.get_total_resting_order_value()
                resting_value = resting_data["total_resting_order_value_dollars"]
                max_resting = mm_config.get('max_resting_order_value', 10000)
                balance_hysteresis_ratio = mm_config.get('balance_hysteresis_ratio', 0.85)
                
                if resting_value > max_resting:
                    if not self._resting_blocked.get(ticker, False):
                        self.logger.warning(f"Too much margin locked: ${resting_value} > ${max_resting}")
                        self._resting_blocked[ticker] = True
                    return
                elif self._resting_blocked.get(ticker, False) and resting_value <= (max_resting * balance_hysteresis_ratio):
                    self.logger.info(f"Resting value recovered: ${resting_value} <= ${max_resting * balance_hysteresis_ratio}")
                    self._resting_blocked[ticker] = False
            except Exception as e:
                self.logger.warning(f"Could not check resting order value: {e}")
            
            # Check if we should disallow new sessions when positions exist
            disallow_if_positions = mm_config.get('disallow_new_sessions_if_open_positions_exist', False)
            
            if disallow_if_positions and ticker in self.active_sessions:
                future, start_time, _, _ = self.active_sessions[ticker]
                session_age = (datetime.now(timezone.utc) - start_time).total_seconds()
                if session_age < 60:  # Recent session likely has position
                    self.logger.info(
                        f"Skipping {ticker}: existing session likely has open position "
                        f"(age={session_age:.1f}s, disallow_if_positions=True)"
                    )
                    return
            
            # Check global cap
            if len(self.active_sessions) >= self.max_concurrent_sessions:
                self.logger.warning(
                    f"Max concurrent sessions reached ({self.max_concurrent_sessions}), "
                    f"skipping {ticker}"
                )
                return
            
            # Spawn session
            self._spawn_session(event)
    
    def handle_volatility_ended(self, event: VolatilityEndedEvent):
        """Handle volatility ended event - terminate session"""
        ticker = event.ticker
        
        with self.lock:
            if ticker in self.active_sessions:
                self.logger.info(f"Terminating session for {ticker}: {event.reason}")
                future, _, _, _ = self.active_sessions[ticker]
                future.cancel()
                self._cleanup_session(ticker, event.reason)
    
    def _spawn_session(self, event: VolatilityEvent):
        """Spawn a new MM session for a volatility event"""
        ticker = event.ticker
        self.logger.info(f"Spawning volatility MM session for {ticker}")
        
        try:
            # Get market data (we need full market dict for config generation)
            # We'll need to fetch it or get it from scanner state
            market_data = self._get_market_data(ticker)
            if not market_data:
                self.logger.warning(f"Could not get market data for {ticker}, skipping")
                return
            
            # Generate volatility-specific config
            vol_config = self._generate_volatility_config(event, market_data)
            if not vol_config:
                # Log more details about why config generation failed
                yes_bid = market_data.get('yes_bid', 0) / 100
                yes_ask = market_data.get('yes_ask', 0) / 100
                mid_price = (yes_bid + yes_ask) / 2 if yes_bid > 0 and yes_ask > 0 else 0.5
                from kalshi_mm.config import get_extreme_price_band
                extreme_band = get_extreme_price_band(mid_price, self.config)
                self.logger.warning(
                    f"Could not generate config for {ticker}, skipping. "
                    f"mid_price={mid_price:.4f}, extreme_band={extreme_band}, "
                    f"volume_24h=${market_data.get('volume_24h', 0)/100:.1f}"
                )
                return
            
            # Create config name
            config_name = f"{ticker}-VOL-{int(time.time())}"
            
            # Submit session to thread pool
            future = self.executor.submit(
                self._run_volatility_session,
                config_name,
                vol_config,
                event
            )
            
            # Track session
            start_time = datetime.now(timezone.utc)
            run_id = None  # Will be set when session starts
            self.active_sessions[ticker] = (future, start_time, event, run_id)
            
            self.logger.info(f"Session spawned for {ticker}, total active: {len(self.active_sessions)}")
        
        except Exception as e:
            self.logger.error(f"Error spawning session for {ticker}: {e}", exc_info=True)
    
    def _get_market_data(self, ticker: str) -> Optional[Dict]:
        """Get market data for a ticker"""
        try:
            # Fetch single market via API
            response = self.api.make_request('GET', f'/markets/{ticker}')
            return response.get('market')
        except Exception as e:
            self.logger.warning(f"Could not fetch market data for {ticker}: {e}")
            return None
    
    def _generate_volatility_config(self, event: VolatilityEvent, market_data: Dict) -> Optional[Dict]:
        """Generate volatility-specific config"""
        mm_config = self.config.get('volatility_mm', {})
        side = 'yes'  # Default side, could be configurable
        
        # Get regime from event
        regime = event.regime if hasattr(event, 'regime') else None
        
        # Refinement 11: Check regime before spawning session
        # Special case: Allow TRENDING regime when sigma is above threshold (matching scanner logic)
        allowed_regimes = mm_config.get('allowed_regimes_for_sessions', ['MEAN_REVERTING', 'QUIET'])
        scanner_config = self.config.get('volatility_scanner', {})
        sigma_threshold = scanner_config.get('sigma_threshold', 0.0075)
        
        if regime and regime not in allowed_regimes:
            # Allow TRENDING when sigma is above threshold (TRENDING markets with high volatility are tradeable)
            if regime == "TRENDING" and event.sigma >= sigma_threshold:
                self.logger.debug(
                    f"Allowing TRENDING regime for {market_data.get('ticker', 'unknown')} "
                    f"due to high sigma (sigma={event.sigma:.4f} >= {sigma_threshold})"
                )
            elif regime in ["CHAOTIC", "NOISE", "UNKNOWN"]:
                # Always block these regimes regardless of sigma
                self.logger.info(
                    f"Skipping {market_data.get('ticker', 'unknown')}: regime '{regime}' "
                    f"not in allowed list {allowed_regimes}"
                )
                return None
            else:
                # Other disallowed regimes
                self.logger.info(
                    f"Skipping {market_data.get('ticker', 'unknown')}: regime '{regime}' "
                    f"not in allowed list {allowed_regimes}"
                )
                return None
        
        # Get category and map to profile
        category = self.discovery.get_category(market_data)
        category_mapping = self.config.get('category_mapping', {})
        fallback_profile = category_mapping.get('fallback', 'mean_reverting')
        if not category or category not in category_mapping:
            profile_name = fallback_profile
            self.logger.debug(f"Using fallback profile '{fallback_profile}' for category '{category}'")
        else:
            profile_name = category_mapping.get(category, category_mapping.get('default', fallback_profile))
        
        # Get category profile
        category_profiles = self.config.get('category_profiles', {})
        profile = category_profiles.get(profile_name, category_profiles.get('mean_reverting', {}))
        
        # Deep copy profile to avoid mutating original
        import copy
        profile = copy.deepcopy(profile)
        
        # Phase 3: Profile-Regime Overlay
        # Merge regime-specific overrides into profile
        if regime and profile.get('by_regime') and regime in profile['by_regime']:
            regime_overrides = profile['by_regime'][regime]
            # Overlay regime overrides onto profile
            for key, value in regime_overrides.items():
                if key != 'by_regime':  # Don't overwrite the by_regime dict itself
                    profile[key] = value
        
        # Check if excluded after regime merge
        if profile.get('exclude', False):
            self.logger.info(
                f"Skipping {market_data.get('ticker', 'unknown')}: category '{category}' "
                f"profile '{profile_name}' with regime '{regime}' is excluded"
            )
            return None
        
        # Manager-level logging before config generation
        self.logger.info(
            "Generating volatility MM config for %s-%s (category=%s, profile=%s, regime=%s) "
            "with event sigma=%0.6f, signal=%0.3f",
            market_data.get('ticker', 'unknown'),
            side,
            category,
            profile_name,
            regime or 'unknown',
            event.sigma if event else None,
            event.signal_strength if event else None,
        )
        
        # Get base config from dynamic_config (sigma blending happens inside)
        base_config = generate_config_for_market(
            market_data,
            side=side,
            discovery=self.discovery,
            config=self.config,
            n_active_markets=len(self.active_sessions) + 1,
            volatility_event=event,
            category_profile=profile,
            profile_name=profile_name,
            regime=regime
        )
        
        if not base_config:
            return None
        
        # Override with volatility-specific parameters
        # Calculate T based on market close time if available
        if event.close_time:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            # Ensure close_time is timezone-aware for comparison
            close_time = event.close_time
            if close_time.tzinfo is None:
                close_time = close_time.replace(tzinfo=timezone.utc)
            elif close_time.tzinfo != timezone.utc:
                close_time = close_time.astimezone(timezone.utc)
            
            time_to_close = (close_time - now).total_seconds()
            buffer_seconds = self.guardrails.get('expiration_danger_window_seconds', 180)
            
            # Set T to finish before market close (with buffer)
            calculated_ttl = max(60, time_to_close - buffer_seconds)  # At least 1 minute
            
            # Cap at max session_ttl_minutes, but don't exceed time_to_close
            max_ttl = self.session_ttl_minutes * 60
            base_config['market_maker']['T'] = min(calculated_ttl, max_ttl)
            
            self.logger.info(
                f"Session TTL set from market close: {base_config['market_maker']['T']:.1f}s "
                f"(time_to_close={time_to_close:.1f}s, buffer={buffer_seconds}s)"
            )
        else:
            # Fallback to fixed TTL if no close_time available
            base_config['market_maker']['T'] = self.session_ttl_minutes * 60
            self.logger.info(
                f"Session TTL set to fixed value: {base_config['market_maker']['T']:.1f}s "
                f"(no close_time in event)"
            )
        
        base_config['dt'] = mm_config.get('quote_refresh_seconds', 3)
        
        # Cap max_position for volatility sessions (very conservative for first fixed run)
        # Sports drift hard - even 5 contracts can trap you if line moves 10-20 cents
        base_max_position = base_config['market_maker'].get('max_position', 100)
        base_config['market_maker']['max_position'] = min(base_max_position, 3)
        
        # Apply category-specific spread caps from profile (now regime-aware after merge)
        spread_tight = profile.get('spread_tight', 0.02)
        spread_wide = profile.get('spread_wide', 0.04)
        
        # Use dynamic spread based on volatility
        if event.sigma > 0.05:  # High volatility
            max_spread = spread_wide
        else:
            max_spread = spread_tight
        
        # Refinement 14: Apply regime-based position unwinding multipliers
        max_position_factor = profile.get('max_position_factor', 1.0)
        if max_position_factor != 1.0:
            current_max = base_config['market_maker'].get('max_position', 3)
            base_config['market_maker']['max_position'] = max(1, int(current_max * max_position_factor))
            self.logger.info(
                f"Applied regime position factor {max_position_factor}: "
                f"max_position={base_config['market_maker']['max_position']}"
            )
        
        base_config['market_maker']['max_spread'] = max_spread
        self.logger.info(
            f"Applied category spread caps: tight={spread_tight:.3f}, wide={spread_wide:.3f}, "
            f"using={max_spread:.3f} (sigma={event.sigma:.4f})"
        )
        
        # Add guardrails to config
        base_config['guardrails'] = self.guardrails
        base_config['volatility_event'] = event.to_dict()
        base_config['category'] = category
        base_config['profile_name'] = profile_name
        base_config['ticker'] = event.ticker  # For regime lookup
        base_config['scanner'] = self.scanner  # For regime access in MM
        
        return base_config
    
    def _run_volatility_session(
        self,
        config_name: str,
        config: Dict,
        event: VolatilityEvent
    ):
        """Run a volatility MM session"""
        logger = logging.getLogger(f"VolSession_{config_name}")
        
        # Create dedicated log file for this session
        log_dir = Path(self.config.get('logging', {}).get('log_dir', 'logs'))
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log file name: mm_session_{ticker}_{timestamp}.log
        timestamp = int(time.time())
        log_file = log_dir / f"mm_session_{event.ticker}_{timestamp}.log"
        
        # Create file handler for this session
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10485760,  # 10MB
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
        
        # Don't propagate to root logger to avoid duplicate logs in main file
        logger.propagate = False
        
        logger.info(f"Starting volatility session for {event.ticker}")
        logger.info(f"Session log file: {log_file}")
        
        try:
            # Create strategy run in database
            api_config = config['api']
            mm_config = config['market_maker']
            config_params = {
                'gamma': mm_config.get('gamma', 0.1),
                'k': mm_config.get('k', 1.5),
                'sigma': mm_config.get('sigma', 0.5),
                'T': mm_config.get('T', 1200),
                'max_position': mm_config.get('max_position', 100),
                'order_expiration': mm_config.get('order_expiration', 300),
                'min_spread': mm_config.get('min_spread', 0.01),
                'position_limit_buffer': mm_config.get('position_limit_buffer', 0.1),
                'inventory_skew_factor': mm_config.get('inventory_skew_factor', 0.01),
                'dt': config.get('dt', 3.0),
                'extreme_band': mm_config.get('extreme_band', 'normal'),
                'one_sided_quoting': mm_config.get('one_sided_quoting', False),
                'volatility_event': event.to_dict()
            }
            
            run_id = self.analytics_db.create_strategy_run(
                strategy_name=config_name,
                market_ticker=api_config['market_ticker'],
                trade_side=api_config.get('trade_side', 'yes'),
                config_params=config_params
            )
            
            # Update active_sessions with run_id
            with self.lock:
                if event.ticker in self.active_sessions:
                    future, start_time, evt, _ = self.active_sessions[event.ticker]
                    self.active_sessions[event.ticker] = (future, start_time, evt, run_id)
            
            # Log volatility event
            try:
                self.analytics_db.log_volatility_event(event, run_id)
            except Exception as e:
                logger.warning(f"Failed to log volatility event: {e}")
            
            logger.info(f"Created analytics run_id: {run_id}")
            
            # Create API
            api = create_api(config['api'], logger, analytics_db=self.analytics_db, run_id=run_id)
            
            # Create market maker with guardrails
            market_maker = create_market_maker(
                config['market_maker'],
                config['api'],
                api,
                logger,
                analytics_db=self.analytics_db,
                run_id=run_id,
                full_config=self.config,
                ws_client=self.ws_client,  # Phase 4.2
                state_store=self.state_store  # Phase 4.2
            )
            
            # Add guardrail monitoring to market maker
            # We'll need to extend AvellanedaMarketMaker to check guardrails
            # For now, run with shorter T (session TTL)
            
            try:
                # Run market maker
                market_maker.run(config.get('dt', 3.0))
            except KeyboardInterrupt:
                logger.info("Market maker stopped by user")
            except Exception as e:
                logger.error(f"An error occurred: {e}", exc_info=True)
            finally:
                # Cleanup
                api.logout()
                if self.analytics_db and run_id:
                    try:
                        self.analytics_db.end_strategy_run(run_id)
                        # Log session end
                        self.analytics_db.log_volatility_session_end(
                            event.ticker, run_id, "completed"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to end strategy run in analytics: {e}")
        
        except Exception as e:
            logger.error(f"Error in volatility session: {e}", exc_info=True)
        finally:
            # Close file handler
            for handler in logger.handlers[:]:
                if isinstance(handler, logging.handlers.RotatingFileHandler):
                    handler.close()
                    logger.removeHandler(handler)
            
            # Cleanup session
            with self.lock:
                self._cleanup_session(event.ticker, "session_completed")
            
            # Notify scanner that event ended
            if self.scanner:
                self.scanner.mark_event_ended(event.ticker)
    
    def _cleanup_session(self, ticker: str, reason: str):
        """Clean up a session"""
        if ticker in self.active_sessions:
            _, start_time, event, run_id = self.active_sessions.pop(ticker)
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.logger.info(
                f"Session ended for {ticker}: {reason} "
                f"(duration: {duration:.1f}s, active: {len(self.active_sessions)})"
            )
            
            # Log session end if we have run_id
            if run_id and self.analytics_db:
                try:
                    self.analytics_db.log_volatility_session_end(ticker, run_id, reason)
                except Exception as e:
                    self.logger.warning(f"Failed to log session end: {e}")
    
    def get_active_sessions(self) -> Dict[str, Dict]:
        """Get info about active sessions"""
        with self.lock:
            return {
                ticker: {
                    'start_time': start_time.isoformat(),
                    'event': event.to_dict() if hasattr(event, 'to_dict') else str(event),
                    'run_id': run_id
                }
                for ticker, (_, start_time, event, run_id) in self.active_sessions.items()
            }

