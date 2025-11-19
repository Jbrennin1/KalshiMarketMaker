"""
Volatility MM Manager - Spawns, tracks, and kills short-lived MM sessions
"""
import logging
import time
import threading
from typing import Dict, Optional, Callable
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, Future

from mm import KalshiTradingAPI, AvellanedaMarketMaker
from analytics import AnalyticsDB
from market_discovery import MarketDiscovery
from dynamic_config import generate_config_for_market
from volatility_models import VolatilityEvent, VolatilityEndedEvent
from runner import create_api, create_market_maker, run_strategy


class VolatilityMMManager:
    """Manages ephemeral MM sessions spawned from volatility events"""
    
    def __init__(
        self,
        api: KalshiTradingAPI,
        discovery: MarketDiscovery,
        analytics_db: AnalyticsDB,
        config: Dict,
        scanner: Optional[object] = None  # VolatilityScanner instance
    ):
        self.api = api
        self.discovery = discovery
        self.analytics_db = analytics_db
        self.config = config
        self.scanner = scanner
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
                self.logger.warning(f"Could not generate config for {ticker}, skipping")
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
        
        # Get base config from dynamic_config
        base_config = generate_config_for_market(
            market_data,
            side='yes',  # Default to yes side, could be configurable
            discovery=self.discovery,
            config=self.config,
            n_active_markets=len(self.active_sessions) + 1
        )
        
        if not base_config:
            return None
        
        # Override with volatility-specific parameters
        base_config['market_maker']['T'] = self.session_ttl_minutes * 60  # Convert to seconds
        base_config['dt'] = mm_config.get('quote_refresh_seconds', 3)
        
        # Apply spread caps
        spread_cap_tight = mm_config.get('spread_cap_tight', 0.03)
        spread_cap_wide = mm_config.get('spread_cap_wide', 0.05)
        # Use dynamic spread based on volatility
        if event.sigma > 0.05:  # High volatility
            max_spread = spread_cap_wide
        else:
            max_spread = spread_cap_tight
        
        base_config['market_maker']['max_spread'] = max_spread
        
        # Add guardrails to config
        base_config['guardrails'] = self.guardrails
        base_config['volatility_event'] = event.to_dict()
        
        return base_config
    
    def _run_volatility_session(
        self,
        config_name: str,
        config: Dict,
        event: VolatilityEvent
    ):
        """Run a volatility MM session"""
        logger = logging.getLogger(f"VolSession_{config_name}")
        logger.info(f"Starting volatility session for {event.ticker}")
        
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
                full_config=self.config
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

