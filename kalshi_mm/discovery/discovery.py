import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from kalshi_mm.api import KalshiTradingAPI
from kalshi_mm.utils import get_price_field, parse_kalshi_timestamp


class MarketDiscovery:
    """Discovers and filters markets from Kalshi API"""
    
    def __init__(self, api: KalshiTradingAPI, config: Optional[Dict] = None):
        self.api = api
        self.logger = logging.getLogger("MarketDiscovery")
        self.config = config or {}
        self.sigma_cache = {}  # Cache for sigma estimates: {(ticker, bucket): (sigma, timestamp)}
    
    def get_all_open_markets(self, limit: int = 1000) -> List[Dict]:
        """Fetch all open/active markets from Kalshi API"""
        all_markets = []
        cursor = None
        
        self.logger.info("Fetching all open markets from Kalshi...")
        
        while True:
            params = {
                'status': 'open',  # Only open markets
                'limit': min(limit, 1000)  # API max is 1000
            }
            if cursor:
                params['cursor'] = cursor
            
            try:
                response = self.api.make_request('GET', '/markets', params=params)
                markets = response.get('markets', [])
                all_markets.extend(markets)
                
                self.logger.info(f"Fetched {len(markets)} markets (total: {len(all_markets)})")
                
                cursor = response.get('cursor')
                if not cursor or len(markets) < limit:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching markets: {e}")
                break
        
        self.logger.info(f"Total markets found: {len(all_markets)}")
        return all_markets
    
    def calculate_spread(self, market: Dict) -> float:
        """Calculate spread for a market in dollars"""
        try:
            yes_spread = (market.get('yes_ask', 0) - market.get('yes_bid', 0)) / 100
            no_spread = (market.get('no_ask', 0) - market.get('no_bid', 0)) / 100
            return min(yes_spread, no_spread)  # Use tighter spread
        except:
            return 999.0  # Invalid spread
    
    def get_mid_price(self, market: Dict, side: str = 'yes') -> float:
        """Get mid price for a market side in dollars"""
        try:
            if side == 'yes':
                mid = (market.get('yes_bid', 0) + market.get('yes_ask', 0)) / 200
            else:
                mid = (market.get('no_bid', 0) + market.get('no_ask', 0)) / 200
            return mid
        except:
            return 0.0
    
    def filter_binary_markets(self, markets: List[Dict]) -> List[Dict]:
        """Filter to only binary markets (exclude scalar/combo markets)"""
        binary = []
        market_types = {'binary': 0, 'scalar': 0, 'combo': 0, 'unknown': 0}
        sample_non_binary = []
        
        for m in markets:
            mtype = m.get('market_type', 'unknown')
            market_types[mtype] = market_types.get(mtype, 0) + 1
            if mtype == 'binary':
                binary.append(m)
            elif len(sample_non_binary) < 5:
                sample_non_binary.append(m)
        
        non_binary_count = len(markets) - len(binary)
        self.logger.info(
            f"Market type breakdown: binary={market_types['binary']}, "
            f"scalar={market_types['scalar']}, combo={market_types['combo']}, "
            f"unknown={market_types['unknown']}"
        )
        self.logger.info(
            f"Filtered to {len(binary)} binary markets "
            f"(removed {non_binary_count} non-binary, {non_binary_count/len(markets)*100:.1f}%)"
        )
        
        if sample_non_binary:
            self.logger.debug("Sample non-binary markets:")
            for m in sample_non_binary:
                self.logger.debug(f"  - {m.get('ticker', 'unknown')}: type={m.get('market_type', 'unknown')}")
        
        return binary
    
    def get_expiry_iso_string(self, market: Dict) -> Optional[str]:
        """
        Get expiry as ISO string with fallback chain:
        expected_expiration_time > expiration_time > latest_expiration_time > close_time
        """
        return (
            market.get("expected_expiration_time")
            or market.get("expiration_time")
            or market.get("latest_expiration_time")
            or market.get("close_time")
        )
    
    def get_time_to_expiry_days(self, market: Dict) -> Optional[float]:
        """Calculate time to expiry in days from market data"""
        try:
            # First try ISO string fields (most common in Kalshi API)
            expiry_str = self.get_expiry_iso_string(market)
            if expiry_str:
                # Parse ISO format: '2025-12-03T00:00:00Z' or '2025-12-03T00:00:00+00:00'
                try:
                    # Handle 'Z' timezone indicator
                    if expiry_str.endswith('Z'):
                        expiry_str = expiry_str[:-1] + '+00:00'
                    expiry_dt = datetime.fromisoformat(expiry_str)
                    # Get current time (naive or aware depending on expiry_dt)
                    if expiry_dt.tzinfo:
                        now = datetime.now(timezone.utc)
                        # Convert expiry_dt to UTC if needed
                        if expiry_dt.tzinfo != timezone.utc:
                            expiry_dt = expiry_dt.astimezone(timezone.utc)
                    else:
                        now = datetime.now()
                    delta = expiry_dt - now
                    days = delta.total_seconds() / 86400
                    return days
                except ValueError as ve:
                    self.logger.debug(f"Failed to parse ISO string '{expiry_str}': {ve}")
                    # Fall through to timestamp fields
            
            # Fallback to timestamp fields (if API uses them)
            expiry_ts = market.get('expiration_ts') or market.get('expires_at') or market.get('settlement_ts')
            if expiry_ts:
                # Handle both seconds and milliseconds timestamps
                if expiry_ts > 1e10:  # Likely milliseconds
                    expiry_ts = expiry_ts / 1000
                expiry_dt = datetime.fromtimestamp(expiry_ts)
                now = datetime.now()
                delta = expiry_dt - now
                days = delta.total_seconds() / 86400
                return days
            
            return None
        except Exception as e:
            ticker = market.get('ticker', 'unknown')
            self.logger.debug(f"Error calculating time to expiry for {ticker}: {e}")
            return None
    
    def get_category(self, market: Dict) -> str:
        """Extract category from market data"""
        # Try different possible fields
        category = market.get('category', '') or market.get('subtitle', '') or market.get('series_ticker', '')
        if not category:
            # Try to infer from ticker
            ticker = market.get('ticker', '')
            if 'BTC' in ticker or 'CRYPTO' in ticker:
                return 'crypto'
            elif 'FED' in ticker or 'RATE' in ticker:
                return 'rates'
            elif any(sport in ticker for sport in [
                'NBA', 'NFL', 'MLB', 'NHL', 
                'EPL', 'EPLGAME', 
                'NCAAMBGAME', 'NCAA',
                'SERIEAGAME',  # Serie A soccer
                'LALIGAGAME',  # La Liga soccer
                'SB-',  # Super Bowl (pattern: KXSB-26-...)
            ]):
                return 'sports'
        return category.lower() if category else 'default'
    
    def get_category_profile(self, market: Dict, config: Optional[Dict] = None) -> Tuple[str, Dict]:
        """
        Get category profile for a market.
        
        Returns:
            Tuple of (profile_name, profile_dict)
        """
        if config is None:
            config = self.config or {}
        
        category = self.get_category(market)
        category_mapping = config.get('category_mapping', {})
        profile_name = category_mapping.get(category, category_mapping.get('default', 'mean_reverting'))
        
        category_profiles = config.get('category_profiles', {})
        profile = category_profiles.get(profile_name, category_profiles.get('mean_reverting', {}))
        
        return profile_name, profile
    
    def pre_filter_markets(self, markets: List[Dict]) -> List[Dict]:
        """
        Apply hard filters before scoring:
        - Time-to-expiry filter (category-specific)
        - Category exclusion list
        - Recent activity filter (softer for scoring)
        """
        discovery_config = self.config.get('discovery', {})
        time_windows = discovery_config.get('time_windows', {})
        excluded_categories = discovery_config.get('excluded_categories', [])
        recent_activity = discovery_config.get('recent_activity', {}).get('scoring', {})
        
        # Log discovery config at start
        self.logger.info("=" * 80)
        self.logger.info("Discovery Configuration")
        self.logger.info("=" * 80)
        self.logger.info(f"Expiry fallback order: expected_expiration_time > expiration_time > latest_expiration_time > close_time")
        default_window = time_windows.get('default', {})
        self.logger.info(f"Time window (default): {default_window.get('min_days', 1)}-{default_window.get('max_days', 60)} days")
        scoring_config = self.config.get('scoring', {})
        self.logger.info(f"Volume min 24h: ${scoring_config.get('min_volume_24h', 1000.0):,.0f}")
        min_trades = recent_activity.get('min_trades_last_minutes', 0)
        self.logger.info(f"Recent activity: min_trades_last_minutes={min_trades} (disabled if 0)")
        
        # Log risk config
        risk_config = self.config.get('risk', {})
        self.logger.info("Risk Configuration:")
        self.logger.info(f"  alpha: {risk_config.get('alpha', 0.1)}")
        self.logger.info(f"  per_market_cap: ${risk_config.get('per_market_cap', 2000):,.0f}")
        self.logger.info(f"  global_risk_budget: ${risk_config.get('global_risk_budget', 10000):,.0f}")
        self.logger.info(f"  contract_value: $1")
        self.logger.info("=" * 80)
        
        filtered = []
        skipped_reasons = {'no_expiry': 0, 'category_excluded': 0, 'time_window': 0, 'low_activity': 0, 'expiry_error': 0, 'extreme_price': 0}
        
        for market in markets:
            # Check category exclusion
            category = self.get_category(market)
            if category in excluded_categories:
                skipped_reasons['category_excluded'] += 1
                continue
            
            # Check extreme price bands - skip soft_extreme and hard_extreme markets
            try:
                # Phase 1.2: Use get_price_field() utility
                yes_bid = get_price_field(market, 'yes_bid') or 0.0
                yes_ask = get_price_field(market, 'yes_ask') or 0.0
                no_bid = get_price_field(market, 'no_bid') or 0.0
                no_ask = get_price_field(market, 'no_ask') or 0.0
                
                # Use yes side mid price (or no side if yes not available)
                if yes_bid > 0 and yes_ask > 0:
                    mid_price = (yes_bid + yes_ask) / 2
                elif no_bid > 0 and no_ask > 0:
                    mid_price = (no_bid + no_ask) / 2
                else:
                    # Can't determine price, skip this check
                    mid_price = None
                
                if mid_price is not None:
                    # Local import to avoid circular dependency
                    from kalshi_mm.config import get_extreme_price_band
                    extreme_band = get_extreme_price_band(mid_price, self.config)
                    if extreme_band in ['soft_extreme', 'hard_extreme']:
                        skipped_reasons['extreme_price'] += 1
                        continue
            except Exception as e:
                # If we can't check extreme price, log but continue (don't skip)
                self.logger.debug(f"Error checking extreme price for {market.get('ticker', 'unknown')}: {e}")
            
            # Check time-to-expiry (make it optional - if we can't determine, allow it)
            time_to_expiry = self.get_time_to_expiry_days(market)
            if time_to_expiry is None:
                # If we can't determine expiry, still allow if volume is decent
                volume_24h = get_price_field(market, 'volume_24h') or 0.0
                if volume_24h < 1000:  # Require at least $1k volume if no expiry info
                    skipped_reasons['no_expiry'] += 1
                    continue
                # Allow markets without expiry info if they have volume
                filtered.append(market)
                continue
            
            # Get time window for this category
            window = time_windows.get(category, time_windows.get('default', {}))
            min_days = window.get('min_days', 1)
            min_hours = window.get('min_hours', 0)
            max_days = window.get('max_days', 60)
            
            min_days_total = min_days + (min_hours / 24)
            if time_to_expiry < min_days_total or time_to_expiry > max_days:
                skipped_reasons['time_window'] += 1
                continue
            
            # Check recent activity (softer for scoring)
            # Phase 1.2: Use get_price_field() utility
            volume_24h = get_price_field(market, 'volume_24h') or 0.0
            min_volume_for_bypass = recent_activity.get('min_volume_24h_for_bypass', 10000)
            
            # If 24h volume is high enough, allow even with no recent trades
            if volume_24h >= min_volume_for_bypass:
                filtered.append(market)
                continue
            
            # Otherwise, check last trade timestamp if available
            # Phase 1.1: Use parse_kalshi_timestamp() utility
            last_trade_ts = market.get('last_trade_ts')
            if last_trade_ts:
                last_trade_dt = parse_kalshi_timestamp(last_trade_ts)
                if last_trade_dt:
                    if last_trade_dt.tzinfo is None:
                        last_trade_dt = last_trade_dt.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    minutes_since_trade = (now - last_trade_dt).total_seconds() / 60
                else:
                    minutes_since_trade = None
                min_trades_minutes = recent_activity.get('min_trades_last_minutes', 0)
                if minutes_since_trade <= min_trades_minutes:
                    filtered.append(market)
                    continue
            
            # If no last_trade_ts, allow if volume is reasonable
            if volume_24h >= 1000:  # At least $1k volume
                filtered.append(market)
            else:
                skipped_reasons['low_activity'] += 1
        
        self.logger.info(f"Pre-filtered from {len(markets)} to {len(filtered)} markets")
        self.logger.info(f"Skipped reasons: {skipped_reasons}")
        return filtered
    
    def get_book_depth(self, market: Dict, levels: int = 3) -> float:
        """
        Calculate book depth: sum of size at best bid/ask and next N levels
        Returns depth in dollars
        """
        try:
            # For now, use liquidity as proxy for book depth
            # In a full implementation, we'd fetch order book and sum sizes
            liquidity = market.get('liquidity', 0) / 100  # Convert cents to dollars
            
            # If we have order book data, use it
            # Otherwise, use liquidity as approximation
            return liquidity
        except Exception as e:
            self.logger.debug(f"Error calculating book depth: {e}")
            return 0.0
    
    def get_recent_volume_1h(self, market: Dict) -> float:
        """
        Get recent volume in last hour (in dollars)
        Uses 24h volume as proxy if API doesn't provide hourly breakdown
        """
        # For now, approximate as 24h_volume / 24
        # In a full implementation, we'd track recent trades
        volume_24h = market.get('volume_24h', 0) / 100  # Convert cents to dollars
        return volume_24h / 24.0
    
    def get_recent_trade_activity(self, market: Dict, minutes: int = 15) -> bool:
        """
        Check if market has recent trade activity
        Returns True if last trade was within specified minutes, or if 24h volume is high
        """
        last_trade_ts = market.get('last_trade_ts')
        if last_trade_ts:
            last_trade_dt = datetime.fromtimestamp(last_trade_ts)
            minutes_since_trade = (datetime.now() - last_trade_dt).total_seconds() / 60
            return minutes_since_trade <= minutes
        
        # If no last_trade_ts, use volume as proxy
        volume_24h = market.get('volume_24h', 0) / 100
        return volume_24h >= 5000  # Assume active if > $5k volume
    
    def estimate_sigma(self, market: Dict, mid_price: float) -> float:
        """
        Estimate realized volatility (sigma) for a market
        Starts with simple proxy, can be upgraded to use historical data
        """
        discovery_config = self.config.get('discovery', {})
        sigma_config = self.config.get('as_model', {}).get('sigma', {})
        baseline = sigma_config.get('baseline', 0.05)
        use_proxy = sigma_config.get('use_proxy', True)
        
        if use_proxy:
            # Simple proxy: variance of price for a binary is highest around 0.5
            # sigma = sigma0 * (1 + 2 * (p*(1-p)))
            p = mid_price
            sigma = baseline * (1 + 2 * (p * (1 - p)))
            return sigma
        
        # TODO: Use analytics database or poll for historical data
        # For now, fall back to proxy
        p = mid_price
        sigma = baseline * (1 + 2 * (p * (1 - p)))
        return sigma

