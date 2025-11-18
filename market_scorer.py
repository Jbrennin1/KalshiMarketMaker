import logging
from typing import List, Dict, Optional
from market_discovery import MarketDiscovery


class MarketScorer:
    """Scores markets for market-making attractiveness using normalized features"""
    
    def __init__(self, discovery: MarketDiscovery, config: Optional[Dict] = None):
        self.discovery = discovery
        self.logger = logging.getLogger("MarketScorer")
        self.config = config or {}
        
        # Log configuration at startup
        self._log_configuration()
    
    def _log_configuration(self):
        """Log all thresholds and weights at startup"""
        scoring_config = self.config.get('scoring', {})
        weights = scoring_config.get('weights', {})
        volume_bands = scoring_config.get('volume_bands', {})
        
        self.logger.info("=" * 80)
        self.logger.info("Market Scorer Configuration")
        self.logger.info("=" * 80)
        self.logger.info(f"Scoring weights: {weights}")
        self.logger.info(f"Volume bands: {volume_bands}")
        self.logger.info(f"Min volume 24h: {scoring_config.get('min_volume_24h', 1000.0)}")
        self.logger.info("=" * 80)
    
    def normalize_feature(self, value: float, all_values: List[float]) -> float:
        """
        Normalize a feature to [0, 1] using min-max scaling over all candidate markets
        Handles edge cases (all same values, empty list)
        """
        if not all_values or len(all_values) == 0:
            return 0.0
        
        min_val = min(all_values)
        max_val = max(all_values)
        
        if max_val == min_val:
            return 0.5  # All values are the same, return middle
        
        normalized = (value - min_val) / (max_val - min_val)
        return max(0.0, min(1.0, normalized))  # Clamp to [0, 1]
    
    def get_volume_label(self, normalized_volume: float) -> str:
        """Get volume label based on normalized value"""
        volume_bands = self.config.get('scoring', {}).get('volume_bands', {})
        very_low = volume_bands.get('very_low', 0.25)
        low = volume_bands.get('low', 0.5)
        medium = volume_bands.get('medium', 0.75)
        
        if normalized_volume < very_low:
            return "Very low"
        elif normalized_volume < low:
            return "Low"
        elif normalized_volume < medium:
            return "Medium"
        else:
            return "High"
    
    def calculate_time_penalty(self, time_to_expiry_days: Optional[float]) -> float:
        """
        Calculate time-to-expiry penalty (higher for closer expiry)
        Returns normalized penalty [0, 1] where 1 = very close expiry
        """
        if time_to_expiry_days is None:
            return 0.5  # Unknown, use middle value
        
        # Penalty increases as expiry approaches
        # Markets expiring in < 1 day get max penalty (1.0)
        # Markets expiring in > 60 days get min penalty (0.0)
        if time_to_expiry_days < 1:
            return 1.0
        elif time_to_expiry_days > 60:
            return 0.0
        else:
            # Linear interpolation: 1 day = 1.0, 60 days = 0.0
            return 1.0 - ((time_to_expiry_days - 1) / 59)
    
    def score_markets_batch(self, markets: List[Dict]) -> List[Dict]:
        """
        Score a batch of markets using normalized features
        Must score all together to compute percentiles correctly
        """
        if not markets:
            return []
        
        # Extract all feature values for normalization
        volumes_24h = []
        recent_volumes_1h = []
        book_liquidities = []
        spreads = []
        time_penalties = []
        
        for market in markets:
            volume_24h = market.get('volume_24h', 0) / 100  # Convert cents to dollars
            volumes_24h.append(volume_24h)
            
            recent_vol_1h = self.discovery.get_recent_volume_1h(market)
            recent_volumes_1h.append(recent_vol_1h)
            
            book_liq = self.discovery.get_book_depth(market)
            book_liquidities.append(book_liq)
            
            spread = self.discovery.calculate_spread(market)
            spreads.append(spread)
            
            time_to_expiry = self.discovery.get_time_to_expiry_days(market)
            time_penalty = self.calculate_time_penalty(time_to_expiry)
            time_penalties.append(time_penalty)
        
        # Get weights from config
        scoring_config = self.config.get('scoring', {})
        weights = scoring_config.get('weights', {})
        w1 = weights.get('volume_24h', 0.30)
        w2 = weights.get('recent_volume_1h', 0.25)
        w3 = weights.get('book_liquidity', 0.20)
        w4 = abs(weights.get('spread', -0.15))  # Make positive for normalization
        w5 = abs(weights.get('time_penalty', -0.10))  # Make positive for normalization
        
        # Score each market
        scored_markets = []
        for i, market in enumerate(markets):
            # Normalize features
            norm_v24h = self.normalize_feature(volumes_24h[i], volumes_24h)
            norm_recent_v1h = self.normalize_feature(recent_volumes_1h[i], recent_volumes_1h)
            norm_book_liq = self.normalize_feature(book_liquidities[i], book_liquidities)
            norm_spread = self.normalize_feature(spreads[i], spreads)
            norm_time_penalty = time_penalties[i]  # Already normalized [0,1]
            
            # Calculate weighted score (explicit formula)
            score = (
                w1 * norm_v24h
                + w2 * norm_recent_v1h
                + w3 * norm_book_liq
                - w4 * norm_spread  # Negative because tighter is better
                - w5 * norm_time_penalty  # Negative because closer expiry is riskier
            )
            
            # Build reasons using same normalized features
            reasons = []
            vol_label = self.get_volume_label(norm_v24h)
            reasons.append(f"{vol_label} volume: ${volumes_24h[i]:,.0f}")
            
            if norm_spread < 0.25:
                reasons.append(f"Tight spread: ${spreads[i]:.3f}")
            elif norm_spread < 0.5:
                reasons.append(f"Medium spread: ${spreads[i]:.3f}")
            else:
                reasons.append(f"Wide spread: ${spreads[i]:.3f}")
            
            yes_mid = self.discovery.get_mid_price(market, 'yes')
            reasons.append(f"Mid price: ${yes_mid:.2f}")
            
            if norm_book_liq > 0.75:
                reasons.append(f"High book liquidity: ${book_liquidities[i]:,.0f}")
            elif norm_book_liq > 0.5:
                reasons.append(f"Medium book liquidity: ${book_liquidities[i]:,.0f}")
            else:
                reasons.append(f"Low book liquidity: ${book_liquidities[i]:,.0f}")
            
            scored_markets.append({
                'ticker': market.get('ticker'),
                'title': market.get('title', 'N/A'),
                'score': score,
                'volume_24h': volumes_24h[i],
                'spread': spreads[i],
                'mid_price': yes_mid,
                'liquidity': book_liquidities[i],
                'open_interest': market.get('open_interest', 0),
                'reasons': reasons,
                'market_data': market  # Keep full market data for later use
            })
        
        return scored_markets
    
    def score_market(self, market: Dict, all_markets: Optional[List[Dict]] = None) -> Dict:
        """
        Score a single market (for backward compatibility)
        If all_markets provided, uses them for normalization; otherwise returns unnormalized score
        """
        if all_markets:
            scored = self.score_markets_batch([market])
            return scored[0] if scored else {}
        
        # Fallback: return basic score without normalization
        volume_24h = market.get('volume_24h', 0) / 100
        spread = self.discovery.calculate_spread(market)
        yes_mid = self.discovery.get_mid_price(market, 'yes')
        book_liq = self.discovery.get_book_depth(market)
        
        return {
            'ticker': market.get('ticker'),
            'title': market.get('title', 'N/A'),
            'score': 0.0,  # Unnormalized
            'volume_24h': volume_24h,
            'spread': spread,
            'mid_price': yes_mid,
            'liquidity': book_liq,
            'open_interest': market.get('open_interest', 0),
            'reasons': [],
            'market_data': market
        }
    
    def filter_and_sort_markets(self, markets: List[Dict], min_volume_24h: Optional[float] = None, max_markets: int = 20) -> List[Dict]:
        """
        Score and sort markets by profitability for market making using normalized features
        
        Args:
            markets: List of market dictionaries
            min_volume_24h: Minimum 24h volume in dollars (default: from config)
            max_markets: Maximum number of markets to return (default: 20)
        """
        self.logger.info(f"Scoring {len(markets)} markets for profitability...")
        
        # Get min volume from config if not provided
        if min_volume_24h is None:
            scoring_config = self.config.get('scoring', {})
            min_volume_24h = scoring_config.get('min_volume_24h', 1000.0)
        
        # Filter by minimum volume first
        filtered = []
        for market in markets:
            try:
                volume_24h = market.get('volume_24h', 0) / 100  # Convert cents to dollars
                if volume_24h >= min_volume_24h:
                    filtered.append(market)
            except Exception as e:
                self.logger.warning(f"Error filtering market {market.get('ticker')}: {e}")
        
        self.logger.info(f"Filtered to {len(filtered)} markets with volume >= ${min_volume_24h:,.0f}")
        
        if not filtered:
            return []
        
        # Score all markets together (needed for normalization)
        try:
            scored = self.score_markets_batch(filtered)
        except Exception as e:
            self.logger.error(f"Error scoring markets: {e}")
            return []
        
        # Sort by score (highest first)
        sorted_markets = sorted(scored, key=lambda x: x['score'], reverse=True)
        
        self.logger.info(f"Top {min(max_markets, len(sorted_markets))} markets selected")
        
        # Return top N
        return sorted_markets[:max_markets]

