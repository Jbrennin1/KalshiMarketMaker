"""
Test config generation for markets that passed volatility screening
Uses mock market data for closed markets
"""
import yaml
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from kalshi_mm.api import KalshiTradingAPI
from kalshi_mm.discovery import MarketDiscovery
from kalshi_mm.volatility.models import VolatilityEvent
from kalshi_mm.volatility import VolatilityMMManager
from kalshi_mm.analytics import AnalyticsDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestVolatilityConfigs")

# Markets that passed all filters (from your logs)
PASSED_MARKETS = [
    {
        'ticker': 'KXNBAGAME-25NOV18PHXPOR-PHX',
        'timestamp': '2025-11-19T05:16:36.637625+00:00',
        'sigma': 0.052724,
        'volume_multiplier': 15.94,
        'volume_delta': 1782.6,
        'estimated_trades': 594.2,
        'jump_magnitude': None,
        'mid_price': 0.5800,
        'spread': 0.0200,
        'volume_24h_dollars': 50000,  # Estimated - will be converted to cents
    },
    {
        'ticker': 'KXNBAGAME-25NOV18PHXPOR-POR',
        'timestamp': '2025-11-19T05:16:50.361562+00:00',
        'sigma': 0.111722,
        'volume_multiplier': 16.20,
        'volume_delta': 2182.7,
        'estimated_trades': 727.6,
        'jump_magnitude': None,
        'mid_price': 0.4200,
        'spread': 0.0200,
        'volume_24h_dollars': 60000,  # Estimated
    },
    {
        'ticker': 'KXNBAGAME-25NOV19WASMIN-WAS',
        'timestamp': '2025-11-19T13:46:19.989593+00:00',
        'sigma': 0.020797,
        'volume_multiplier': 157.90,
        'volume_delta': 115.4,
        'estimated_trades': 38.5,
        'jump_magnitude': None,
        'mid_price': 0.0900,
        'spread': 0.0200,
        'volume_24h_dollars': 30000,  # Estimated
    },
    {
        'ticker': 'KXNBAGAME-25NOV19SACOKC-SAC',
        'timestamp': '2025-11-19T16:46:25.595679+00:00',
        'sigma': 0.027196,
        'volume_multiplier': 30.70,
        'volume_delta': 101.1,
        'estimated_trades': 33.7,
        'jump_magnitude': None,
        'mid_price': 0.0700,
        'spread': 0.0200,
        'volume_24h_dollars': 25000,  # Estimated
    },
    {
        'ticker': 'KXNBAGAME-25NOV19SACOKC-SAC',
        'timestamp': '2025-11-19T17:01:10.978339+00:00',
        'sigma': 0.039669,
        'volume_multiplier': 4.95,
        'volume_delta': 41.7,
        'estimated_trades': 13.9,
        'jump_magnitude': None,
        'mid_price': 0.0700,
        'spread': 0.0200,
        'volume_24h_dollars': 25000,  # Estimated
    },
]


def create_mock_market_data(market_data: Dict) -> Dict:
    """Create mock market data for closed markets (API format: prices in cents)"""
    mid_price = market_data['mid_price']
    spread = market_data['spread']
    
    # Calculate bid/ask from mid and spread (convert to cents for API format)
    half_spread = spread / 2
    yes_bid = int((mid_price - half_spread) * 100)  # Convert to cents
    yes_ask = int((mid_price + half_spread) * 100)
    no_bid = int((1 - mid_price - half_spread) * 100)
    no_ask = int((1 - mid_price + half_spread) * 100)
    
    # volume_24h should be in CENTS (API format)
    volume_24h_cents = int(market_data.get('volume_24h_dollars', 50000) * 100)
    
    # Estimate liquidity from volume (rough approximation)
    # Higher volume = higher liquidity
    estimated_liquidity = max(100, volume_24h_cents / 100)  # At least $100 liquidity
    
    return {
        'ticker': market_data['ticker'],
        'status': 'closed',  # Mark as closed for testing
        'yes_bid': yes_bid,
        'yes_ask': yes_ask,
        'no_bid': no_bid,
        'no_ask': no_ask,
        'volume_24h': volume_24h_cents,  # API returns in cents
        'open_interest': 1000,
        'liquidity': int(estimated_liquidity * 100),  # In cents for API format
        'subtitle': f"Mock market data for {market_data['ticker']}",
        'title': f"Test Market - {market_data['ticker']}",
        'category': 'sports',
        'event_ticker': market_data['ticker'].split('-')[0] if '-' in market_data['ticker'] else None,
    }


def create_volatility_event(market_data: Dict) -> VolatilityEvent:
    """Create a VolatilityEvent from log data"""
    # Calculate signal strength (simplified)
    signal_strength = min(1.0, (market_data['sigma'] / 0.1) * 0.5 + 
                          (market_data['volume_multiplier'] / 10) * 0.5)
    
    timestamp = datetime.fromisoformat(market_data['timestamp'].replace('Z', '+00:00'))
    
    return VolatilityEvent(
        ticker=market_data['ticker'],
        timestamp=timestamp,
        jump_magnitude=market_data.get('jump_magnitude'),
        sigma=market_data['sigma'],
        volume_multiplier=market_data['volume_multiplier'],
        volume_delta=market_data['volume_delta'],
        estimated_trades=market_data['estimated_trades'],
        volume_velocity=None,
        close_time=None,  # Would need to fetch from market data
        direction=None,
        signal_strength=signal_strength
    )


def test_config_generation():
    """Test config generation for passed markets using mock data"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Import config generation function directly
    from kalshi_mm.config import generate_config_for_market
    
    logger.info("=" * 80)
    logger.info("TESTING VOLATILITY CONFIG GENERATION")
    logger.info("Mode: Using MOCK data (markets from logs)")
    logger.info("=" * 80)
    logger.info("")
    
    for market_data in PASSED_MARKETS:
        ticker = market_data['ticker']
        logger.info(f"\n{'='*80}")
        logger.info(f"Testing: {ticker}")
        logger.info(f"{'='*80}")
        
        try:
            # Create mock market data
            market = create_mock_market_data(market_data)
            
            # Create volatility event
            event = create_volatility_event(market_data)
            
            logger.info(f"  Event details:")
            logger.info(f"    Sigma: {event.sigma:.6f}")
            logger.info(f"    Volume Multiplier: {event.volume_multiplier:.2f}x")
            logger.info(f"    Volume Delta: {event.volume_delta:.1f}")
            logger.info(f"    Estimated Trades: {event.estimated_trades:.1f}")
            logger.info(f"    Signal Strength: {event.signal_strength:.3f}")
            
            # Check if market price is in extreme band (will be rejected)
            from kalshi_mm.config import get_extreme_price_band
            mid_price = market_data['mid_price']
            extreme_band = get_extreme_price_band(mid_price, config)
            if extreme_band in ['soft_extreme', 'hard_extreme']:
                logger.warning(f"  ⚠ Market price {mid_price:.4f} is in {extreme_band} band - skipping")
                logger.warning(f"     (Normal: 0.15-0.85, Soft: 0.05-0.15/0.85-0.95, Hard: 0.0-0.05/0.95-1.0)")
                continue
            
            # Generate base config (discovery=None uses fallback sizing, sigma blending happens inside)
            logger.info(f"  Generating config...")
            base_config = generate_config_for_market(
                market,
                side='yes',
                discovery=None,  # None = use fallback volume-based sizing
                config=config,
                n_active_markets=1,
                volatility_event=event
            )
            
            if not base_config:
                logger.warning(f"  ✗ Base config generation failed (unknown reason)")
                continue
            
            # Apply volatility-specific overrides (same as manager does)
            mm_config = config.get('volatility_mm', {})
            base_config['market_maker']['T'] = mm_config.get('session_ttl_minutes', 20) * 60
            base_config['dt'] = mm_config.get('quote_refresh_seconds', 3)
            
            # Apply spread caps based on volatility
            spread_cap_tight = mm_config.get('spread_cap_tight', 0.03)
            spread_cap_wide = mm_config.get('spread_cap_wide', 0.05)
            if event.sigma > 0.05:
                max_spread = spread_cap_wide
            else:
                max_spread = spread_cap_tight
            base_config['market_maker']['max_spread'] = max_spread
            
            # Add guardrails and event info
            base_config['guardrails'] = mm_config.get('guardrails', {})
            base_config['volatility_event'] = event.to_dict()
            
            vol_config = base_config
            
            if not vol_config:
                logger.warning(f"  ✗ Config generation failed")
                continue
            
            # Print config summary
            logger.info(f"  ✓ Config generated successfully!")
            logger.info(f"  Config summary:")
            logger.info(f"    Market: {vol_config['api']['market_ticker']}")
            logger.info(f"    Side: {vol_config['api'].get('trade_side', 'yes')}")
            logger.info(f"    T (session duration): {vol_config['market_maker']['T']}s ({vol_config['market_maker']['T']/60:.1f} min)")
            logger.info(f"    dt (refresh rate): {vol_config.get('dt', 3.0)}s")
            logger.info(f"    Max Spread: {vol_config['market_maker']['max_spread']:.4f}")
            logger.info(f"    Max Position: {vol_config['market_maker']['max_position']}")
            logger.info(f"    Gamma: {vol_config['market_maker']['gamma']:.4f}")
            logger.info(f"    K: {vol_config['market_maker']['k']:.4f}")
            logger.info(f"    Sigma: {vol_config['market_maker']['sigma']:.4f}")
            logger.info(f"    Extreme Band: {vol_config['market_maker'].get('extreme_band', 'normal')}")
            
            # Validate config
            logger.info(f"  Validating config...")
            issues = []
            
            if vol_config['market_maker']['T'] < 60:
                issues.append("Session TTL too short (< 1 min)")
            if vol_config['market_maker']['max_spread'] > 0.10:
                issues.append("Max spread too wide (> 10 cents)")
            if vol_config['market_maker']['max_position'] <= 0:
                issues.append("Max position is zero or negative")
            
            if issues:
                logger.warning(f"  ⚠ Config validation issues:")
                for issue in issues:
                    logger.warning(f"    - {issue}")
            else:
                logger.info(f"  ✓ Config validation passed")
            
            # Save config to file
            config_filename = f"vol_config_{ticker.replace('-', '_')}.json"
            with open(config_filename, 'w') as f:
                json.dump(vol_config, f, indent=2, default=str)
            logger.info(f"  Config saved to: {config_filename}")
            
        except Exception as e:
            logger.error(f"  ✗ Error testing {ticker}: {e}", exc_info=True)
    
    logger.info(f"\n{'='*80}")
    logger.info("TEST COMPLETE")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    test_config_generation()

