#!/usr/bin/env python3
"""
Test script for market discovery and scoring system
Run this to see what markets would be selected before integrating into main bot
"""

import logging
import os
import yaml
from dotenv import load_dotenv
from mm import KalshiTradingAPI
from market_discovery import MarketDiscovery
from market_scorer import MarketScorer
from dynamic_config import generate_config_for_market

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MarketDiscoveryTest")

def main():
    """Test market discovery and scoring"""
    
    # Load config
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("config.yaml not found, using defaults")
        config = {}
    
    # Initialize API (we need a dummy ticker for initialization)
    api = KalshiTradingAPI(
        access_key=os.getenv("KALSHI_ACCESS_KEY"),
        private_key=os.getenv("KALSHI_PRIVATE_KEY"),
        market_ticker="dummy",  # Not used for discovery
        base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
        logger=logger
    )
    
    # Initialize discovery and scorer with config
    discovery = MarketDiscovery(api, config=config)
    scorer = MarketScorer(discovery, config=config)
    
    print("=" * 80)
    print("MARKET DISCOVERY AND SCORING TEST")
    print("=" * 80)
    print()
    
    # Step 1: Discover markets
    print("Step 1: Discovering all open markets...")
    print("-" * 80)
    markets = discovery.get_all_open_markets(limit=1000)
    print(f"Found {len(markets)} open markets")
    print()
    
    # Step 2: Filter to binary markets only
    print("Step 2: Filtering to binary markets...")
    print("-" * 80)
    binary_markets = discovery.filter_binary_markets(markets)
    print(f"Found {len(binary_markets)} binary markets")
    print()
    
    # Step 2.5: Pre-filter markets (time-to-expiry, category, recent activity)
    print("Step 2.5: Pre-filtering markets (time-to-expiry, category, recent activity)...")
    print("-" * 80)
    pre_filtered = discovery.pre_filter_markets(binary_markets)
    print(f"Pre-filtered to {len(pre_filtered)} markets")
    print()
    
    # Step 3: Score and sort by profitability
    print("Step 3: Scoring markets for profitability (normalized features)...")
    print("-" * 80)
    top_markets = scorer.filter_and_sort_markets(
        pre_filtered,
        min_volume_24h=None,  # Use config default
        max_markets=20  # Top 20 markets
    )
    print(f"Found {len(top_markets)} most profitable markets")
    print()
    
    # Step 4: Display results
    print("=" * 80)
    print("TOP MARKETS FOR MARKET MAKING")
    print("=" * 80)
    print()
    
    for i, market in enumerate(top_markets, 1):
        print(f"{i}. {market['ticker']}")
        print(f"   Title: {market.get('title', 'N/A')}")
        print(f"   Profitability Score: {market['score']:.4f} (normalized)")
        print(f"   Volume (24h): ${market['volume_24h']:,.2f}")
        print(f"   Spread: ${market['spread']:.3f}")
        print(f"   Mid Price: ${market['mid_price']:.2f}")
        print(f"   Liquidity: ${market['liquidity']:,.2f}")
        print(f"   Open Interest: {market['open_interest']:,}")
        print(f"   Key Factors: {', '.join(market['reasons'][:3])}")
        print()
        
        # Show generated config
        print(f"   Generated Config (YES side):")
        gen_config = generate_config_for_market(market, 'yes', discovery=discovery, config=config, n_active_markets=len(top_markets))
        if gen_config:
            mm_config = gen_config['market_maker']
            print(f"     - max_position: {mm_config['max_position']} (band: {mm_config.get('extreme_band', 'normal')})")
            print(f"     - sigma: {mm_config['sigma']:.4f}")
            print(f"     - min_spread: {mm_config['min_spread']:.3f}")
            print(f"     - inventory_skew_factor: {mm_config['inventory_skew_factor']:.6f}")
            print(f"     - k: {mm_config['k']}")
            print(f"     - one_sided_quoting: {mm_config.get('one_sided_quoting', False)}")
            print(f"     - dt: {gen_config['dt']}")
        else:
            print(f"     - SKIPPED (extreme price)")
        print()
        print("-" * 80)
        print()
    
    # Step 5: Summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    if top_markets:
        avg_score = sum(m['score'] for m in top_markets) / len(top_markets)
        avg_volume = sum(m['volume_24h'] for m in top_markets) / len(top_markets)
        avg_spread = sum(m['spread'] for m in top_markets) / len(top_markets)
        
        print(f"Total markets discovered: {len(markets)}")
        print(f"Binary markets: {len(binary_markets)}")
        print(f"Pre-filtered markets: {len(pre_filtered)}")
        print(f"Top profitable markets: {len(top_markets)}")
        print(f"Average profitability score: {avg_score:.4f} (normalized)")
        print(f"Average 24h volume: ${avg_volume:,.2f}")
        print(f"Average spread: ${avg_spread:.3f}")
    else:
        print("No markets found!")
    
    print()
    print("=" * 80)
    print("Test complete! Review the results above.")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

