#!/usr/bin/env python3
"""
Diagnostic script to inspect Kalshi API market response structure
Shows all fields, sample values, and helps diagnose filter issues

Usage:
    python test_api_response_structure.py

This will:
1. Fetch 10-20 markets from the API
2. Print complete JSON structure of first 3 markets
3. List all unique field names found
4. Show sample values for key filtering fields
"""

import json
import logging
import os
from collections import defaultdict
from dotenv import load_dotenv
from kalshi_mm.api import KalshiTradingAPI
from kalshi_mm.discovery import MarketDiscovery

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("APIDiagnostic")


def main():
    """Diagnostic test to inspect API response structure"""
    
    logger.info("=" * 80)
    logger.info("KALSHI API RESPONSE STRUCTURE DIAGNOSTIC")
    logger.info("=" * 80)
    logger.info("")
    
    # Get API credentials
    access_key = os.getenv("KALSHI_ACCESS_KEY")
    private_key = os.getenv("KALSHI_PRIVATE_KEY")
    base_url = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    
    if not access_key or not private_key:
        logger.error("Missing KALSHI_ACCESS_KEY or KALSHI_PRIVATE_KEY in environment")
        return
    
    # Initialize API
    try:
        api = KalshiTradingAPI(
            access_key=access_key,
            private_key=private_key,
            market_ticker="dummy",
            base_url=base_url,
            logger=logger
        )
        logger.info("✓ API initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize API: {e}")
        return
    
    # Initialize discovery
    try:
        discovery = MarketDiscovery(api, config={})
        logger.info("✓ Market discovery initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize discovery: {e}")
        return
    
    logger.info("")
    logger.info("Fetching markets from API (limiting to 20 for diagnostics)...")
    logger.info("")
    
    # Fetch markets (limit to 20 for diagnostic purposes)
    # Note: get_all_open_markets doesn't respect limit properly, so we'll fetch in batches and stop early
    try:
        all_markets = []
        cursor = None
        max_markets = 20
        
        while len(all_markets) < max_markets:
            params = {
                'status': 'open',
                'limit': min(1000, max_markets - len(all_markets))  # API max is 1000
            }
            if cursor:
                params['cursor'] = cursor
            
            response = api.make_request('GET', '/markets', params=params)
            markets = response.get('markets', [])
            
            # Only take what we need
            needed = max_markets - len(all_markets)
            all_markets.extend(markets[:needed])
            
            cursor = response.get('cursor')
            if not cursor or len(markets) == 0:
                break
        
        markets = all_markets
        logger.info(f"✓ Fetched {len(markets)} markets")
    except Exception as e:
        logger.error(f"✗ Failed to fetch markets: {e}")
        return
    
    if not markets:
        logger.warning("No markets returned from API")
        return
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("ANALYSIS RESULTS")
    logger.info("=" * 80)
    logger.info("")
    
    # Collect all field names
    all_field_names = set()
    field_value_samples = defaultdict(list)  # field_name -> [sample values]
    status_values = []
    
    for market in markets:
        # Collect all field names
        all_field_names.update(market.keys())
        
        # Collect sample values for each field (limit to 5 samples per field)
        for field_name, value in market.items():
            if len(field_value_samples[field_name]) < 5:
                field_value_samples[field_name].append(value)
        
        # Specifically track status field
        if 'status' in market:
            status_values.append(market['status'])
    
    # Print field names
    logger.info("ALL FIELD NAMES FOUND:")
    logger.info("-" * 80)
    sorted_fields = sorted(all_field_names)
    for field in sorted_fields:
        logger.info(f"  - {field}")
    logger.info("")
    
    # Print status field analysis
    logger.info("STATUS FIELD ANALYSIS:")
    logger.info("-" * 80)
    if status_values:
        unique_statuses = set(status_values)
        logger.info(f"  Status field found: YES")
        logger.info(f"  Unique status values: {unique_statuses}")
        logger.info(f"  Total markets with status field: {len(status_values)}/{len(markets)}")
        for status_val in unique_statuses:
            count = status_values.count(status_val)
            logger.info(f"    '{status_val}': {count} markets")
    else:
        logger.info("  Status field found: NO")
        logger.info("  None of the markets have a 'status' field")
    logger.info("")
    
    # Print sample values for key filtering fields
    key_fields = [
        'status',
        'created_time',
        'volume_24h',
        'yes_bid',
        'yes_ask',
        'yes_bid_normalized',
        'yes_ask_normalized',
        'liquidity',
        'open_interest',
        'close_time',
        'ticker',
        'title'
    ]
    
    logger.info("SAMPLE VALUES FOR KEY FILTERING FIELDS:")
    logger.info("-" * 80)
    for field in key_fields:
        if field in field_value_samples:
            samples = field_value_samples[field][:3]  # Show first 3 samples
            logger.info(f"  {field}:")
            for i, sample in enumerate(samples, 1):
                # Truncate long values
                sample_str = str(sample)
                if len(sample_str) > 100:
                    sample_str = sample_str[:100] + "..."
                logger.info(f"    [{i}] {sample_str}")
        else:
            logger.info(f"  {field}: NOT FOUND")
    logger.info("")
    
    # Print complete structure of first 3 markets
    logger.info("=" * 80)
    logger.info("COMPLETE STRUCTURE OF FIRST 3 MARKETS")
    logger.info("=" * 80)
    logger.info("")
    
    for i, market in enumerate(markets[:3], 1):
        logger.info(f"MARKET #{i} (Ticker: {market.get('ticker', 'N/A')}):")
        logger.info("-" * 80)
        # Pretty print JSON
        market_json = json.dumps(market, indent=2, default=str)
        logger.info(market_json)
        logger.info("")
    
    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total markets analyzed: {len(markets)}")
    logger.info(f"Total unique fields found: {len(all_field_names)}")
    logger.info(f"Status field present: {'YES' if status_values else 'NO'}")
    if status_values:
        logger.info(f"Status values: {set(status_values)}")
    logger.info("")
    logger.info("Check the output above to understand the API response structure.")
    logger.info("Use this information to fix the filter in volatility_scanner.py")


if __name__ == "__main__":
    main()

