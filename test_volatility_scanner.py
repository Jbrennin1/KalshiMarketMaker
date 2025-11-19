#!/usr/bin/env python3
"""
Test script for volatility scanner
Run this to see volatility events detected (read-only, no trading)

Usage:
    python test_volatility_scanner.py

This will:
1. Start the volatility scanner
2. Monitor markets for volatility events
3. Print detected events (no trading occurs)
4. Run for 5 minutes by default (or until Ctrl+C)
"""

import logging
import os
import time
import yaml
from dotenv import load_dotenv
from mm import KalshiTradingAPI
from market_discovery import MarketDiscovery
from volatility_scanner import VolatilityScanner
from volatility_models import VolatilityEvent, VolatilityEndedEvent

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("VolatilityScannerTest")

# Track events for summary
events_detected = []
events_ended = []


def on_volatility_event(event: VolatilityEvent):
    """Callback when volatility event is detected"""
    events_detected.append(event)
    
    logger.info("=" * 80)
    logger.info(f"🔥 VOLATILITY EVENT DETECTED: {event.ticker}")
    logger.info("=" * 80)
    logger.info(f"  Timestamp: {event.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"  Signal Strength: {event.signal_strength:.3f} (0-1 scale)")
    logger.info(f"  Sigma (volatility): {event.sigma:.4f}")
    
    if event.jump_magnitude:
        logger.info(f"  Price Jump: {event.jump_magnitude:.2f} cents (in {event.timestamp})")
    else:
        logger.info(f"  Price Jump: None (sigma-based trigger)")
    
    logger.info(f"  Volume Multiplier: {event.volume_multiplier:.2f}x baseline")
    logger.info(f"  Volume Delta: {event.volume_delta:.1f} contracts")
    logger.info(f"  Estimated Trades: {event.estimated_trades:.1f}")
    
    if event.volume_velocity:
        logger.info(f"  Volume Velocity: {event.volume_velocity:.1f} contracts/scan")
    
    if event.direction:
        logger.info(f"  Direction: {event.direction}")
    
    if event.close_time:
        logger.info(f"  Close Time: {event.close_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    logger.info("=" * 80)
    logger.info("  ⚠️  In live mode, this would spawn a volatility MM session")
    logger.info("=" * 80)
    logger.info("")


def on_volatility_ended(event: VolatilityEndedEvent):
    """Callback when volatility ends"""
    events_ended.append(event)
    logger.info(f"📉 Volatility ended for {event.ticker}: {event.reason}")


def main():
    """Test volatility scanner"""
    
    logger.info("=" * 80)
    logger.info("VOLATILITY SCANNER TEST")
    logger.info("=" * 80)
    logger.info("")
    
    # Load config
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        logger.info("✓ Config loaded from config.yaml")
    except FileNotFoundError:
        logger.error("✗ config.yaml not found")
        return
    
    # Check for required environment variables
    access_key = os.getenv("KALSHI_ACCESS_KEY")
    private_key = os.getenv("KALSHI_PRIVATE_KEY")
    
    if not access_key or not private_key:
        logger.error("✗ Missing KALSHI_ACCESS_KEY or KALSHI_PRIVATE_KEY in .env file")
        logger.error("   Please create a .env file with your Kalshi credentials")
        return
    
    logger.info("✓ Environment variables loaded")
    logger.info("")
    
    # Get scanner config
    scanner_config = config.get('volatility_scanner', {})
    refresh_interval = scanner_config.get('refresh_interval_seconds', 45)
    logger.info(f"Scanner Configuration:")
    logger.info(f"  Refresh Interval: {refresh_interval} seconds")
    logger.info(f"  Rolling Window: {scanner_config.get('rolling_window_minutes', 15)} minutes")
    logger.info(f"  Volatility Window: {scanner_config.get('volatility_window_minutes', 10)} minutes")
    logger.info(f"  Price Jump Threshold: {scanner_config.get('price_jump_threshold_cents', 12)} cents")
    logger.info(f"  Sigma Threshold: {scanner_config.get('sigma_threshold', 0.02)}")
    logger.info(f"  Volume Burst Multiplier: {scanner_config.get('volume_burst_multiplier', 2.5)}x")
    logger.info(f"  Min Absolute Volume: {scanner_config.get('min_absolute_volume_per_5m', 30)} contracts")
    logger.info(f"  Min Estimated Trades: {scanner_config.get('min_estimated_trades', 3)}")
    logger.info("")
    
    # Initialize API
    try:
        api = KalshiTradingAPI(
            access_key=access_key,
            private_key=private_key,
            market_ticker="dummy",
            base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
            logger=logger
        )
        logger.info("✓ API initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize API: {e}")
        return
    
    # Initialize discovery
    try:
        discovery = MarketDiscovery(api, config=config)
        logger.info("✓ Market discovery initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize discovery: {e}")
        return
    
    # Create scanner with callbacks
    try:
        scanner = VolatilityScanner(
            api=api,
            discovery=discovery,
            config=config,
            event_callback=on_volatility_event,
            ended_callback=on_volatility_ended
        )
        logger.info("✓ Volatility scanner initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize scanner: {e}")
        return
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("STARTING SCANNER (READ-ONLY MODE)")
    logger.info("=" * 80)
    logger.info("")
    logger.info("The scanner will:")
    logger.info("  1. Monitor top 200 most active markets (round-robin)")
    logger.info("  2. Detect volatility events (high vol + high volume)")
    logger.info("  3. Print detected events (NO TRADING WILL OCCUR)")
    logger.info("  4. Refresh top 200 every 10 minutes to catch new hot markets")
    logger.info("")
    logger.info("Test will run for 5 minutes (or press Ctrl+C to stop early)")
    logger.info("")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Start scanner
        scanner.start()
        logger.info("🚀 Scanner started!")
        logger.info("")
        
        # Give thread a moment to start
        time.sleep(0.5)
        
        logger.info("Waiting for initial scan to complete...")
        logger.info("")
        
        # Wait for initial scan to complete (check every second)
        max_wait_time = 300  # 5 minutes max wait
        wait_start = time.time()
        while not scanner.initial_scan_complete:
            time.sleep(1)
            elapsed = time.time() - wait_start
            if elapsed > max_wait_time:
                logger.error("Initial scan timed out after 5 minutes")
                return
            
            # Show progress every 10 seconds
            if int(elapsed) % 10 == 0 and elapsed > 0:
                logger.info(f"   Still scanning... ({int(elapsed)}s elapsed)")
        
        # Get tracked count after initial scan
        with scanner.lock:
            tracked_count = len(scanner.tracked_tickers)
        logger.info(f"✓ Initial scan complete! Tracking {tracked_count} markets")
        logger.info("")
        
        # Run for 5 minutes (or until interrupted)
        # Calculate iterations: 5 minutes = 300 seconds, divided by refresh interval
        test_duration_seconds = 300
        iterations = int(test_duration_seconds / refresh_interval) + 1
        
        logger.info(f"Starting incremental sweeps: {iterations} sweeps over ~{test_duration_seconds // 60} minutes")
        logger.info("")
        
        for i in range(iterations):
            time.sleep(refresh_interval)
            
            # Show tracked markets count
            with scanner.lock:
                tracked_count = len(scanner.tracked_tickers)
            
            logger.info(f"⏱️  Sweep {i+1}/{iterations} | Tracking: {tracked_count} markets | Events: {len(events_detected)}")
        
        logger.info("")
        logger.info("⏹️  Test duration complete - stopping scanner")
        
    except KeyboardInterrupt:
        logger.info("")
        logger.info("⏹️  Test interrupted by user - stopping scanner")
    except Exception as e:
        logger.error(f"✗ Error during test: {e}", exc_info=True)
    finally:
        scanner.stop()
        api.logout()
        logger.info("✓ Scanner stopped, API logged out")
    
    # Print summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total volatility events detected: {len(events_detected)}")
    logger.info(f"Total volatility ended events: {len(events_ended)}")
    logger.info("")
    
    if events_detected:
        logger.info("Detected Events:")
        for i, event in enumerate(events_detected, 1):
            logger.info(f"  {i}. {event.ticker} - Signal: {event.signal_strength:.3f}, "
                       f"Vol: {event.volume_multiplier:.2f}x, "
                       f"Sigma: {event.sigma:.4f}")
    else:
        logger.info("No volatility events detected during test period.")
        logger.info("")
        logger.info("This could mean:")
        logger.info("  - Markets are currently calm (low volatility)")
        logger.info("  - Volume thresholds are too high")
        logger.info("  - Volatility thresholds are too high")
        logger.info("  - Try adjusting thresholds in config.yaml if needed")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("Test complete!")
    logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        exit(1)

