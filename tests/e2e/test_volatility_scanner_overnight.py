#!/usr/bin/env python3
"""
Overnight test script for volatility scanner
Run this to collect detailed sweep cycle logs overnight

Usage:
    python test_volatility_scanner_overnight.py
    
    Or run in background:
    nohup python test_volatility_scanner_overnight.py > overnight_console.log 2>&1 &
    
    Or with screen:
    screen -S volatility_test
    python test_volatility_scanner_overnight.py
    # Press Ctrl+A then D to detach

This will:
1. Start the volatility scanner
2. Monitor markets for volatility events
3. Log detailed sweep cycle information to volatility_sweep_details_YYYYMMDD.log
4. Run indefinitely until Ctrl+C
"""

import logging
import os
import signal
import sys
import time
import yaml
from datetime import datetime
from dotenv import load_dotenv
from kalshi_mm.api import KalshiTradingAPI
from kalshi_mm.discovery import MarketDiscovery
from kalshi_mm.volatility import VolatilityScanner, VolatilityEvent, VolatilityEndedEvent

# Load environment variables
load_dotenv()

# Set up logging to both console and file
log_filename = f"volatility_test_overnight_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VolatilityScannerOvernight")

# Track events for summary
events_detected = []
events_ended = []
start_time = datetime.now()


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("SHUTDOWN REQUESTED")
    logger.info("=" * 80)
    logger.info("Stopping scanner...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


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
        logger.info(f"  Price Jump: {event.jump_magnitude:.2f} cents")
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
    logger.info("")


def on_volatility_ended(event: VolatilityEndedEvent):
    """Callback when volatility ends"""
    events_ended.append(event)
    logger.info(f"📉 Volatility ended for {event.ticker}: {event.reason}")


def main():
    """Run volatility scanner overnight"""
    
    logger.info("=" * 80)
    logger.info("VOLATILITY SCANNER - OVERNIGHT TEST")
    logger.info("=" * 80)
    logger.info("")
    logger.info(f"Log file: {log_filename}")
    logger.info(f"Detailed sweep logs: volatility_sweep_details_{datetime.now().strftime('%Y%m%d')}.log")
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
    refresh_interval = scanner_config.get('refresh_interval_seconds', 3)
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
    logger.info("  3. Log detailed sweep cycle information to file")
    logger.info("  4. Run indefinitely until Ctrl+C")
    logger.info("")
    logger.info("Press Ctrl+C to stop")
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
        
        # Wait for initial scan to complete
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
        logger.info("Scanner is now running. Detailed sweep cycle logs will be written to:")
        logger.info(f"  volatility_sweep_details_{datetime.now().strftime('%Y%m%d')}.log")
        logger.info("")
        logger.info("Running indefinitely... (Press Ctrl+C to stop)")
        logger.info("")
        
        # Run indefinitely, logging status every 5 minutes
        last_status_log = time.time()
        status_interval = 300  # 5 minutes
        
        while True:
            time.sleep(60)  # Check every minute
            
            current_time = time.time()
            if current_time - last_status_log >= status_interval:
                elapsed_hours = (current_time - wait_start) / 3600
                with scanner.lock:
                    tracked_count = len(scanner.tracked_tickers)
                    cycle_num = scanner.cycle_number
                
                logger.info("")
                logger.info("=" * 80)
                logger.info(f"STATUS UPDATE - Running for {elapsed_hours:.1f} hours")
                logger.info("=" * 80)
                logger.info(f"  Markets tracked: {tracked_count}")
                logger.info(f"  Cycles completed: {cycle_num}")
                logger.info(f"  Events detected: {len(events_detected)}")
                logger.info(f"  Events ended: {len(events_ended)}")
                logger.info("=" * 80)
                logger.info("")
                
                last_status_log = current_time
        
    except KeyboardInterrupt:
        logger.info("")
        logger.info("⏹️  Test interrupted by user - stopping scanner")
    except Exception as e:
        logger.error(f"✗ Error during test: {e}", exc_info=True)
    finally:
        scanner.stop()
        api.logout()
        
        # Print final summary
        total_runtime = (datetime.now() - start_time).total_seconds() / 3600
        logger.info("")
        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total runtime: {total_runtime:.2f} hours")
        logger.info(f"Total volatility events detected: {len(events_detected)}")
        logger.info(f"Total volatility ended events: {len(events_ended)}")
        logger.info("")
        logger.info(f"Log files:")
        logger.info(f"  Console log: {log_filename}")
        logger.info(f"  Detailed sweep logs: volatility_sweep_details_{datetime.now().strftime('%Y%m%d')}.log")
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

