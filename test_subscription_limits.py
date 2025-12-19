"""
Test script to determine Kalshi WebSocket subscription limits
"""
import os
import sys
import time
import asyncio
import json
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kalshi_mm.api import KalshiTradingAPI, KalshiWebsocketClient
from kalshi_mm.state import MarketStateStore
import yaml
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SubscriptionTest")

def load_config():
    """Load config.yaml"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

async def test_subscription_limits():
    """Test different subscription scenarios to determine limits"""
    load_dotenv()
    config = load_config()
    
    # Create API to get private_key_obj (reuse the loading logic)
    api = KalshiTradingAPI(
        access_key=os.getenv("KALSHI_ACCESS_KEY"),
        private_key=os.getenv("KALSHI_PRIVATE_KEY"),
        market_ticker="DUMMY",  # Not used for WebSocket
        base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
        logger=logger
    )
    
    state_store = MarketStateStore(config=config)
    
    # Create WebSocket client
    ws_client = KalshiWebsocketClient(
        access_key=os.getenv("KALSHI_ACCESS_KEY"),
        private_key_obj=api.private_key_obj,  # Reuse loaded key
        url=config.get('websockets', {}).get('url', 'wss://api.elections.kalshi.com'),
        logger=logger,
        config=config,
        state_store=state_store
    )
    
    ws_client.connect()
    ws_client.start()
    
    # Wait for connection
    logger.info("Waiting for connection...")
    for i in range(30):
        if ws_client._connection_ready:
            break
        await asyncio.sleep(0.1)
    
    if not ws_client._connection_ready:
        logger.error("Failed to connect")
        return
    
    logger.info("Connected. Starting subscription tests...")
    
    # Test 1: Subscribe to global channels first
    logger.info("\n=== TEST 1: Global Subscriptions ===")
    global_subs = []
    
    if config.get('websockets', {}).get('subscribe_user_fills', True):
        sid = ws_client.subscribe_user_fills(lambda msg_type, msg: None)
        global_subs.append(('fills', sid))
        logger.info(f"Subscribed to fills: {sid}")
        await asyncio.sleep(0.5)
    
    if config.get('websockets', {}).get('subscribe_positions', True):
        sid = ws_client.subscribe_positions(lambda msg_type, msg: None)
        global_subs.append(('positions', sid))
        logger.info(f"Subscribed to positions: {sid}")
        await asyncio.sleep(0.5)
    
    if config.get('websockets', {}).get('subscribe_lifecycle', True):
        sid = ws_client.subscribe_lifecycle(lambda msg_type, msg: None)
        global_subs.append(('lifecycle', sid))
        logger.info(f"Subscribed to lifecycle: {sid}")
        await asyncio.sleep(0.5)
    
    # Wait for confirmations
    await asyncio.sleep(2)
    
    with ws_client._lock:
        active_count = len(ws_client._subscriptions)
        pending_count = len(ws_client._pending_subscriptions)
    logger.info(f"After global subs: {active_count} active, {pending_count} pending")
    
    # Test 2: Subscribe to same market, different channels
    logger.info("\n=== TEST 2: Same Market, Different Channels ===")
    test_ticker = "KXNFLGAME-25NOV23TBLA-LA"  # Use a real ticker
    
    market_subs = []
    sid = ws_client.subscribe_ticker(test_ticker, lambda msg_type, msg: None)
    market_subs.append(('ticker', sid))
    logger.info(f"Subscribed to ticker for {test_ticker}: {sid}")
    await asyncio.sleep(0.5)
    
    sid = ws_client.subscribe_orderbook(test_ticker, lambda msg_type, msg: None)
    market_subs.append(('orderbook', sid))
    logger.info(f"Subscribed to orderbook for {test_ticker}: {sid}")
    await asyncio.sleep(0.5)
    
    sid = ws_client.subscribe_trades(test_ticker, lambda msg_type, msg: None)
    market_subs.append(('trades', sid))
    logger.info(f"Subscribed to trades for {test_ticker}: {sid}")
    await asyncio.sleep(0.5)
    
    # Wait for confirmations
    await asyncio.sleep(2)
    
    with ws_client._lock:
        active_count = len(ws_client._subscriptions)
        pending_count = len(ws_client._pending_subscriptions)
    logger.info(f"After same market subs: {active_count} active, {pending_count} pending")
    
    # Test 3: Subscribe to different markets, same channel
    logger.info("\n=== TEST 3: Different Markets, Same Channel (Ticker) ===")
    test_tickers = [
        "KXNFLGAME-25NOV23TBLA-TB",
        "KXNBAGAME-25NOV23SASPHX-PHX",
        "KXNFLSPREAD-25NOV23TBLA-LA7",
        "KXNFLTOTAL-25NOV23TBLA-49",
        "KXNBAGAME-25NOV23BKNTOR-TOR",
    ]
    
    ticker_subs = []
    for ticker in test_tickers:
        sid = ws_client.subscribe_ticker(ticker, lambda msg_type, msg: None)
        ticker_subs.append((ticker, sid))
        logger.info(f"Subscribed to ticker for {ticker}: {sid}")
        await asyncio.sleep(0.5)
    
    # Wait for confirmations
    await asyncio.sleep(3)
    
    with ws_client._lock:
        active_count = len(ws_client._subscriptions)
        pending_count = len(ws_client._pending_subscriptions)
        active_ids = list(ws_client._subscriptions.keys())
        pending_ids = list(ws_client._pending_subscriptions.keys())
    
    logger.info(f"After different market ticker subs: {active_count} active, {pending_count} pending")
    logger.info(f"Active subscription IDs: {active_ids}")
    logger.info(f"Pending subscription IDs: {pending_ids}")
    
    # Test 4: Try subscribing to orderbook for a new market
    logger.info("\n=== TEST 4: Orderbook for New Market ===")
    new_ticker = "KXNBAGAME-25NOV23BKNTOR-BKN"
    sid = ws_client.subscribe_orderbook(new_ticker, lambda msg_type, msg: None)
    logger.info(f"Subscribed to orderbook for {new_ticker}: {sid}")
    await asyncio.sleep(2)
    
    with ws_client._lock:
        active_count = len(ws_client._subscriptions)
        pending_count = len(ws_client._pending_subscriptions)
    
    logger.info(f"After new market orderbook: {active_count} active, {pending_count} pending")
    
    # Final summary
    logger.info("\n=== FINAL SUMMARY ===")
    with ws_client._lock:
        active_subs = {}
        for msg_id, (ticker, _, channel) in ws_client._subscriptions.items():
            key = f"{channel}_{ticker or 'global'}"
            active_subs[key] = msg_id
        
        pending_subs = {}
        for msg_id, (ticker, _, channel) in ws_client._pending_subscriptions.items():
            key = f"{channel}_{ticker or 'global'}"
            pending_subs[key] = msg_id
    
    logger.info(f"ACTIVE SUBSCRIPTIONS ({len(active_subs)}):")
    for key, msg_id in sorted(active_subs.items()):
        logger.info(f"  {msg_id}: {key}")
    
    logger.info(f"\nPENDING SUBSCRIPTIONS ({len(pending_subs)}):")
    for key, msg_id in sorted(pending_subs.items()):
        logger.info(f"  {msg_id}: {key}")
    
    # Analysis
    logger.info("\n=== ANALYSIS ===")
    logger.info(f"Total active: {len(active_subs)}")
    logger.info(f"Total pending: {len(pending_subs)}")
    
    # Count by channel type
    channel_counts = {}
    for key in active_subs.keys():
        channel = key.split('_')[0]
        channel_counts[channel] = channel_counts.get(channel, 0) + 1
    
    logger.info(f"Active subscriptions by channel: {channel_counts}")
    
    # Check if it's a per-channel limit
    max_per_channel = max(channel_counts.values()) if channel_counts else 0
    logger.info(f"Max subscriptions per channel: {max_per_channel}")
    
    ws_client.stop()
    logger.info("Test complete")

if __name__ == "__main__":
    asyncio.run(test_subscription_limits())

