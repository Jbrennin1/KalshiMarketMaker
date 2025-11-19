#!/usr/bin/env python3
"""
Close All Positions Script
Closes out all open positions at market price

Usage:
    python close_all_positions.py

This will:
1. Get all unsettled positions from your portfolio
2. For each position, place market orders to close it out
3. Uses market bid (for long positions) or ask (for short positions)
4. Shows progress and summary

WARNING: This will execute real trades. Use with caution.
"""

import logging
import os
import time
from dotenv import load_dotenv
from mm import KalshiTradingAPI

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ClosePositions")

def get_all_positions(api: KalshiTradingAPI):
    """Get all unsettled positions from portfolio"""
    logger.info("Fetching all positions...")
    path = "/portfolio/positions"
    params = {"settlement_status": "unsettled"}  # Get all unsettled positions
    response = api.make_request("GET", path, params=params)
    positions = response.get("market_positions", [])
    logger.info(f"Found {len(positions)} positions")
    return positions

def get_market_price(api: KalshiTradingAPI, ticker: str):
    """Get current market prices for a ticker"""
    try:
        path = f"/markets/{ticker}"
        data = api.make_request("GET", path)
        market = data.get("market", {})
        
        yes_bid = float(market.get("yes_bid", 0)) / 100
        yes_ask = float(market.get("yes_ask", 0)) / 100
        no_bid = float(market.get("no_bid", 0)) / 100
        no_ask = float(market.get("no_ask", 0)) / 100
        
        return {
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask
        }
    except Exception as e:
        logger.error(f"Error getting market price for {ticker}: {e}")
        return None

def close_position(api: KalshiTradingAPI, ticker: str, position: int, side: str):
    """Close a position by placing market order"""
    if position == 0:
        return None
    
    # Get market prices
    prices = get_market_price(api, ticker)
    if not prices:
        logger.error(f"Could not get prices for {ticker}, skipping")
        return None
    
    # Determine action and price
    if position > 0:
        # Long position - need to sell
        action = "sell"
        if side == "yes":
            price = prices["yes_bid"]  # Sell at bid
        else:
            price = prices["no_bid"]  # Sell at bid
        quantity = abs(position)
    else:
        # Short position - need to buy
        action = "buy"
        if side == "yes":
            price = prices["yes_ask"]  # Buy at ask
        else:
            price = prices["no_ask"]  # Buy at ask
        quantity = abs(position)
    
    # Check if price is valid
    if price <= 0 or price > 1.0:
        logger.error(f"Invalid price {price} for {ticker}, skipping")
        return None
    
    logger.info(f"Closing {ticker} {side}: {action} {quantity} @ ${price:.2f}")
    
    try:
        # Temporarily set market_ticker for this order
        original_ticker = api.market_ticker
        api.market_ticker = ticker
        
        # Place order
        order_id = api.place_order(action, side, price, quantity)
        
        # Restore original ticker
        api.market_ticker = original_ticker
        
        logger.info(f"✓ Order placed: {order_id}")
        return order_id
    
    except Exception as e:
        logger.error(f"✗ Failed to place order for {ticker}: {e}")
        # Restore original ticker
        api.market_ticker = original_ticker
        return None

def main():
    """Main function to close all positions"""
    
    logger.info("=" * 80)
    logger.info("CLOSE ALL POSITIONS SCRIPT")
    logger.info("=" * 80)
    logger.info("")
    
    # Check for required environment variables
    access_key = os.getenv("KALSHI_ACCESS_KEY")
    private_key = os.getenv("KALSHI_PRIVATE_KEY")
    
    if not access_key or not private_key:
        logger.error("✗ Missing KALSHI_ACCESS_KEY or KALSHI_PRIVATE_KEY in .env file")
        return
    
    logger.info("✓ Environment variables loaded")
    logger.info("")
    
    # Initialize API
    try:
        api = KalshiTradingAPI(
            access_key=access_key,
            private_key=private_key,
            market_ticker="dummy",  # Will be changed per position
            base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
            logger=logger
        )
        logger.info("✓ API initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize API: {e}")
        return
    
    logger.info("")
    logger.info("⚠️  WARNING: This will execute real trades to close all positions!")
    logger.info("")
    
    # Get confirmation
    try:
        response = input("Type 'YES' to continue, or anything else to cancel: ")
        if response != "YES":
            logger.info("Cancelled by user")
            return
    except KeyboardInterrupt:
        logger.info("Cancelled by user")
        return
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("FETCHING POSITIONS")
    logger.info("=" * 80)
    
    try:
        # Get all positions
        positions = get_all_positions(api)
        
        if not positions:
            logger.info("✓ No open positions found. Nothing to close.")
            return
        
        logger.info("")
        logger.info(f"Found {len(positions)} positions to close:")
        for pos in positions:
            ticker = pos.get("ticker", "unknown")
            position = pos.get("position", 0)
            side = pos.get("side", "yes")
            logger.info(f"  - {ticker} ({side}): {position:+d}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("CLOSING POSITIONS")
        logger.info("=" * 80)
        logger.info("")
        
        # Close each position
        closed = []
        failed = []
        
        for pos in positions:
            ticker = pos.get("ticker", "unknown")
            position = pos.get("position", 0)
            side = pos.get("side", "yes")
            
            if position == 0:
                logger.info(f"Skipping {ticker} ({side}): position is 0")
                continue
            
            logger.info(f"Processing {ticker} ({side}): {position:+d}")
            
            order_id = close_position(api, ticker, position, side)
            
            if order_id:
                closed.append({
                    "ticker": ticker,
                    "side": side,
                    "position": position,
                    "order_id": order_id
                })
            else:
                failed.append({
                    "ticker": ticker,
                    "side": side,
                    "position": position
                })
            
            # Small delay between orders to avoid rate limits
            time.sleep(0.5)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total positions: {len(positions)}")
        logger.info(f"Successfully closed: {len(closed)}")
        logger.info(f"Failed: {len(failed)}")
        logger.info("")
        
        if closed:
            logger.info("Closed positions:")
            for item in closed:
                logger.info(f"  ✓ {item['ticker']} ({item['side']}): {item['position']:+d} - Order: {item['order_id']}")
        
        if failed:
            logger.info("Failed positions:")
            for item in failed:
                logger.info(f"  ✗ {item['ticker']} ({item['side']}): {item['position']:+d}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("Done! Check your portfolio to verify positions are closed.")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)
    finally:
        api.logout()
        logger.info("✓ API logged out")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nCancelled by user")
        exit(1)
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        exit(1)

