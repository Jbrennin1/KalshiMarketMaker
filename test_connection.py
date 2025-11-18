import logging
import os
from dotenv import load_dotenv
from mm import KalshiTradingAPI

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ConnectionTest")

def test_connection():
    """Test Kalshi API connection"""
    try:
        # Get credentials from environment
        access_key = os.getenv("KALSHI_ACCESS_KEY")
        private_key = os.getenv("KALSHI_PRIVATE_KEY")
        base_url = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
        
        if not access_key or not private_key:
            logger.error("Missing KALSHI_ACCESS_KEY or KALSHI_PRIVATE_KEY in .env file")
            return False
        
        logger.info("Testing Kalshi API connection...")
        logger.info(f"Base URL: {base_url}")
        logger.info(f"Access Key: {access_key[:10]}...")  # Show first 10 chars only
        
        # Use a test market ticker - you can change this to any valid market
        # For testing, we'll use one from your config or a simple one
        test_ticker = "RATECUTCOUNT-24DEC31-T4"  # Change to a valid active market
        
        logger.info(f"Creating API instance for market: {test_ticker}")
        api = KalshiTradingAPI(
            access_key=access_key,
            private_key=private_key,
            market_ticker=test_ticker,
            base_url=base_url,
            logger=logger
        )
        
        logger.info("✓ API instance created successfully")
        
        # Test 1: Get market price (public endpoint, but tests authentication)
        logger.info("\nTest 1: Getting market price...")
        prices = api.get_price()
        logger.info(f"✓ Successfully retrieved prices: {prices}")
        
        # Test 2: Get position (authenticated endpoint)
        logger.info("\nTest 2: Getting portfolio position...")
        position = api.get_position()
        logger.info(f"✓ Successfully retrieved position: {position}")
        
        # Test 3: Get orders (authenticated endpoint)
        logger.info("\nTest 3: Getting orders...")
        orders = api.get_orders()
        logger.info(f"✓ Successfully retrieved {len(orders)} orders")
        
        logger.info("\n" + "="*50)
        logger.info("✓ All tests passed! Connection is working.")
        logger.info("="*50)
        
        return True
        
    except Exception as e:
        logger.error(f"\n✗ Connection test failed: {e}")
        logger.exception("Full error details:")
        return False

if __name__ == "__main__":
    success = test_connection()
    exit(0 if success else 1)

