import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
import yaml
from dotenv import load_dotenv
import os
from typing import Dict

from mm import KalshiTradingAPI, AvellanedaMarketMaker
from analytics import AnalyticsDB

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def create_api(api_config, logger, analytics_db=None, run_id=None):
    return KalshiTradingAPI(
        access_key=os.getenv("KALSHI_ACCESS_KEY"),
        private_key=os.getenv("KALSHI_PRIVATE_KEY"),
        market_ticker=api_config['market_ticker'],
        base_url=os.getenv("KALSHI_BASE_URL"),
        logger=logger,
        analytics_db=analytics_db,
        run_id=run_id,
    )

def create_market_maker(mm_config, api_config, api, logger, analytics_db=None, run_id=None):
    return AvellanedaMarketMaker(
        logger=logger,
        api=api,
        gamma=mm_config.get('gamma', 0.1),
        k=mm_config.get('k', 1.5),
        sigma=mm_config.get('sigma', 0.5),
        T=mm_config.get('T', 3600),
        max_position=mm_config.get('max_position', 100),
        order_expiration=mm_config.get('order_expiration', 300),
        min_spread=mm_config.get('min_spread', 0.01),
        position_limit_buffer=mm_config.get('position_limit_buffer', 0.1),
        inventory_skew_factor=mm_config.get('inventory_skew_factor', 0.01),
        trade_side=api_config.get('trade_side', 'yes'),
        analytics_db=analytics_db,
        run_id=run_id,
    )

def run_strategy(config_name: str, config: Dict):
    # Create a logger for this specific strategy
    logger = logging.getLogger(f"Strategy_{config_name}")
    logger.setLevel(config.get('log_level', 'INFO'))

    # Create file handler
    fh = logging.FileHandler(f"{config_name}.log")
    fh.setLevel(config.get('log_level', 'INFO'))
    
    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(config.get('log_level', 'INFO'))
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Starting strategy: {config_name}")

    # Initialize analytics database
    analytics_db = AnalyticsDB(db_path="trading_analytics.db", logger=logger)
    
    # Create strategy run in database
    api_config = config['api']
    mm_config = config['market_maker']
    config_params = {
        'gamma': mm_config.get('gamma', 0.1),
        'k': mm_config.get('k', 1.5),
        'sigma': mm_config.get('sigma', 0.5),
        'T': mm_config.get('T', 3600),
        'max_position': mm_config.get('max_position', 100),
        'order_expiration': mm_config.get('order_expiration', 300),
        'min_spread': mm_config.get('min_spread', 0.01),
        'position_limit_buffer': mm_config.get('position_limit_buffer', 0.1),
        'inventory_skew_factor': mm_config.get('inventory_skew_factor', 0.01),
        'dt': config.get('dt', 1.0),
    }
    
    run_id = analytics_db.create_strategy_run(
        strategy_name=config_name,
        market_ticker=api_config['market_ticker'],
        trade_side=api_config.get('trade_side', 'yes'),
        config_params=config_params
    )
    
    logger.info(f"Created analytics run_id: {run_id}")

    # Create API
    api = create_api(config['api'], logger, analytics_db=analytics_db, run_id=run_id)

    # Create market maker
    market_maker = create_market_maker(
        config['market_maker'], 
        config['api'], 
        api, 
        logger,
        analytics_db=analytics_db,
        run_id=run_id
    )

    try:
        # Run market maker
        market_maker.run(config.get('dt', 1.0))
    except KeyboardInterrupt:
        logger.info("Market maker stopped by user")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Ensure logout happens even if an exception occurs
        api.logout()
        # End strategy run in analytics
        if analytics_db and run_id:
            try:
                analytics_db.end_strategy_run(run_id)
            except Exception as e:
                logger.warning(f"Failed to end strategy run in analytics: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi Market Making Algorithm")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    # Load all configurations
    configs = load_config(args.config)

    # Load environment variables
    load_dotenv()

    # Print the name of every strategy being run
    print("Starting the following strategies:")
    for config_name in configs:
        print(f"- {config_name}")

    # Run all strategies in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(configs)) as executor:
        for config_name, config in configs.items():
            executor.submit(run_strategy, config_name, config)