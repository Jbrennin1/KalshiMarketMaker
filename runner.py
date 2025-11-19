import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import yaml
import json
from dotenv import load_dotenv
import os
from typing import Dict, List, Optional

from mm import KalshiTradingAPI, AvellanedaMarketMaker
from analytics import AnalyticsDB
from market_discovery import MarketDiscovery
from market_scorer import MarketScorer
from dynamic_config import generate_config_for_market

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

def create_market_maker(mm_config, api_config, api, logger, analytics_db=None, run_id=None, full_config=None):
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
        config=full_config,  # Pass full config for throttling settings
        extreme_band=mm_config.get('extreme_band', 'normal'),
        one_sided_quoting=mm_config.get('one_sided_quoting', False),
    )

def run_strategy(config_name: str, config: Dict, full_config: Dict = None):
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
    
    # Log model version if available
    if full_config and 'discovery' in full_config:
        model_version = full_config['discovery'].get('model_version', 'unknown')
        logger.info(f"Model version: {model_version}")

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
        'extreme_band': mm_config.get('extreme_band', 'normal'),
        'one_sided_quoting': mm_config.get('one_sided_quoting', False),
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
        run_id=run_id,
        full_config=full_config
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

def discover_and_generate_configs(config: Dict, analytics_db: AnalyticsDB) -> List[Dict]:
    """
    Run market discovery, scoring, and config generation.
    Logs everything to database and returns list of generated configs.
    """
    logger = logging.getLogger("Discovery")
    logger.info("=" * 80)
    logger.info("Starting Market Discovery and Config Generation")
    logger.info("=" * 80)
    
    try:
        # Get discovery settings
        discovery_config = config.get('discovery', {})
        max_markets = discovery_config.get('max_markets_to_trade', 20)
        sides_to_trade = discovery_config.get('sides_to_trade', ['yes'])
        model_version = discovery_config.get('model_version', 'AS-v1.0')
        
        # Initialize API for discovery
        api = KalshiTradingAPI(
            access_key=os.getenv("KALSHI_ACCESS_KEY"),
            private_key=os.getenv("KALSHI_PRIVATE_KEY"),
            market_ticker="dummy",  # Not used for discovery
            base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
            logger=logger
        )
        
        # Initialize discovery and scorer
        discovery = MarketDiscovery(api, config=config)
        scorer = MarketScorer(discovery, config=config)
        
        # Run discovery pipeline
        logger.info("Step 1: Discovering all open markets...")
        all_markets = discovery.get_all_open_markets(limit=1000)
        logger.info(f"Found {len(all_markets)} open markets")
        
        logger.info("Step 2: Filtering to binary markets...")
        binary_markets = discovery.filter_binary_markets(all_markets)
        logger.info(f"Found {len(binary_markets)} binary markets")
        
        logger.info("Step 3: Pre-filtering markets...")
        pre_filtered = discovery.pre_filter_markets(binary_markets)
        logger.info(f"Pre-filtered to {len(pre_filtered)} markets")
        
        logger.info("Step 4: Scoring markets...")
        top_markets = scorer.filter_and_sort_markets(pre_filtered, max_markets=max_markets)
        logger.info(f"Selected top {len(top_markets)} markets")
        
        # Create discovery session in database
        session_id = analytics_db.create_discovery_session(
            total_markets=len(all_markets),
            binary_markets=len(binary_markets),
            pre_filtered=len(pre_filtered),
            scored=len(pre_filtered),  # All pre-filtered markets are scored
            top_markets_count=len(top_markets),
            configs_generated=0,  # Will update after generation
            model_version=model_version
        )
        logger.info(f"Created discovery session {session_id}")
        
        # Log all pre-filtered markets to database (even if not selected)
        logger.info("Logging all pre-filtered markets to database...")
        for market in pre_filtered:
            try:
                analytics_db.log_discovered_market(
                    session_id=session_id,
                    ticker=market.get('ticker', 'unknown'),
                    title=market.get('title'),
                    score=market.get('score'),
                    volume_24h=market.get('volume_24h'),
                    spread=market.get('spread'),
                    mid_price=market.get('mid_price'),
                    liquidity=market.get('liquidity'),
                    open_interest=market.get('open_interest'),
                    reasons=market.get('reasons'),
                    generated_config=None,
                    selected_for_trading=False,
                    trade_side=None
                )
            except Exception as e:
                logger.warning(f"Error logging market {market.get('ticker', 'unknown')}: {e}")
        
        # Generate configs for top markets
        logger.info("Step 5: Generating configs for top markets...")
        generated_configs = []
        total_configs = 0
        
        for market in top_markets:
            for side in sides_to_trade:
                try:
                    gen_config = generate_config_for_market(
                        market, side, discovery=discovery,
                        config=config, n_active_markets=len(top_markets) * len(sides_to_trade)
                    )
                    
                    if gen_config:
                        # Update existing log entry to mark as selected
                        ticker = market.get('ticker', 'unknown')
                        with analytics_db.get_connection() as conn:
                            conn.execute("""
                                UPDATE discovered_markets
                                SET selected_for_trading = 1,
                                    trade_side = ?,
                                    generated_config = ?
                                WHERE session_id = ? AND ticker = ?
                            """, (
                                side,
                                json.dumps(gen_config),
                                session_id,
                                ticker
                            ))
                            conn.commit()
                        
                        # Create config name
                        config_name = f"{ticker}-{side.upper()}"
                        generated_configs.append((config_name, gen_config))
                        total_configs += 1
                        logger.info(f"Generated config for {config_name}")
                    else:
                        logger.warning(f"Config generation returned None for {market.get('ticker', 'unknown')}-{side}")
                except Exception as e:
                    logger.error(f"Error generating config for {market.get('ticker', 'unknown')}-{side}: {e}", exc_info=True)
                    # Continue with next market/side
        
        # Update discovery session with final config count
        with analytics_db.get_connection() as conn:
            conn.execute("""
                UPDATE discovery_sessions
                SET configs_generated = ?
                WHERE session_id = ?
            """, (total_configs, session_id))
            conn.commit()
        
        logger.info(f"Discovery complete: Generated {total_configs} configs from {len(top_markets)} markets")
        logger.info("=" * 80)
        
        return generated_configs
        
    except Exception as e:
        logger.error(f"Discovery failed: {e}", exc_info=True)
        return []


if __name__ == "__main__":
    # Set up basic logging configuration FIRST (before any loggers are used)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    parser = argparse.ArgumentParser(description="Kalshi Market Making Algorithm")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    # Load configuration (only settings, no static strategy configs)
    full_config = load_config(args.config)
    
    # Load environment variables
    load_dotenv()
    
    # Set up main logger
    main_logger = logging.getLogger("Main")
    main_logger.setLevel(logging.INFO)
    
    # Initialize analytics database
    analytics_db = AnalyticsDB(db_path="trading_analytics.db", logger=main_logger)
    
    # Initialize API for scanner and manager
    api = KalshiTradingAPI(
        access_key=os.getenv("KALSHI_ACCESS_KEY"),
        private_key=os.getenv("KALSHI_PRIVATE_KEY"),
        market_ticker="dummy",  # Not used for scanner
        base_url=os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"),
        logger=main_logger
    )
    
    # Initialize discovery (used by scanner for market fetching)
    discovery = MarketDiscovery(api, config=full_config)
    
    # Initialize volatility scanner
    from volatility_scanner import VolatilityScanner
    from vol_mm_manager import VolatilityMMManager
    
    # Create manager first (needed for callbacks)
    manager = VolatilityMMManager(
        api=api,
        discovery=discovery,
        analytics_db=analytics_db,
        config=full_config
    )
    
    # Create scanner with callbacks
    scanner = VolatilityScanner(
        api=api,
        discovery=discovery,
        config=full_config,
        event_callback=manager.handle_volatility_event,
        ended_callback=manager.handle_volatility_ended
    )
    
    # Link manager to scanner (for mark_event_ended)
    manager.scanner = scanner
    
    # Start manager and scanner
    main_logger.info("Starting volatility-driven market making system...")
    manager.start()
    scanner.start()
    
    main_logger.info("System started. Scanner monitoring markets for volatility events...")
    main_logger.info("Press Ctrl+C to stop")
    
    try:
        # Main thread sleeps/logs until shutdown
        while True:
            time.sleep(60)  # Log status every minute
            active_sessions = manager.get_active_sessions()
            main_logger.info(f"Active volatility sessions: {len(active_sessions)}")
            if active_sessions:
                for ticker, info in active_sessions.items():
                    main_logger.info(f"  - {ticker}: started at {info['start_time']}")
    except KeyboardInterrupt:
        main_logger.info("Shutting down...")
    finally:
        # Cleanup
        scanner.stop()
        manager.stop()
        api.logout()
        main_logger.info("System stopped")