"""
Import historical log data into analytics database
Parses existing .log files and extracts structured data for analysis
"""
import re
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional
import json

from kalshi_mm.analytics import AnalyticsDB


class LogImporter:
    """Import data from log files into analytics database"""
    
    def __init__(self, db_path: str = "data/trading_analytics.db"):
        self.analytics_db = AnalyticsDB(db_path=db_path)
        self.logger = logging.getLogger(__name__)
    
    def parse_log_file(self, log_file: str) -> Dict:
        """
        Parse a log file and extract structured data
        
        Returns dictionary with:
        - strategy_name: extracted from log file name
        - events: list of parsed events
        """
        strategy_name = log_file.replace('.log', '').replace('-NO', '')
        
        events = []
        current_run = None
        
        with open(log_file, 'r') as f:
            for line in f:
                # Parse timestamp and log level
                timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)', line)
                if not timestamp_match:
                    continue
                
                timestamp_str = timestamp_match.group(1)
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                except:
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        continue
                
                # Parse different event types
                if 'Starting strategy:' in line:
                    # Extract strategy name and create run
                    match = re.search(r'Starting strategy: (.+)', line)
                    if match:
                        strategy_name = match.group(1)
                        # We'll create the run later when we have config
                        events.append({
                            'type': 'strategy_start',
                            'timestamp': timestamp,
                            'strategy_name': strategy_name
                        })
                
                elif 'Current position:' in line:
                    match = re.search(r'Current position: (-?\d+)', line)
                    if match:
                        position = int(match.group(1))
                        events.append({
                            'type': 'position',
                            'timestamp': timestamp,
                            'position': position
                        })
                
                elif 'Current mid price for' in line:
                    # Extract mid price and inventory
                    match = re.search(r'Current mid price for (\w+): ([\d.]+), Inventory: (-?\d+)', line)
                    if match:
                        side, price, inventory = match.group(1), float(match.group(2)), int(match.group(3))
                        events.append({
                            'type': 'market_snapshot',
                            'timestamp': timestamp,
                            'side': side,
                            'mid_price': price,
                            'inventory': inventory
                        })
                
                elif 'Current yes mid-market price:' in line:
                    # Extract yes/no bid/ask prices
                    yes_match = re.search(r'yes mid-market price: \$([\d.]+) \(bid: \$([\d.]+), ask: \$([\d.]+)\)', line)
                    no_match = None
                    # Next line should have no prices
                    if yes_match:
                        yes_mid = float(yes_match.group(1))
                        yes_bid = float(yes_match.group(2))
                        yes_ask = float(yes_match.group(3))
                        events.append({
                            'type': 'market_prices',
                            'timestamp': timestamp,
                            'yes_mid': yes_mid,
                            'yes_bid': yes_bid,
                            'yes_ask': yes_ask
                        })
                
                elif 'Current no mid-market price:' in line:
                    no_match = re.search(r'no mid-market price: \$([\d.]+) \(bid: \$([\d.]+), ask: \$([\d.]+)\)', line)
                    if no_match:
                        no_mid = float(no_match.group(1))
                        no_bid = float(no_match.group(2))
                        no_ask = float(no_match.group(3))
                        events.append({
                            'type': 'market_prices_no',
                            'timestamp': timestamp,
                            'no_mid': no_mid,
                            'no_bid': no_bid,
                            'no_ask': no_ask
                        })
                
                elif 'Reservation price:' in line:
                    match = re.search(r'Reservation price: ([\d.]+)', line)
                    if match:
                        reservation_price = float(match.group(1))
                        events.append({
                            'type': 'reservation_price',
                            'timestamp': timestamp,
                            'reservation_price': reservation_price
                        })
                
                elif 'Computed desired bid:' in line:
                    match = re.search(r'Computed desired bid: ([\d.]+), ask: ([\d.]+)', line)
                    if match:
                        bid = float(match.group(1))
                        ask = float(match.group(2))
                        events.append({
                            'type': 'computed_quotes',
                            'timestamp': timestamp,
                            'bid': bid,
                            'ask': ask
                        })
                
                elif 'Placed' in line and 'order for' in line and 'order ID:' in line:
                    # Parse order placement
                    match = re.search(r'Placed (\w+) order for (\w+) side at price \$([\d.]+) with quantity (\d+), order ID: ([a-f0-9-]+)', line)
                    if match:
                        action, side, price, quantity, order_id = match.group(1), match.group(2), float(match.group(3)), int(match.group(4)), match.group(5)
                        events.append({
                            'type': 'order_placed',
                            'timestamp': timestamp,
                            'action': action,
                            'side': side,
                            'price': price,
                            'quantity': quantity,
                            'order_id': order_id
                        })
                
                elif 'Canceled order with ID' in line and 'success:' in line:
                    match = re.search(r'Canceled order with ID ([a-f0-9-]+), success: (\w+)', line)
                    if match:
                        order_id, success = match.group(1), match.group(2) == 'True'
                        if success:
                            events.append({
                                'type': 'order_cancelled',
                                'timestamp': timestamp,
                                'order_id': order_id
                            })
        
        return {
            'strategy_name': strategy_name,
            'events': events
        }
    
    def import_log_file(self, log_file: str, market_ticker: str, trade_side: str, config_params: Dict):
        """
        Import a log file into the database
        
        Args:
            log_file: Path to log file
            market_ticker: Market ticker for this strategy
            trade_side: "yes" or "no"
            config_params: Configuration parameters dictionary
        """
        self.logger.info(f"Importing log file: {log_file}")
        
        parsed = self.parse_log_file(log_file)
        strategy_name = parsed['strategy_name']
        events = parsed['events']
        
        if not events:
            self.logger.warning(f"No events found in {log_file}")
            return
        
        # Create strategy run
        run_id = self.analytics_db.create_strategy_run(
            strategy_name=strategy_name,
            market_ticker=market_ticker,
            trade_side=trade_side,
            config_params=config_params
        )
        
        self.logger.info(f"Created run_id {run_id} for strategy {strategy_name}")
        
        # Process events in chronological order
        market_data = {}
        current_position = None
        reservation_price = None
        computed_bid = None
        computed_ask = None
        
        for event in events:
            if event['type'] == 'position':
                current_position = event['position']
                self.analytics_db.log_position(run_id, current_position)
            
            elif event['type'] == 'market_prices':
                market_data['yes_mid'] = event['yes_mid']
                market_data['yes_bid'] = event['yes_bid']
                market_data['yes_ask'] = event['yes_ask']
            
            elif event['type'] == 'market_prices_no':
                market_data['no_mid'] = event['no_mid']
                market_data['no_bid'] = event['no_bid']
                market_data['no_ask'] = event['no_ask']
            
            elif event['type'] == 'reservation_price':
                reservation_price = event['reservation_price']
            
            elif event['type'] == 'computed_quotes':
                computed_bid = event['bid']
                computed_ask = event['ask']
                
                # If we have all the data, log a market snapshot
                if market_data and current_position is not None and reservation_price is not None:
                    self.analytics_db.log_market_snapshot(
                        run_id,
                        market_data,
                        current_position,
                        reservation_price,
                        computed_bid,
                        computed_ask
                    )
            
            elif event['type'] == 'order_placed':
                self.analytics_db.log_order_placed(
                    run_id,
                    event['order_id'],
                    event['action'],
                    event['side'],
                    event['price'],
                    event['quantity']
                )
            
            elif event['type'] == 'order_cancelled':
                self.analytics_db.log_order_cancelled(event['order_id'])
        
        # End the strategy run
        self.analytics_db.end_strategy_run(run_id)
        self.logger.info(f"Completed import of {log_file}, processed {len(events)} events")


def main():
    parser = argparse.ArgumentParser(description="Import log files into analytics database")
    parser.add_argument("log_file", help="Path to log file to import")
    parser.add_argument("--market-ticker", required=True, help="Market ticker symbol")
    parser.add_argument("--trade-side", choices=['yes', 'no'], required=True, help="Trade side")
    parser.add_argument("--config", help="Path to config file to extract parameters (optional)")
    parser.add_argument("--db", default="data/trading_analytics.db", help="Database path")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Load config if provided
    config_params = {}
    if args.config:
        import yaml
        with open(args.config, 'r') as f:
            configs = yaml.safe_load(f)
            # Try to find matching config by log file name
            log_name = args.log_file.replace('.log', '')
            if log_name in configs:
                mm_config = configs[log_name].get('market_maker', {})
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
                }
    
    # Import the log file
    importer = LogImporter(db_path=args.db)
    importer.import_log_file(
        args.log_file,
        args.market_ticker,
        args.trade_side,
        config_params
    )


if __name__ == "__main__":
    main()

