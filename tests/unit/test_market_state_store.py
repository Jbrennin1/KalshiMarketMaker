"""
Unit tests for Market State Store
Tests deep copying, staleness, buffers, and thread safety
"""
import pytest
import copy
import threading
import time
from datetime import datetime, timezone, timedelta
from tests.unit.test_websocket_utils import (
    create_mock_ticker_message, create_mock_orderbook_message,
    create_mock_fill_message, create_mock_position_message
)
from kalshi_mm.state import MarketStateStore


class TestMarketStateStore:
    """Unit tests for MarketStateStore"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10,
                'position_staleness_seconds': 30,
                'max_price_history': 100,  # Changed from price_history_length
                'max_trades_per_ticker': 200  # Changed from trade_history_length
            }
        }
    
    @pytest.fixture
    def store(self, config):
        return MarketStateStore(config)
    
    def test_store_deep_copy(self, store):
        """Verify messages are deep copied (modify original, verify store unchanged)"""
        ticker = 'TEST-TICKER'
        original_msg = {
            'market_ticker': ticker,
            'price': '45',
            'yes_bid': '44',
            'yes_ask': '46',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        
        store.update_ticker(ticker, original_msg)
        
        # Modify original
        original_msg['price'] = '99'
        original_msg['yes_bid'] = '98'
        
        # Verify store unchanged
        stored = store.get_ticker(ticker)
        assert stored is not None
        assert stored['price'] == '45'  # Original value, not modified
        assert stored['yes_bid'] == '44'
    
    def test_store_out_of_order_rejection(self, store):
        """Verify older timestamps are rejected"""
        ticker = 'TEST-TICKER'
        now = datetime.now(timezone.utc)
        
        # First message with current time
        msg1 = {
            'market_ticker': ticker,
            'price': '45',
            'ts': now.isoformat()
        }
        store.update_ticker(ticker, msg1)
        
        # Second message with older time (should be rejected)
        msg2 = {
            'market_ticker': ticker,
            'price': '99',  # Different price
            'ts': (now - timedelta(seconds=5)).isoformat()
        }
        store.update_ticker(ticker, msg2)
        
        # Verify first message still stored (second rejected)
        stored = store.get_ticker(ticker)
        assert stored['price'] == '45'
    
    def test_store_ticker_staleness(self, store, config):
        """Verify get_ticker returns None when stale"""
        ticker = 'TEST-TICKER'
        stale_time = datetime.now(timezone.utc) - timedelta(seconds=config['websockets']['ticker_staleness_seconds'] + 1)
        
        msg = {
            'market_ticker': ticker,
            'price': '45',
            'ts': stale_time.isoformat()
        }
        store.update_ticker(ticker, msg)
        
        # Should return None due to staleness
        result = store.get_ticker(ticker)
        assert result is None
    
    def test_store_orderbook_staleness(self, store, config):
        """Verify get_orderbook returns None when stale"""
        ticker = 'TEST-TICKER'
        stale_time = datetime.now(timezone.utc) - timedelta(seconds=config['websockets']['orderbook_staleness_seconds'] + 1)
        
        msg = {
            'market_ticker': ticker,
            'bids': [[45, 100]],
            'asks': [[46, 100]],
            'ts': stale_time.isoformat()
        }
        store.update_orderbook(ticker, msg)
        
        result = store.get_orderbook(ticker)
        assert result is None
    
    def test_store_price_history_buffer(self, store, config):
        """Verify price history ring buffer (maxlen respected)"""
        ticker = 'TEST-TICKER'
        maxlen = config['websockets']['max_price_history']  # Use correct key
        
        # Add more than maxlen messages
        for i in range(maxlen + 50):
            msg = {
                'market_ticker': ticker,
                'yes_bid': str(44 + i),
                'yes_ask': str(46 + i),
                'ts': datetime.now(timezone.utc).isoformat()
            }
            store.update_ticker(ticker, msg)
        
        # Verify buffer size is capped
        history = store.get_price_history(ticker)
        assert len(history) == maxlen
        
        # Verify oldest entries were dropped (newest kept)
        assert history[-1]['mid'] > history[0]['mid']
    
    def test_store_last_price_change(self, store):
        """Verify last_price_change only updates on price change"""
        ticker = 'TEST-TICKER'
        now = datetime.now(timezone.utc)
        
        # First update
        msg1 = {
            'market_ticker': ticker,
            'price': '45',
            'yes_bid': '44',
            'yes_ask': '46',
            'ts': now.isoformat()
        }
        store.update_ticker(ticker, msg1)
        change1 = store.last_price_change.get(ticker)
        
        time.sleep(0.1)
        
        # Second update with same price (should not update last_price_change)
        msg2 = {
            'market_ticker': ticker,
            'price': '45',  # Same price
            'yes_bid': '44',
            'yes_ask': '46',
            'ts': (now + timedelta(seconds=1)).isoformat()
        }
        store.update_ticker(ticker, msg2)
        change2 = store.last_price_change.get(ticker)
        
        # Should be same timestamp (no change)
        assert change1 == change2
        
        # Third update with different price (should update)
        time.sleep(0.1)
        msg3 = {
            'market_ticker': ticker,
            'price': '50',  # Different price
            'yes_bid': '49',
            'yes_ask': '51',
            'ts': (now + timedelta(seconds=2)).isoformat()
        }
        store.update_ticker(ticker, msg3)
        change3 = store.last_price_change.get(ticker)
        
        # Should be newer timestamp
        assert change3 > change2
    
    def test_store_normalized_ticker(self, store):
        """Verify get_normalized_ticker uses get_price_field correctly"""
        ticker = 'TEST-TICKER'
        
        # Test with string dollars format
        msg = {
            'market_ticker': ticker,
            'yes_bid_dollars': '0.44',
            'yes_ask_dollars': '0.46',
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg)
        
        normalized = store.get_normalized_ticker(ticker)
        assert normalized is not None
        assert normalized['yes_bid'] == 0.44
        assert normalized['yes_ask'] == 0.46
        
        # Test with cents format
        msg2 = {
            'market_ticker': ticker,
            'yes_bid': '44',  # Cents
            'yes_ask': '46',
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg2)
        
        normalized2 = store.get_normalized_ticker(ticker)
        assert normalized2 is not None
        assert normalized2['yes_bid'] == 0.44
        assert normalized2['yes_ask'] == 0.46
    
    def test_store_orderbook_imbalance(self, store):
        """Verify imbalance calculation is correct"""
        ticker = 'TEST-TICKER'
        
        # Balanced book
        msg1 = {
            'market_ticker': ticker,
            'bids': [[45, 100], [44, 200]],
            'asks': [[46, 100], [47, 200]],
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_orderbook(ticker, msg1)
        ob1 = store.get_orderbook(ticker)
        assert abs(ob1['imbalance']) < 0.01  # Should be ~0
        
        # Imbalanced book (more bids)
        msg2 = {
            'market_ticker': ticker,
            'bids': [[45, 500], [44, 300]],
            'asks': [[46, 100], [47, 100]],
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_orderbook(ticker, msg2)
        ob2 = store.get_orderbook(ticker)
        assert ob2['imbalance'] > 0  # Positive = bid-heavy
        
        # Imbalanced book (more asks)
        msg3 = {
            'market_ticker': ticker,
            'bids': [[45, 100], [44, 100]],
            'asks': [[46, 500], [47, 300]],
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_orderbook(ticker, msg3)
        ob3 = store.get_orderbook(ticker)
        assert ob3['imbalance'] < 0  # Negative = ask-heavy
    
    def test_store_fill_queue(self, store):
        """Verify user fills queue drains correctly"""
        ticker = 'TEST-TICKER'
        
        # Add multiple fills
        for i in range(5):
            msg = {
                'ticker': ticker,
                'order_id': f'order_{i}',
                'fill_id': f'fill_{i}',
                'price': '45',
                'count': '5',
                'action': 'buy',
                'side': 'yes',
                'ts': datetime.now(timezone.utc).isoformat()
            }
            store.add_user_fill(msg)
        
        # Drain fills (get_user_fills returns a list, not a single fill)
        fills = store.get_user_fills()  # Returns all fills and clears queue
        
        assert len(fills) == 5
        assert all(f['ticker'] == ticker for f in fills)
    
    def test_store_last_fill_ts_tracking(self, store):
        """Verify last_fill_ts updates correctly"""
        ticker = 'TEST-TICKER'
        now = datetime.now(timezone.utc)
        
        msg1 = {
            'ticker': ticker,
            'order_id': 'order1',
            'fill_id': 'fill1',
            'price': '45',
            'count': '5',
            'ts': now.isoformat()
        }
        store.add_user_fill(msg1)
        assert store.last_fill_ts == now
        
        msg2 = {
            'ticker': ticker,
            'order_id': 'order2',
            'fill_id': 'fill2',
            'price': '46',
            'count': '3',
            'ts': (now + timedelta(seconds=1)).isoformat()
        }
        store.add_user_fill(msg2)
        assert store.last_fill_ts > now
    
    def test_store_string_numeric_handling(self, store):
        """Verify string and numeric fields both work (CRITICAL FIX 2.2)"""
        ticker = 'TEST-TICKER'
        
        # Test with string prices
        msg_str = {
            'market_ticker': ticker,
            'yes_bid': '44',  # String
            'yes_ask': '46',  # String
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg_str)
        normalized_str = store.get_normalized_ticker(ticker)
        assert normalized_str['yes_bid'] == 0.44
        assert normalized_str['yes_ask'] == 0.46
        
        # Test with numeric prices
        msg_num = {
            'market_ticker': ticker,
            'yes_bid': 44,  # Numeric
            'yes_ask': 46,  # Numeric
            'price': 45,
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg_num)
        normalized_num = store.get_normalized_ticker(ticker)
        assert normalized_num['yes_bid'] == 0.44
        assert normalized_num['yes_ask'] == 0.46
        
        # Test with dollars string
        msg_dollars = {
            'market_ticker': ticker,
            'yes_bid_dollars': '0.44',  # Dollars string
            'yes_ask_dollars': '0.46',
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg_dollars)
        normalized_dollars = store.get_normalized_ticker(ticker)
        assert normalized_dollars['yes_bid'] == 0.44
        assert normalized_dollars['yes_ask'] == 0.46
    
    def test_store_thread_safety(self, store):
        """Verify concurrent updates don't corrupt data (multi-threaded)"""
        ticker = 'TEST-TICKER'
        errors = []
        
        def update_worker(worker_id):
            try:
                for i in range(100):
                    msg = {
                        'market_ticker': ticker,
                        'price': str(worker_id * 1000 + i),
                        'yes_bid': str(44 + i),
                        'yes_ask': str(46 + i),
                        'ts': datetime.now(timezone.utc).isoformat()
                    }
                    store.update_ticker(ticker, msg)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads updating simultaneously
        threads = [threading.Thread(target=update_worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify no errors
        assert len(errors) == 0
        
        # Verify final state is consistent
        stored = store.get_ticker(ticker)
        assert stored is not None
        assert 'price' in stored

