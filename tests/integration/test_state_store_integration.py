"""
Integration tests for state store with WebSocket updates
"""
import pytest
import threading
import time
from datetime import datetime, timezone, timedelta
from tests.unit.test_websocket_utils import (
    create_mock_ticker_message, create_mock_orderbook_message,
    create_mock_fill_message, wait_for_condition
)
from kalshi_mm.state import MarketStateStore


class TestStateStoreIntegration:
    """Integration tests for state store"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10,
                'position_staleness_seconds': 30,
                'price_history_length': 1000,
                'trade_history_length': 2000
            }
        }
    
    @pytest.fixture
    def store(self, config):
        return MarketStateStore(config)
    
    def test_store_updates_from_ws(self, store):
        """Verify WS messages update store correctly"""
        ticker = 'TEST-TICKER'
        
        # Simulate WS ticker update
        msg = create_mock_ticker_message(ticker, yes_bid=0.44, yes_ask=0.46)
        store.update_ticker(ticker, msg['msg'])
        
        # Verify stored
        normalized = store.get_normalized_ticker(ticker)
        assert normalized is not None
        assert normalized['yes_bid'] == 0.44
        assert normalized['yes_ask'] == 0.46
        
        # Simulate orderbook update
        ob_msg = create_mock_orderbook_message(ticker)
        store.update_orderbook(ticker, ob_msg['msg'])
        
        ob = store.get_orderbook(ticker)
        assert ob is not None
        assert 'bids' in ob
        assert 'asks' in ob
    
    def test_store_scanner_read(self, store):
        """Verify scanner can read from store"""
        ticker = 'TEST-TICKER'
        
        # Update store with ticker data
        msg = create_mock_ticker_message(ticker)
        store.update_ticker(ticker, msg['msg'])
        
        # Scanner would read like this:
        normalized = store.get_normalized_ticker(ticker)
        assert normalized is not None
        
        # Verify price history available
        history = store.get_price_history(ticker)
        assert len(history) > 0
    
    def test_store_mm_read(self, store):
        """Verify MM can read from store"""
        ticker = 'TEST-TICKER'
        
        # Update with ticker, orderbook, position
        ticker_msg = create_mock_ticker_message(ticker)
        store.update_ticker(ticker, ticker_msg['msg'])
        
        ob_msg = create_mock_orderbook_message(ticker)
        store.update_orderbook(ticker, ob_msg['msg'])
        
        pos_msg = {
            'ticker': ticker,
            'position': '10',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_position(ticker, pos_msg)
        
        # MM would read like this:
        price_data = store.get_normalized_ticker(ticker)
        assert price_data is not None
        
        ob = store.get_orderbook(ticker)
        assert ob is not None
        
        pos = store.get_position(ticker)
        assert pos is not None
        assert pos == 10  # get_position returns int, not dict
    
    def test_store_concurrent_read_write(self, store):
        """Verify scanner and MM can read while WS writes (thread safety)"""
        ticker = 'TEST-TICKER'
        read_errors = []
        write_errors = []
        
        def writer():
            try:
                for i in range(100):
                    msg = create_mock_ticker_message(
                        ticker,
                        yes_bid=0.44 + i * 0.001,
                        yes_ask=0.46 + i * 0.001
                    )
                    store.update_ticker(ticker, msg['msg'])
                    time.sleep(0.01)
            except Exception as e:
                write_errors.append(e)
        
        def reader():
            try:
                for _ in range(100):
                    normalized = store.get_normalized_ticker(ticker)
                    ob = store.get_orderbook(ticker)
                    history = store.get_price_history(ticker)
                    time.sleep(0.01)
            except Exception as e:
                read_errors.append(e)
        
        # Start writer and multiple readers
        threads = [threading.Thread(target=writer)] + \
                 [threading.Thread(target=reader) for _ in range(3)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify no errors
        assert len(write_errors) == 0
        assert len(read_errors) == 0
    
    def test_store_fallback_on_stale(self, store, config):
        """Verify components fall back to REST when store is stale"""
        ticker = 'TEST-TICKER'
        stale_time = datetime.now(timezone.utc) - timedelta(
            seconds=config['websockets']['ticker_staleness_seconds'] + 1
        )
        
        # Update with stale data
        msg = {
            'market_ticker': ticker,
            'price': '45',
            'ts': stale_time.isoformat()
        }
        store.update_ticker(ticker, msg)
        
        # Component should detect staleness and fall back to REST
        normalized = store.get_normalized_ticker(ticker)
        assert normalized is None  # Stale data returns None
        
        # Component would then call REST API as fallback
    
    def test_store_price_history_accumulation(self, store):
        """Verify price history accumulates correctly over time"""
        ticker = 'TEST-TICKER'
        
        # Add multiple price updates
        for i in range(50):
            msg = create_mock_ticker_message(
                ticker,
                yes_bid=0.44 + i * 0.001,
                yes_ask=0.46 + i * 0.001
            )
            store.update_ticker(ticker, msg['msg'])
            time.sleep(0.01)
        
        # Verify history accumulated
        history = store.get_price_history(ticker)
        assert len(history) == 50
        
        # Verify history is ordered (newest last)
        assert history[-1]['mid'] > history[0]['mid']
        
        # Verify spreads calculated
        assert all('spread' in h for h in history)
        assert all(h['spread'] > 0 for h in history)

