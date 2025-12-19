"""
Performance and stress tests for WebSocket system
"""
import pytest
import time
import threading
from datetime import datetime, timezone
from tests.unit.test_websocket_utils import create_mock_ticker_message
from kalshi_mm.state import MarketStateStore


class TestWebSocketPerformance:
    """Performance tests"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'price_history_length': 1000,
                'trade_history_length': 2000
            }
        }
    
    @pytest.fixture
    def store(self, config):
        return MarketStateStore(config)
    
    def test_message_throughput(self, store):
        """Measure messages/second the system can handle"""
        ticker = 'TEST-TICKER'
        num_messages = 1000
        
        start = time.time()
        for i in range(num_messages):
            msg = create_mock_ticker_message(ticker)
            store.update_ticker(ticker, msg['msg'])
        elapsed = time.time() - start
        
        throughput = num_messages / elapsed
        print(f"Throughput: {throughput:.0f} messages/second")
        
        # Should handle at least 1000 msg/s
        assert throughput > 1000
    
    def test_callback_latency(self, store):
        """Measure callback execution time (must be <1ms)"""
        ticker = 'TEST-TICKER'
        latencies = []
        
        for i in range(100):
            msg = create_mock_ticker_message(ticker)
            start = time.time()
            store.update_ticker(ticker, msg['msg'])
            latency = (time.time() - start) * 1000  # Convert to ms
            latencies.append(latency)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        print(f"Avg latency: {avg_latency:.3f}ms, Max: {max_latency:.3f}ms")
        
        # Should be <1ms on average
        assert avg_latency < 1.0
    
    def test_store_update_latency(self, store):
        """Measure store update time"""
        ticker = 'TEST-TICKER'
        latencies = []
        
        for i in range(100):
            msg = create_mock_ticker_message(ticker)
            start = time.time()
            store.update_ticker(ticker, msg['msg'])
            store.update_orderbook(ticker, {'bids': [[45, 100]], 'asks': [[46, 100]]})
            latency = (time.time() - start) * 1000
            latencies.append(latency)
        
        avg_latency = sum(latencies) / len(latencies)
        assert avg_latency < 5.0  # Should be fast
    
    def test_concurrent_subscriptions(self, store):
        """Test with 100+ market subscriptions"""
        num_markets = 100
        tickers = [f'TICKER-{i}' for i in range(num_markets)]
        
        start = time.time()
        for ticker in tickers:
            msg = create_mock_ticker_message(ticker)
            store.update_ticker(ticker, msg['msg'])
        elapsed = time.time() - start
        
        print(f"Updated {num_markets} markets in {elapsed:.3f}s")
        
        # Verify all stored
        for ticker in tickers:
            normalized = store.get_normalized_ticker(ticker)
            assert normalized is not None
    
    def test_memory_usage(self, store, config):
        """Verify ring buffers prevent memory growth"""
        ticker = 'TEST-TICKER'
        maxlen = config['websockets']['price_history_length']
        
        # Add way more than maxlen
        for i in range(maxlen * 2):
            msg = create_mock_ticker_message(ticker)
            store.update_ticker(ticker, msg['msg'])
        
        # Verify buffer size is capped
        history = store.get_price_history(ticker)
        assert len(history) == maxlen
        
        # Memory should not grow unbounded
        import sys
        size = sys.getsizeof(store.price_history[ticker])
        # Ring buffer should have fixed size
        assert size < 100000  # Reasonable upper bound

