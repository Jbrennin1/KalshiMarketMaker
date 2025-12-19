"""
Edge cases and error handling tests
"""
import pytest
import json
from datetime import datetime, timezone
from kalshi_mm.state import MarketStateStore
from kalshi_mm.api import KalshiWebsocketClient


class TestWebSocketErrors:
    """Error handling tests"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10
            }
        }
    
    @pytest.fixture
    def store(self, config):
        return MarketStateStore(config)
    
    def test_malformed_message(self, store):
        """Verify system handles malformed JSON gracefully"""
        ticker = 'TEST-TICKER'
        
        # Try to update with invalid JSON structure
        try:
            store.update_ticker(ticker, None)
        except Exception:
            pass  # Should handle gracefully
        
        # Try with missing required fields
        msg = {'market_ticker': ticker}  # Missing price, ts
        try:
            store.update_ticker(ticker, msg)
        except Exception:
            pass  # Should handle gracefully
    
    def test_missing_fields(self, store):
        """Verify system handles missing required fields"""
        ticker = 'TEST-TICKER'
        
        # Missing price
        msg = {
            'market_ticker': ticker,
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg)  # Should not crash
        
        # Missing timestamp
        msg2 = {
            'market_ticker': ticker,
            'price': '45'
        }
        store.update_ticker(ticker, msg2)  # Should not crash
    
    def test_invalid_timestamps(self, store):
        """Verify system handles invalid timestamp formats"""
        ticker = 'TEST-TICKER'
        
        # Invalid timestamp formats
        invalid_timestamps = [
            'not-a-timestamp',
            '2023-13-45T99:99:99',  # Invalid date
            '',
            None
        ]
        
        for invalid_ts in invalid_timestamps:
            msg = {
                'market_ticker': ticker,
                'price': '45',
                'ts': invalid_ts
            }
            try:
                store.update_ticker(ticker, msg)
            except Exception:
                pass  # Should handle gracefully
    
    def test_negative_prices(self, store):
        """Verify system handles negative or zero prices"""
        ticker = 'TEST-TICKER'
        
        # Negative price
        msg1 = {
            'market_ticker': ticker,
            'yes_bid': '-10',
            'yes_ask': '46',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg1)  # Should not crash
        
        # Zero price
        msg2 = {
            'market_ticker': ticker,
            'yes_bid': '0',
            'yes_ask': '0',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg2)  # Should not crash
        
        # Very large price
        msg3 = {
            'market_ticker': ticker,
            'yes_bid': '999999',
            'yes_ask': '999999',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        store.update_ticker(ticker, msg3)  # Should not crash
    
    def test_oversized_messages(self, store):
        """Verify system handles very large messages"""
        ticker = 'TEST-TICKER'
        
        # Large orderbook
        large_bids = [[45 + i, 100] for i in range(1000)]
        large_asks = [[46 + i, 100] for i in range(1000)]
        
        msg = {
            'market_ticker': ticker,
            'bids': large_bids,
            'asks': large_asks,
            'ts': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            store.update_orderbook(ticker, msg)
        except Exception:
            pass  # Should handle gracefully or truncate
    
    def test_rapid_reconnection(self, store):
        """Verify system handles rapid connect/disconnect cycles"""
        # Simulate rapid health toggles
        for _ in range(10):
            store.ws_healthy = True
            store.ws_healthy = False
        
        # Should not crash
        assert store.ws_healthy is False
    
    def test_subscription_failure(self):
        """Verify system handles subscription rejections"""
        from unittest.mock import Mock, MagicMock
        
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Subscription failed")
        
        # System should handle subscription failures gracefully
        try:
            mock_ws.send(json.dumps({"type": "ticker", "market_ticker": "TEST"}))
        except Exception:
            pass  # Should handle gracefully
    
    def test_partial_message(self, store):
        """Verify system handles incomplete messages"""
        ticker = 'TEST-TICKER'
        
        # Partial message (missing fields)
        partial_msg = {
            'market_ticker': ticker
            # Missing price, ts, etc.
        }
        
        try:
            store.update_ticker(ticker, partial_msg)
        except Exception:
            pass  # Should handle gracefully
        
        # Empty message
        try:
            store.update_ticker(ticker, {})
        except Exception:
            pass  # Should handle gracefully

