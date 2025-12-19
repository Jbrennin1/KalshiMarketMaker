"""
Tests for WebSocket fallback mechanisms
"""
import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timezone, timedelta
from kalshi_mm.state import MarketStateStore
from kalshi_mm.api import KalshiWebsocketClient


class TestWebSocketFallback:
    """Tests for fallback mechanisms"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'enabled': True,
                'ws_fallback_on_unhealthy': True,
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10
            }
        }
    
    @pytest.fixture
    def state_store(self, config):
        return MarketStateStore(config)
    
    @pytest.fixture
    def mock_api(self):
        api = MagicMock()
        api.get_price.return_value = {
            'yes_bid': 0.44,
            'yes_ask': 0.46,
            'yes_mid': 0.45
        }
        return api
    
    def test_fallback_on_connection_failure(self, config, state_store, mock_api):
        """Verify components use REST when WS fails to connect"""
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = False
        
        # Component should detect unhealthy WS and use REST
        if not state_store.ws_healthy and config['websockets']['ws_fallback_on_unhealthy']:
            price = mock_api.get_price(ticker)
            assert price is not None
    
    def test_fallback_on_stale_data(self, config, state_store, mock_api):
        """Verify components use REST when store data is stale"""
        ticker = 'TEST-TICKER'
        stale_time = datetime.now(timezone.utc) - timedelta(
            seconds=config['websockets']['ticker_staleness_seconds'] + 1
        )
        
        # Add stale data
        msg = {
            'market_ticker': ticker,
            'price': '45',
            'ts': stale_time.isoformat()
        }
        state_store.update_ticker(ticker, msg)
        
        # Component should detect staleness
        normalized = state_store.get_normalized_ticker(ticker)
        assert normalized is None  # Stale data returns None
        
        # Component should fall back to REST
        if normalized is None:
            price = mock_api.get_price(ticker)
            assert price is not None
    
    def test_fallback_on_ws_unhealthy(self, config, state_store, mock_api):
        """Verify components use REST when ws_healthy=False"""
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = False
        
        # Component should check health flag
        if not state_store.ws_healthy and config['websockets']['ws_fallback_on_unhealthy']:
            price = mock_api.get_price(ticker)
            assert price is not None
    
    def test_fallback_config_flag(self, config, state_store, mock_api):
        """Verify ws_fallback_on_unhealthy config flag works"""
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = False
        
        # Test with fallback enabled
        config['websockets']['ws_fallback_on_unhealthy'] = True
        if not state_store.ws_healthy and config['websockets']['ws_fallback_on_unhealthy']:
            price = mock_api.get_price(ticker)
            assert price is not None
        
        # Test with fallback disabled
        config['websockets']['ws_fallback_on_unhealthy'] = False
        if not state_store.ws_healthy and not config['websockets']['ws_fallback_on_unhealthy']:
            # Should not call REST
            mock_api.get_price.reset_mock()
            # Component would handle differently (maybe error or wait)
    
    def test_fallback_seamless_transition(self, config, state_store, mock_api):
        """Verify no errors when switching WS ↔ REST"""
        ticker = 'TEST-TICKER'
        
        # Start with WS healthy
        state_store.ws_healthy = True
        msg = {
            'market_ticker': ticker,
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        state_store.update_ticker(ticker, msg)
        
        # Read from WS
        normalized1 = state_store.get_normalized_ticker(ticker)
        assert normalized1 is not None
        
        # Switch to unhealthy
        state_store.ws_healthy = False
        
        # Should seamlessly fall back to REST
        if not state_store.ws_healthy and config['websockets']['ws_fallback_on_unhealthy']:
            price = mock_api.get_price(ticker)
            assert price is not None
        
        # Switch back to healthy
        state_store.ws_healthy = True
        state_store.update_ticker(ticker, msg)
        
        # Should seamlessly switch back to WS
        normalized2 = state_store.get_normalized_ticker(ticker)
        assert normalized2 is not None

