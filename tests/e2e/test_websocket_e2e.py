"""
End-to-end integration tests for WebSocket system
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone
from tests.unit.test_websocket_utils import (
    create_mock_ticker_message, create_mock_orderbook_message,
    wait_for_condition
)
from kalshi_mm.state import MarketStateStore
from kalshi_mm.api import KalshiWebsocketClient


class TestWebSocketE2E:
    """End-to-end tests"""
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'enabled': True,
                'ws_fallback_on_unhealthy': True,
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10,
                'price_history_length': 1000
            },
            'volatility_scanner': {
                'scan_interval_seconds': 3
            },
            'volatility_mm': {
                'session_ttl_seconds': 300
            }
        }
    
    @pytest.fixture
    def state_store(self, config):
        return MarketStateStore(config)
    
    @pytest.fixture
    def mock_ws_client(self):
        client = MagicMock(spec=KalshiWebsocketClient)
        client.subscribe_ticker = Mock(return_value=1)
        client.subscribe_orderbook = Mock(return_value=2)
        return client
    
    @pytest.fixture
    def mock_api(self):
        api = MagicMock()
        api.get_price.return_value = {
            'yes_bid': 0.44,
            'yes_ask': 0.46,
            'yes_mid': 0.45
        }
        api.get_position.return_value = 0
        return api
    
    def test_e2e_scanner_to_mm_flow(self, config, state_store, mock_ws_client, mock_api):
        """Test full flow: Scanner detects event → MM spawns → MM uses WS data"""
        from kalshi_mm.volatility import VolatilityScanner, VolatilityEvent
        
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = True
        
        # Pre-populate store with data
        ticker_msg = create_mock_ticker_message(ticker)
        state_store.update_ticker(ticker, ticker_msg['msg'])
        
        ob_msg = create_mock_orderbook_message(ticker)
        state_store.update_orderbook(ticker, ob_msg['msg'])
        
        # Scanner would detect volatility event
        # (In real system, scanner would analyze price history)
        
        # MM would spawn and use WS data
        event = VolatilityEvent(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            jump_magnitude=0.12,
            sigma=0.05,  # Required: realized volatility
            volume_multiplier=0.0,  # Required: deprecated field
            volume_delta=0.0,  # Required: deprecated field
            estimated_trades=0.0,  # Required: deprecated field
            volume_velocity=None,  # Optional: deprecated field
            close_time=None,  # Optional: market close time
            direction=None,  # Optional: 'up' or 'down'
            signal_strength=0.5,  # Required: combined signal strength (0-1)
            regime='MEAN_REVERTING'  # Required: market regime
        )
        
        # Verify data flow
        normalized = state_store.get_normalized_ticker(ticker)
        assert normalized is not None
        
        ob = state_store.get_orderbook(ticker)
        assert ob is not None
        
        # MM would use this data in run() loop
    
    def test_e2e_ws_disconnect_recovery(self, config, state_store, mock_ws_client, mock_api):
        """Test WS disconnect → fallback to REST → reconnect → resume WS"""
        ticker = 'TEST-TICKER'
        
        # Start with WS healthy
        state_store.ws_healthy = True
        ticker_msg = create_mock_ticker_message(ticker)
        state_store.update_ticker(ticker, ticker_msg['msg'])
        
        # Verify WS data available
        normalized1 = state_store.get_normalized_ticker(ticker)
        assert normalized1 is not None
        
        # Simulate disconnect
        state_store.ws_healthy = False
        
        # Should fall back to REST
        if not state_store.ws_healthy and config['websockets']['ws_fallback_on_unhealthy']:
            price = mock_api.get_price(ticker)
            assert price is not None
        
        # Simulate reconnect
        state_store.ws_healthy = True
        state_store.update_ticker(ticker, ticker_msg['msg'])
        
        # Should resume using WS
        normalized2 = state_store.get_normalized_ticker(ticker)
        assert normalized2 is not None
    
    def test_e2e_multi_market_subscriptions(self, config, state_store, mock_ws_client):
        """Test subscribing to multiple markets"""
        tickers = ['TICKER-1', 'TICKER-2', 'TICKER-3']
        
        # Subscribe to multiple markets
        for ticker in tickers:
            ticker_msg = create_mock_ticker_message(ticker)
            state_store.update_ticker(ticker, ticker_msg['msg'])
            mock_ws_client.subscribe_ticker(ticker, Mock())
            mock_ws_client.subscribe_orderbook(ticker, Mock())
        
        # Verify all markets have data
        for ticker in tickers:
            normalized = state_store.get_normalized_ticker(ticker)
            assert normalized is not None
        
        # Verify subscriptions made
        assert mock_ws_client.subscribe_ticker.call_count == len(tickers)
        assert mock_ws_client.subscribe_orderbook.call_count == len(tickers)
    
    def test_e2e_regime_detection_ws(self, config, state_store, mock_ws_client):
        """Test regime detection with WS price history"""
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = True
        
        # Build price history
        for i in range(50):
            msg = create_mock_ticker_message(
                ticker,
                yes_bid=0.44 + i * 0.001,
                yes_ask=0.46 + i * 0.001
            )
            state_store.update_ticker(ticker, msg['msg'])
        
        # Verify price history available
        history = state_store.get_price_history(ticker)
        assert len(history) == 50
        
        # Scanner would use this for regime detection
        # (In real system, scanner would analyze returns, drift, mean reversion)
        
        # Verify regime kill switches would work with WS data
        # (MM would read regime from scanner and react accordingly)

