"""
Integration tests for VolatilityScanner WebSocket integration
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone, timedelta
from tests.unit.test_websocket_utils import (
    create_mock_ticker_message, create_mock_orderbook_message,
    wait_for_condition
)
from kalshi_mm.state import MarketStateStore
from kalshi_mm.api import KalshiWebsocketClient


class TestScannerWebSocket:
    """Tests for scanner WebSocket integration"""
    
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
            }
        }
    
    @pytest.fixture
    def state_store(self, config):
        return MarketStateStore(config)
    
    @pytest.fixture
    def mock_ws_client(self):
        """Mock WebSocket client"""
        client = MagicMock(spec=KalshiWebsocketClient)
        client.subscribe_ticker = Mock(return_value=1)
        client.subscribe_orderbook = Mock(return_value=2)
        client.unsubscribe = Mock()
        return client
    
    @pytest.fixture
    def mock_api(self):
        """Mock API client"""
        api = MagicMock()
        return api
    
    @pytest.fixture
    def mock_discovery(self):
        """Mock market discovery"""
        discovery = MagicMock()
        discovery.get_tracked_markets.return_value = {
            'TEST-TICKER': {
                'ticker': 'TEST-TICKER',
                'category': 'politics'
            }
        }
        return discovery
    
    def test_scanner_subscribes_on_discovery(self, config, state_store, mock_ws_client,
                                            mock_api, mock_discovery):
        """Verify scanner subscribes to markets when discovered"""
        from kalshi_mm.volatility import VolatilityScanner
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Add market to tracked_tickers and trigger subscription
        ticker = 'TEST-TICKER'
        with scanner.lock:
            scanner.tracked_tickers.add(ticker)
        
        # Manually trigger subscription (this is what happens in _initial_scan)
        scanner._subscribe_to_market(ticker)
        
        # Verify subscriptions made
        assert mock_ws_client.subscribe_ticker.called
        assert mock_ws_client.subscribe_orderbook.called
    
    def test_scanner_unsubscribes_on_removal(self, config, state_store, mock_ws_client,
                                            mock_api, mock_discovery):
        """Verify scanner unsubscribes when markets removed"""
        from kalshi_mm.volatility import VolatilityScanner
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # First, subscribe to a market
        ticker = 'TEST-TICKER'
        with scanner.lock:
            scanner.tracked_tickers.add(ticker)
        scanner._subscribe_to_market(ticker)
        initial_sub_count = mock_ws_client.subscribe_ticker.call_count
        
        # Remove market from tracking and trigger unsubscribe
        with scanner.lock:
            scanner.tracked_tickers.discard(ticker)
        scanner._unsubscribe_from_market(ticker)
        
        # Verify unsubscribe called
        assert mock_ws_client.unsubscribe.called
    
    def test_scanner_uses_ws_data(self, config, state_store, mock_ws_client,
                                  mock_api, mock_discovery):
        """Verify scanner uses WS ticker/orderbook data instead of REST"""
        from kalshi_mm.volatility import VolatilityScanner
        
        ticker = 'TEST-TICKER'
        
        # Pre-populate store with WS data
        ticker_msg = create_mock_ticker_message(ticker)
        state_store.update_ticker(ticker, ticker_msg['msg'])
        
        ob_msg = create_mock_orderbook_message(ticker)
        state_store.update_orderbook(ticker, ob_msg['msg'])
        
        state_store.ws_healthy = True
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Scanner should read from store, not call API
        markets = list(mock_discovery.get_tracked_markets().values())
        scanner._update_tracked_states(markets=markets)
        
        # Verify API not called for price data
        assert not mock_api.get_price.called
    
    def test_scanner_fallback_to_rest(self, config, state_store, mock_ws_client,
                                      mock_api, mock_discovery):
        """Verify scanner falls back to REST when WS unhealthy"""
        from kalshi_mm.volatility import VolatilityScanner
        
        ticker = 'TEST-TICKER'
        state_store.ws_healthy = False
        
        # Mock REST response for orderbook (what scanner actually calls)
        mock_api.get_orderbook = Mock(return_value={
            'bids': [[44, 100], [43, 200]],
            'asks': [[46, 100], [47, 200]],
            'best_bid': 44,
            'best_ask': 46
        })
        
        # Set discovery.api to use our mock BEFORE creating scanner
        mock_discovery.api = mock_api
        # Mock discovery methods to return actual numbers
        mock_discovery.get_mid_price = Mock(return_value=0.45)
        mock_discovery.calculate_spread = Mock(return_value=0.02)
        mock_discovery.get_book_depth = Mock(return_value=1000.0)
        mock_discovery.get_time_to_expiry_days = Mock(return_value=1.0)
        mock_discovery.get_expiry_iso_string = Mock(return_value=None)
        mock_discovery.get_category = Mock(return_value='politics')
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Add market to tracked_tickers first
        with scanner.lock:
            scanner.tracked_tickers.add(ticker)
        
        # Create market dict with required fields
        market = {
            'ticker': ticker,
            'yes_bid': 44,
            'yes_ask': 46,
            'volume_24h': 10000
        }
        
        # Initialize market state with enough data to trigger orderbook call
        # (needs at least 3 price snapshots)
        current_time = datetime.now(timezone.utc)
        for i in range(3):
            scanner._update_market_state(market, current_time - timedelta(minutes=i))
        
        # Verify REST API called for orderbook (when WS unhealthy)
        assert mock_api.get_orderbook.called
    
    def test_scanner_ws_health_check(self, config, state_store, mock_ws_client,
                                    mock_api, mock_discovery):
        """Verify scanner respects ws_healthy flag"""
        from kalshi_mm.volatility import VolatilityScanner
        
        ticker = 'TEST-TICKER'
        
        # Mock orderbook API
        mock_api.get_orderbook = Mock(return_value={
            'bids': [[44, 100], [43, 200]],
            'asks': [[46, 100], [47, 200]],
            'best_bid': 44,
            'best_ask': 46
        })
        # Set discovery.api BEFORE creating scanner
        mock_discovery.api = mock_api
        # Mock discovery methods to return actual numbers
        mock_discovery.get_mid_price = Mock(return_value=0.45)
        mock_discovery.calculate_spread = Mock(return_value=0.02)
        mock_discovery.get_book_depth = Mock(return_value=1000.0)
        mock_discovery.get_time_to_expiry_days = Mock(return_value=1.0)
        mock_discovery.get_expiry_iso_string = Mock(return_value=None)
        mock_discovery.get_category = Mock(return_value='politics')
        
        # Test with healthy WS
        state_store.ws_healthy = True
        state_store.update_ticker(ticker, create_mock_ticker_message(ticker)['msg'])
        state_store.update_orderbook(ticker, {
            'bids': [[44, 100], [43, 200]],
            'asks': [[46, 100], [47, 200]]
        })
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Add market to tracked_tickers
        with scanner.lock:
            scanner.tracked_tickers.add(ticker)
        
        market = {
            'ticker': ticker,
            'yes_bid': 44,
            'yes_ask': 46,
            'volume_24h': 10000
        }
        
        # Initialize market state with enough data (needs at least 3 price snapshots)
        current_time = datetime.now(timezone.utc)
        for i in range(3):
            scanner._update_market_state(market, current_time - timedelta(minutes=i))
        
        # Should not call REST when WS healthy (uses state_store)
        mock_api.get_orderbook.reset_mock()
        scanner._update_market_state(market, current_time)
        assert not mock_api.get_orderbook.called
        
        # Test with unhealthy WS
        state_store.ws_healthy = False
        scanner._update_market_state(market, current_time)
        
        # Should call REST for orderbook when WS unhealthy
        assert mock_api.get_orderbook.called
    
    def test_scanner_direct_price_update(self, config, state_store, mock_ws_client,
                                         mock_api, mock_discovery):
        """Verify scanner updates MarketState.mid_prices directly from WebSocket ticker updates"""
        from kalshi_mm.volatility import VolatilityScanner
        from kalshi_mm.volatility.models import MarketState
        from datetime import datetime, timezone
        
        ticker = 'TEST-TICKER'
        
        state_store.ws_healthy = True
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Initialize market state (required before WebSocket updates)
        market = {
            'ticker': ticker,
            'status': 'active',
            'yes_bid': 44,
            'yes_ask': 46,
            'volume_24h': 5000.0
        }
        scanner._update_market_state(market, datetime.now(timezone.utc))
        
        # Verify market state exists
        assert ticker in scanner.market_states
        state = scanner.market_states[ticker]
        initial_points = len(state.mid_prices)
        
        # Simulate WebSocket ticker updates - should update MarketState directly
        for i in range(20):
            msg = create_mock_ticker_message(
                ticker,
                yes_bid=44 + i,
                yes_ask=46 + i
            )
            # Call _on_ticker_update directly (simulating WebSocket callback)
            scanner._on_ticker_update(ticker, 'ticker', msg['msg'])
        
        # Verify price points were added directly to MarketState
        assert len(state.mid_prices) == initial_points + 20
        assert len(state.spreads) == initial_points + 20
        assert len(state.timestamps) == initial_points + 20
    
    def test_scanner_orderbook_usage(self, config, state_store, mock_ws_client,
                                    mock_api, mock_discovery):
        """Verify scanner uses orderbook from store for depth calculations"""
        from kalshi_mm.volatility import VolatilityScanner
        
        ticker = 'TEST-TICKER'
        
        # Add orderbook to store
        ob_msg = create_mock_orderbook_message(
            ticker,
            bids=[[45, 100], [44, 200], [43, 300]],
            asks=[[46, 100], [47, 200], [48, 300]]
        )
        state_store.update_orderbook(ticker, ob_msg['msg'])
        state_store.ws_healthy = True
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Scanner should read orderbook
        ob = state_store.get_orderbook(ticker)
        assert ob is not None
        assert 'bids' in ob
        assert 'asks' in ob
        assert 'imbalance' in ob
    
    def test_scanner_normalized_ticker(self, config, state_store, mock_ws_client,
                                      mock_api, mock_discovery):
        """Verify scanner uses get_normalized_ticker() helper"""
        from kalshi_mm.volatility import VolatilityScanner
        
        ticker = 'TEST-TICKER'
        
        # Add ticker with string prices
        msg = {
            'market_ticker': ticker,
            'yes_bid': '44',  # String cents
            'yes_ask': '46',
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        state_store.update_ticker(ticker, msg)
        state_store.ws_healthy = True
        
        scanner = VolatilityScanner(
            api=mock_api,
            discovery=mock_discovery,
            config=config,
            ws_client=mock_ws_client,
            state_store=state_store
        )
        
        # Scanner should use normalized ticker
        normalized = state_store.get_normalized_ticker(ticker)
        assert normalized is not None
        assert normalized['yes_bid'] == 0.44
        assert normalized['yes_ask'] == 0.46

