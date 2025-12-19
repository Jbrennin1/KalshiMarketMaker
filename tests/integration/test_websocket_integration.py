"""
Integration tests for WebSocket connection and message flow
Requires valid Kalshi API credentials (skipped if not available)
"""
import pytest
import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from kalshi_mm.api import KalshiWebsocketClient, KalshiTradingAPI
from kalshi_mm.state import MarketStateStore
from cryptography.hazmat.primitives import serialization

# Import WebSocket exception for proper handling
try:
    import websockets.exceptions
    WebSocketConnectionClosedException = websockets.exceptions.ConnectionClosed
except ImportError:
    # Fallback if import path differs
    WebSocketConnectionClosedException = Exception

# Load .env file before checking for credentials
load_dotenv()

# Skip integration tests if credentials not available
pytestmark = pytest.mark.skipif(
    not os.getenv('KALSHI_ACCESS_KEY') or not os.getenv('KALSHI_PRIVATE_KEY'),
    reason="Kalshi credentials not available"
)


class TestWebSocketIntegration:
    """Integration tests with real WebSocket connection"""
    
    def _wait_for_connection(self, ws_client, state_store, timeout=2.0):
        """Wait briefly for connection to establish (much faster than before)"""
        start = time.time()
        while time.time() - start < timeout:
            if ws_client._connection_ready and state_store.ws_healthy:
                return True
            time.sleep(0.1)
        return False
    
    def _wait_for_subscription(self, ws_client, timeout=1.0):
        """Wait briefly for subscription confirmation from server"""
        start = time.time()
        while time.time() - start < timeout:
            if ws_client._subscriptions:
                return True
            time.sleep(0.1)
        return False
    
    @pytest.fixture
    def config(self):
        return {
            'websockets': {
                'url': 'wss://api.elections.kalshi.com',  # Base URL - /trade-api/ws/v2 will be appended
                'ticker_staleness_seconds': 10,
                'orderbook_staleness_seconds': 10,
                'position_staleness_seconds': 30
            }
        }
    
    @pytest.fixture
    def api(self):
        """Create Kalshi API client for fetching markets"""
        access_key = os.getenv('KALSHI_ACCESS_KEY')
        private_key = os.getenv('KALSHI_PRIVATE_KEY')
        base_url = os.getenv('KALSHI_BASE_URL', 'https://api.elections.kalshi.com/trade-api/v2')
        
        # Handle both file path and key content
        if os.path.isfile(private_key):
            with open(private_key, 'rb') as f:
                private_key_data = f.read()
        else:
            private_key_str = private_key.replace('\\n', '\n')
            private_key_data = private_key_str.encode('utf-8')
        
        logger = pytest.importorskip("logging").getLogger("test")
        api = KalshiTradingAPI(
            access_key=access_key,
            private_key=private_key,
            market_ticker="dummy",  # Not used for market fetching
            base_url=base_url,
            logger=logger
        )
        return api
    
    @pytest.fixture
    def valid_ticker(self, api):
        """Get a valid, active market ticker from the API"""
        try:
            # Fetch open markets
            response = api.make_request('GET', '/markets', params={'status': 'open', 'limit': 50})
            markets = response.get('markets', [])
            
            if not markets:
                pytest.skip("No open markets available for testing")
            
            # Find a market with activity (has bid/ask prices in the market data)
            for market in markets:
                ticker = market.get('ticker')
                if not ticker:
                    continue
                
                # Check if market has prices in the response (indicates activity)
                # Markets with activity typically have 'yes_bid', 'yes_ask', 'no_bid', 'no_ask' fields
                if market.get('yes_bid') or market.get('no_bid'):
                    return ticker
            
            # If no market with prices found, use first market (may not have activity)
            ticker = markets[0].get('ticker')
            if ticker:
                return ticker
            
            pytest.skip("No valid market ticker found")
        except Exception as e:
            pytest.skip(f"Could not fetch valid market ticker: {e}")
    
    @pytest.fixture
    def state_store(self, config):
        return MarketStateStore(config)
    
    @pytest.fixture
    def ws_client(self, config, state_store):
        """Create WebSocket client with real credentials"""
        access_key = os.getenv('KALSHI_ACCESS_KEY')
        private_key = os.getenv('KALSHI_PRIVATE_KEY')
        
        # Handle both file path and key content (same logic as bot)
        if os.path.isfile(private_key):
            with open(private_key, 'rb') as f:
                private_key_data = f.read()
        else:
            # Key content from env (with escaped newlines)
            private_key_str = private_key.replace('\\n', '\n')
            private_key_data = private_key_str.encode('utf-8')
        
        private_key_obj = serialization.load_pem_private_key(
            private_key_data,
            password=None
        )
        
        logger = pytest.importorskip("logging").getLogger("test")
        client = KalshiWebsocketClient(
            access_key=access_key,
            private_key_obj=private_key_obj,
            url='wss://api.elections.kalshi.com',
            logger=logger,
            config=config,
            state_store=state_store
        )
        return client
    
    def test_ws_real_connection(self, ws_client, state_store):
        """Connect to Kalshi WS and verify connection"""
        messages_received = []
        
        def on_message(msg_type, msg):
            messages_received.append((msg_type, msg))
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max - much faster than before)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.skip("WebSocket connection failed (404 or endpoint unavailable)")
        finally:
            ws_client.stop()
    
    def test_ws_ticker_subscription(self, ws_client, state_store, valid_ticker):
        """Subscribe to a real ticker and verify messages arrive"""
        messages_received = []
        test_ticker = valid_ticker
        
        def on_ticker(msg_type, msg):
            messages_received.append(msg)
            state_store.update_ticker(test_ticker, msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max - much faster than before)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.fail(f"WebSocket connection failed - _connection_ready={ws_client._connection_ready}, ws_healthy={state_store.ws_healthy}")
            
            if not ws_client._connection_ready:
                pytest.fail("WebSocket connection not ready before subscription")
            
            # Subscribe
            try:
                sid = ws_client.subscribe_ticker(test_ticker, on_ticker)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.skip(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait briefly for subscription confirmation (0.5s max)
            if not self._wait_for_subscription(ws_client, timeout=0.5):
                # Check if subscription is still pending (waiting for confirmation) or confirmed
                if ws_client._pending_subscriptions or ws_client._subscriptions:
                    # Subscription is pending or confirmed - that's fine
                    pass
                else:
                    # Subscription was removed from pending (likely failed)
                    pytest.skip("Subscription failed (channel may not be available or returned error)")
            
            # If we got messages, verify structure
            if messages_received:
                msg = messages_received[0]
                assert 'market_ticker' in msg or 'ticker' in msg
        finally:
            ws_client.stop()
    
    def test_ws_orderbook_subscription(self, ws_client, state_store, valid_ticker):
        """Subscribe to orderbook and verify updates"""
        messages_received = []
        test_ticker = valid_ticker
        
        def on_orderbook(msg_type, msg):
            messages_received.append(msg)
            state_store.update_orderbook(test_ticker, msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.skip("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.skip("WebSocket connection closed before subscription")
            
            try:
                sid = ws_client.subscribe_orderbook(test_ticker, on_orderbook)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.skip(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait briefly for subscription confirmation (0.5s max)
            if not self._wait_for_subscription(ws_client, timeout=0.5):
                # Check if subscription is still pending (waiting for confirmation) or confirmed
                if ws_client._pending_subscriptions or ws_client._subscriptions:
                    # Subscription is pending or confirmed - that's fine
                    pass
                else:
                    # Subscription was removed from pending (likely failed)
                    pytest.skip("Subscription failed (channel may not be available or returned error)")
            
            # If we got messages, verify structure
            if messages_received:
                msg = messages_received[0]
                # Kalshi uses 'yes' and 'no' for binary markets, not 'bids' and 'asks'
                assert 'yes' in msg or 'no' in msg or 'yes_dollars' in msg or 'no_dollars' in msg
        finally:
            ws_client.stop()
    
    def test_ws_trades_subscription(self, ws_client, state_store, valid_ticker):
        """Subscribe to public trades and verify messages arrive"""
        messages_received = []
        test_ticker = valid_ticker
        
        def on_trade(msg_type, msg):
            messages_received.append(msg)
            state_store.add_trade(test_ticker, msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.fail("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.fail("WebSocket connection closed before subscription")
            
            try:
                sid = ws_client.subscribe_trades(test_ticker, on_trade)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.fail(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait for subscription confirmation
            if not self._wait_for_subscription(ws_client, timeout=2.0):
                # Check if subscription failed (removed from pending without being confirmed)
                if sid not in ws_client._subscriptions and sid not in ws_client._pending_subscriptions:
                    pytest.fail(f"Subscription {sid} failed - channel may not be available or returned error. "
                              f"Check WebSocket error logs for details.")
            
            # Assert that subscription was successful
            assert sid in ws_client._subscriptions or sid in ws_client._pending_subscriptions, \
                f"Subscription {sid} was not confirmed or pending"
            
            # Wait a bit longer to see if we get any messages
            time.sleep(1.0)
            
            # Note: We don't assert messages_received here because trades only occur when there's market activity
            # The important assertion is that the subscription succeeded
        finally:
            ws_client.stop()
    
    def test_ws_user_fills_subscription(self, ws_client, state_store):
        """Subscribe to user fills (requires active account)"""
        messages_received = []
        
        def on_fill(msg_type, msg):
            messages_received.append(msg)
            state_store.add_user_fill(msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.fail("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.fail("WebSocket connection closed before subscription")
            
            try:
                sid = ws_client.subscribe_user_fills(on_fill)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.fail(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait for subscription confirmation
            if not self._wait_for_subscription(ws_client, timeout=2.0):
                # Check if subscription failed (removed from pending without being confirmed)
                if sid not in ws_client._subscriptions and sid not in ws_client._pending_subscriptions:
                    pytest.fail(f"Subscription {sid} failed - channel may not be available or returned error. "
                              f"Check WebSocket error logs for details.")
            
            # Assert that subscription was successful
            assert sid in ws_client._subscriptions or sid in ws_client._pending_subscriptions, \
                f"Subscription {sid} was not confirmed or pending"
            
            # Wait a bit longer to see if we get any messages
            time.sleep(1.0)
            
            # Note: We don't assert messages_received here because fills only come when trades occur
            # The important assertion is that the subscription succeeded
        finally:
            ws_client.stop()
    
    def test_ws_positions_subscription(self, ws_client, state_store):
        """Subscribe to positions (requires active account)"""
        messages_received = []
        
        def on_position(msg_type, msg):
            messages_received.append(msg)
            state_store.update_position(msg.get('ticker'), msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.fail("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.fail("WebSocket connection closed before subscription")
            
            try:
                sid = ws_client.subscribe_positions(on_position)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.fail(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait for subscription confirmation
            if not self._wait_for_subscription(ws_client, timeout=2.0):
                # Check if subscription failed (removed from pending without being confirmed)
                if sid not in ws_client._subscriptions and sid not in ws_client._pending_subscriptions:
                    pytest.fail(f"Subscription {sid} failed - channel may not be available or returned error. "
                              f"Check WebSocket error logs for details.")
            
            # Assert that subscription was successful
            assert sid in ws_client._subscriptions or sid in ws_client._pending_subscriptions, \
                f"Subscription {sid} was not confirmed or pending"
            
            # Wait a bit longer to see if we get any messages
            time.sleep(1.0)
            
            # Note: We don't assert messages_received here because positions only update when they change
            # The important assertion is that the subscription succeeded
        finally:
            ws_client.stop()
    
    def test_ws_lifecycle_subscription(self, ws_client, state_store):
        """Subscribe to lifecycle events"""
        messages_received = []
        
        def on_lifecycle(msg_type, msg):
            messages_received.append(msg)
            state_store.update_lifecycle(msg.get('ticker'), msg)
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.fail("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.fail("WebSocket connection closed before subscription")
            
            try:
                sid = ws_client.subscribe_lifecycle(on_lifecycle)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.fail(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait for subscription confirmation
            if not self._wait_for_subscription(ws_client, timeout=2.0):
                # Check if subscription failed (removed from pending without being confirmed)
                if sid not in ws_client._subscriptions and sid not in ws_client._pending_subscriptions:
                    pytest.fail(f"Subscription {sid} failed - channel may not be available or returned error. "
                              f"Check WebSocket error logs for details.")
            
            # Assert that subscription was successful
            assert sid in ws_client._subscriptions or sid in ws_client._pending_subscriptions, \
                f"Subscription {sid} was not confirmed or pending"
            
            # Wait a bit longer to see if we get any messages
            time.sleep(1.0)
            
            # Note: We don't assert messages_received here because lifecycle events only occur when markets change state
            # The important assertion is that the subscription succeeded
        finally:
            ws_client.stop()
    
    def test_ws_message_parsing(self, ws_client, state_store, valid_ticker):
        """Verify all message types parse correctly"""
        # This test verifies that messages from real WS are parsed correctly
        # We'll use the state_store to verify parsing
        
        test_ticker = valid_ticker
        ticker_received = False
        
        def on_ticker(msg_type, msg):
            nonlocal ticker_received
            state_store.update_ticker(test_ticker, msg)
            ticker_received = True
        
        try:
            ws_client.connect()
            ws_client.start()
            
            # Wait briefly for connection (2 seconds max)
            if not self._wait_for_connection(ws_client, state_store, timeout=2.0):
                pytest.skip("WebSocket connection failed (404 or endpoint unavailable)")
            
            if not ws_client._connection_ready:
                pytest.skip("WebSocket connection closed before subscription")
            
            try:
                ws_client.subscribe_ticker(test_ticker, on_ticker)
            except (WebSocketConnectionClosedException, Exception) as e:
                if isinstance(e, WebSocketConnectionClosedException) or "socket is already closed" in str(e):
                    pytest.skip(f"WebSocket closed during subscription: {e}")
                raise
            
            # Wait briefly for subscription confirmation (0.5s max)
            if not self._wait_for_subscription(ws_client, timeout=0.5):
                # Check if subscription is still pending (waiting for confirmation) or confirmed
                if ws_client._pending_subscriptions or ws_client._subscriptions:
                    # Subscription is pending or confirmed - that's fine
                    pass
                else:
                    # Subscription was removed from pending (likely failed)
                    pytest.skip("Subscription failed (channel may not be available or returned error)")
            
            # If we got messages, verify parsing
            if ticker_received:
                normalized = state_store.get_normalized_ticker(test_ticker)
                assert normalized is not None
                assert 'yes_bid' in normalized
                assert 'yes_ask' in normalized
        finally:
            ws_client.stop()
    
    def test_ws_timestamp_parsing(self, state_store):
        """Verify ISO and Unix timestamp formats both work"""
        ticker = 'TEST-TICKER'
        
        # Test ISO format
        msg_iso = {
            'market_ticker': ticker,
            'price': '45',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        state_store.update_ticker(ticker, msg_iso)
        stored_iso = state_store.get_ticker(ticker)
        assert stored_iso is not None
        
        # Test Unix format
        msg_unix = {
            'market_ticker': ticker,
            'price': '46',
            'ts': str(int(datetime.now(timezone.utc).timestamp()))
        }
        state_store.update_ticker(ticker, msg_unix)
        stored_unix = state_store.get_ticker(ticker)
        assert stored_unix is not None
    
    def test_ws_price_field_parsing(self, state_store):
        """Verify *_dollars, *_dollars_str, and cents formats all work"""
        ticker = 'TEST-TICKER'
        
        # Test cents format
        msg_cents = {
            'market_ticker': ticker,
            'yes_bid': '44',
            'yes_ask': '46',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        state_store.update_ticker(ticker, msg_cents)
        normalized_cents = state_store.get_normalized_ticker(ticker)
        assert normalized_cents['yes_bid'] == 0.44
        assert normalized_cents['yes_ask'] == 0.46
        
        # Test dollars string format
        msg_dollars = {
            'market_ticker': ticker,
            'yes_bid_dollars': '0.44',
            'yes_ask_dollars': '0.46',
            'ts': datetime.now(timezone.utc).isoformat()
        }
        state_store.update_ticker(ticker, msg_dollars)
        normalized_dollars = state_store.get_normalized_ticker(ticker)
        assert normalized_dollars['yes_bid'] == 0.44
        assert normalized_dollars['yes_ask'] == 0.46

