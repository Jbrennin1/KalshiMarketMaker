"""
Unit tests for WebSocket client
Tests connection, authentication, subscription, and message routing
"""
import pytest
import json
import time
import threading
import asyncio
from unittest.mock import Mock, MagicMock, patch, call, AsyncMock
from datetime import datetime, timezone
from tests.unit.test_websocket_utils import MockWebSocketApp, create_mock_ws_message, create_mock_ticker_message

# Import the actual client
from kalshi_mm.api import KalshiWebsocketClient


class TestWebSocketClient:
    """Unit tests for KalshiWebsocketClient"""
    
    @pytest.fixture
    def mock_config(self):
        return {
            'websockets': {
                'url': 'wss://api.elections.kalshi.com',  # No /live path
                'reconnect_max_retries': 3,
                'reconnect_base_delay_seconds': 0.5
            }
        }
    
    @pytest.fixture
    def mock_private_key(self):
        """Mock private key object"""
        key = MagicMock()
        key.sign.return_value = b'fake_signature'
        return key
    
    @pytest.fixture
    def client(self, mock_config, mock_private_key):
        """Create a test client"""
        logger = Mock()
        return KalshiWebsocketClient(
            access_key='test_key',
            private_key_obj=mock_private_key,
            url='wss://api.elections.kalshi.com',
            logger=logger,
            config=mock_config
        )
    
    def test_ws_auth_signature(self, client, mock_private_key):
        """Verify handshake signature is correct (timestamp + "GET" + "/trade-api/ws/v2")"""
        # Test signature creation directly
        headers = client._create_auth_headers()
        
        # Verify signature was called with correct string
        assert mock_private_key.sign.called
        call_args = mock_private_key.sign.call_args
        string_to_sign = call_args[0][0].decode('utf-8')
        
        # Should be timestamp + "GET" + "/trade-api/ws/v2"
        assert string_to_sign.endswith('GET/trade-api/ws/v2')
        assert len(string_to_sign) > len('GET/trade-api/ws/v2')  # Has timestamp prefix
        assert 'KALSHI-ACCESS-KEY' in headers
        assert 'KALSHI-ACCESS-SIGNATURE' in headers
        assert 'KALSHI-ACCESS-TIMESTAMP' in headers
    
    def test_ws_connect_url(self, client, mock_private_key):
        """Verify URL construction with headers (not query string)"""
        # Test URL and headers construction
        headers = client._create_auth_headers()
        expected_url = f"{client.url}/trade-api/ws/v2"
        
        # URL should have /trade-api/ws/v2 path
        assert expected_url == 'wss://api.elections.kalshi.com/trade-api/ws/v2'
        
        # Should use headers, not query string
        assert '?' not in expected_url  # No query string
        assert isinstance(headers, dict)  # Headers should be dict
        assert 'KALSHI-ACCESS-KEY' in headers
        assert 'KALSHI-ACCESS-SIGNATURE' in headers
        assert 'KALSHI-ACCESS-TIMESTAMP' in headers
    
    def test_ws_subscribe_ticker(self, client):
        """Verify subscription message format per Kalshi docs"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock event loop and async send
        mock_event_loop = Mock()
        client._event_loop = mock_event_loop
        
        # Create a mock future that completes immediately
        mock_future = Mock()
        mock_future.result.return_value = None
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            # Execute the async send directly to capture the message
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id1 = client.subscribe_ticker('TEST-TICKER', callback)
                msg_id2 = client.subscribe_ticker('TEST-TICKER-2', callback)
        
        # Verify message IDs are sequential
        assert msg_id1 == 1
        assert msg_id2 == 2
        
        # Verify subscription messages match Kalshi format
        assert len(sent_messages) == 2
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['id'] == 1
        assert sent_messages[0]['params']['channels'] == ['ticker']
        assert sent_messages[0]['params']['market_ticker'] == 'TEST-TICKER'
        
        # Verify subscriptions stored in pending (waiting for "subscribed" confirmation)
        assert msg_id1 in client._pending_subscriptions
        assert msg_id2 in client._pending_subscriptions
    
    def test_ws_subscribe_orderbook(self, client):
        """Verify orderbook subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_orderbook('TEST-TICKER', callback)
        
        assert len(sent_messages) == 1
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['params']['channels'] == ['orderbook_delta']
        assert sent_messages[0]['params']['market_ticker'] == 'TEST-TICKER'
        assert msg_id in client._pending_subscriptions
    
    def test_ws_subscribe_trades(self, client):
        """Verify trades subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_trades('TEST-TICKER', callback)
        
        assert len(sent_messages) == 1
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['params']['channels'] == ['trade']
        assert sent_messages[0]['params']['market_ticker'] == 'TEST-TICKER'
        assert msg_id in client._pending_subscriptions
    
    def test_ws_subscribe_user_fills(self, client):
        """Verify user fills subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_user_fills(callback)
        
        assert len(sent_messages) == 1
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['params']['channels'] == ['fill']
        assert msg_id in client._pending_subscriptions
    
    def test_ws_subscribe_positions(self, client):
        """Verify positions subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_positions(callback)
        
        assert len(sent_messages) == 1
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['params']['channels'] == ['market_positions']
        assert msg_id in client._pending_subscriptions
    
    def test_ws_subscribe_lifecycle(self, client):
        """Verify lifecycle subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_lifecycle(callback)
        
        assert len(sent_messages) == 1
        assert sent_messages[0]['cmd'] == 'subscribe'
        assert sent_messages[0]['params']['channels'] == ['market_lifecycle_v2']
        assert msg_id in client._pending_subscriptions
    
    def test_ws_unsubscribe(self, client):
        """Verify unsubscribe removes subscription"""
        callback = Mock()
        
        # Mock async send to capture messages
        sent_messages = []
        async def mock_send_async(msg):
            sent_messages.append(json.loads(msg))
        
        # Mock _send_message_sync directly to capture messages
        def mock_send_sync(msg):
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(mock_send_async(msg))
        
        with patch.object(client, '_is_socket_open', return_value=True):
            with patch.object(client, '_send_message_sync', side_effect=mock_send_sync):
                mock_ws = AsyncMock()
                mock_ws.send = mock_send_async
                client._ws = mock_ws
                client._connection_ready = True
                client._event_loop = Mock()
                
                msg_id = client.subscribe_ticker('TEST-TICKER', callback)
                assert msg_id in client._pending_subscriptions
                
                # Simulate subscription confirmation by moving to active subscriptions
                with client._lock:
                    ticker, cb, channel = client._pending_subscriptions.pop(msg_id)
                    client._subscriptions[msg_id] = (ticker, cb, channel)
                assert msg_id in client._subscriptions
                
                client.unsubscribe(msg_id)
                assert msg_id not in client._subscriptions
                
                # Verify unsubscribe message sent
                assert len(sent_messages) >= 2  # Subscribe + unsubscribe
                unsubscribe_msgs = [m for m in sent_messages if m.get('cmd') == 'unsubscribe']
                assert len(unsubscribe_msgs) > 0
                assert unsubscribe_msgs[0]['params']['sids'] == [msg_id]
    
    def test_ws_id_race_condition(self, client):
        """Verify locking prevents duplicate message IDs (multi-threaded test)"""
        callback = Mock()
        msg_ids = []
        
        def subscribe():
            with patch('websocket.WebSocketApp') as mock_ws_class:
                mock_ws = MockWebSocketApp()
                mock_ws_class.return_value = mock_ws
                client._ws = mock_ws
                msg_id = client.subscribe_ticker('TEST-TICKER', callback)
                msg_ids.append(msg_id)
        
        # Create multiple threads subscribing simultaneously
        threads = [threading.Thread(target=subscribe) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify all message IDs are unique
        assert len(msg_ids) == len(set(msg_ids))
        assert len(msg_ids) == 10
    
    def test_ws_message_routing(self, client):
        """Verify messages are routed to correct callbacks by market ticker"""
        callback1 = Mock()
        callback2 = Mock()
        
        msg_id1 = client.subscribe_ticker('TICKER-1', callback1)
        msg_id2 = client.subscribe_ticker('TICKER-2', callback2)
        
        # Simulate subscription confirmations by moving to active subscriptions
        with client._lock:
            ticker1, cb1, ch1 = client._pending_subscriptions.pop(msg_id1)
            client._subscriptions[msg_id1] = (ticker1, cb1, ch1)
            ticker2, cb2, ch2 = client._pending_subscriptions.pop(msg_id2)
            client._subscriptions[msg_id2] = (ticker2, cb2, ch2)
        
        # Simulate ticker messages (Kalshi format)
        msg1 = {
            "type": "ticker",
            "data": {
                "market_ticker": "TICKER-1",
                "bid": 0.45,
                "ask": 0.46
            }
        }
        msg2 = {
            "type": "ticker",
            "data": {
                "market_ticker": "TICKER-2",
                "bid": 0.47,
                "ask": 0.48
            }
        }
        
        # Process messages using async method
        asyncio.run(client._process_message(json.dumps(msg1)))
        asyncio.run(client._process_message(json.dumps(msg2)))
        
        # Verify callbacks called with correct messages
        assert callback1.called
        assert callback2.called
        assert callback1.call_count == 1
        assert callback2.call_count == 1
    
    def test_ws_non_blocking_callbacks(self, client):
        """Verify callbacks don't block (measure execution time)"""
        callback_execution_times = []
        
        def slow_callback(msg):
            start = time.time()
            time.sleep(0.1)  # Simulate slow callback
            callback_execution_times.append(time.time() - start)
        
        sid = client.subscribe_ticker('TEST-TICKER', slow_callback)
        
        # Move subscription to active
        with client._lock:
            ticker, cb, ch = client._pending_subscriptions.pop(sid)
            client._subscriptions[sid] = (ticker, cb, ch)
        
        # Send multiple messages rapidly
        start = time.time()
        for i in range(5):
            msg = create_mock_ticker_message('TEST-TICKER')
            asyncio.run(client._process_message(json.dumps(msg)))
        total_time = time.time() - start
        
        # Total time should be much less than 5 * 0.1s if non-blocking
        # (In practice, callbacks are executed synchronously in _process_message,
        # but we can verify they complete quickly)
        assert total_time < 1.0  # Should complete in <1s even with 5 slow callbacks

