"""
Kalshi WebSocket Client
Handles real-time WebSocket connections to Kalshi API

Uses websockets library (async) as specified in Kalshi documentation.
The websockets library automatically handles ping/pong frames.
"""
import websockets
import asyncio
import json
import threading
import time
import logging
from typing import Dict, Callable, Optional, List
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import base64


class KalshiWebsocketClient:
    def __init__(
        self,
        access_key: str,
        private_key_obj,
        url: str,
        logger: logging.Logger,
        config: Dict,
        state_store: Optional[object] = None
    ):
        self.url = url  # e.g. wss://api.elections.kalshi.com
        self.access_key = access_key
        self.private_key_obj = private_key_obj  # Loaded private key object
        self.logger = logger
        self.config = config.get('websockets', {})
        self.state_store = state_store
        self._ws = None  # websockets.WebSocketServerProtocol object
        self._subscriptions = {}  # msg_id -> (ticker, callback, channel)
        self._pending_subscriptions = {}  # msg_id -> (ticker, callback, channel)
        self._pending_subscription_timestamps = {}  # msg_id -> timestamp when added to pending
        self._unsent_subscriptions = {}  # msg_id -> (ticker, callback, channel, subscription_json) - subscriptions that failed to send
        self._next_id = 1  # Message ID counter
        self._running = False
        self._reconnect_attempts = 0
        self._lock = threading.Lock()
        self._connection_ready = False
        self._event_loop = None  # asyncio event loop
        self._loop_thread = None  # Thread running the event loop
        self._receive_task = None  # asyncio task for receive loop
        
        # Message rate tracking for summary logging
        self._message_count = 0
        self._last_summary_time = time.time()
        self._summary_interval = 60  # Log summary every 60 seconds
        
        # Message rate tracking for summary logging
        self._message_count = 0
        self._last_summary_time = time.time()
        self._summary_interval = 60  # Log summary every 60 seconds
        
    def _is_socket_open(self) -> bool:
        """Check if WebSocket socket is actually open
        
        Primary check: _connection_ready flag (set in async context when connection succeeds)
        Secondary check: websocket state (but don't fail if we can't check it from sync thread)
        """
        # Primary check: _connection_ready flag
        if not self._connection_ready:
            return False
        if not self._ws:
            return False
        # Secondary check: websocket state (but don't fail if we can't check it)
        try:
            # websockets library uses closed property (inverted)
            if hasattr(self._ws, 'closed'):
                return not self._ws.closed
            # Fallback: check open property
            if hasattr(self._ws, 'open'):
                return self._ws.open
        except Exception:
            # If we can't check the property (e.g., from wrong thread), trust _connection_ready
            # This can happen when checking from sync thread while websocket is in async context
            pass
        # If _connection_ready is True and _ws exists, trust it
        return True
        
    def _sign_ws_handshake(self, string_to_sign: str) -> str:
        """Sign WebSocket handshake using RSA-PSS (same as REST)"""
        signature = self.private_key_obj.sign(
            string_to_sign.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    
    def _create_auth_headers(self) -> Dict[str, str]:
        """Create authentication headers for WebSocket connection
        
        Per Kalshi docs:
        - Signature: timestamp + "GET" + "/trade-api/ws/v2"
        - Headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP
        """
        timestamp = str(int(time.time() * 1000))
        path = "/trade-api/ws/v2"
        string_to_sign = f"{timestamp}GET{path}"
        
        self.logger.debug(f"Signing WebSocket handshake: '{string_to_sign}'")
        self.logger.debug(f"Signature components: timestamp='{timestamp}', method='GET', path='{path}'")
        
        signature = self._sign_ws_handshake(string_to_sign)
        
        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.access_key,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }
        
        return headers
    
    async def _connect_async(self):
        """Establish WebSocket connection (async)"""
        self.logger.debug("=== _connect_async() method called ===")
        self._connection_ready = False
        
        # Create auth headers
        headers = self._create_auth_headers()
        
        # Full URL with path
        ws_url = f"{self.url}/trade-api/ws/v2"
        
        self.logger.debug(f"Attempting WebSocket connection to: {ws_url}")
        self.logger.debug(f"Auth headers: KALSHI-ACCESS-KEY={self.access_key[:8]}..., TIMESTAMP={headers['KALSHI-ACCESS-TIMESTAMP']}, SIGNATURE={headers['KALSHI-ACCESS-SIGNATURE'][:20]}...")
        self.logger.debug(f"All headers: {list(headers.keys())}")
        
        try:
            # Use websockets.connect with additional_headers as per Kalshi docs
            self._ws = await websockets.connect(
                ws_url,
                additional_headers=headers
            )
            self.logger.info("WebSocket connection established successfully")
            self._connection_ready = True
            if self.state_store:
                self.state_store.ws_healthy = True
                self.state_store.last_message_time = datetime.now(timezone.utc)
            self._reconnect_attempts = 0
            
            # Retry any subscriptions that failed to send before connection was ready
            await self._retry_unsent_subscriptions()
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}", exc_info=True)
            self._connection_ready = False
            if self.state_store:
                self.state_store.ws_healthy = False
            raise
    
    def connect(self):
        """Establish WebSocket connection (sync wrapper)
        
        Note: If event loop is not running, connection will be established
        automatically when start() is called.
        """
        self.logger.debug("=== connect() method called ===")
        if self._event_loop is None:
            self.logger.debug("Event loop not running, connection will be established when start() is called")
            return
        
        # Run async connect in event loop
        future = asyncio.run_coroutine_threadsafe(self._connect_async(), self._event_loop)
        try:
            future.result(timeout=10.0)  # Wait up to 10 seconds
        except Exception as e:
            self.logger.error(f"Connection failed: {e}", exc_info=True)
            raise
    
    async def _send_message_async(self, message: str):
        """Send message over WebSocket (async)"""
        if not self._ws or not self._connection_ready:
            raise Exception("WebSocket not connected")
        await self._ws.send(message)
    
    def _send_message_sync(self, message: str):
        """Send message over WebSocket (sync wrapper)"""
        if self._event_loop is None:
            raise Exception("Event loop not running")
        future = asyncio.run_coroutine_threadsafe(self._send_message_async(message), self._event_loop)
        future.result(timeout=5.0)
    
    def _wait_for_connection(self, timeout: float = 30.0) -> bool:
        """Wait for WebSocket connection to be ready
        
        Args:
            timeout: Maximum time to wait in seconds (default 30.0)
            
        Returns:
            True if connection became ready, False if timeout
        """
        start_time = time.time()
        poll_interval = 0.1  # Check every 100ms
        
        while time.time() - start_time < timeout:
            with self._lock:
                if self._connection_ready and self._is_socket_open():
                    return True
            time.sleep(poll_interval)
        
        # Timeout reached
        return False
    
    def _build_subscription_message(self, msg_id: int, channel: str, ticker: Optional[str] = None) -> str:
        """Rebuild subscription JSON message from channel type and ticker
        
        Args:
            msg_id: Message ID for the subscription
            channel: Channel type ('ticker', 'orderbook_delta', 'trades', 'fills', 'positions', 'lifecycle')
            ticker: Optional market ticker for market-specific subscriptions
            
        Returns:
            JSON string of the subscription message
        """
        # Map channel names to Kalshi channel names
        channel_map = {
            'ticker': 'ticker',
            'orderbook_delta': 'orderbook_delta',
            'trades': 'trade',
            'fills': 'fill',
            'positions': 'position',
            'lifecycle': 'lifecycle'
        }
        
        kalshi_channel = channel_map.get(channel, channel)
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": [kalshi_channel]
            }
        }
        
        # Add market_ticker for market-specific subscriptions
        if ticker:
            subscription["params"]["market_ticker"] = ticker
        
        return json.dumps(subscription)
    
    def subscribe_ticker(self, ticker: Optional[str] = None, callback: Optional[Callable] = None) -> int:
        """Subscribe to ticker channel
        
        Per Kalshi docs format:
        - Global: {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"]}}
        - Specific: {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_ticker": "..."}}
        """
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"]
            }
        }
        
        if ticker:
            subscription["params"]["market_ticker"] = ticker
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing ticker subscription {msg_id} (ticker={ticker}): {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for ticker subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for ticker subscription {msg_id} (ticker={ticker}). "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (ticker, callback, 'ticker', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent ticker subscription {msg_id} (ticker={ticker})")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (ticker, callback, 'ticker')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (ticker={ticker}, channel=ticker)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send ticker subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Ticker: {ticker}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (ticker, callback, 'ticker', msg_json)
        return msg_id
        
    def subscribe_orderbook(self, ticker: str, callback: Callable) -> int:
        """Subscribe to orderbook updates for specific market"""
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_ticker": ticker
            }
        }
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing orderbook subscription {msg_id} (ticker={ticker}): {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for orderbook subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for orderbook subscription {msg_id} (ticker={ticker}). "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (ticker, callback, 'orderbook_delta', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent orderbook subscription {msg_id} (ticker={ticker})")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (ticker, callback, 'orderbook_delta')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (ticker={ticker}, channel=orderbook_delta)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send orderbook subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Ticker: {ticker}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (ticker, callback, 'orderbook_delta', msg_json)
        return msg_id
    
    def subscribe_trades(self, ticker: str, callback: Callable) -> int:
        """Subscribe to public trades for specific market"""
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["trade"],
                "market_ticker": ticker
            }
        }
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing trades subscription {msg_id} (ticker={ticker}): {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for trades subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for trades subscription {msg_id} (ticker={ticker}). "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (ticker, callback, 'trades', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent trades subscription {msg_id} (ticker={ticker})")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (ticker, callback, 'trades')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (ticker={ticker}, channel=trades)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send trades subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Ticker: {ticker}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (ticker, callback, 'trades', msg_json)
        return msg_id
    
    def subscribe_user_fills(self, callback: Callable) -> int:
        """Subscribe to user fills (authenticated channel)"""
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["fill"]
            }
        }
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing fills subscription {msg_id}: {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for fills subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for fills subscription {msg_id}. "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (None, callback, 'fills', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent fills subscription {msg_id}")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (None, callback, 'fills')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (channel=fills)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send fills subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (None, callback, 'fills', msg_json)
        return msg_id
    
    def subscribe_positions(self, callback: Callable) -> int:
        """Subscribe to market positions (authenticated channel)"""
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["market_positions"]
            }
        }
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing positions subscription {msg_id}: {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for positions subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for positions subscription {msg_id}. "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (None, callback, 'positions', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent positions subscription {msg_id}")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (None, callback, 'positions')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (channel=positions)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send positions subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (None, callback, 'positions', msg_json)
        return msg_id
    
    def subscribe_lifecycle(self, callback: Callable) -> int:
        """Subscribe to market lifecycle events"""
        with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            # DON'T add to _pending_subscriptions yet - only after we successfully send
        
        subscription = {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["market_lifecycle_v2"]
            }
        }
        
        msg_json = json.dumps(subscription)
        self.logger.info(f"Preparing lifecycle subscription {msg_id}: {msg_json}")
        
        # Wait for connection if not ready
        if not self._is_socket_open():
            self.logger.warning(f"Connection not ready for lifecycle subscription {msg_id}, waiting...")
            if not self._wait_for_connection(timeout=30.0):
                self.logger.error(
                    f"TIMEOUT waiting for connection for lifecycle subscription {msg_id}. "
                    f"Connection ready: {self._connection_ready}, Socket exists: {self._ws is not None}. "
                    f"Will retry later."
                )
                with self._lock:
                    self._unsent_subscriptions[msg_id] = (None, callback, 'lifecycle', msg_json)
                return msg_id
        
        try:
            self._send_message_sync(msg_json)
            self.logger.info(f"Successfully sent lifecycle subscription {msg_id}")
            # NOW add to pending - we've actually sent it and are waiting for confirmation
            with self._lock:
                self._pending_subscriptions[msg_id] = (None, callback, 'lifecycle')
                self._pending_subscription_timestamps[msg_id] = time.time()
                self._unsent_subscriptions.pop(msg_id, None)  # Remove from unsent if it was there
            self.logger.info(f"Subscription {msg_id} added to pending, waiting for confirmation (channel=lifecycle)")
            # Rate limiting: small delay between subscriptions to avoid overwhelming Kalshi
            time.sleep(0.05)  # 50ms delay between subscriptions
        except Exception as e:
            self.logger.error(
                f"FAILED to send lifecycle subscription {msg_id}: {e}\n"
                f"  Message: {msg_json}\n"
                f"  Connection ready: {self._connection_ready}, Socket open: {self._is_socket_open()}",
                exc_info=True
            )
            # Add to unsent for retry - but NOT to pending since we didn't send it
            with self._lock:
                self._unsent_subscriptions[msg_id] = (None, callback, 'lifecycle', msg_json)
        return msg_id
    
    async def _retry_unsent_subscriptions(self):
        """Retry all subscriptions that failed to send when socket wasn't ready
        
        Also retries old pending subscriptions (>30 seconds old) that were likely never sent.
        """
        current_time = time.time()
        old_pending_threshold = 30.0  # seconds
        
        # First, retry unsent subscriptions (new tracking)
        with self._lock:
            unsent_snapshot = list(self._unsent_subscriptions.items())
        
        if unsent_snapshot:
            retry_list = [f"{msg_id}({ch},{t or 'global'})" for msg_id, (t, _, ch, _) in unsent_snapshot]
            self.logger.info(f"Retrying {len(unsent_snapshot)} unsent subscriptions: {', '.join(retry_list)}")
            
            for msg_id, (ticker, callback, channel, msg_json) in unsent_snapshot:
                try:
                    await self._send_message_async(msg_json)
                    self.logger.info(f"Successfully retried subscription {msg_id} ({channel}, ticker={ticker or 'global'})")
                    # Remove from unsent - it will be moved to active when confirmed
                    with self._lock:
                        self._unsent_subscriptions.pop(msg_id, None)
                except Exception as e:
                    self.logger.error(
                        f"FAILED to retry subscription {msg_id} ({channel}, ticker={ticker or 'global'}): {e}\n"
                        f"  Message: {msg_json}",
                        exc_info=True
                    )
                    # Keep in unsent for next retry attempt
        
        # Second, check for old pending subscriptions that were likely never sent
        with self._lock:
            old_pending = []
            for msg_id, (ticker, callback, channel) in self._pending_subscriptions.items():
                timestamp = self._pending_subscription_timestamps.get(msg_id, 0)
                age = current_time - timestamp
                if age > old_pending_threshold:
                    old_pending.append((msg_id, ticker, callback, channel, age))
        
        if old_pending:
            retry_list = [f"{msg_id}({ch},{t or 'global'},{age:.1f}s)" for msg_id, t, _, ch, age in old_pending]
            self.logger.warning(f"Found {len(old_pending)} old pending subscriptions (>30s old), retrying: {', '.join(retry_list)}")
            
            for msg_id, ticker, callback, channel, age in old_pending:
                try:
                    # Rebuild the subscription message
                    msg_json = self._build_subscription_message(msg_id, channel, ticker)
                    await self._send_message_async(msg_json)
                    self.logger.info(
                        f"Retried old pending subscription {msg_id} ({channel}, ticker={ticker or 'global'}, age={age:.1f}s)"
                    )
                    # Update timestamp to prevent immediate re-retry
                    with self._lock:
                        self._pending_subscription_timestamps[msg_id] = current_time
                except Exception as e:
                    self.logger.error(
                        f"FAILED to retry old pending subscription {msg_id} ({channel}, ticker={ticker or 'global'}): {e}\n"
                        f"  Message: {msg_json}",
                        exc_info=True
                    )
    
    def unsubscribe(self, msg_id: int):
        """Unsubscribe from a channel"""
        with self._lock:
            if msg_id in self._subscriptions:
                unsubscribe_msg = {
                    "id": self._next_id,
                    "cmd": "unsubscribe",
                    "params": {
                        "sids": [msg_id]
                    }
                }
                self._next_id += 1
                if self._ws and self._connection_ready:
                    self._send_message_sync(json.dumps(unsubscribe_msg))
                del self._subscriptions[msg_id]
            if msg_id in self._pending_subscriptions:
                del self._pending_subscriptions[msg_id]
            if msg_id in self._pending_subscription_timestamps:
                del self._pending_subscription_timestamps[msg_id]
            if msg_id in self._unsent_subscriptions:
                del self._unsent_subscriptions[msg_id]
    
    async def _process_message(self, message: str):
        """Process incoming WebSocket message (async)
        
        Per Kalshi docs format:
        - Subscribed: {"type": "subscribed", "id": 1}
        - Ticker: {"type": "ticker", "data": {...}}
        - Orderbook: {"type": "orderbook_delta", "data": {...}}
        - Error: {"type": "error", "msg": {"code": 1, "msg": "..."}}
        """
        # Remove verbose per-message logging - too spammy
        # Only log at DEBUG level if needed for troubleshooting
        
        # Track message rate for summary logging
        self._message_count += 1
        current_time = time.time()
        if current_time - self._last_summary_time >= self._summary_interval:
            self._log_message_summary()
            self._message_count = 0
            self._last_summary_time = current_time
        
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            # Temporarily log all incoming messages at INFO level for debugging subscription issues
            if msg_type in ["subscribed", "error"]:
                # Always log subscription confirmations and errors
                self.logger.info(f"Received WS message: type={msg_type}, data={json.dumps(data)}")
            else:
                # Log other message types at DEBUG (too spammy for INFO)
                self.logger.debug(f"Received WS message type: {msg_type}")
            
            if self.state_store:
                self.state_store.last_message_time = datetime.now(timezone.utc)
            
            # Handle subscription confirmation
            if msg_type == "subscribed":
                msg_id = data.get("id")
                with self._lock:
                    if msg_id in self._pending_subscriptions:
                        ticker, callback, channel = self._pending_subscriptions.pop(msg_id)
                        timestamp = self._pending_subscription_timestamps.pop(msg_id, None)
                        age = time.time() - timestamp if timestamp else 0
                        self._subscriptions[msg_id] = (ticker, callback, channel)
                        # Also remove from unsent if it was there
                        self._unsent_subscriptions.pop(msg_id, None)
                        self.logger.info(
                            f"Subscription CONFIRMED: id={msg_id}, channel={channel}, ticker={ticker or 'global'}, "
                            f"was pending for {age:.2f}s"
                        )
                    else:
                        self.logger.warning(
                            f"Received subscription confirmation for unknown id: {msg_id}. "
                            f"Active subscriptions: {list(self._subscriptions.keys())}, "
                            f"Pending subscriptions: {list(self._pending_subscriptions.keys())}"
                        )
                return
            
            # Handle errors
            if msg_type == "error":
                error_data = data.get("msg", {})
                error_code = error_data.get("code")
                error_msg = error_data.get("msg")
                msg_id = data.get("id")
                
                error_descriptions = {
                    1: "Unable to process message",
                    2: "Params required",
                    3: "Channels required",
                    4: "Subscription IDs required",
                    5: "Unknown command",
                    7: "Unknown subscription ID",
                    8: "Unknown channel name",
                    9: "Authentication required",
                    10: "Channel error",
                    11: "Invalid parameter",
                    12: "Exactly one subscription ID required",
                    13: "Unsupported action",
                    14: "Market ticker required",
                    15: "Action required",
                    16: "Market not found",
                    17: "Internal error"
                }
                
                desc = error_descriptions.get(error_code, "Unknown error")
                
                # Clean up failed subscription from pending list
                if msg_id is not None:
                    with self._lock:
                        if msg_id in self._pending_subscriptions:
                            ticker, callback, channel = self._pending_subscriptions.pop(msg_id)
                            timestamp = self._pending_subscription_timestamps.pop(msg_id, None)
                            age = time.time() - timestamp if timestamp else 0
                            self._unsent_subscriptions.pop(msg_id, None)
                            self.logger.error(
                                f"Subscription FAILED: id={msg_id}, channel={channel}, ticker={ticker or 'global'}, "
                                f"error_code={error_code} ({desc}), error_msg={error_msg}, "
                                f"was pending for {age:.2f}s"
                            )
                        else:
                            self.logger.error(
                                f"WebSocket error {error_code} ({desc}): {error_msg} "
                                f"(subscription id: {msg_id}, but not found in pending subscriptions)"
                            )
                else:
                    self.logger.error(
                        f"WebSocket error {error_code} ({desc}): {error_msg} "
                        f"(no subscription id provided)"
                    )
                
                return
            
            # Handle data messages
            # Map API message types to internal channel names
            channel_map = {
                "ticker": "ticker",
                "orderbook_delta": "orderbook_delta",
                "orderbook_snapshot": "orderbook_delta",
                "trade": "trades",  # API sends "trade", map to internal "trades"
                "fill": "fills",  # API sends "fill", map to internal "fills"
                "market_positions": "positions",  # API sends "market_positions", map to internal "positions"
                "market_lifecycle_v2": "lifecycle"  # API sends "market_lifecycle_v2", map to internal "lifecycle"
            }
            
            channel = channel_map.get(msg_type)
            if not channel:
                return
            
            # Kalshi messages can use either "data" or "msg" field for the payload
            # Check both to handle different message formats
            ticker_data = data.get("data") or data.get("msg", {})
            market_ticker = ticker_data.get("market_ticker")
            
            # Diagnostic logging for orderbook messages
            if msg_type in ["orderbook_delta", "orderbook_snapshot"]:
                if msg_type == "orderbook_snapshot":
                    yes_levels = len(ticker_data.get("yes", []))
                    no_levels = len(ticker_data.get("no", []))
                    self.logger.info(
                        f"Received ORDERBOOK_SNAPSHOT for {market_ticker}: "
                        f"yes_levels={yes_levels}, no_levels={no_levels}"
                    )
                elif msg_type == "orderbook_delta":
                    price = ticker_data.get("price")
                    delta = ticker_data.get("delta")
                    side = ticker_data.get("side")
                    self.logger.debug(
                        f"Received ORDERBOOK_DELTA for {market_ticker}: "
                        f"side={side}, price={price}, delta={delta}"
                    )
            
            normalized_data = self._normalize_message_data(msg_type, ticker_data)
            
            with self._lock:
                for msg_id, (sub_ticker, callback, sub_channel) in self._subscriptions.items():
                    if sub_channel == channel:
                        if market_ticker and sub_ticker:
                            if market_ticker == sub_ticker:
                                callback(msg_type, normalized_data)
                                break
                        else:
                            callback(msg_type, normalized_data)
                            break
                            
        except Exception as e:
            self.logger.error(f"Error processing WS message: {e}", exc_info=True)
    
    def _normalize_message_data(self, msg_type: str, data: Dict) -> Dict:
        """Normalize Kalshi WebSocket message format to state_store expected format"""
        normalized = data.copy()
        
        if msg_type == "ticker":
            # Kalshi sends yes_bid/yes_ask directly as numeric values in cents
            # Preserve them and convert to strings for consistency
            if "yes_bid" in data:
                normalized["yes_bid"] = str(data["yes_bid"])
            if "yes_ask" in data:
                normalized["yes_ask"] = str(data["yes_ask"])
            
            # Fallback: if bid/ask in dollars exist, convert to yes_bid/yes_ask in cents
            if "yes_bid" not in normalized:
                bid = data.get("bid")
                if bid is not None:
                    normalized["yes_bid"] = str(int(bid * 100))
            if "yes_ask" not in normalized:
                ask = data.get("ask")
                if ask is not None:
                    normalized["yes_ask"] = str(int(ask * 100))
            
            # Calculate price if we have both
            if "yes_bid" in normalized and "yes_ask" in normalized:
                try:
                    yes_bid_val = float(normalized["yes_bid"])
                    yes_ask_val = float(normalized["yes_ask"])
                    mid = (yes_bid_val + yes_ask_val) / 2
                    normalized["price"] = str(int(mid))
                except (ValueError, TypeError):
                    pass
            
            if "market_ticker" not in normalized and "ticker" in normalized:
                normalized["market_ticker"] = normalized["ticker"]
        
        elif msg_type in ["orderbook_delta", "orderbook_snapshot"]:
            # Detect if this is a snapshot (has yes/no arrays) or delta (has price/delta/side)
            is_snapshot = "yes" in normalized or "no" in normalized
            is_delta = "price" in normalized and "delta" in normalized and "side" in normalized
            
            if is_snapshot:
                # SNAPSHOT: Full orderbook replacement
                # Kalshi sends 'yes' and 'no' arrays for binary markets
                # Convert to 'bids' and 'asks' format expected by state_store
                # For binary markets: YES_price + NO_price = 100 cents
                if "bids" not in normalized and "asks" not in normalized:
                    # Use 'yes' array as bids (people wanting to buy YES contracts)
                    # Convert 'no' array to asks (people buying NO = selling YES)
                    if "yes" in normalized:
                        yes_orders = normalized.get("yes", [])
                        # Sort bids descending (highest bid first) - standard orderbook format
                        yes_orders_sorted = sorted(yes_orders, key=lambda x: x[0] if len(x) >= 1 else 0, reverse=True)
                        normalized["bids"] = yes_orders_sorted
                    
                    # Convert 'no' array to asks: if NO is at price P, selling YES is at (100 - P)
                    if "no" in normalized:
                        no_orders = normalized.get("no", [])
                        asks_from_no = []
                        for order in no_orders:
                            if len(order) >= 2:
                                no_price_cents = order[0]
                                size = order[1]
                                # Convert NO price to YES ask price: YES_ask = 100 - NO_price
                                yes_ask_price = 100 - int(no_price_cents)
                                asks_from_no.append([yes_ask_price, size])
                        # Sort asks ascending (lowest ask first) - standard orderbook format
                        asks_from_no_sorted = sorted(asks_from_no, key=lambda x: x[0] if len(x) >= 1 else 0)
                        normalized["asks"] = asks_from_no_sorted
                    elif "yes" in normalized and "asks" not in normalized:
                        # If no 'no' array, use empty asks
                        normalized["asks"] = []
                
                # Mark as snapshot for state_store
                normalized["_is_snapshot"] = True
            elif is_delta:
                # DELTA: Incremental update - pass through as-is, state_store will apply it
                normalized["_is_delta"] = True
                # Delta format: {"price": 96, "price_dollars": "0.960", "delta": -54, "side": "yes"}
                # state_store will handle the incremental update
            
            if "ts" not in normalized:
                normalized["ts"] = datetime.now(timezone.utc).isoformat()
        
        elif msg_type == "trade":  # API sends "trade", not "trades"
            if "ts" not in normalized and "timestamp" in normalized:
                normalized["ts"] = normalized["timestamp"]
        
        elif msg_type == "fill":  # API sends "fill", not "fills"
            if "ticker" in normalized and "market_ticker" not in normalized:
                normalized["market_ticker"] = normalized["ticker"]
        
        elif msg_type == "market_positions":  # API sends "market_positions", not "positions"
            if "ticker" in normalized and "market_ticker" not in normalized:
                normalized["market_ticker"] = normalized["ticker"]
        
        elif msg_type == "market_lifecycle_v2":  # API sends "market_lifecycle_v2", not "lifecycle"
            if "ticker" in normalized and "market_ticker" not in normalized:
                normalized["market_ticker"] = normalized["ticker"]
        
        return normalized
    
    def _log_message_summary(self):
        """Log WebSocket message rate and subscription summary"""
        current_time = time.time()
        old_pending_threshold = 30.0  # seconds
        
        with self._lock:
            active_subscriptions = len(self._subscriptions)
            pending_subscriptions = len(self._pending_subscriptions)
            unsent_subscriptions = len(self._unsent_subscriptions)
            
            # Build detailed pending list
            pending_list = []
            old_pending_count = 0
            for msg_id, (ticker, _, channel) in self._pending_subscriptions.items():
                timestamp = self._pending_subscription_timestamps.get(msg_id, 0)
                age = current_time - timestamp if timestamp > 0 else 0
                if age > old_pending_threshold:
                    old_pending_count += 1
                pending_list.append((msg_id, ticker, channel, age))
        
        # Count subscriptions by channel type
        channel_counts = {}
        with self._lock:
            for _, (_, _, channel) in self._subscriptions.items():
                channel_counts[channel] = channel_counts.get(channel, 0) + 1
        
        channel_summary = ", ".join([f"{ch}: {count}" for ch, count in channel_counts.items()]) if channel_counts else "none"
        
        msg_rate = self._message_count / self._summary_interval if self._summary_interval > 0 else 0
        unsent_info = f", {unsent_subscriptions} unsent" if unsent_subscriptions > 0 else ""
        old_pending_info = f", {old_pending_count} old pending (>30s)" if old_pending_count > 0 else ""
        
        # Log summary
        self.logger.info(
            f"WebSocket stats: {msg_rate:.1f} msg/sec, "
            f"{active_subscriptions} active subscriptions ({channel_summary}), "
            f"{pending_subscriptions} pending{unsent_info}{old_pending_info}"
        )
        
        # Log detailed pending subscriptions if any
        if pending_subscriptions > 0:
            pending_details = []
            for msg_id, ticker, channel, age in sorted(pending_list, key=lambda x: x[3], reverse=True)[:10]:  # Show oldest 10
                ticker_str = ticker or "global"
                pending_details.append(f"id={msg_id}({channel},{ticker_str},{age:.1f}s)")
            
            pending_msg = "; ".join(pending_details)
            if pending_subscriptions > 10:
                pending_msg += f" ... and {pending_subscriptions - 10} more"
            
            self.logger.warning(
                f"PENDING SUBSCRIPTIONS ({pending_subscriptions}): {pending_msg}"
            )
        
        # Log unsent subscriptions if any
        if unsent_subscriptions > 0:
            with self._lock:
                unsent_list = []
                for msg_id, (ticker, _, channel, _) in list(self._unsent_subscriptions.items())[:10]:
                    ticker_str = ticker or "global"
                    unsent_list.append(f"id={msg_id}({channel},{ticker_str})")
            
            unsent_msg = "; ".join(unsent_list)
            if unsent_subscriptions > 10:
                unsent_msg += f" ... and {unsent_subscriptions - 10} more"
            
            self.logger.warning(
                f"UNSENT SUBSCRIPTIONS ({unsent_subscriptions}): {unsent_msg}"
            )
    
    async def _receive_loop(self):
        """Main receive loop (async) - processes messages from WebSocket"""
        self.logger.debug("=== _receive_loop started ===")
        
        while self._running:
            try:
                # Connect if not already connected
                if not self._ws or not self._connection_ready:
                    try:
                        await self._connect_async()
                    except Exception as e:
                        self.logger.error(f"Failed to connect in receive loop: {e}", exc_info=True)
                        await asyncio.sleep(5)  # Wait before retry
                        continue
                
                # Process messages
                async for message in self._ws:
                    if not self._running:
                        break
                    await self._process_message(message)
                    
            except websockets.exceptions.ConnectionClosed as e:
                self.logger.warning(f"WebSocket connection closed: code={e.code}, reason={e.reason}")
                self._connection_ready = False
                if self.state_store:
                    self.state_store.ws_healthy = False
                
                # Check if we should reconnect
                if self._running:
                    await self._handle_reconnect_async()
                else:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in receive loop: {e}", exc_info=True)
                self._connection_ready = False
                if self.state_store:
                    self.state_store.ws_healthy = False
                
                # Check if we should reconnect
                if self._running:
                    await asyncio.sleep(1)  # Brief delay before reconnection
                    await self._handle_reconnect_async()
                else:
                    break
    
    async def _handle_reconnect_async(self):
        """Reconnect with exponential backoff (async)"""
        if self.state_store:
            self.state_store.ws_healthy = False
        
        delay = min(
            self.config.get('reconnect_base_delay_seconds', 0.5) * (2 ** self._reconnect_attempts),
            self.config.get('reconnect_max_delay_seconds', 5.0)
        )
        self.logger.info(f"Reconnecting in {delay:.2f}s (attempt {self._reconnect_attempts + 1})")
        await asyncio.sleep(delay)
        self._reconnect_attempts += 1
        
        max_retries = self.config.get('reconnect_max_retries', 0)
        if max_retries > 0 and self._reconnect_attempts > max_retries:
            self.logger.error(f"Max reconnection attempts ({max_retries}) reached")
            self._running = False
            return
        
        try:
            await self._connect_async()
            
            # Re-subscribe all active subscriptions
            with self._lock:
                subscriptions_snapshot = list(self._subscriptions.items())
            
            for msg_id, (ticker, callback, channel) in subscriptions_snapshot:
                try:
                    if channel == 'ticker' and ticker:
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_ticker": ticker}
                        }))
                        self._next_id += 1
                    elif channel == 'orderbook_delta' and ticker:
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["orderbook_delta"], "market_ticker": ticker}
                        }))
                        self._next_id += 1
                    elif channel == 'trades' and ticker:
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["trade"], "market_ticker": ticker}
                        }))
                        self._next_id += 1
                    elif channel == 'fills':
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["fill"]}
                        }))
                        self._next_id += 1
                    elif channel == 'positions':
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["market_positions"]}
                        }))
                        self._next_id += 1
                    elif channel == 'lifecycle':
                        await self._send_message_async(json.dumps({
                            "id": self._next_id,
                            "cmd": "subscribe",
                            "params": {"channels": ["market_lifecycle_v2"]}
                        }))
                        self._next_id += 1
                except Exception as e:
                    self.logger.error(f"Error re-subscribing {channel} for {ticker}: {e}")
            
            if self.state_store:
                self.state_store.ws_healthy = True
                self.state_store.last_message_time = datetime.now(timezone.utc)
            
            self.logger.info("WebSocket reconnected and re-subscribed")
            
        except Exception as e:
            self.logger.error(f"Reconnection failed: {e}")
    
    def _run_event_loop(self):
        """Run asyncio event loop in background thread"""
        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()
    
    def start(self):
        """Start the WebSocket connection and receive loop"""
        self.logger.error("=== start() method called ===")
        self._running = True
        
        # Start event loop in background thread
        if self._loop_thread is None or not self._loop_thread.is_alive():
            self.logger.error("Starting event loop thread...")
            self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
            self._loop_thread.start()
            # Wait for event loop to start
            while self._event_loop is None:
                time.sleep(0.1)
            self.logger.error("Event loop thread started")
        
        # Start receive loop (it will connect internally)
        if self._receive_task is None or self._receive_task.done():
            self.logger.error("Starting receive task...")
            self._receive_task = asyncio.run_coroutine_threadsafe(self._receive_loop(), self._event_loop)
            self.logger.error("Receive task started")
    
    def stop(self):
        """Stop the WebSocket connection"""
        self.logger.error("=== stop() method called ===")
        self._running = False
        
        # Cancel receive task
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                self._receive_task.result(timeout=2.0)
            except Exception:
                pass
        
        # Close WebSocket
        if self._ws and self._event_loop:
            async def close_ws():
                try:
                    await self._ws.close()
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket: {e}")
            
            future = asyncio.run_coroutine_threadsafe(close_ws(), self._event_loop)
            try:
                future.result(timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error waiting for WebSocket close: {e}")
            self._ws = None
        
        self._connection_ready = False
        if self.state_store:
            self.state_store.ws_healthy = False
