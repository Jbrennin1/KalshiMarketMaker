"""
WebSocket Connection Pool for Kalshi API
Manages multiple WebSocket connections to support up to 20 markets
(1 market per connection due to Kalshi's 1 subscription per channel type limit)
"""
import threading
import time
import logging
from typing import Dict, Optional, Callable, List, Set
from kalshi_mm.api.websocket import KalshiWebsocketClient


class WebSocketConnectionPool:
    """Manages multiple WebSocket connections for market sharding"""
    
    def __init__(
        self,
        access_key: str,
        private_key_obj,
        url: str,
        logger: logging.Logger,
        config: Dict,
        state_store: Optional[object] = None
    ):
        self.access_key = access_key
        self.private_key_obj = private_key_obj
        self.url = url
        self.logger = logger
        self.config = config
        self.state_store = state_store
        
        ws_config = config.get('websockets', {})
        self.max_connections = ws_config.get('max_connections', 20)
        self.max_markets_per_connection = ws_config.get('max_markets_per_connection', 1)
        
        # Connection management
        self._connections: List[KalshiWebsocketClient] = []
        self._market_to_connection: Dict[str, KalshiWebsocketClient] = {}
        self._connection_to_markets: Dict[KalshiWebsocketClient, Set[str]] = {}
        self._connection_to_global_subs: Dict[KalshiWebsocketClient, Dict[str, int]] = {}
        # Maps msg_id -> (connection, ticker, callback, channel) for unsubscribe
        self._msg_id_to_connection: Dict[int, KalshiWebsocketClient] = {}
        self._lock = threading.Lock()
        
        # Global subscription callbacks (stored to apply to new connections)
        self._global_fills_callback: Optional[Callable] = None
        self._global_positions_callback: Optional[Callable] = None
        self._global_lifecycle_callback: Optional[Callable] = None
        
        self._running = False
        
        self.logger.info(
            f"WebSocketConnectionPool initialized: max_connections={self.max_connections}, "
            f"max_markets_per_connection={self.max_markets_per_connection}"
        )
    
    def _get_connection_for_market(self, ticker: str) -> Optional[KalshiWebsocketClient]:
        """Get or create a connection for a market using consistent hashing
        
        Args:
            ticker: Market ticker
            
        Returns:
            KalshiWebsocketClient instance, or None if at capacity
        """
        with self._lock:
            # Check if market already has a connection
            if ticker in self._market_to_connection:
                conn = self._market_to_connection[ticker]
                # Verify connection is still valid
                if conn in self._connections:
                    return conn
                # Connection was removed, clean up
                del self._market_to_connection[ticker]
            
            # Find connection with available capacity
            for conn in self._connections:
                markets = self._connection_to_markets.get(conn, set())
                if len(markets) < self.max_markets_per_connection:
                    # Assign market to this connection
                    markets.add(ticker)
                    self._market_to_connection[ticker] = conn
                    self.logger.info(
                        f"Assigned market {ticker} to connection {len(self._connections)} "
                        f"(connection has {len(markets)}/{self.max_markets_per_connection} markets)"
                    )
                    return conn
            
            # No available connection, create new one if under limit
            if len(self._connections) >= self.max_connections:
                self.logger.warning(
                    f"Cannot assign market {ticker}: all {self.max_connections} connections at capacity "
                    f"({self.max_markets_per_connection} markets each)"
                )
                return None
            
            # Create new connection
            conn_idx = len(self._connections) + 1
            self.logger.info(f"Creating new WebSocket connection {conn_idx}/{self.max_connections}")
            
            conn = KalshiWebsocketClient(
                access_key=self.access_key,
                private_key_obj=self.private_key_obj,
                url=self.url,
                logger=self.logger,
                config=self.config,
                state_store=self.state_store
            )
            
            self._connections.append(conn)
            self._connection_to_markets[conn] = {ticker}
            self._connection_to_global_subs[conn] = {}
            self._market_to_connection[ticker] = conn
            
            # Start the connection (async, will connect in background)
            conn.connect()
            conn.start()
            
            # Wait a bit for connection to establish, then subscribe to global channels
            # (KalshiWebsocketClient's subscribe methods will wait if connection isn't ready)
            time.sleep(0.5)  # Give connection time to start
            self._subscribe_globals_on_connection(conn)
            
            self.logger.info(
                f"Created and started connection {conn_idx} for market {ticker} "
                f"({len(self._connections)}/{self.max_connections} connections active)"
            )
            
            return conn
    
    def _subscribe_globals_on_connection(self, conn: KalshiWebsocketClient):
        """Subscribe to global channels on a connection"""
        ws_config = self.config.get('websockets', {})
        
        # Subscribe to fills if enabled and callback exists
        if ws_config.get('subscribe_user_fills', True) and self._global_fills_callback:
            try:
                msg_id = conn.subscribe_user_fills(self._global_fills_callback)
                self._connection_to_global_subs[conn]['fills'] = msg_id
                self._msg_id_to_connection[msg_id] = conn
            except Exception as e:
                self.logger.error(f"Failed to subscribe to fills on connection: {e}", exc_info=True)
        
        # Subscribe to positions if enabled and callback exists
        if ws_config.get('subscribe_positions', True) and self._global_positions_callback:
            try:
                msg_id = conn.subscribe_positions(self._global_positions_callback)
                self._connection_to_global_subs[conn]['positions'] = msg_id
                self._msg_id_to_connection[msg_id] = conn
            except Exception as e:
                self.logger.error(f"Failed to subscribe to positions on connection: {e}", exc_info=True)
        
        # Subscribe to lifecycle if enabled and callback exists
        if ws_config.get('subscribe_lifecycle', True) and self._global_lifecycle_callback:
            try:
                msg_id = conn.subscribe_lifecycle(self._global_lifecycle_callback)
                self._connection_to_global_subs[conn]['lifecycle'] = msg_id
                self._msg_id_to_connection[msg_id] = conn
            except Exception as e:
                self.logger.error(f"Failed to subscribe to lifecycle on connection: {e}", exc_info=True)
    
    def start(self):
        """Start all connections and subscribe to global channels"""
        self.logger.info("Starting WebSocket connection pool...")
        self._running = True
        
        # Create initial connection for global subscriptions
        # (markets will create their own connections as needed)
        if len(self._connections) == 0:
            self.logger.info("Creating initial connection for global subscriptions")
            conn = KalshiWebsocketClient(
                access_key=self.access_key,
                private_key_obj=self.private_key_obj,
                url=self.url,
                logger=self.logger,
                config=self.config,
                state_store=self.state_store
            )
            conn.connect()
            conn.start()
            self._connections.append(conn)
            self._connection_to_markets[conn] = set()
            self._connection_to_global_subs[conn] = {}
    
    def stop(self):
        """Stop all connections"""
        self.logger.info("Stopping WebSocket connection pool...")
        self._running = False
        
        with self._lock:
            for conn in self._connections:
                try:
                    conn.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping connection: {e}", exc_info=True)
            
            self._connections.clear()
            self._market_to_connection.clear()
            self._connection_to_markets.clear()
            self._connection_to_global_subs.clear()
            self._msg_id_to_connection.clear()
        
        self.logger.info("All connections stopped")
    
    def subscribe_ticker(self, ticker: Optional[str] = None, callback: Optional[Callable] = None) -> int:
        """Subscribe to ticker channel
        
        For market-specific subscriptions, routes to appropriate connection.
        For global subscriptions, subscribes on all connections.
        """
        if ticker is None:
            # Global ticker subscription - subscribe on all connections
            msg_ids = []
            with self._lock:
                for conn in self._connections:
                    try:
                        msg_id = conn.subscribe_ticker(None, callback)
                        msg_ids.append(msg_id)
                        self._msg_id_to_connection[msg_id] = conn
                    except Exception as e:
                        self.logger.error(f"Failed to subscribe to global ticker on connection: {e}", exc_info=True)
            # Return first msg_id (caller may not need all of them)
            return msg_ids[0] if msg_ids else 0
        
        # Market-specific subscription
        conn = self._get_connection_for_market(ticker)
        if conn is None:
            self.logger.error(f"Cannot subscribe to ticker for {ticker}: no available connection")
            return 0
        
        try:
            msg_id = conn.subscribe_ticker(ticker, callback)
            self._msg_id_to_connection[msg_id] = conn
            return msg_id
        except Exception as e:
            self.logger.error(f"Failed to subscribe to ticker for {ticker}: {e}", exc_info=True)
            return 0
    
    def subscribe_orderbook(self, ticker: str, callback: Callable) -> int:
        """Subscribe to orderbook updates for specific market"""
        conn = self._get_connection_for_market(ticker)
        if conn is None:
            self.logger.error(f"Cannot subscribe to orderbook for {ticker}: no available connection")
            return 0
        
        try:
            msg_id = conn.subscribe_orderbook(ticker, callback)
            self._msg_id_to_connection[msg_id] = conn
            return msg_id
        except Exception as e:
            self.logger.error(f"Failed to subscribe to orderbook for {ticker}: {e}", exc_info=True)
            return 0
    
    def subscribe_trades(self, ticker: str, callback: Callable) -> int:
        """Subscribe to public trades for specific market"""
        conn = self._get_connection_for_market(ticker)
        if conn is None:
            self.logger.error(f"Cannot subscribe to trades for {ticker}: no available connection")
            return 0
        
        try:
            msg_id = conn.subscribe_trades(ticker, callback)
            self._msg_id_to_connection[msg_id] = conn
            return msg_id
        except Exception as e:
            self.logger.error(f"Failed to subscribe to trades for {ticker}: {e}", exc_info=True)
            return 0
    
    def subscribe_user_fills(self, callback: Callable) -> int:
        """Subscribe to user fills on all connections"""
        self._global_fills_callback = callback
        
        msg_ids = []
        with self._lock:
            for conn in self._connections:
                try:
                    msg_id = conn.subscribe_user_fills(callback)
                    msg_ids.append(msg_id)
                    self._connection_to_global_subs[conn]['fills'] = msg_id
                    self._msg_id_to_connection[msg_id] = conn
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to fills on connection: {e}", exc_info=True)
        
        # Return first msg_id
        return msg_ids[0] if msg_ids else 0
    
    def subscribe_positions(self, callback: Callable) -> int:
        """Subscribe to market positions on all connections"""
        self._global_positions_callback = callback
        
        msg_ids = []
        with self._lock:
            for conn in self._connections:
                try:
                    msg_id = conn.subscribe_positions(callback)
                    msg_ids.append(msg_id)
                    self._connection_to_global_subs[conn]['positions'] = msg_id
                    self._msg_id_to_connection[msg_id] = conn
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to positions on connection: {e}", exc_info=True)
        
        # Return first msg_id
        return msg_ids[0] if msg_ids else 0
    
    def subscribe_lifecycle(self, callback: Callable) -> int:
        """Subscribe to market lifecycle events on all connections"""
        self._global_lifecycle_callback = callback
        
        msg_ids = []
        with self._lock:
            for conn in self._connections:
                try:
                    msg_id = conn.subscribe_lifecycle(callback)
                    msg_ids.append(msg_id)
                    self._connection_to_global_subs[conn]['lifecycle'] = msg_id
                    self._msg_id_to_connection[msg_id] = conn
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to lifecycle on connection: {e}", exc_info=True)
        
        # Return first msg_id
        return msg_ids[0] if msg_ids else 0
    
    def unsubscribe(self, msg_id: int):
        """Unsubscribe from a channel"""
        with self._lock:
            conn = self._msg_id_to_connection.get(msg_id)
            if conn:
                try:
                    conn.unsubscribe(msg_id)
                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe {msg_id}: {e}", exc_info=True)
                del self._msg_id_to_connection[msg_id]
            else:
                self.logger.warning(f"Could not find connection for msg_id {msg_id}")
    
    def remove_market_assignment(self, ticker: str):
        """Remove market assignment from connection pool, freeing up the connection slot
        
        This should be called when a market is no longer being tracked to ensure
        the connection slot becomes available for new markets.
        
        Args:
            ticker: Market ticker to remove
        """
        with self._lock:
            if ticker not in self._market_to_connection:
                # Market not assigned, nothing to do
                return
            
            conn = self._market_to_connection[ticker]
            
            # Remove from market-to-connection mapping
            del self._market_to_connection[ticker]
            
            # Remove from connection-to-markets mapping
            if conn in self._connection_to_markets:
                markets = self._connection_to_markets[conn]
                markets.discard(ticker)
                
                self.logger.info(
                    f"Removed market {ticker} from connection "
                    f"(connection now has {len(markets)}/{self.max_markets_per_connection} markets)"
                )
                
                # Optionally close connection if it has no markets and no global subscriptions
                # For now, we'll keep connections open for reuse (they can be reused immediately)
                # This is more efficient than closing/reopening connections
    
    def get_pool_status(self) -> Dict:
        """Get status of connection pool"""
        with self._lock:
            status = {
                'total_connections': len(self._connections),
                'max_connections': self.max_connections,
                'total_markets': len(self._market_to_connection),
                'connections': []
            }
            
            for idx, conn in enumerate(self._connections):
                markets = self._connection_to_markets.get(conn, set())
                conn_status = {
                    'index': idx + 1,
                    'markets': list(markets),
                    'market_count': len(markets),
                    'max_markets': self.max_markets_per_connection,
                    'connection_ready': conn._connection_ready if hasattr(conn, '_connection_ready') else False
                }
                status['connections'].append(conn_status)
            
            return status
    
    def log_pool_status(self):
        """Log current pool status"""
        status = self.get_pool_status()
        self.logger.info(
            f"Connection Pool Status: {status['total_connections']}/{status['max_connections']} connections, "
            f"{status['total_markets']} markets tracked"
        )
        for conn_status in status['connections']:
            self.logger.info(
                f"  Connection {conn_status['index']}: {conn_status['market_count']}/{conn_status['max_markets']} markets, "
                f"ready={conn_status['connection_ready']}, markets={conn_status['markets']}"
            )

