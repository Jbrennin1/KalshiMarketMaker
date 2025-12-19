import abc
import time
import threading
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import requests
import logging
import uuid
import math
import os
import json
import base64
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# Phase 5.1: Structured Kalshi Error Handling
class KalshiAPIError(Exception):
    """Base exception for Kalshi API errors"""
    pass

class InsufficientBalanceError(KalshiAPIError):
    """INSUFFICIENT_BALANCE error"""
    pass

class MarketHaltedError(KalshiAPIError):
    """MARKET_HALTED error"""
    pass

class OrderRejectedError(KalshiAPIError):
    """ORDER_REJECTED error"""
    pass

class InvalidPriceError(KalshiAPIError):
    """INVALID_PRICE error"""
    pass

# Import shared utilities (moved to kalshi_utils.py to prevent circular imports)
from kalshi_mm.utils import parse_kalshi_timestamp, parse_kalshi_timestamp_cached, get_price_field

class AbstractTradingAPI(abc.ABC):
    @abc.abstractmethod
    def get_price(self) -> float:
        pass

    @abc.abstractmethod
    def place_order(self, action: str, side: str, price: float, quantity: int, expiration_ts: int = None) -> str:
        pass

    @abc.abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        pass

    @abc.abstractmethod
    def get_position(self) -> int:
        pass

    @abc.abstractmethod
    def get_orders(self) -> List[Dict]:
        pass

class KalshiTradingAPI(AbstractTradingAPI):
    def __init__(
        self,
        access_key: str,
        private_key: str,
        market_ticker: str,
        base_url: str,
        logger: logging.Logger,
        analytics_db: Optional[object] = None,
        run_id: Optional[int] = None,
    ):
        self.access_key = access_key
        self.market_ticker = market_ticker
        self.logger = logger
        self.base_url = base_url
        self.analytics_db = analytics_db
        self.run_id = run_id
        # Extract the path prefix from base_url (e.g., "/trade-api/v2")
        parsed = urlparse(base_url)
        self.path_prefix = parsed.path if parsed.path else "/trade-api/v2"
        
        # Load private key - handle both file path and key content
        if os.path.isfile(private_key):
            with open(private_key, 'rb') as f:
                private_key_data = f.read()
        else:
            # Handle private key from environment variable
            # It might have escaped newlines (\n) or actual newlines
            if isinstance(private_key, str):
                # Replace escaped newlines with actual newlines
                private_key_str = private_key.replace('\\n', '\n')
                # Ensure it starts with proper PEM header
                if not private_key_str.strip().startswith('-----BEGIN'):
                    self.logger.error("Private key doesn't appear to be in PEM format (should start with -----BEGIN)")
                private_key_data = private_key_str.encode('utf-8')
            else:
                private_key_data = private_key
        
        try:
            self.private_key_obj = serialization.load_pem_private_key(
                private_key_data,
                password=None,
                backend=default_backend()
            )
        except Exception as e:
            self.logger.error(f"Failed to load private key: {e}")
            self.logger.error("Make sure your private key is in PEM format and properly formatted.")
            self.logger.error("If storing in .env file, use \\n for newlines or store the key in a separate file.")
            raise
        
        self.logger.info("Kalshi API initialized with API key authentication")
        
        # Phase 5.3: Clock skew detection on startup
        self.check_clock_skew()
        
        # Start background clock skew checker (every 15 minutes)
        self._clock_skew_thread = None
        self._start_clock_skew_monitor()

    def logout(self):
        # API key authentication doesn't require explicit logout
        self.logger.info("Logout called (no-op for API key authentication)")
        # Stop clock skew monitor
        if hasattr(self, '_clock_skew_thread') and self._clock_skew_thread:
            self._clock_skew_stop = True
    
    def _parse_kalshi_error(self, response_text: str) -> Optional[KalshiAPIError]:
        """Parse Kalshi error response and return appropriate exception"""
        try:
            error_data = json.loads(response_text)
            # Handle multiple error layers (error.code, code, error_code)
            error_code = (
                error_data.get("error", {}).get("code") or
                error_data.get("code") or
                error_data.get("error_code") or
                ""
            )
            
            error_map = {
                "INSUFFICIENT_BALANCE": InsufficientBalanceError,
                "MARKET_HALTED": MarketHaltedError,
                "ORDER_REJECTED": OrderRejectedError,
                "INVALID_PRICE": InvalidPriceError,
            }
            
            error_class = error_map.get(error_code, KalshiAPIError)
            error_message = (
                error_data.get("error", {}).get("message") or
                error_data.get("message") or
                "Unknown error"
            )
            return error_class(error_message)
        except:
            return None
    
    def check_clock_skew(self) -> bool:
        """Check if local clock is skewed vs Kalshi server"""
        try:
            # HEAD request to get server time from headers
            response = requests.head(self.base_url, timeout=5)
            server_time_str = response.headers.get("Date")
            if server_time_str:
                server_time = parsedate_to_datetime(server_time_str)
                if server_time.tzinfo is None:
                    server_time = server_time.replace(tzinfo=timezone.utc)
                local_time = datetime.now(timezone.utc)
                skew_seconds = abs((server_time - local_time).total_seconds())
                
                if skew_seconds > 2:
                    self.logger.error(
                        f"Clock skew detected: {skew_seconds:.1f}s difference. "
                        f"Signatures may be rejected!"
                    )
                    return False
                else:
                    self.logger.debug(f"Clock skew check passed: {skew_seconds:.2f}s difference")
        except Exception as e:
            self.logger.warning(f"Could not check clock skew: {e}")
        return True
    
    def _start_clock_skew_monitor(self):
        """Start background thread to check clock skew every 15 minutes"""
        self._clock_skew_stop = False
        
        def monitor_loop():
            while not self._clock_skew_stop:
                time.sleep(15 * 60)  # 15 minutes
                if not self._clock_skew_stop:
                    self.check_clock_skew()
        
        self._clock_skew_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._clock_skew_thread.start()

    def _sign_request(self, method: str, path: str, timestamp: str, body: str = "") -> str:
        """Sign a request using RSA-PSS with SHA256"""
        # Strip query parameters from path before signing (per Kalshi API docs)
        path_without_query = path.split('?')[0]
        # Construct full path including API version prefix
        full_path = f"{self.path_prefix}{path_without_query}"
        # Create the string to sign: timestamp + method + path (body NOT included per Kalshi API)
        string_to_sign = f"{timestamp}{method}{full_path}"
        
        # Sign using RSA-PSS with SHA256
        signature = self.private_key_obj.sign(
            string_to_sign.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        
        # Return base64 encoded signature
        return base64.b64encode(signature).decode('utf-8')

    def get_headers(self, method: str = "GET", path: str = "", body: str = ""):
        """Generate headers with API key authentication"""
        timestamp = str(int(time.time() * 1000))  # Timestamp in milliseconds
        
        signature = self._sign_request(method, path, timestamp, body)
        
        return {
            "KALSHI-ACCESS-KEY": self.access_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

    def make_request(
        self, method: str, path: str, params: Dict = None, data: Dict = None, max_retries: int = 3
    ):
        """Make request with rate limit retry logic and structured error handling"""
        url = f"{self.base_url}{path}"
        
        # Prepare body for signing - sort keys for consistent ordering
        body = ""
        if data:
            # Sort keys to ensure consistent JSON string for signing
            body = json.dumps(data, separators=(',', ':'), sort_keys=True)
        
        for attempt in range(max_retries):
            try:
                # Get signed headers
                headers = self.get_headers(method, path, body)
                
                # For POST/PUT with body, send the exact JSON string we signed
                if data and method in ['POST', 'PUT', 'PATCH']:
                    headers['Content-Type'] = 'application/json'
                    response = requests.request(
                        method, url, headers=headers, params=params, data=body.encode('utf-8'),
                        timeout=30  # 30 second timeout to prevent hanging
                    )
                else:
                    response = requests.request(
                        method, url, headers=headers, params=params, json=data if data else None,
                        timeout=30  # 30 second timeout to prevent hanging
                    )
                self.logger.debug(f"Request URL: {response.url}")
                self.logger.debug(f"Request headers: {response.request.headers}")
                self.logger.debug(f"Request params: {params}")
                self.logger.debug(f"Request data: {data}")
                self.logger.debug(f"Response status code: {response.status_code}")
                self.logger.debug(f"Response content: {response.text}")
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                # Phase 5.2: Rate limit handling with Retry-After header
                if e.response.status_code == 429:  # Rate limited
                    if attempt < max_retries - 1:
                        # Check for Retry-After header
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait_time = float(retry_after)
                            except ValueError:
                                wait_time = 0.25 * (2 ** attempt)  # Fallback to exponential backoff
                        else:
                            wait_time = 0.25 * (2 ** attempt)  # Exponential backoff
                        
                        self.logger.warning(f"Rate limited, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error("Rate limit exceeded after retries")
                
                # Phase 5.1: Structured error handling
                if hasattr(e, "response") and e.response is not None:
                    kalshi_error = self._parse_kalshi_error(e.response.text)
                    if kalshi_error:
                        self.logger.error(f"Kalshi API error: {kalshi_error}")
                        raise kalshi_error
                    self.logger.error(f"Response content: {e.response.text}")
                
                raise
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed: {e}")
                if hasattr(e, "response") and e.response is not None:
                    self.logger.error(f"Response content: {e.response.text}")
                raise

    def get_position(self) -> int:
        self.logger.info("Retrieving position...")
        path = "/portfolio/positions"
        # Phase 1.3: Add count_filter with fallback
        params = {
            "ticker": self.market_ticker,
            "settlement_status": "unsettled",
            "count_filter": "position",  # NEW
        }
        try:
            response = self.make_request("GET", path, params=params)
        except requests.exceptions.HTTPError as e:
            # Retry without count_filter if not supported
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 400:
                params.pop("count_filter")
                response = self.make_request("GET", path, params=params)
            else:
                raise
        positions = response.get("market_positions", [])

        total_position = 0
        for position in positions:
            if position["ticker"] == self.market_ticker:
                total_position += position["position"]

        self.logger.info(f"Current position: {total_position}")
        return total_position

    def get_price(self) -> Dict[str, float]:
        self.logger.info("Retrieving market data...")
        path = f"/markets/{self.market_ticker}"
        data = self.make_request("GET", path)
        market = data.get("market", {})

        # Phase 1.2: Use get_price_field() utility
        yes_bid = get_price_field(market, "yes_bid") or 0.0
        yes_ask = get_price_field(market, "yes_ask") or 0.0
        no_bid = get_price_field(market, "no_bid") or 0.0
        no_ask = get_price_field(market, "no_ask") or 0.0
        
        yes_mid_price = round((yes_bid + yes_ask) / 2, 2)
        no_mid_price = round((no_bid + no_ask) / 2, 2)

        self.logger.info(f"Current yes mid-market price: ${yes_mid_price:.2f} (bid: ${yes_bid:.2f}, ask: ${yes_ask:.2f})")
        self.logger.info(f"Current no mid-market price: ${no_mid_price:.2f} (bid: ${no_bid:.2f}, ask: ${no_ask:.2f})")
        return {
            "yes": yes_mid_price,
            "no": no_mid_price,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask
        }

    def place_order(self, action: str, side: str, price: float, quantity: int, expiration_ts: int = None) -> str:
        self.logger.info(f"Placing {action} order for {side} side at price ${price:.2f} with quantity {quantity}...")
        
        # Phase 1.4: Price validation
        if price < 0.01 or price > 0.99:
            raise InvalidPriceError(f"Invalid price {price} outside 1-99¢ range")
        
        path = "/portfolio/orders"
        data = {
            "ticker": self.market_ticker,
            "action": action.lower(),  # 'buy' or 'sell'
            "type": "limit",
            "side": side,  # 'yes' or 'no'
            "count": quantity,
            "client_order_id": str(uuid.uuid4()),
        }
        price_to_send = int(price * 100) # Convert dollars to cents

        # Phase 1.4: Ensure only relevant price field is sent
        if side == "yes":
            data["yes_price"] = price_to_send
            # Explicitly ensure no_price is not sent
            if "no_price" in data:
                del data["no_price"]
        else:
            data["no_price"] = price_to_send
            # Explicitly ensure yes_price is not sent
            if "yes_price" in data:
                del data["yes_price"]

        if expiration_ts is not None:
            data["expiration_ts"] = expiration_ts

        try:
            response = self.make_request("POST", path, data=data)
            order_id = response["order"]["order_id"]
            self.logger.info(f"Placed {action} order for {side} side at price ${price:.2f} with quantity {quantity}, order ID: {order_id}")
            
            # Log to analytics database
            if self.analytics_db and self.run_id:
                try:
                    # expiration_ts is already in Unix timestamp format if provided
                    self.analytics_db.log_order_placed(
                        self.run_id, str(order_id), action, side, price, quantity, expiration_ts
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to log order to analytics: {e}")
            
            return str(order_id)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to place order: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
                self.logger.error(f"Request data: {data}")
            raise

    def cancel_order(self, order_id: int) -> bool:
        self.logger.info(f"Canceling order with ID {order_id}...")
        path = f"/portfolio/orders/{order_id}"
        response = self.make_request("DELETE", path)
        success = response["reduced_by"] > 0
        self.logger.info(f"Canceled order with ID {order_id}, success: {success}")
        
        # Log to analytics database
        if self.analytics_db and success:
            try:
                self.analytics_db.log_order_cancelled(str(order_id))
            except Exception as e:
                self.logger.warning(f"Failed to log order cancellation to analytics: {e}")
        
        return success

    def get_orders(self) -> List[Dict]:
        self.logger.info("Retrieving orders...")
        path = "/portfolio/orders"
        # Phase 1.5: Filter client-side instead of server-side
        params = {"ticker": self.market_ticker}  # Remove status filter
        response = self.make_request("GET", path, params=params)
        orders = response.get("orders", [])
        
        # Filter client-side for active/resting orders (all known statuses)
        ACTIVE_STATUSES = ["open", "active", "resting", "pending", "working"]
        active_orders = [
            o for o in orders 
            if o.get("status") in ACTIVE_STATUSES
        ]
        self.logger.info(f"Retrieved {len(active_orders)} active orders (from {len(orders)} total)")
        return active_orders

    def get_fills(
        self,
        ticker: Optional[str] = None,
        order_id: Optional[str] = None,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
    ) -> Dict:
        """
        Thin wrapper around GET /portfolio/fills endpoint.
        
        Args:
            ticker: Market ticker to filter by (defaults to self.market_ticker)
            order_id: Single order ID to filter by
            min_ts: Minimum Unix timestamp (seconds) for filtering
            max_ts: Maximum Unix timestamp (seconds) for filtering
            limit: Number of results per page (1-200, default 200)
            cursor: Pagination cursor for retrieving subsequent pages
        
        Returns:
            Response dictionary with 'fills' list and optional 'cursor'
        """
        path = "/portfolio/fills"
        params: Dict[str, object] = {"limit": limit}
        
        if ticker is not None:
            params["ticker"] = ticker
        elif self.market_ticker:
            params["ticker"] = self.market_ticker
        if order_id is not None:
            params["order_id"] = order_id
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        if cursor is not None:
            params["cursor"] = cursor
        
        response = self.make_request("GET", path, params=params)
        return response

    def iter_fills_since(
        self,
        min_ts: int,
        ticker: Optional[str] = None,
        max_ts: Optional[int] = None,
        page_limit: int = 200,
    ):
        """
        Forward-only iterator that yields all fills since min_ts, handling pagination.
        
        Args:
            min_ts: Minimum Unix timestamp (seconds) - only fills with ts >= min_ts
            ticker: Market ticker to filter by (defaults to self.market_ticker)
            max_ts: Maximum Unix timestamp (seconds) for filtering
            page_limit: Results per page (default 200)
        
        Yields:
            Fill dictionaries from Kalshi API
        """
        cursor = None
        while True:
            resp = self.get_fills(
                ticker=ticker,
                min_ts=min_ts,
                max_ts=max_ts,
                limit=page_limit,
                cursor=cursor,
            )
            fills = resp.get("fills", [])
            if not fills:
                break
            
            for fill in fills:
                yield fill
            
            cursor = resp.get("cursor")
            if not cursor:
                break
    
    # Phase 2.1: Get Balance Endpoint
    def get_balance(self) -> Dict[str, Any]:
        """Get portfolio balance with all fields"""
        path = "/portfolio/balance"
        response = self.make_request("GET", path)
        # Convert string fields to floats, handle NULL safely
        if "available_balance_dollars" in response:
            response["available_balance_dollars"] = float(response.get("available_balance_dollars") or 0)
        if "balance_dollars" in response:
            response["balance_dollars"] = float(response.get("balance_dollars") or 0)
        return response
    
    # Phase 2.2: Get Market Orderbook with Normalization
    def get_orderbook(self, ticker: Optional[str] = None, depth: int = 10) -> Dict[str, Any]:
        """Get full orderbook for market"""
        ticker = ticker or self.market_ticker
        path = "/markets/orderbook"  # Correct endpoint path
        params = {"ticker": ticker, "depth": depth}
        response = self.make_request("GET", path, params=params)
        return self.normalize_orderbook(response)
    
    def normalize_orderbook(self, ob: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize orderbook to consistent format"""
        bids = [(float(p), int(s)) for p, s in ob.get("bids", [])]
        asks = [(float(p), int(s)) for p, s in ob.get("asks", [])]
        
        # Calculate metrics
        total_bid_depth = sum(size for _, size in bids)
        total_ask_depth = sum(size for _, size in asks)
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        
        # Top-5 weighted midprice
        top5_bids = bids[:5]
        top5_asks = asks[:5]
        weighted_mid = None
        if top5_bids and top5_asks:
            bid_weight = sum(size for _, size in top5_bids)
            ask_weight = sum(size for _, size in top5_asks)
            if bid_weight > 0 and ask_weight > 0:
                bid_avg = sum(price * size for price, size in top5_bids) / bid_weight
                ask_avg = sum(price * size for price, size in top5_asks) / ask_weight
                weighted_mid = (bid_avg + ask_avg) / 2
        
        return {
            "bids": bids,
            "asks": asks,
            "total_bid_depth": total_bid_depth,
            "total_ask_depth": total_ask_depth,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "weighted_mid_5": weighted_mid,
            "spread": best_ask - best_bid if (best_bid and best_ask) else None
        }
    
    # Phase 2.3: Total Resting Order Value
    def get_total_resting_order_value(self) -> Dict[str, float]:
        """Get total resting order value and exposure metrics"""
        path = "/portfolio/total-resting-order-value"
        response = self.make_request("GET", path)
        
        # Extract all useful fields, handle NULL safely
        return {
            "total_resting_order_value_dollars": float(
                response.get("total_resting_order_value_dollars") or 0
            ),
            "pending_pnl_dollars": float(
                response.get("pending_pnl_dollars") or 0
            ),
            "total_exposure_dollars": float(
                response.get("total_exposure_dollars") or 0
            )
        }
    
    # Phase 2.4: Cancel All Orders with Rate Limiting
    def cancel_all_orders(self, ticker: Optional[str] = None) -> int:
        """Cancel all orders with rate limiting"""
        # Temporarily set ticker if provided
        original_ticker = self.market_ticker
        if ticker:
            self.market_ticker = ticker
        
        try:
            orders = self.get_orders()  # Already filtered by ticker if set
            cancelled_count = 0
            
            for order in orders:
                try:
                    if self.cancel_order(order['order_id']):
                        cancelled_count += 1
                    # Rate limit: ~7 cancels/sec
                    time.sleep(0.15)
                except Exception as e:
                    self.logger.warning(f"Failed to cancel order {order['order_id']}: {e}")
            
            return cancelled_count
        finally:
            # Restore original ticker
            if ticker:
                self.market_ticker = original_ticker

class AvellanedaMarketMaker:
    def __init__(
        self,
        logger: logging.Logger,
        api: AbstractTradingAPI,
        gamma: float,
        k: float,
        sigma: float,
        T: float,
        max_position: int,
        order_expiration: int,
        min_spread: float = 0.01,
        position_limit_buffer: float = 0.1,
        inventory_skew_factor: float = 0.01,
        trade_side: str = "yes",
        analytics_db: Optional[object] = None,
        run_id: Optional[int] = None,
        config: Optional[Dict] = None,
        extreme_band: str = "normal",
        one_sided_quoting: bool = False,
        ws_client: Optional[object] = None,  # KalshiWebsocketClient
        state_store: Optional[object] = None,  # MarketStateStore
    ):
        self.api = api
        self.logger = logger
        self.base_gamma = gamma
        self.k = k
        self.sigma = sigma
        self.T = T
        self.max_position = max_position
        self.order_expiration = order_expiration
        self.min_spread = min_spread
        self.max_spread = 0.50  # Maximum spread cap
        self.position_limit_buffer = position_limit_buffer
        self.inventory_skew_factor = inventory_skew_factor
        self.trade_side = trade_side
        self.analytics_db = analytics_db
        self.run_id = run_id
        self.last_position = None
        self.config = config or {}
        self.extreme_band = extreme_band
        self.one_sided_quoting = one_sided_quoting
        
        # Throttling state
        self.throttle_state = {
            'spread_widened': {'count': 0, 'active': False},
            'book_thin': {'count': 0, 'active': False},
            'adverse_selection': {'count': 0, 'active': False}
        }
        
        # Adverse selection tracking
        self.fill_history = []  # List of (timestamp, action, price_change) tuples
        self.price_history = []  # List of (timestamp, mid_price) tuples for price change calculation
        
        # Soft exit tracking
        self.soft_exit_start_time = None  # Track when soft exit mode started
        
        # Phase 5.4: Order Acknowledgement Tracking
        self.order_tracking = {}  # order_id -> {'timestamp': datetime, 'price': float, 'side': str, 'action': str}
        self.last_bid_order_id = None  # Track last bid order
        self.last_ask_order_id = None  # Track last ask order
        
        # Phase 3.1: WebSocket integration
        self.ws_client = ws_client
        self.state_store = state_store
        self.ws_enabled = config.get('websockets', {}).get('enabled', False) if config else False
        self.ws_fallback = config.get('websockets', {}).get('ws_fallback_on_unhealthy', True) if config else True
        
        # Get throttling config
        throttling_config = self.config.get('throttling', {})
        self.trigger_threshold = throttling_config.get('trigger_threshold', 3)
        self.recovery_threshold = throttling_config.get('recovery_threshold', 5)
        self.max_spread_threshold = throttling_config.get('max_spread_threshold', 0.20)
        self.spread_widen_ticks = throttling_config.get('spread_widen_ticks', 5)
        self.min_book_depth = throttling_config.get('min_book_depth', 100)
        self.book_thin_widen_ticks = throttling_config.get('book_thin_widen_ticks', 3)
        self.adverse_selection_window = throttling_config.get('adverse_selection_window_seconds', 300)
        self.adverse_selection_threshold = throttling_config.get('adverse_selection_threshold', 0.7)
        
        # Get risk horizon from config
        as_config = self.config.get('as_model', {})
        self.risk_horizon_seconds = as_config.get('risk_horizon_seconds', 3600)
        self.min_total_spread = as_config.get('min_total_spread', 0.02)
        self.max_total_spread = as_config.get('max_total_spread', 0.05)
        
        # Session tracking for fill reconciliation
        self.session_start_ts = int(time.time())
        self.last_fill_ts = self.session_start_ts
    
    def _get_orderbook(self) -> Optional[Dict]:
        """
        Get orderbook data with WebSocket-first, REST fallback logic.
        Returns normalized orderbook dict or None if unavailable.
        """
        ticker = self.api.market_ticker
        
        # Try WebSocket first if enabled and healthy
        if self.ws_enabled and self.state_store and (
            self.state_store.ws_healthy or not self.ws_fallback
        ):
            ws_orderbook = self.state_store.get_orderbook(ticker)
            if ws_orderbook:
                bid_depth = ws_orderbook.get("total_bid_depth", 0)
                ask_depth = ws_orderbook.get("total_ask_depth", 0)
                bid_levels = len(ws_orderbook.get("bids", []))
                ask_levels = len(ws_orderbook.get("asks", []))
                self.logger.debug(
                    f"Using WebSocket orderbook for {ticker}: "
                    f"bid_depth={bid_depth}, ask_depth={ask_depth}, "
                    f"bid_levels={bid_levels}, ask_levels={ask_levels}, "
                    f"best_bid={ws_orderbook.get('best_bid')}, best_ask={ws_orderbook.get('best_ask')}"
                )
                # WebSocket format already normalized by state_store
                # Ensure it has all expected fields
                return {
                    "bids": ws_orderbook.get("bids", []),
                    "asks": ws_orderbook.get("asks", []),
                    "total_bid_depth": bid_depth,
                    "total_ask_depth": ask_depth,
                    "best_bid": ws_orderbook.get("best_bid"),
                    "best_ask": ws_orderbook.get("best_ask"),
                    "spread": ws_orderbook.get("spread"),
                    "imbalance": ws_orderbook.get("imbalance"),  # WebSocket-specific
                }
            else:
                # WebSocket orderbook unavailable or stale, fallback to REST
                if self.ws_fallback:
                    self.logger.debug(f"WebSocket orderbook unavailable for {ticker}, falling back to REST")
                else:
                    self.logger.warning(f"WebSocket orderbook unavailable for {ticker} and fallback disabled")
                    return None
        
        # REST fallback
        try:
            rest_orderbook = self.api.get_orderbook(ticker)
            if rest_orderbook:
                self.logger.debug(f"Using REST orderbook for {ticker}")
                # REST format already normalized by api.normalize_orderbook()
                # Calculate imbalance if not present (WebSocket has it, REST doesn't)
                if "imbalance" not in rest_orderbook:
                    total_bid = rest_orderbook.get("total_bid_depth", 0)
                    total_ask = rest_orderbook.get("total_ask_depth", 0)
                    imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1) if (total_bid + total_ask) > 0 else 0
                    rest_orderbook["imbalance"] = imbalance
                return rest_orderbook
        except requests.exceptions.HTTPError as e:
            # Handle 404 specifically - some markets don't have orderbook data available
            if hasattr(e.response, 'status_code') and e.response.status_code == 404:
                self.logger.debug(f"REST orderbook not available for {ticker} (404) - using spread heuristic")
            else:
                self.logger.warning(f"Failed to get REST orderbook for {ticker}: {e}")
        except Exception as e:
            # Check if it's a 404 error in the error message
            error_str = str(e).lower()
            if '404' in error_str or 'not found' in error_str:
                self.logger.debug(f"REST orderbook not available for {ticker} (404) - using spread heuristic")
            else:
                self.logger.warning(f"Failed to get REST orderbook for {ticker}: {e}")
        
        return None
    
    # Phase 5.4: Order Tracking Methods
    def _track_order(self, order_id: str, price: float, side: str, action: str):
        """Track order placement for staleness detection"""
        self.order_tracking[order_id] = {
            'timestamp': datetime.now(timezone.utc),
            'price': price,
            'side': side,
            'action': action
        }
        # Track last bid/ask order IDs
        if action == 'buy':
            self.last_bid_order_id = order_id
        elif action == 'sell':
            self.last_ask_order_id = order_id
    
    def _check_order_staleness(self, max_age_seconds: int = 30) -> List[str]:
        """Return list of stale order IDs"""
        now = datetime.now(timezone.utc)
        stale_orders = []
        
        for order_id, info in list(self.order_tracking.items()):
            age = (now - info['timestamp']).total_seconds()
            if age > max_age_seconds:
                stale_orders.append(order_id)
        
        return stale_orders

    def check_spread_widening(self, current_spread: float) -> bool:
        """Check if spread has widened beyond threshold"""
        return current_spread > self.max_spread_threshold
    
    def check_book_health(self, market_data: Dict) -> bool:
        """
        Check if book depth is sufficient using real orderbook data.
        Falls back to spread heuristic if orderbook unavailable.
        """
        # Try to get orderbook data
        orderbook = self._get_orderbook()
        
        if orderbook:
            # Use actual book depth
            total_bid_depth = orderbook.get("total_bid_depth", 0)
            total_ask_depth = orderbook.get("total_ask_depth", 0)
            bid_levels = len(orderbook.get("bids", []))
            ask_levels = len(orderbook.get("asks", []))
            
            # If orderbook is completely empty (both sides = 0), fall back to spread heuristic
            # This handles temporary orderbook emptiness more gracefully
            if total_bid_depth == 0 and total_ask_depth == 0:
                self.logger.info(
                    f"Book health check: Orderbook empty (bid_depth=0, ask_depth=0, "
                    f"bid_levels={bid_levels}, ask_levels={ask_levels}) - falling back to spread heuristic"
                )
                # Fall through to spread heuristic below
            elif total_bid_depth < self.min_book_depth or total_ask_depth < self.min_book_depth:
                self.logger.info(
                    f"Book health check FAILED: bid_depth={total_bid_depth}, ask_depth={total_ask_depth}, "
                    f"bid_levels={bid_levels}, ask_levels={ask_levels}, min_required={self.min_book_depth}"
                )
                return False
            else:
                # Book has sufficient depth - check spread as secondary validation
                orderbook_spread = orderbook.get("spread")
                if orderbook_spread is not None:
                    # If orderbook spread is extremely wide, book is likely unhealthy
                    if orderbook_spread > self.max_spread_threshold * 2:
                        self.logger.info(
                            f"Book health check FAILED: orderbook spread too wide ({orderbook_spread:.4f} > "
                            f"{self.max_spread_threshold * 2:.4f}), bid_depth={total_bid_depth}, ask_depth={total_ask_depth}"
                        )
                        return False
                
                # Log successful health check with depth info
                spread_str = f"{orderbook_spread:.4f}" if orderbook_spread is not None else "None"
                self.logger.debug(
                    f"Book health check PASSED: bid_depth={total_bid_depth}, ask_depth={total_ask_depth}, "
                    f"bid_levels={bid_levels}, ask_levels={ask_levels}, spread={spread_str}"
                )
                return True
        
        # Fallback to spread heuristic if orderbook unavailable or empty
        bid = market_data.get(f"{self.trade_side}_bid", 0)
        ask = market_data.get(f"{self.trade_side}_ask", 0)
        spread = ask - bid
        # If spread is very wide, assume book is thin
        self.logger.debug(f"Orderbook unavailable or empty, using spread heuristic: {spread:.4f}")
        return spread < self.max_spread_threshold * 2
    
    def detect_adverse_selection(self) -> bool:
        """Detect adverse selection: fills on wrong side of price moves"""
        current_time = time.time()
        # Remove old entries
        self.fill_history = [(ts, a, pc) for ts, a, pc in self.fill_history 
                            if current_time - ts < self.adverse_selection_window]
        
        if len(self.fill_history) < 5:
            return False  # Not enough data
        
        # Count fills on wrong side of moves
        wrong_side_count = 0
        for _, action, price_change in self.fill_history:
            # If we bought and price went down, or sold and price went up
            if (action == 'buy' and price_change < 0) or (action == 'sell' and price_change > 0):
                wrong_side_count += 1
        
        ratio = wrong_side_count / len(self.fill_history)
        
        if ratio >= self.adverse_selection_threshold:
            self.logger.warning(
                f"Adverse selection detected: {wrong_side_count}/{len(self.fill_history)} "
                f"fills on wrong side (threshold: {self.adverse_selection_threshold:.2f})"
            )
        
        return ratio >= self.adverse_selection_threshold
    
    def _track_fill(self, order_id: str, action: str, fill_price: float, 
                    fill_quantity: int, fill_timestamp: float):
        """
        Track a fill in fill_history for adverse selection detection.
        
        Args:
            order_id: Order ID that was filled
            action: 'buy' or 'sell'
            fill_price: Price at which order was filled
            fill_quantity: Quantity filled
            fill_timestamp: Timestamp when fill occurred
        """
        # Calculate price change: compare price at fill time vs price from before fill
        # For adverse selection, we want to know if price moved against us after fill
        price_change_window = 30  # seconds - look back this far for previous price
        cutoff_time = fill_timestamp - price_change_window
        
        # Find price from before the fill (ideally from price_change_window seconds ago)
        previous_price = None
        for ts, price in reversed(self.price_history):
            if ts <= cutoff_time:
                previous_price = price
                break
        
        # If no price from window ago, use most recent price before fill time
        if previous_price is None:
            for ts, price in reversed(self.price_history):
                if ts < fill_timestamp:
                    previous_price = price
                    break
        
        # Get price at/after fill time (use most recent price in history, or fill_price as fallback)
        # Note: price_history is updated in run() loop, so most recent entry might be slightly after fill
        current_price = fill_price
        if self.price_history:
            # Use most recent price (should be close to fill time)
            current_price = self.price_history[-1][1]
        
        # Calculate price change: positive = price went up, negative = price went down
        if previous_price is not None and previous_price > 0:
            price_change = current_price - previous_price
        else:
            # No previous price available, use 0 (neutral) - can't determine direction
            price_change = 0.0
            self.logger.debug(f"No previous price found for fill {order_id}, using price_change=0")
        
        # Add to fill_history
        self.fill_history.append((fill_timestamp, action, price_change))
        
        # Trim old entries beyond adverse_selection_window
        current_time = time.time()
        old_count = len(self.fill_history)
        self.fill_history = [(ts, a, pc) for ts, a, pc in self.fill_history 
                            if current_time - ts < self.adverse_selection_window]
        trimmed_count = old_count - len(self.fill_history)
        if trimmed_count > 0:
            self.logger.debug(f"Trimmed {trimmed_count} old entries from fill_history")
        
        self.logger.info(
            f"Fill tracked: order_id={order_id}, action={action}, "
            f"price_change={price_change:.4f}, qty={fill_quantity}, "
            f"fill_history_size={len(self.fill_history)}"
        )
    
    def reconcile_new_fills(self):
        """
        Pull new fills from WS queue or REST fallback and log them.
        This is the single source of truth for fill tracking.
        """
        if not self.analytics_db or not self.run_id:
            return
        
        ticker = self.api.market_ticker
        min_ts = self.last_fill_ts
        
        new_max_ts = min_ts  # Will bump as we see newer fills
        
        # Phase 3.3: Wire User Fills Channel
        fills_list = []
        if self.ws_enabled and self.state_store and self.state_store.ws_healthy:
            # Drain WS fills queue
            fills_list = self.state_store.get_user_fills()
            # Filter fills for this ticker and since min_ts
            filtered_fills = []
            for fill in fills_list:
                fill_ticker = fill.get('ticker', '')
                if fill_ticker == ticker:
                    ts_raw = fill.get('ts', '')
                    if ts_raw:
                        fill_dt = parse_kalshi_timestamp_cached(str(ts_raw))
                        if fill_dt and fill_dt.timestamp() >= min_ts:
                            filtered_fills.append(fill)
            fills_list = filtered_fills
        else:
            # REST fallback
            try:
                fills_iter = self.api.iter_fills_since(
                    min_ts=min_ts,
                    ticker=ticker,
                )
                fills_list = list(fills_iter)
            except Exception as e:
                self.logger.warning(f"Failed to fetch fills from Kalshi: {e}", exc_info=True)
                return
        
        # 2) Map our own orders (this run) by order_id
        with self.analytics_db.get_connection() as conn:
            my_orders = {
                row["order_id"]: row
                for row in conn.execute(
                    """
                    SELECT order_id, placed_quantity
                    FROM orders
                    WHERE run_id = ?
                    """,
                    (self.run_id,),
                ).fetchall()
            }
            
            for fill in fills_list:
                # Phase 1.1: Use cached timestamp parser
                ts_raw = fill.get("ts")
                if ts_raw:
                    # Convert to string for caching
                    ts_str = str(ts_raw)
                    fill_dt = parse_kalshi_timestamp_cached(ts_str)
                    if fill_dt:
                        ts_val = int(fill_dt.timestamp())
                        if ts_val > new_max_ts:
                            new_max_ts = ts_val
                    else:
                        # Fallback to direct int parsing
                        try:
                            ts_val = int(ts_raw)
                            if ts_val > 1e10:  # Milliseconds
                                ts_val = ts_val // 1000
                            if ts_val > new_max_ts:
                                new_max_ts = ts_val
                        except:
                            ts_val = 0
                else:
                    ts_val = 0
                
                order_id = str(fill.get("order_id", ""))
                
                # Ignore fills for orders we didn't place in this run
                if order_id not in my_orders:
                    continue
                
                fill_id = str(fill.get("fill_id", ""))
                if not fill_id:
                    self.logger.warning(f"Fill missing fill_id: {fill}")
                    continue
                
                # CRITICAL FIX 2.2: Handle string or numeric count
                count_raw = fill.get("count", 0)
                count = int(count_raw) if count_raw else 0
                if count <= 0:
                    continue
                
                # CRITICAL FIX 2.2: Handle string or numeric price
                price_raw = fill.get("price", 0)
                if isinstance(price_raw, str):
                    price_cents = int(float(price_raw))
                else:
                    price_cents = int(price_raw)
                price_dollars = price_cents / 100.0
                action = fill.get("action", "")  # 'buy' | 'sell'
                side = fill.get("side", "")  # 'yes' | 'no'
                
                # 3) Skip if we already logged this fill
                existing = conn.execute(
                    "SELECT 1 FROM order_fills WHERE fill_id = ?",
                    (fill_id,),
                ).fetchone()
                if existing:
                    continue
                
                # 4) Insert into order_fills table
                self.analytics_db.log_fill_event(
                    fill_id=fill_id,
                    order_id=order_id,
                    run_id=self.run_id,
                    ticker=ticker,
                    side=side,
                    action=action,
                    count=count,
                    price_cents=price_cents,
                    ts=ts_val,
                )
                
                # 5) Call existing analytics hooks
                self.analytics_db.log_order_filled(
                    order_id=order_id,
                    price=price_dollars,
                    filled_quantity=count,
                )
                
                # Track for adverse selection detection
                fill_timestamp = float(ts_val)
                self._track_fill(
                    order_id=order_id,
                    action=action,
                    fill_price=price_dollars,
                    fill_quantity=count,
                    fill_timestamp=fill_timestamp,
                )
                
                # Phase 5.5: Partial Fill Adjustments
                remaining_count = fill.get("remaining_count", 0)
                if remaining_count > 0:  # Partial fill
                    self.logger.info(
                        f"Partial fill: {count} of {count + remaining_count} "
                        f"filled. Remaining: {remaining_count}"
                    )
                    # Phase 5.5: Automatic k/gamma adjustments for partial fills
                    # Increase risk aversion temporarily
                    self.base_gamma *= 1.2
                    # Reduce order sizes
                    self.max_position = max(1, int(self.max_position * 0.8))
                    self.logger.info(
                        f"Adjusted risk parameters: gamma={self.base_gamma:.4f}, "
                        f"max_position={self.max_position}"
                    )
                
                self.logger.info(
                    f"New fill {fill_id} for order {order_id}: "
                    f"{count}@{price_dollars} ({ticker})"
                )
            
            # 6) Advance the "watermark" for next run
            self.last_fill_ts = max(self.last_fill_ts, new_max_ts)
    
    def update_throttle_state(self, check_name: str, is_bad: bool):
        """Update throttle state with hysteresis"""
        state = self.throttle_state[check_name]
        
        if is_bad:
            state['count'] += 1
            if state['count'] >= self.trigger_threshold and not state['active']:
                state['active'] = True
                self.logger.warning(f"THROTTLE_REASON={check_name.upper()} triggered for {self.api.market_ticker}")
        else:
            if state['active']:
                state['count'] = max(0, state['count'] - 1)
                if state['count'] == 0:
                    # Need N consecutive good checks to recover
                    recovery_count = getattr(self, f'_{check_name}_recovery_count', 0)
                    recovery_count += 1
                    setattr(self, f'_{check_name}_recovery_count', recovery_count)
                    if recovery_count >= self.recovery_threshold:
                        state['active'] = False
                        setattr(self, f'_{check_name}_recovery_count', 0)
                        self.logger.info(f"THROTTLE_REASON={check_name.upper()} recovered for {self.api.market_ticker}")
            else:
                state['count'] = max(0, state['count'] - 1)
                setattr(self, f'_{check_name}_recovery_count', 0)

    def close_positions(self) -> bool:
        """
        Close all open positions for this market/side.
        Basic implementation: places limit order at current market price.
        
        Returns:
            True if order placed successfully, False otherwise
        """
        try:
            # Get current position
            position = self.api.get_position()
            
            if position == 0:
                self.logger.info("No open position to close")
                return True
            
            # Get market prices
            market_prices = self.api.get_price()
            
            # Determine action and price
            if position > 0:
                # Long position - need to sell
                action = "sell"
                price = market_prices.get(f"{self.trade_side}_bid", 0)
            else:
                # Short position - need to buy
                action = "buy"
                price = market_prices.get(f"{self.trade_side}_ask", 0)
            
            quantity = abs(position)
            
            # Validate price
            if price <= 0 or price > 1.0:
                self.logger.error(f"Invalid price {price} for closing position, skipping")
                return False
            
            # Place order
            self.logger.info(f"Closing position: {action} {quantity} @ ${price:.2f}")
            order_id = self.api.place_order(action, self.trade_side, price, quantity)
            # Phase 5.4: Track order placement
            self._track_order(order_id, price, self.trade_side, action)
            self.logger.info(f"Position close order placed: {order_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error closing positions: {e}", exc_info=True)
            return False

    def run(self, dt: float):
        start_time = time.time()
        last_price = None
        ttl_expired = False
        quoting_frozen = False
        
        # Check for passive exit mode
        vol_mm_config = self.config.get('volatility_mm', {})
        passive_exit = vol_mm_config.get('passive_exit', False)
        ttl_soft_stop = vol_mm_config.get('ttl_soft_stop', False)
        ttl_close_if_small = vol_mm_config.get('ttl_close_if_small', 2)
        
        try:
            while time.time() - start_time < self.T:
                current_time = time.time() - start_time
                
                # Check if TTL expired (for passive exit mode)
                if ttl_soft_stop and current_time >= self.T:
                    if not ttl_expired:
                        ttl_expired = True
                        quoting_frozen = True
                        self.soft_exit_start_time = time.time()  # Track start time
                        self.logger.info("TTL expired - freezing quotes (passive exit mode)")
                        
                        # Get current position
                        inventory = self.api.get_position()
                        
                        # If position is small, close it immediately
                        if abs(inventory) < ttl_close_if_small:
                            self.logger.info(f"Position is small ({inventory}), closing immediately...")
                            try:
                                success = self.close_positions()
                                if success:
                                    self.logger.info("Small position closed successfully")
                                else:
                                    self.logger.warning("Failed to close small position")
                            except Exception as e:
                                self.logger.error(f"Error closing small position: {e}", exc_info=True)
                            break  # Exit loop after closing small position
                        else:
                            self.logger.info(
                                f"Position is large ({inventory}), keeping open. "
                                f"Will place passive limit orders after wait period."
                            )
                            # Wait a bit for mean reversion, then place passive orders
                            time.sleep(15)  # Wait 15 seconds
                            
                            # Place passive limit orders at better prices (with slippage tolerance)
                            try:
                                mid_prices = self.api.get_price()
                                inventory = self.api.get_position()
                                max_slippage_cents = vol_mm_config.get('max_slippage_for_limit_exit_cents', 4)
                                max_slippage = max_slippage_cents / 100.0  # Convert to dollars
                                
                                if inventory != 0:
                                    mid_price = mid_prices.get(self.trade_side, 0.5)
                                    
                                    if inventory > 0:
                                        # Long position - place sell order within slippage tolerance
                                        action = "sell"
                                        min_sell_price = mid_price - max_slippage
                                        price = mid_prices.get(f"{self.trade_side}_ask", mid_price)
                                        if price < min_sell_price:
                                            self.logger.warning(
                                                f"Skipping passive sell: price {price:.4f} below slippage tolerance "
                                                f"(min={min_sell_price:.4f})"
                                            )
                                            break
                                    else:
                                        # Short position - place buy order within slippage tolerance
                                        action = "buy"
                                        max_buy_price = mid_price + max_slippage
                                        price = mid_prices.get(f"{self.trade_side}_bid", mid_price)
                                        if price > max_buy_price:
                                            self.logger.warning(
                                                f"Skipping passive buy: price {price:.4f} above slippage tolerance "
                                                f"(max={max_buy_price:.4f})"
                                            )
                                            break
                                    
                                    quantity = abs(inventory)
                                    if price > 0 and price <= 1.0:
                                        self.logger.info(
                                            f"Placing passive {action} order: {quantity} @ ${price:.2f} "
                                            f"(slippage tolerance: {max_slippage_cents}c)"
                                        )
                                        order_id = self.api.place_order(action, self.trade_side, price, quantity)
                                        # Phase 5.4: Track order placement
                                        self._track_order(order_id, price, self.trade_side, action)
                            except Exception as e:
                                self.logger.error(f"Error placing passive exit orders: {e}", exc_info=True)
                            
                            break  # Exit loop after placing passive orders
                
                # Check soft exit timeout
                if self.soft_exit_start_time:
                    soft_exit_max_wait = vol_mm_config.get('soft_exit_max_wait_seconds', 20)
                    time_in_soft_exit = time.time() - self.soft_exit_start_time
                    if time_in_soft_exit > soft_exit_max_wait:
                        self.logger.warning(
                            f"Soft exit timeout ({time_in_soft_exit:.1f}s > {soft_exit_max_wait}s), "
                            f"forcing hard exit"
                        )
                        try:
                            self.close_positions()
                        except Exception as e:
                            self.logger.error(f"Error in soft exit timeout: {e}", exc_info=True)
                        break
                
                # If quoting is frozen, skip the main loop
                if quoting_frozen:
                    time.sleep(dt)
                    continue
                
                self.logger.info(f"Running Avellaneda market maker at {current_time:.2f}")

                # Phase 3.2: Replace get_price() with Store Read
                # CRITICAL FIX 3.4: Check WS health
                mid_prices = None
                if self.ws_enabled and self.state_store and (
                    self.state_store.ws_healthy or not self.ws_fallback
                ):
                    ticker_data = self.state_store.get_normalized_ticker(self.api.market_ticker)
                    if ticker_data:
                        # Extract prices using get_price_field() helper
                        yes_bid = ticker_data.get("yes_bid") or 0.0
                        yes_ask = ticker_data.get("yes_ask") or 0.0
                        no_bid = ticker_data.get("no_bid") or 0.0
                        no_ask = ticker_data.get("no_ask") or 0.0
                        yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid > 0 and yes_ask > 0) else 0.0
                        no_mid = (no_bid + no_ask) / 2 if (no_bid > 0 and no_ask > 0) else 0.0
                        mid_prices = {
                            "yes": yes_mid,
                            "no": no_mid,
                            "yes_bid": yes_bid,
                            "yes_ask": yes_ask,
                            "no_bid": no_bid,
                            "no_ask": no_ask
                        }
                    else:
                        # Fallback to REST
                        mid_prices = self.api.get_price()
                else:
                    # REST-only or WS unhealthy
                    mid_prices = self.api.get_price()
                
                # ENHANCEMENT 7: Freeze quoting if WS unhealthy for >5s
                if self.ws_enabled and self.state_store and not self.state_store.ws_healthy:
                    ws_unhealthy_seconds = (datetime.now(timezone.utc) - self.state_store.last_message_time).total_seconds()
                    ws_staleness_freeze = self.config.get('websockets', {}).get('ws_staleness_freeze_seconds', 5)
                    if ws_unhealthy_seconds > ws_staleness_freeze:
                        self.logger.warning(f"WS unhealthy >{ws_staleness_freeze}s — freezing quoting")
                        quoting_frozen = True
                        continue
                
                mid_price = mid_prices[self.trade_side]
                
                # Phase 3.4: Replace get_position() with Store Read
                if self.ws_enabled and self.state_store and self.state_store.ws_healthy:
                    position = self.state_store.get_position(self.api.market_ticker)
                    if position is not None:
                        inventory = position
                    else:
                        # Fallback to REST
                        inventory = self.api.get_position()
                else:
                    # REST-only or WS unhealthy
                    inventory = self.api.get_position()
                self.logger.info(f"Current mid price for {self.trade_side}: {mid_price:.4f}, Inventory: {inventory}")

                # Track price history for fill tracking
                current_timestamp = time.time()
                self.price_history.append((current_timestamp, mid_price))
                # Trim price_history to keep only last 10 minutes
                cutoff_time = current_timestamp - 600  # 10 minutes
                self.price_history = [(ts, price) for ts, price in self.price_history if ts >= cutoff_time]

                # Phase 5.2: Regime kill switch check
                vol_mm_config = self.config.get('volatility_mm', {})
                regime_kill_config = vol_mm_config.get('regime_kill', {})
                if regime_kill_config.get('enabled', False):
                    scanner = self.config.get('scanner')  # Passed from manager
                    ticker = self.api.market_ticker
                    
                    if scanner:
                        regime_info = scanner.get_regime(ticker)
                        
                        # Check regime data freshness (consolidated checks)
                        if regime_info:
                            regime_timestamp = regime_info.get('timestamp')
                            if regime_timestamp:
                                from datetime import datetime, timezone
                                now = datetime.now(timezone.utc)
                                if regime_timestamp.tzinfo is None:
                                    regime_timestamp = regime_timestamp.replace(tzinfo=timezone.utc)
                                age_seconds = (now - regime_timestamp).total_seconds()
                                
                                # Check shared state age
                                regimes_config = self.config.get('regimes', {})
                                max_shared_state_age = regimes_config.get('max_shared_state_age_seconds', 4)
                                if age_seconds > max_shared_state_age:
                                    self.logger.warning(
                                        f"Shared state age exceeded ({age_seconds:.1f}s > {max_shared_state_age}s), "
                                        f"freezing quotes"
                                    )
                                    quoting_frozen = True
                                    continue  # Skip this iteration
                                
                                # Check regime staleness
                                scanner_config = self.config.get('volatility_scanner', {})
                                regime_staleness_seconds = scanner_config.get('regime_staleness_seconds', 4)
                                if age_seconds > regime_staleness_seconds:
                                    self.logger.warning(
                                        f"Regime data stale ({age_seconds:.1f}s > {regime_staleness_seconds}s), "
                                        f"freezing quotes"
                                    )
                                    quoting_frozen = True
                                    continue  # Skip this iteration
                                
                                # Check for regime stale kill (hard kill threshold)
                                regime_stale_kill_seconds = vol_mm_config.get('regime_stale_kill_seconds', 4)
                                if age_seconds > regime_stale_kill_seconds:
                                    self.logger.error(
                                        f"Regime data critically stale ({age_seconds:.1f}s > {regime_stale_kill_seconds}s), "
                                        f"hard killing session"
                                    )
                                    try:
                                        self.close_positions()
                                    except Exception as e:
                                        self.logger.error(f"Error in stale regime hard kill: {e}", exc_info=True)
                                    break
                        
                        if regime_info:
                            current_regime = regime_info.get('regime')
                            bad_regimes = regime_kill_config.get('bad_regimes', ['TRENDING', 'CHAOTIC'])
                            require_adverse = regime_kill_config.get('require_adverse_selection', True)
                            
                            if current_regime in bad_regimes:
                                # Check if we should kill
                                should_kill = True
                                if require_adverse:
                                    adverse_selection = self.detect_adverse_selection()
                                    should_kill = adverse_selection
                                
                                if should_kill:
                                    kill_mode = regime_kill_config.get('kill_modes', {}).get(current_regime, 'soft')
                                    self.logger.warning(
                                        f"Regime kill switch triggered: regime={current_regime}, mode={kill_mode}"
                                    )
                                    
                                    if kill_mode == 'hard':
                                        # Hard kill: exit immediately
                                        self.logger.warning("Hard kill: closing positions immediately")
                                        try:
                                            self.close_positions()
                                        except Exception as e:
                                            self.logger.error(f"Error in hard kill: {e}", exc_info=True)
                                        break
                                    else:
                                        # Soft kill: freeze quoting, wait for limit exits
                                        quoting_frozen = True
                                        self.logger.warning("Soft kill: freezing quotes, waiting for passive exits")
                                        # Cancel all orders
                                        try:
                                            self.api.cancel_all_orders()
                                        except Exception as e:
                                            self.logger.error(f"Error canceling orders: {e}", exc_info=True)
                
                # Check throttling conditions
                current_spread = mid_prices[f"{self.trade_side}_ask"] - mid_prices[f"{self.trade_side}_bid"]
                
                # Check for spread shock (instant spread jumps)
                extreme_config = self.config.get('extreme_prices', {})
                spread_shock_threshold = extreme_config.get('spread_shock_threshold', 0.15)
                if current_spread > spread_shock_threshold:
                    self.logger.warning(
                        f"Spread shock detected: {current_spread:.4f} > {spread_shock_threshold:.4f}, "
                        f"freezing quotes"
                    )
                    quoting_frozen = True
                    continue  # Skip this iteration
                
                spread_widened = self.check_spread_widening(current_spread)
                book_healthy = self.check_book_health(mid_prices)
                adverse_selection = self.detect_adverse_selection()
                
                # Log book depth when throttling is triggered
                if not book_healthy:
                    orderbook = self._get_orderbook()
                    if orderbook:
                        total_bid_depth = orderbook.get("total_bid_depth", 0)
                        total_ask_depth = orderbook.get("total_ask_depth", 0)
                        self.logger.warning(
                            f"Book health check failed - triggering throttling: "
                            f"bid_depth={total_bid_depth}, ask_depth={total_ask_depth}, "
                            f"min_required={self.min_book_depth}"
                        )
                
                self.update_throttle_state('spread_widened', spread_widened)
                self.update_throttle_state('book_thin', not book_healthy)
                self.update_throttle_state('adverse_selection', adverse_selection)
                
                # Track last price for reference (price changes now tracked in fill_history)
                last_price = mid_price

                reservation_price = self.calculate_reservation_price(mid_price, inventory, current_time)
                bid_price, ask_price = self.calculate_asymmetric_quotes(mid_price, inventory, current_time)
                buy_size, sell_size = self.calculate_order_sizes(inventory)

                self.logger.info(f"Reservation price: {reservation_price:.4f}")
                self.logger.info(f"Computed desired bid: {bid_price:.4f}, ask: {ask_price:.4f}")

                # Log market snapshot to analytics database
                if self.analytics_db and self.run_id:
                    try:
                        # Get regime info if scanner available
                        regime = None
                        regime_duration = None
                        prev_regime = None
                        regime_transition = False
                        
                        scanner = self.config.get('scanner')
                        ticker = self.api.market_ticker
                        if scanner:
                            regime_info = scanner.get_regime(ticker)
                            if regime_info:
                                regime = regime_info.get('regime')
                                # Calculate duration (simplified - would need to track regime start time)
                                # For now, just log the regime
                        
                        self.analytics_db.log_market_snapshot(
                            self.run_id,
                            mid_prices,
                            inventory,
                            reservation_price,
                            bid_price,
                            ask_price,
                            regime=regime,
                            regime_duration_seconds=regime_duration,
                            prev_regime=prev_regime,
                            regime_transition=regime_transition
                        )
                        # Log position change if it changed
                        if self.last_position != inventory:
                            self.analytics_db.log_position(self.run_id, inventory)
                            self.last_position = inventory
                    except Exception as e:
                        self.logger.warning(f"Failed to log market snapshot to analytics: {e}")

                self.manage_orders(bid_price, ask_price, buy_size, sell_size)

                time.sleep(dt)
        finally:
            # Only close positions if NOT in passive exit mode, or if passive exit didn't handle it
            if not passive_exit or not ttl_expired:
                close_on_exit = self.config.get('volatility_mm', {}).get('close_positions_on_exit', True)
                if close_on_exit:
                    try:
                        self.logger.info("Closing positions before session end...")
                        success = self.close_positions()
                        if success:
                            self.logger.info("Position close order placed successfully")
                        else:
                            self.logger.warning("Failed to place position close order")
                    except Exception as e:
                        self.logger.error(f"Error closing positions: {e}", exc_info=True)
            else:
                self.logger.info("Passive exit mode: positions left open or handled by passive orders")
            
            self.logger.info("Avellaneda market maker finished running")
            
            # Mark strategy run as ended
            if self.analytics_db and self.run_id:
                try:
                    self.analytics_db.end_strategy_run(self.run_id)
                except Exception as e:
                    self.logger.warning(f"Failed to end strategy run in analytics: {e}")

    def calculate_asymmetric_quotes(self, mid_price: float, inventory: int, t: float) -> Tuple[float, float]:
        reservation_price = self.calculate_reservation_price(mid_price, inventory, t)
        base_spread = self.calculate_optimal_spread(t, inventory)
        
        # Apply throttling adjustments if active
        spread_adjustment_multiplier = 1.0
        if self.throttle_state['spread_widened']['active']:
            spread_adjustment_multiplier += self.spread_widen_ticks * 0.01  # Widen by N ticks
        if self.throttle_state['book_thin']['active']:
            spread_adjustment_multiplier += self.book_thin_widen_ticks * 0.01
        
        base_spread *= spread_adjustment_multiplier
        
        # Enforce max_total_widened_spread cap
        throttling_config = self.config.get('throttling', {})
        max_total_widened_spread = throttling_config.get('max_total_widened_spread', 0.50)
        if base_spread > max_total_widened_spread:
            self.logger.warning(
                f"Spread widening capped: {base_spread:.4f} > {max_total_widened_spread:.4f}"
            )
            base_spread = max_total_widened_spread
        
        position_ratio = inventory / self.max_position
        inventory_adjustment = base_spread * abs(position_ratio) * 3
        
        if inventory > 0:
            bid_spread = base_spread / 2 + inventory_adjustment
            ask_spread = max(base_spread / 2 - inventory_adjustment, self.min_spread / 2)
        else:
            bid_spread = max(base_spread / 2 - inventory_adjustment, self.min_spread / 2)
            ask_spread = base_spread / 2 + inventory_adjustment
        
        # Calculate base total spread (sum of bid_spread + ask_spread)
        base_total_spread = bid_spread + ask_spread
        
        # Cap total spread to min/max limits
        total_spread = max(self.min_total_spread, min(base_total_spread, self.max_total_spread))
        
        # Log if spread was capped
        if base_total_spread > self.max_total_spread:
            self.logger.info(f"Capped total spread from {base_total_spread:.4f} to {self.max_total_spread:.4f}")
        
        # Recalculate half-spread from capped total
        half_spread = total_spread / 2
        
        # Apply symmetric spread around reservation price
        # Inventory skew is already handled via reservation_price adjustment
        bid_price = max(0.01, reservation_price - half_spread)
        ask_price = min(0.99, reservation_price + half_spread)
        
        # Handle one-sided quoting for hard extremes
        if self.one_sided_quoting:
            if mid_price < 0.05:
                # Only post bids (we like being long cheap)
                ask_price = 1.0  # Set to max to effectively disable
            elif mid_price > 0.95:
                # Only post offers (we don't want to be long expensive)
                bid_price = 0.0  # Set to min to effectively disable
        
        return bid_price, ask_price

    def calculate_reservation_price(self, mid_price: float, inventory: int, t: float) -> float:
        dynamic_gamma = self.calculate_dynamic_gamma(inventory)
        inventory_skew = inventory * self.inventory_skew_factor * mid_price
        return mid_price + inventory_skew - inventory * dynamic_gamma * (self.sigma**2) * (1 - t/self.T)

    def calculate_optimal_spread(self, t: float, inventory: int) -> float:
        """
        Calculate optimal half-spread using AS formula with shorter risk horizon
        δ* ≈ (γσ²(T-t))/2 + (1/γ)*ln(1+γ/k)
        """
        dynamic_gamma = self.calculate_dynamic_gamma(inventory)
        
        # Use shorter risk horizon instead of full T-t
        time_to_expiry = self.T - t
        risk_horizon = min(time_to_expiry, self.risk_horizon_seconds)
        
        # Log risk horizon on first call (to avoid spam)
        if not hasattr(self, '_risk_horizon_logged'):
            self.logger.info(f"Using risk horizon: {risk_horizon:.0f} seconds for AS formula (min(time_to_expiry={time_to_expiry:.0f}s, {self.risk_horizon_seconds}s))")
            self._risk_horizon_logged = True
        
        # AS formula: δ* ≈ (γσ²(T-t))/2 + (1/γ)*ln(1+γ/k)
        term1 = (dynamic_gamma * (self.sigma**2) * risk_horizon) / 2
        term2 = (1 / dynamic_gamma) * math.log(1 + (dynamic_gamma / self.k))
        base_spread = term1 + term2
        
        # Adjust for inventory (widen when inventory is high)
        position_ratio = abs(inventory) / self.max_position
        spread_adjustment = 1 + (position_ratio ** 2)  # Widen when inventory is high
        
        target_spread = base_spread * spread_adjustment
        
        # Clamp to min/max spread
        target_spread = max(self.min_spread, min(target_spread, self.max_spread))
        
        return target_spread

    def calculate_dynamic_gamma(self, inventory: int) -> float:
        position_ratio = inventory / self.max_position
        return self.base_gamma * math.exp(-abs(position_ratio))

    def calculate_order_sizes(self, inventory: int) -> Tuple[int, int]:
        remaining_capacity = self.max_position - abs(inventory)
        buffer_size = int(self.max_position * self.position_limit_buffer)
        
        if inventory > 0:
            buy_size = max(1, min(buffer_size, remaining_capacity))
            sell_size = max(1, self.max_position)
        else:
            buy_size = max(1, self.max_position)
            sell_size = max(1, min(buffer_size, remaining_capacity))
        
        return buy_size, sell_size

    def manage_orders(self, bid_price: float, ask_price: float, buy_size: int, sell_size: int):
        current_orders = self.api.get_orders()
        self.logger.info(f"Retrieved {len(current_orders)} total orders")

        buy_orders = []
        sell_orders = []

        for order in current_orders:
            if order['side'] == self.trade_side:
                if order['action'] == 'buy':
                    buy_orders.append(order)
                elif order['action'] == 'sell':
                    sell_orders.append(order)

        self.logger.info(f"Current buy orders: {len(buy_orders)}")
        self.logger.info(f"Current sell orders: {len(sell_orders)}")

        # Handle buy orders
        self.handle_order_side('buy', buy_orders, bid_price, buy_size)

        # Handle sell orders
        self.handle_order_side('sell', sell_orders, ask_price, sell_size)
        
        # Phase 3.4: Wire Lifecycle Events (check before quoting)
        if self.ws_enabled and self.state_store:
            lifecycle = self.state_store.get_lifecycle(self.api.market_ticker)
            if lifecycle:
                event_type = lifecycle.get('event_type')
                if event_type == 'MARKET_HALTED':
                    # Trigger regime kill / hard stop
                    self.logger.error("Market halted, stopping quotes")
                    # Note: quoting_frozen flag should be set at a higher level
                    try:
                        self.close_positions()
                    except:
                        pass
                    return  # Exit early if market is halted
                elif event_type in ['MARKET_CLOSED', 'SETTLED']:
                    # Stop quoting & exit session
                    self.logger.info(f"Market {event_type}, exiting session")
                    return  # Exit early if market is closed/settled
        
        # Reconcile fills from Kalshi API (single source of truth)
        self.reconcile_new_fills()
        
        # Phase 5.6: Order Price Comparison (check if existing orders are still competitive)
        # Use WS data if available, otherwise REST
        if self.ws_enabled and self.state_store and self.state_store.ws_healthy:
            ticker_data = self.state_store.get_normalized_ticker(self.api.market_ticker)
            if ticker_data:
                # Calculate mid price from bid/ask (ticker_data has yes_bid/yes_ask, not yes mid)
                if self.trade_side == 'yes':
                    yes_bid = ticker_data.get("yes_bid") or 0.0
                    yes_ask = ticker_data.get("yes_ask") or 0.0
                    market_mid = (yes_bid + yes_ask) / 2 if (yes_bid > 0 and yes_ask > 0) else 0.0
                else:  # no side
                    no_bid = ticker_data.get("no_bid") or 0.0
                    no_ask = ticker_data.get("no_ask") or 0.0
                    market_mid = (no_bid + no_ask) / 2 if (no_bid > 0 and no_ask > 0) else 0.0
            else:
                current_market = self.api.get_price()
                market_mid = current_market[self.trade_side]
        else:
            current_market = self.api.get_price()
            market_mid = current_market[self.trade_side]
        
        # Log calculated market_mid for debugging
        if market_mid == 0.0:
            self.logger.warning(f"Market mid price is 0.0 for {self.trade_side} side - price comparison may be incorrect")
        else:
            self.logger.debug(f"Market mid price for {self.trade_side}: {market_mid:.4f}")
        
        for order_id, order_info in list(self.order_tracking.items()):
            order_price = order_info['price']
            # Phase 5.6: Scale tolerance by volatility
            delta = abs(order_price - market_mid)
            allowed = max(0.05, self.sigma * 2)  # Widen tolerance in high vol
            
            if delta > allowed:
                self.logger.info(
                    f"Order {order_id} far from market ({delta:.4f} > {allowed:.4f}), cancelling"
                )
                try:
                    self.api.cancel_order(order_id)
                    self.order_tracking.pop(order_id, None)
                except Exception as e:
                    self.logger.debug(f"Could not cancel order {order_id}: {e}")
        
        # Phase 5.4: Cancel stale orders
        stale_order_ids = self._check_order_staleness()
        for order_id in stale_order_ids:
            self.logger.info(f"Cancelling stale order: {order_id}")
            try:
                self.api.cancel_order(order_id)
            except:
                pass
            self.order_tracking.pop(order_id, None)
        
    def handle_order_side(self, action: str, orders: List[Dict], desired_price: float, desired_size: int):
        # Skip if one-sided quoting and this is the disabled side
        if self.one_sided_quoting:
            mid_prices = self.api.get_price()
            mid_price = mid_prices[self.trade_side]
            if mid_price < 0.05 and action == 'sell':
                self.logger.info(f"Skipping {action} order due to one-sided quoting (hard extreme, p={mid_price:.2f})")
                # Cancel any existing orders on this side
                for order in orders:
                    try:
                        self.api.cancel_order(order['order_id'])
                    except:
                        pass
                return
            elif mid_price > 0.95 and action == 'buy':
                self.logger.info(f"Skipping {action} order due to one-sided quoting (hard extreme, p={mid_price:.2f})")
                # Cancel any existing orders on this side
                for order in orders:
                    try:
                        self.api.cancel_order(order['order_id'])
                    except:
                        pass
                return
        
        keep_order = None
        for order in orders:
            current_price = float(order['yes_price']) / 100 if self.trade_side == 'yes' else float(order['no_price']) / 100
            if keep_order is None and abs(current_price - desired_price) < 0.01 and order['remaining_count'] == desired_size:
                keep_order = order
                self.logger.info(f"Keeping existing {action} order. ID: {order['order_id']}, Price: {current_price:.4f}")
            else:
                self.logger.info(f"Cancelling extraneous {action} order. ID: {order['order_id']}, Price: {current_price:.4f}")
                try:
                    self.api.cancel_order(order['order_id'])
                except Exception as e:
                    # Order might already be filled or expired
                    self.logger.debug(f"Could not cancel order {order['order_id']}: {e}")

        if keep_order is None:
            # Get actual market bid/ask prices
            market_data = self.api.get_price()
            actual_bid = market_data.get(f"{self.trade_side}_bid")
            actual_ask = market_data.get(f"{self.trade_side}_ask")
            mid_price = market_data.get(self.trade_side)
            
            tick = 0.01  # 1 cent minimum tick
            
            # Clamp price to competitive level inside NBBO
            if actual_bid is None or actual_ask is None:
                # Empty book - place at desired price but clip to valid range
                price = max(0.01, min(0.99, desired_price))
                self.logger.info(f"Empty book: placing {action} order at desired price {price:.4f}")
            elif action == 'buy':
                # Don't cross the ask - place one tick inside spread
                price = min(desired_price, actual_ask - tick)
                # Be at least one tick better than current best bid if possible
                price = max(price, actual_bid + tick)
                
                # If that would cross the book, place one tick below ask instead of skipping
                if price >= actual_ask:
                    price = actual_ask - tick
                    if price <= actual_bid:
                        # Spread is too tight (1 tick or less), place at bid
                        price = actual_bid
                        self.logger.debug(
                            f"Spread too tight for buy: placing at bid {price:.4f} "
                            f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                        )
                    else:
                        self.logger.debug(
                            f"Adjusted buy price to one tick inside spread: {price:.4f} "
                            f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                        )
                
                # Log clamping if price changed
                if abs(price - desired_price) > 0.0001:
                    self.logger.info(
                        f"Clamped buy from {desired_price:.4f} to {price:.4f} "
                        f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                    )
            elif action == 'sell':
                # Don't cross the bid - place one tick inside spread
                price = max(desired_price, actual_bid + tick)
                # Be at least one tick better than current best ask if possible
                price = min(price, actual_ask - tick)
                
                # If that would cross the book, place one tick above bid instead of skipping
                if price <= actual_bid:
                    price = actual_bid + tick
                    if price >= actual_ask:
                        # Spread is too tight (1 tick or less), place at ask
                        price = actual_ask
                        self.logger.debug(
                            f"Spread too tight for sell: placing at ask {price:.4f} "
                            f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                        )
                    else:
                        self.logger.debug(
                            f"Adjusted sell price to one tick inside spread: {price:.4f} "
                            f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                        )
                
                # Log clamping if price changed
                if abs(price - desired_price) > 0.0001:
                    self.logger.info(
                        f"Clamped sell from {desired_price:.4f} to {price:.4f} "
                        f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                    )
            else:
                price = desired_price
            
            # Place the order at clamped price
            try:
                order_id = self.api.place_order(action, self.trade_side, price, desired_size, int(time.time()) + self.order_expiration)
                # Phase 5.4: Track order placement
                self._track_order(order_id, price, self.trade_side, action)
                self.logger.info(f"Placed new {action} order. ID: {order_id}, Price: {price:.4f}, Size: {desired_size}")
            except Exception as e:
                self.logger.error(f"Failed to place {action} order: {str(e)}")
