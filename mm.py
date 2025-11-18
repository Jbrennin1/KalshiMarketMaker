import abc
import time
from typing import Dict, List, Tuple, Optional
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

    def logout(self):
        # API key authentication doesn't require explicit logout
        self.logger.info("Logout called (no-op for API key authentication)")

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
        self, method: str, path: str, params: Dict = None, data: Dict = None
    ):
        url = f"{self.base_url}{path}"
        
        # Prepare body for signing - sort keys for consistent ordering
        body = ""
        if data:
            # Sort keys to ensure consistent JSON string for signing
            body = json.dumps(data, separators=(',', ':'), sort_keys=True)
        
        # Get signed headers
        headers = self.get_headers(method, path, body)

        try:
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
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
            raise

    def get_position(self) -> int:
        self.logger.info("Retrieving position...")
        path = "/portfolio/positions"
        params = {"ticker": self.market_ticker, "settlement_status": "unsettled"}
        response = self.make_request("GET", path, params=params)
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

        yes_bid = float(data["market"]["yes_bid"]) / 100
        yes_ask = float(data["market"]["yes_ask"]) / 100
        no_bid = float(data["market"]["no_bid"]) / 100
        no_ask = float(data["market"]["no_ask"]) / 100
        
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

        if side == "yes":
            data["yes_price"] = price_to_send
        else:
            data["no_price"] = price_to_send

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
        params = {"ticker": self.market_ticker, "status": "resting"}
        response = self.make_request("GET", path, params=params)
        orders = response.get("orders", [])
        self.logger.info(f"Retrieved {len(orders)} orders")
        return orders

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

    def check_spread_widening(self, current_spread: float) -> bool:
        """Check if spread has widened beyond threshold"""
        return current_spread > self.max_spread_threshold
    
    def check_book_health(self, market_data: Dict) -> bool:
        """Check if book depth is sufficient"""
        # For now, use a simple heuristic based on bid/ask prices
        # In full implementation, would fetch order book depth
        bid = market_data.get(f"{self.trade_side}_bid", 0)
        ask = market_data.get(f"{self.trade_side}_ask", 0)
        spread = ask - bid
        # If spread is very wide, assume book is thin
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
        return ratio >= self.adverse_selection_threshold
    
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

    def run(self, dt: float):
        start_time = time.time()
        last_price = None
        
        while time.time() - start_time < self.T:
            current_time = time.time() - start_time
            self.logger.info(f"Running Avellaneda market maker at {current_time:.2f}")

            mid_prices = self.api.get_price()
            mid_price = mid_prices[self.trade_side]
            inventory = self.api.get_position()
            self.logger.info(f"Current mid price for {self.trade_side}: {mid_price:.4f}, Inventory: {inventory}")

            # Check throttling conditions
            current_spread = mid_prices[f"{self.trade_side}_ask"] - mid_prices[f"{self.trade_side}_bid"]
            spread_widened = self.check_spread_widening(current_spread)
            book_healthy = self.check_book_health(mid_prices)
            adverse_selection = self.detect_adverse_selection()
            
            self.update_throttle_state('spread_widened', spread_widened)
            self.update_throttle_state('book_thin', not book_healthy)
            self.update_throttle_state('adverse_selection', adverse_selection)
            
            # Track price changes for adverse selection
            if last_price is not None:
                price_change = mid_price - last_price
                # Store in fill history (will be populated when fills occur)
                # For now, just track price changes
                pass
            last_price = mid_price

            reservation_price = self.calculate_reservation_price(mid_price, inventory, current_time)
            bid_price, ask_price = self.calculate_asymmetric_quotes(mid_price, inventory, current_time)
            buy_size, sell_size = self.calculate_order_sizes(inventory)

            self.logger.info(f"Reservation price: {reservation_price:.4f}")
            self.logger.info(f"Computed desired bid: {bid_price:.4f}, ask: {ask_price:.4f}")

            # Log market snapshot to analytics database
            if self.analytics_db and self.run_id:
                try:
                    self.analytics_db.log_market_snapshot(
                        self.run_id,
                        mid_prices,
                        inventory,
                        reservation_price,
                        bid_price,
                        ask_price
                    )
                    # Log position change if it changed
                    if self.last_position != inventory:
                        self.analytics_db.log_position(self.run_id, inventory)
                        self.last_position = inventory
                except Exception as e:
                    self.logger.warning(f"Failed to log market snapshot to analytics: {e}")

            self.manage_orders(bid_price, ask_price, buy_size, sell_size)

            time.sleep(dt)

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

        # Check for order fills
        if self.analytics_db and self.run_id:
            try:
                # Get all orders from database for this run
                with self.analytics_db.get_connection() as conn:
                    db_orders = conn.execute("""
                        SELECT order_id, status, placed_quantity, filled_quantity
                        FROM orders WHERE run_id = ? AND status = 'placed'
                    """, (self.run_id,)).fetchall()
                    
                    # Check each order to see if it was filled
                    for db_order in db_orders:
                        order_id = db_order['order_id']
                        # Check if order is still in current_orders
                        found = False
                        for current_order in current_orders:
                            if str(current_order.get('order_id')) == str(order_id):
                                found = True
                                # Check if order was partially or fully filled
                                remaining = current_order.get('remaining_count', 0)
                                placed_qty = db_order['placed_quantity']
                                if remaining < placed_qty:
                                    # Order was filled (partially or fully)
                                    filled_qty = placed_qty - remaining
                                    # Get fill price from order (use yes_price or no_price)
                                    fill_price_key = f"{self.trade_side}_price"
                                    fill_price = float(current_order.get(fill_price_key, 0)) / 100
                                    if fill_price > 0:
                                        self.analytics_db.log_order_filled(str(order_id), fill_price, filled_qty)
                                break
                        
                        # If order not found in current orders, it might be fully filled
                        if not found:
                            # Try to get order details from API to see if it was filled
                            # For now, we'll leave it - the order might have been cancelled
                            pass
            except Exception as e:
                self.logger.warning(f"Failed to check order fills: {e}")

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
                # Don't cross the ask
                price = min(desired_price, actual_ask - tick)
                # Be at least one tick better than current best bid if possible
                price = max(price, actual_bid + tick)
                
                # If that would cross the book, skip
                if price >= actual_ask:
                    self.logger.info(
                        f"Skipped buy after clamping (would cross book): "
                        f"price={price:.4f}, best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f}"
                    )
                    return
                
                # Log clamping if price changed
                if abs(price - desired_price) > 0.0001:
                    self.logger.info(
                        f"Clamped buy from {desired_price:.4f} to {price:.4f} "
                        f"(best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f})"
                    )
            elif action == 'sell':
                # Don't cross the bid
                price = max(desired_price, actual_bid + tick)
                # Be at least one tick better than current best ask if possible
                price = min(price, actual_ask - tick)
                
                # If that would cross the book, skip
                if price <= actual_bid:
                    self.logger.info(
                        f"Skipped sell after clamping (would cross book): "
                        f"price={price:.4f}, best_bid={actual_bid:.4f}, best_ask={actual_ask:.4f}"
                    )
                    return
                
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
                self.logger.info(f"Placed new {action} order. ID: {order_id}, Price: {price:.4f}, Size: {desired_size}")
            except Exception as e:
                self.logger.error(f"Failed to place {action} order: {str(e)}")
