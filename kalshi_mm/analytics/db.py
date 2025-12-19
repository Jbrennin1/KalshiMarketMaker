"""
Analytics module for trading bot - database operations and data collection
"""
import sqlite3
import json
import logging
from typing import Dict, Optional, List
from datetime import datetime
from contextlib import contextmanager
import os


class AnalyticsDB:
    """Database manager for trading analytics"""
    
    def __init__(self, db_path: str = "trading_analytics.db", logger: Optional[logging.Logger] = None):
        """
        Initialize analytics database connection
        
        Args:
            db_path: Path to SQLite database file
            logger: Optional logger instance
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        # Schema is in project root, go up from package directory
        package_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        schema_path = os.path.join(package_dir, "schema.sql")
        if not os.path.exists(schema_path):
            # Fallback: try current directory
            schema_path = "schema.sql"
        
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.get_connection() as conn:
                conn.executescript(schema_sql)
                conn.commit()
            self.logger.info(f"Database schema initialized from {schema_path}")
        else:
            self.logger.warning(f"Schema file not found at {schema_path}, creating tables manually")
            self._create_tables_manual()
    
    def _create_tables_manual(self):
        """Create tables manually if schema.sql is not available"""
        with self.get_connection() as conn:
            # Strategy runs
            conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    market_ticker TEXT NOT NULL,
                    trade_side TEXT NOT NULL CHECK(trade_side IN ('yes', 'no')),
                    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    config_params TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Market snapshots
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    yes_bid REAL, yes_ask REAL, yes_mid REAL,
                    no_bid REAL, no_ask REAL, no_mid REAL,
                    current_position INTEGER NOT NULL,
                    reservation_price REAL,
                    computed_bid REAL, computed_ask REAL,
                    spread REAL,
                    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
                )
            """)
            
            # Orders
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
                    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
                    placed_price REAL NOT NULL,
                    placed_quantity INTEGER NOT NULL,
                    placed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL DEFAULT 'placed' CHECK(status IN ('placed', 'filled', 'cancelled', 'expired')),
                    filled_price REAL, filled_quantity INTEGER, filled_timestamp TIMESTAMP,
                    cancelled_timestamp TIMESTAMP, expiration_ts TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
                )
            """)
            
            # Trades
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    run_id INTEGER NOT NULL,
                    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
                    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    realized_pnl REAL,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
                )
            """)
            
            # Position history
            conn.execute("""
                CREATE TABLE IF NOT EXISTS position_history (
                    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    position INTEGER NOT NULL,
                    unrealized_pnl REAL,
                    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
                )
            """)
            
            # Discovery sessions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS discovery_sessions (
                    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    total_markets INTEGER NOT NULL,
                    binary_markets INTEGER NOT NULL,
                    pre_filtered INTEGER NOT NULL,
                    scored INTEGER NOT NULL,
                    top_markets_count INTEGER NOT NULL,
                    configs_generated INTEGER NOT NULL,
                    model_version TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Discovered markets
            conn.execute("""
                CREATE TABLE IF NOT EXISTS discovered_markets (
                    discovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    title TEXT,
                    score REAL,
                    volume_24h REAL,
                    spread REAL,
                    mid_price REAL,
                    liquidity REAL,
                    open_interest INTEGER,
                    reasons TEXT,
                    generated_config TEXT,
                    selected_for_trading BOOLEAN NOT NULL DEFAULT 0,
                    trade_side TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES discovery_sessions(session_id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_market_snapshots_run_id ON market_snapshots(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_market_snapshots_timestamp ON market_snapshots(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
                "CREATE INDEX IF NOT EXISTS idx_orders_placed_timestamp ON orders(placed_timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_position_history_run_id ON position_history(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_position_history_timestamp ON position_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_discovery_sessions_timestamp ON discovery_sessions(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_discovered_markets_session_id ON discovered_markets(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_discovered_markets_ticker ON discovered_markets(ticker)",
                "CREATE INDEX IF NOT EXISTS idx_discovered_markets_selected ON discovered_markets(selected_for_trading)",
            ]:
                conn.execute(idx_sql)
            
            conn.commit()
            self.logger.info("Database tables created manually")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper cleanup"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def create_strategy_run(
        self,
        strategy_name: str,
        market_ticker: str,
        trade_side: str,
        config_params: Dict
    ) -> int:
        """
        Create a new strategy run and return run_id
        
        Args:
            strategy_name: Name of the strategy
            market_ticker: Market ticker symbol
            trade_side: "yes" or "no"
            config_params: Dictionary of configuration parameters
            
        Returns:
            run_id: The ID of the created run
        """
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO strategy_runs (strategy_name, market_ticker, trade_side, config_params, start_time)
                VALUES (?, ?, ?, ?, ?)
            """, (
                strategy_name,
                market_ticker,
                trade_side,
                json.dumps(config_params),
                datetime.now().isoformat()
            ))
            conn.commit()
            run_id = cursor.lastrowid
            self.logger.debug(f"Created strategy run {run_id} for {strategy_name}")
            return run_id
    
    def end_strategy_run(self, run_id: int):
        """Mark a strategy run as ended"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE strategy_runs
                SET end_time = ?
                WHERE run_id = ?
            """, (datetime.now().isoformat(), run_id))
            conn.commit()
            self.logger.debug(f"Ended strategy run {run_id}")
    
    def log_market_snapshot(
        self,
        run_id: int,
        market_data: Dict[str, float],
        position: int,
        reservation_price: float,
        computed_bid: float,
        computed_ask: float,
        regime: Optional[str] = None,
        regime_duration_seconds: Optional[float] = None,
        prev_regime: Optional[str] = None,
        regime_transition: bool = False
    ):
        """
        Log a market snapshot
        
        Args:
            run_id: Strategy run ID
            market_data: Dictionary with yes_bid, yes_ask, yes_mid, no_bid, no_ask, no_mid
            position: Current position
            reservation_price: Calculated reservation price
            computed_bid: Computed bid price
            computed_ask: Computed ask price
            regime: Current market regime
            regime_duration_seconds: Time spent in current regime
            prev_regime: Previous regime before transition
            regime_transition: Whether this snapshot represents a regime transition
        """
        spread = computed_ask - computed_bid if computed_ask and computed_bid else None
        
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO market_snapshots (
                    run_id, timestamp, yes_bid, yes_ask, yes_mid,
                    no_bid, no_ask, no_mid, current_position,
                    reservation_price, computed_bid, computed_ask, spread,
                    regime, regime_duration_seconds, prev_regime, regime_transition
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                datetime.now().isoformat(),
                market_data.get('yes_bid'),
                market_data.get('yes_ask'),
                market_data.get('yes'),
                market_data.get('no_bid'),
                market_data.get('no_ask'),
                market_data.get('no'),
                position,
                reservation_price,
                computed_bid,
                computed_ask,
                spread,
                regime,
                regime_duration_seconds,
                prev_regime,
                1 if regime_transition else 0
            ))
            conn.commit()
    
    def log_order_placed(
        self,
        run_id: int,
        order_id: str,
        action: str,
        side: str,
        price: float,
        quantity: int,
        expiration_ts: Optional[int] = None
    ):
        """
        Log an order placement
        
        Args:
            run_id: Strategy run ID
            order_id: Order ID from exchange
            action: "buy" or "sell"
            side: "yes" or "no"
            price: Order price
            quantity: Order quantity
            expiration_ts: Optional expiration timestamp
        """
        expiration_dt = None
        if expiration_ts:
            expiration_dt = datetime.fromtimestamp(expiration_ts).isoformat()
        
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO orders (
                    order_id, run_id, action, side, placed_price,
                    placed_quantity, placed_timestamp, expiration_ts, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'placed')
            """, (
                order_id,
                run_id,
                action,
                side,
                price,
                quantity,
                datetime.now().isoformat(),
                expiration_dt
            ))
            conn.commit()
            self.logger.debug(f"Logged order placement: {order_id}")
    
    def log_order_cancelled(self, order_id: str):
        """Mark an order as cancelled"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE orders
                SET status = 'cancelled', cancelled_timestamp = ?
                WHERE order_id = ?
            """, (datetime.now().isoformat(), order_id))
            conn.commit()
            self.logger.debug(f"Logged order cancellation: {order_id}")
    
    def log_order_filled(
        self,
        order_id: str,
        filled_price: float,
        filled_quantity: int
    ):
        """
        Mark an order as filled (or partially filled) and create trade record.
        Handles partial fills by incrementing filled_quantity.
        
        Args:
            order_id: Order ID
            filled_price: Price at which order was filled (weighted average for partial fills)
            filled_quantity: Additional quantity filled in this event
        """
        with self.get_connection() as conn:
            # Get order details including placed_quantity
            order = conn.execute("""
                SELECT run_id, action, side, placed_quantity, COALESCE(filled_quantity, 0) as current_filled_qty
                FROM orders WHERE order_id = ?
            """, (order_id,)).fetchone()
            
            if not order:
                self.logger.warning(f"Order {order_id} not found when logging fill")
                return
            
            run_id, action, side = order['run_id'], order['action'], order['side']
            placed_quantity = order['placed_quantity']
            current_filled_qty = order['current_filled_qty']
            
            # Calculate new total filled quantity
            new_filled_qty = current_filled_qty + filled_quantity
            
            # Determine status based on completion
            if new_filled_qty >= placed_quantity:
                new_status = 'filled'
            else:
                new_status = 'partially_filled'
            
            # Update order status - increment filled_quantity, update status and price
            conn.execute("""
                UPDATE orders
                SET status = ?,
                    filled_price = ?,
                    filled_quantity = ?,
                    filled_timestamp = ?
                WHERE order_id = ?
            """, (
                new_status,
                filled_price,  # Use latest fill price (could be weighted average in future)
                new_filled_qty,
                datetime.now().isoformat(),
                order_id
            ))
            
            # Create trade record for this fill event
            conn.execute("""
                INSERT INTO trades (order_id, run_id, action, side, price, quantity, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                order_id,
                run_id,
                action,
                side,
                filled_price,
                filled_quantity,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            self.logger.debug(f"Logged order fill: {order_id}, status={new_status}, total_filled={new_filled_qty}/{placed_quantity}")
    
    def log_fill_event(
        self,
        fill_id: str,
        order_id: str,
        run_id: int,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price_cents: int,
        ts: int
    ):
        """
        Log an individual fill event to order_fills table.
        Checks for duplicate fill_id before inserting.
        
        Args:
            fill_id: Unique fill ID from Kalshi
            order_id: Order ID this fill belongs to
            run_id: Strategy run ID
            ticker: Market ticker
            side: 'yes' or 'no'
            action: 'buy' or 'sell'
            count: Quantity filled in this event
            price_cents: Price in cents
            ts: Unix timestamp in seconds
        """
        with self.get_connection() as conn:
            # Check if fill_id already exists
            existing = conn.execute(
                "SELECT 1 FROM order_fills WHERE fill_id = ?",
                (fill_id,)
            ).fetchone()
            
            if existing:
                self.logger.debug(f"Fill {fill_id} already logged, skipping")
                return
            
            # Insert fill event
            conn.execute("""
                INSERT INTO order_fills (
                    fill_id, order_id, run_id, ticker, side, action, count, price_cents, ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fill_id, order_id, run_id, ticker, side, action, count, price_cents, ts
            ))
            
            conn.commit()
            self.logger.debug(f"Logged fill event: {fill_id} for order {order_id}")
    
    def log_position(self, run_id: int, position: int, unrealized_pnl: Optional[float] = None):
        """Log position change"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO position_history (run_id, timestamp, position, unrealized_pnl)
                VALUES (?, ?, ?, ?)
            """, (
                run_id,
                datetime.now().isoformat(),
                position,
                unrealized_pnl
            ))
            conn.commit()
    
    def check_order_fills(self, run_id: int, current_orders: List[Dict]) -> List[Dict]:
        """
        Check for order fills by comparing current orders with database
        
        Args:
            run_id: Strategy run ID
            current_orders: List of current orders from API
            
        Returns:
            List of filled orders that need to be logged
        """
        current_order_ids = {order.get('order_id') for order in current_orders if order.get('order_id')}
        
        with self.get_connection() as conn:
            # Get all placed orders for this run that aren't filled or cancelled
            db_orders = conn.execute("""
                SELECT order_id, status FROM orders
                WHERE run_id = ? AND status = 'placed'
            """, (run_id,)).fetchall()
            
            filled_orders = []
            for db_order in db_orders:
                order_id = db_order['order_id']
                # If order is not in current orders, it might be filled
                if order_id not in current_order_ids:
                    # Check if order has fill information in API response
                    # For now, we'll mark it as potentially filled
                    # The actual fill detection should be done by checking order status via API
                    pass
            
            return filled_orders
    
    def create_discovery_session(
        self,
        total_markets: int,
        binary_markets: int,
        pre_filtered: int,
        scored: int,
        top_markets_count: int,
        configs_generated: int,
        model_version: Optional[str] = None
    ) -> int:
        """
        Create a new discovery session and return session_id
        
        Args:
            total_markets: Total markets discovered
            binary_markets: Number of binary markets
            pre_filtered: Number after pre-filtering
            scored: Number of markets scored
            top_markets_count: Number of top markets selected
            configs_generated: Number of configs generated
            model_version: Optional model version string
            
        Returns:
            session_id: The ID of the created discovery session
        """
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO discovery_sessions (
                    timestamp, total_markets, binary_markets, pre_filtered,
                    scored, top_markets_count, configs_generated, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                total_markets,
                binary_markets,
                pre_filtered,
                scored,
                top_markets_count,
                configs_generated,
                model_version
            ))
            conn.commit()
            session_id = cursor.lastrowid
            self.logger.info(f"Created discovery session {session_id}")
            return session_id
    
    def log_discovered_market(
        self,
        session_id: int,
        ticker: str,
        title: Optional[str],
        score: Optional[float],
        volume_24h: Optional[float],
        spread: Optional[float],
        mid_price: Optional[float],
        liquidity: Optional[float],
        open_interest: Optional[int],
        reasons: Optional[List[str]],
        generated_config: Optional[Dict],
        selected_for_trading: bool = False,
        trade_side: Optional[str] = None
    ):
        """
        Log a discovered market with its score and generated config
        
        Args:
            session_id: Discovery session ID
            ticker: Market ticker
            title: Market title
            score: Profitability score
            volume_24h: 24h volume in dollars
            spread: Market spread in dollars
            mid_price: Mid price
            liquidity: Book liquidity in dollars
            open_interest: Open interest
            reasons: List of scoring reasons
            generated_config: Generated config dict (if available)
            selected_for_trading: Whether this market was selected for trading
            trade_side: 'yes', 'no', or None
        """
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO discovered_markets (
                    session_id, ticker, title, score, volume_24h, spread,
                    mid_price, liquidity, open_interest, reasons, generated_config,
                    selected_for_trading, trade_side
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                ticker,
                title,
                score,
                volume_24h,
                spread,
                mid_price,
                liquidity,
                open_interest,
                json.dumps(reasons) if reasons else None,
                json.dumps(generated_config) if generated_config else None,
                1 if selected_for_trading else 0,
                trade_side
            ))
            conn.commit()
            self.logger.debug(f"Logged discovered market: {ticker}")
    
    def get_discovery_session(self, session_id: int) -> Optional[Dict]:
        """
        Retrieve discovery session details
        
        Args:
            session_id: Discovery session ID
            
        Returns:
            Dictionary with session details or None if not found
        """
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT * FROM discovery_sessions WHERE session_id = ?
            """, (session_id,)).fetchone()
            
            if row:
                return dict(row)
            return None
    
    def get_discovered_markets(self, session_id: int, selected_only: bool = False) -> List[Dict]:
        """
        Get all markets from a discovery session
        
        Args:
            session_id: Discovery session ID
            selected_only: If True, only return markets selected for trading
            
        Returns:
            List of discovered market dictionaries
        """
        with self.get_connection() as conn:
            if selected_only:
                rows = conn.execute("""
                    SELECT * FROM discovered_markets
                    WHERE session_id = ? AND selected_for_trading = 1
                    ORDER BY score DESC
                """, (session_id,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM discovered_markets
                    WHERE session_id = ?
                    ORDER BY score DESC
                """, (session_id,)).fetchall()
            
            markets = []
            for row in rows:
                market = dict(row)
                # Parse JSON fields
                if market.get('reasons'):
                    try:
                        market['reasons'] = json.loads(market['reasons'])
                    except:
                        market['reasons'] = []
                if market.get('generated_config'):
                    try:
                        market['generated_config'] = json.loads(market['generated_config'])
                    except:
                        market['generated_config'] = None
                markets.append(market)
            
            return markets
    
    def log_volatility_event(self, event, run_id: Optional[int] = None):
        """
        Log a detected volatility event (includes regime if available)
        
        Args:
            event: VolatilityEvent object
            run_id: Optional strategy run ID if session was spawned
        """
        from kalshi_mm.volatility.models import VolatilityEvent
        
        if not isinstance(event, VolatilityEvent):
            self.logger.warning(f"Invalid event type: {type(event)}")
            return
        
        with self.get_connection() as conn:
            # Get regime from event if available
            regime = getattr(event, 'regime', None)
            
            conn.execute("""
                INSERT INTO volatility_events (
                    market_ticker, timestamp, signal_strength, sigma,
                    volume_multiplier, volume_delta, estimated_trades,
                    volume_velocity, jump_magnitude, direction, close_time, regime, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.ticker,
                event.timestamp.isoformat(),
                event.signal_strength,
                event.sigma,
                event.volume_multiplier,
                event.volume_delta,
                event.estimated_trades,
                event.volume_velocity,
                event.jump_magnitude,
                event.direction,
                event.close_time.isoformat() if event.close_time else None,
                regime,
                run_id
            ))
            conn.commit()
            self.logger.debug(f"Logged volatility event: {event.ticker}")
    
    def log_volatility_session_end(self, ticker: str, run_id: int, kill_reason: str, final_pnl: Optional[float] = None):
        """
        Log the end of a volatility session
        
        Args:
            ticker: Market ticker
            run_id: Strategy run ID
            kill_reason: Reason session ended
            final_pnl: Optional final PnL
        """
        with self.get_connection() as conn:
            # Get event_id from volatility_events table
            event_row = conn.execute("""
                SELECT event_id FROM volatility_events
                WHERE market_ticker = ? AND run_id = ?
                ORDER BY timestamp DESC LIMIT 1
            """, (ticker, run_id)).fetchone()
            
            event_id = event_row['event_id'] if event_row else None
            
            # Calculate final PnL if not provided
            if final_pnl is None:
                # Try to calculate from trades
                trades = conn.execute("""
                    SELECT SUM(CASE 
                        WHEN action = 'buy' THEN -price * quantity
                        WHEN action = 'sell' THEN price * quantity
                        ELSE 0
                    END) as pnl
                    FROM trades WHERE run_id = ?
                """, (run_id,)).fetchone()
                final_pnl = trades['pnl'] if trades and trades['pnl'] is not None else 0.0
            
            conn.execute("""
                INSERT INTO volatility_sessions (
                    event_id, run_id, start_time, end_time, kill_reason, final_pnl
                ) VALUES (?, ?, 
                    (SELECT start_time FROM strategy_runs WHERE run_id = ?),
                    ?,
                    ?, ?
                )
            """, (
                event_id,
                run_id,
                run_id,
                datetime.now().isoformat(),
                kill_reason,
                final_pnl
            ))
            conn.commit()
            self.logger.debug(f"Logged volatility session end: {ticker}, reason: {kill_reason}")

