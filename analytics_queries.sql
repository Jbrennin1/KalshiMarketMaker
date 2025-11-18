-- Analytics Queries for Trading Bot
-- Example SQL queries for analyzing trading performance

-- ============================================
-- Performance Metrics
-- ============================================

-- 1. Total PnL by Strategy
-- Calculate realized PnL from completed trades
SELECT 
    sr.strategy_name,
    sr.market_ticker,
    sr.trade_side,
    COUNT(DISTINCT t.trade_id) as total_trades,
    SUM(CASE 
        WHEN t.action = 'buy' THEN -t.price * t.quantity
        WHEN t.action = 'sell' THEN t.price * t.quantity
    END) as realized_pnl,
    AVG(t.price) as avg_trade_price,
    SUM(t.quantity) as total_volume
FROM strategy_runs sr
LEFT JOIN trades t ON sr.run_id = t.run_id
WHERE sr.end_time IS NOT NULL
GROUP BY sr.strategy_name, sr.market_ticker, sr.trade_side
ORDER BY realized_pnl DESC;

-- 2. Fill Rate Analysis
-- Calculate order fill rates by strategy
SELECT 
    sr.strategy_name,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(CASE WHEN o.status = 'filled' THEN 1 ELSE 0 END) as filled_orders,
    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
    ROUND(100.0 * SUM(CASE WHEN o.status = 'filled' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.order_id), 2) as fill_rate_pct,
    AVG(CASE WHEN o.status = 'filled' THEN 
        (julianday(o.filled_timestamp) - julianday(o.placed_timestamp)) * 24 * 60 
    END) as avg_time_to_fill_minutes
FROM strategy_runs sr
LEFT JOIN orders o ON sr.run_id = o.run_id
GROUP BY sr.strategy_name
ORDER BY fill_rate_pct DESC;

-- 3. Spread Capture Analysis
-- Analyze how much of the spread was captured
SELECT 
    sr.strategy_name,
    AVG(ms.spread) as avg_spread,
    AVG(ms.computed_bid) as avg_computed_bid,
    AVG(ms.computed_ask) as avg_computed_ask,
    AVG(ms.yes_mid) as avg_market_mid_price,
    COUNT(*) as snapshot_count
FROM strategy_runs sr
JOIN market_snapshots ms ON sr.run_id = ms.run_id
WHERE sr.trade_side = 'yes'
GROUP BY sr.strategy_name
ORDER BY avg_spread DESC;

-- ============================================
-- Order Lifecycle Metrics
-- ============================================

-- 4. Order Statistics by Action Type
SELECT 
    sr.strategy_name,
    o.action,
    o.side,
    COUNT(*) as order_count,
    AVG(o.placed_price) as avg_placed_price,
    AVG(o.placed_quantity) as avg_placed_quantity,
    SUM(CASE WHEN o.status = 'filled' THEN 1 ELSE 0 END) as filled_count,
    AVG(CASE WHEN o.status = 'filled' THEN o.filled_price END) as avg_fill_price,
    AVG(CASE WHEN o.status = 'filled' THEN 
        ABS(o.filled_price - o.placed_price) 
    END) as avg_slippage
FROM strategy_runs sr
JOIN orders o ON sr.run_id = o.run_id
GROUP BY sr.strategy_name, o.action, o.side
ORDER BY sr.strategy_name, o.action;

-- 5. Time to Fill Analysis
SELECT 
    sr.strategy_name,
    o.action,
    COUNT(*) as filled_orders,
    AVG((julianday(o.filled_timestamp) - julianday(o.placed_timestamp)) * 24 * 60) as avg_time_to_fill_minutes,
    MIN((julianday(o.filled_timestamp) - julianday(o.placed_timestamp)) * 24 * 60) as min_time_to_fill_minutes,
    MAX((julianday(o.filled_timestamp) - julianday(o.placed_timestamp)) * 24 * 60) as max_time_to_fill_minutes
FROM strategy_runs sr
JOIN orders o ON sr.run_id = o.run_id
WHERE o.status = 'filled' AND o.filled_timestamp IS NOT NULL
GROUP BY sr.strategy_name, o.action
ORDER BY avg_time_to_fill_minutes;

-- ============================================
-- Position Tracking
-- ============================================

-- 6. Position History Over Time
SELECT 
    sr.strategy_name,
    ph.timestamp,
    ph.position,
    ph.unrealized_pnl,
    ms.yes_mid as market_price_at_time
FROM strategy_runs sr
JOIN position_history ph ON sr.run_id = ph.run_id
LEFT JOIN market_snapshots ms ON sr.run_id = ms.run_id 
    AND ABS(julianday(ph.timestamp) - julianday(ms.timestamp)) < 0.0001  -- Match timestamps
WHERE sr.strategy_name = 'KXBTCMINY-80000'  -- Replace with your strategy name
ORDER BY ph.timestamp;

-- 7. Maximum Position Exposure
SELECT 
    sr.strategy_name,
    MAX(ABS(ph.position)) as max_position,
    AVG(ABS(ph.position)) as avg_position,
    COUNT(*) as position_updates
FROM strategy_runs sr
JOIN position_history ph ON sr.run_id = ph.run_id
GROUP BY sr.strategy_name
ORDER BY max_position DESC;

-- ============================================
-- Market Data Analysis
-- ============================================

-- 8. Market Volatility Analysis
-- Calculate price volatility from snapshots
WITH price_changes AS (
    SELECT 
        sr.run_id,
        sr.strategy_name,
        ms.timestamp,
        ms.yes_mid,
        LAG(ms.yes_mid) OVER (PARTITION BY sr.run_id ORDER BY ms.timestamp) as prev_price,
        ms.yes_mid - LAG(ms.yes_mid) OVER (PARTITION BY sr.run_id ORDER BY ms.timestamp) as price_change
    FROM strategy_runs sr
    JOIN market_snapshots ms ON sr.run_id = ms.run_id
    WHERE sr.trade_side = 'yes'
)
SELECT 
    strategy_name,
    COUNT(*) as price_updates,
    AVG(ABS(price_change)) as avg_absolute_price_change,
    -- SQLite doesn't have STDEV, so we calculate standard deviation manually
    SQRT(AVG(price_change * price_change) - AVG(price_change) * AVG(price_change)) as price_volatility,
    MIN(price_change) as min_price_change,
    MAX(price_change) as max_price_change
FROM price_changes
WHERE prev_price IS NOT NULL
GROUP BY strategy_name
ORDER BY price_volatility DESC;

-- 9. Spread Analysis Over Time
SELECT 
    sr.strategy_name,
    DATE(ms.timestamp) as date,
    AVG(ms.spread) as avg_spread,
    MIN(ms.spread) as min_spread,
    MAX(ms.spread) as max_spread,
    COUNT(*) as snapshots
FROM strategy_runs sr
JOIN market_snapshots ms ON sr.run_id = ms.run_id
GROUP BY sr.strategy_name, DATE(ms.timestamp)
ORDER BY sr.strategy_name, date;

-- ============================================
-- Strategy Comparison
-- ============================================

-- 10. Strategy Parameter Effectiveness
-- Compare performance across different parameter sets
SELECT 
    sr.strategy_name,
    json_extract(sr.config_params, '$.gamma') as gamma,
    json_extract(sr.config_params, '$.k') as k,
    json_extract(sr.config_params, '$.sigma') as sigma,
    json_extract(sr.config_params, '$.max_position') as max_position,
    COUNT(DISTINCT t.trade_id) as total_trades,
    SUM(CASE 
        WHEN t.action = 'buy' THEN -t.price * t.quantity
        WHEN t.action = 'sell' THEN t.price * t.quantity
    END) as realized_pnl,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(CASE WHEN o.status = 'filled' THEN 1 ELSE 0 END) as filled_orders
FROM strategy_runs sr
LEFT JOIN trades t ON sr.run_id = t.run_id
LEFT JOIN orders o ON sr.run_id = o.run_id
WHERE sr.end_time IS NOT NULL
GROUP BY sr.strategy_name, gamma, k, sigma, max_position
ORDER BY realized_pnl DESC;

-- 11. Performance by Market Ticker
SELECT 
    sr.market_ticker,
    COUNT(DISTINCT sr.run_id) as strategy_runs,
    COUNT(DISTINCT t.trade_id) as total_trades,
    SUM(CASE 
        WHEN t.action = 'buy' THEN -t.price * t.quantity
        WHEN t.action = 'sell' THEN t.price * t.quantity
    END) as total_realized_pnl,
    AVG(ms.spread) as avg_spread
FROM strategy_runs sr
LEFT JOIN trades t ON sr.run_id = t.run_id
LEFT JOIN market_snapshots ms ON sr.run_id = ms.run_id
GROUP BY sr.market_ticker
ORDER BY total_realized_pnl DESC;

-- ============================================
-- Risk Metrics
-- ============================================

-- 12. Drawdown Analysis
-- Calculate maximum drawdown from position history
WITH position_pnl AS (
    SELECT 
        sr.strategy_name,
        ph.timestamp,
        ph.position,
        ph.unrealized_pnl,
        SUM(ph.unrealized_pnl) OVER (PARTITION BY sr.run_id ORDER BY ph.timestamp) as cumulative_pnl
    FROM strategy_runs sr
    JOIN position_history ph ON sr.run_id = ph.run_id
    WHERE ph.unrealized_pnl IS NOT NULL
),
running_max AS (
    SELECT 
        strategy_name,
        timestamp,
        cumulative_pnl,
        MAX(cumulative_pnl) OVER (PARTITION BY strategy_name ORDER BY timestamp) as peak_pnl
    FROM position_pnl
)
SELECT 
    strategy_name,
    MIN(cumulative_pnl - peak_pnl) as max_drawdown,
    MAX(cumulative_pnl) as peak_pnl,
    MIN(cumulative_pnl) as trough_pnl
FROM running_max
GROUP BY strategy_name
ORDER BY max_drawdown;

-- 13. Order Size Distribution
SELECT 
    sr.strategy_name,
    o.action,
    AVG(o.placed_quantity) as avg_order_size,
    MIN(o.placed_quantity) as min_order_size,
    MAX(o.placed_quantity) as max_order_size,
    COUNT(*) as order_count
FROM strategy_runs sr
JOIN orders o ON sr.run_id = o.run_id
GROUP BY sr.strategy_name, o.action
ORDER BY sr.strategy_name, o.action;

-- ============================================
-- Recent Activity
-- ============================================

-- 14. Recent Trades (Last 24 hours)
SELECT 
    sr.strategy_name,
    t.timestamp,
    t.action,
    t.side,
    t.price,
    t.quantity,
    t.realized_pnl
FROM strategy_runs sr
JOIN trades t ON sr.run_id = t.run_id
WHERE t.timestamp >= datetime('now', '-1 day')
ORDER BY t.timestamp DESC
LIMIT 100;

-- 15. Active Strategy Runs
SELECT 
    run_id,
    strategy_name,
    market_ticker,
    trade_side,
    start_time,
    datetime('now') as current_time,
    (julianday('now') - julianday(start_time)) * 24 as hours_running
FROM strategy_runs
WHERE end_time IS NULL
ORDER BY start_time DESC;

