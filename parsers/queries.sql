-- ============================================================================
-- COMPREHENSIVE FLOW ANALYSIS QUERIES
-- Sequential execution order using call_order column
-- ============================================================================

USE trade_kg_db;
GO

-- ============================================================================
-- 1. LIST ALL API ENDPOINTS (Entry Points)
-- ============================================================================

SELECT 
    api_method,
    api_endpoint,
    name AS handler_function,
    serviceName,
    parameters
FROM CodeNodes
WHERE api_endpoint IS NOT NULL
ORDER BY serviceName, api_endpoint;

/*
Output:
GET  /                    root              orchestrator
POST /orders             place_order       orchestrator
GET  /orders/{order_id}  get_order_status  orchestrator
POST /pricing/calculate  calculate_pricing pricing_pnl_service
...
*/


-- ============================================================================
-- 2. COMPLETE BUY FLOW (From POST /orders to all leaf functions)
-- WITH SEQUENTIAL ORDERING
-- ============================================================================

;WITH OrderedFlow AS (
    SELECT 
        1 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        c.parameters,
        CAST('1' AS NVARCHAR(MAX)) AS sort_path,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.api_endpoint = '/orders'

    UNION ALL

    SELECT 
        flow.level + 1,
        target.name,
        target.serviceName,
        target.parameters,
        CAST(flow.sort_path + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        CAST(flow.path + ' → ' + target.name AS NVARCHAR(MAX)),
        target.$node_id
    FROM OrderedFlow flow
    INNER JOIN Relationships r ON r.$from_id = flow.current_node_id
    INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
    WHERE flow.level < 15
      AND r.relationshipType IN ('CALLS', 'API_CALLS', 'EXPOSES')
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sort_path) AS execution_step,
    level,
    REPLICATE('  ', level - 1) + function_name AS indented_function,
    service,
    parameters,
    path
FROM OrderedFlow
ORDER BY sort_path;

/*
Output (Sequential Execution):
Step 1: place_order
Step 2:   validate_trade (1st call from place_order)
Step 3:     check_symbol_tradeable (1st call from validate_trade)
Step 4:       get_symbol_metadata
Step 5:     is_market_open (2nd call from validate_trade)
...
Step 12:   calculate_pricing (2nd call from place_order)
Step 13:     get_market_price (1st call from calculate_pricing)
...
*/


-- ============================================================================
-- 3. TRACE BACKWARDS: Given a function, find all paths TO it
-- Shows who calls this function and in what order
-- ============================================================================

DECLARE @TargetFunction NVARCHAR(255) = 'validate_account_balance';

;WITH BackwardFlow AS (
    SELECT 
        0 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        c.parameters,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        CAST('0' AS NVARCHAR(MAX)) AS sort_path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.name = @TargetFunction

    UNION ALL

    SELECT 
        flow.level - 1,
        caller.name,
        caller.serviceName,
        caller.parameters,
        CAST(caller.name + ' → ' + flow.path AS NVARCHAR(MAX)),
        CAST(CAST(flow.level - 1 AS VARCHAR) + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        caller.$node_id
    FROM BackwardFlow flow
    INNER JOIN Relationships r ON r.$to_id = flow.current_node_id
    INNER JOIN CodeNodes caller ON caller.$node_id = r.$from_id
    WHERE flow.level > -15
      AND r.relationshipType IN ('CALLS', 'API_CALLS', 'EXPOSES')
)
SELECT 
    ABS(level) AS depth,
    function_name,
    service,
    parameters,
    path
FROM BackwardFlow
ORDER BY level DESC, sort_path;

/*
Output (Who calls validate_account_balance):
Depth | Function                      | Path
------|-------------------------------|----------------------------------------------
0     | validate_account_balance      | validate_account_balance
1     | validate_order_requirements   | validate_order_requirements → validate_account_balance
2     | validate_trade                | validate_trade → validate_order_requirements → validate_account_balance
3     | place_order                   | place_order → validate_trade → validate_order_requirements → validate_account_balance
4     | /orders                       | POST /orders → place_order → ...
*/


-- ============================================================================
-- 4. TRACE FORWARDS: Given a function, find all downstream calls
-- Shows what this function calls in sequential order
-- ============================================================================

DECLARE @SourceFunction NVARCHAR(255) = 'calculate_pricing';

;WITH ForwardFlow AS (
    SELECT 
        0 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        c.parameters,
        CAST('0' AS NVARCHAR(MAX)) AS sort_path,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.name = @SourceFunction

    UNION ALL

    SELECT 
        flow.level + 1,
        target.name,
        target.serviceName,
        target.parameters,
        CAST(flow.sort_path + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        CAST(flow.path + ' → ' + target.name AS NVARCHAR(MAX)),
        target.$node_id
    FROM ForwardFlow flow
    INNER JOIN Relationships r ON r.$from_id = flow.current_node_id
    INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
    WHERE flow.level < 15
      AND r.relationshipType IN ('CALLS', 'API_CALLS', 'EXPOSES')
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sort_path) AS call_sequence,
    level,
    REPLICATE('  ', level) + function_name AS indented_function,
    service,
    parameters,
    path
FROM ForwardFlow
ORDER BY sort_path;

/*
Output (What calculate_pricing calls in order):
Seq | Level | Function                        | Path
----|-------|---------------------------------|------------------------------------------
1   | 0     | calculate_pricing               | calculate_pricing
2   | 1     |   get_market_price              | calculate_pricing → get_market_price
3   | 2     |     validate_price_components   | calculate_pricing → get_market_price → validate_price_components
4   | 3     |       check_price_range_validity| ...
5   | 1     |   calculate_total_cost          | calculate_pricing → calculate_total_cost
6   | 2     |     validate_cost_breakdown     | ...
*/


-- ============================================================================
-- 5. FIND ALL CROSS-SERVICE API CALLS (with call order)
-- ============================================================================

SELECT 
    source.name AS caller_function,
    source.serviceName AS from_service,
    r.call_order AS call_sequence,
    r.line_number AS source_line,
    target.api_endpoint AS endpoint,
    target.api_method AS method,
    target.name AS target_function,
    target.serviceName AS to_service,
    r.description
FROM Relationships r
INNER JOIN CodeNodes source ON source.$node_id = r.$from_id
INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
WHERE r.relationshipType = 'API_CALLS'
  AND source.serviceName <> target.serviceName
ORDER BY source.name, r.call_order;

/*
Output (Cross-service communication):
Caller          | From Service  | Seq | Line | Endpoint            | Method | Target           | To Service
----------------|---------------|-----|------|---------------------|--------|------------------|------------------
place_order     | orchestrator  | 1   | 52   | /trades/validate    | POST   | validate_trade   | trade_service
place_order     | orchestrator  | 2   | 85   | /pricing/calculate  | POST   | calculate_pricing| pricing_pnl_service
place_order     | orchestrator  | 3   | 110  | /trades/execute     | POST   | execute_trade    | trade_service
place_order     | orchestrator  | 4   | 135  | /risk/assess        | POST   | assess_risk      | risk_service
*/


-- ============================================================================
-- 6. FIND ALL FUNCTIONS WITH SPECIFIC PARAMETER (e.g., order_type)
-- Useful for finding all BUY/SELL logic
-- ============================================================================

DECLARE @ParameterName NVARCHAR(100) = 'order_type';

SELECT 
    c.name AS function_name,
    c.serviceName AS service,
    c.parameters,
    c.api_endpoint,
    c.type,
    -- Count how many functions call this one
    (SELECT COUNT(*) 
     FROM Relationships r 
     WHERE r.$to_id = c.$node_id 
       AND r.relationshipType IN ('CALLS', 'API_CALLS')) AS caller_count
FROM CodeNodes c
WHERE c.parameters LIKE '%' + @ParameterName + '%'
  AND c.type IN ('function', 'method')
ORDER BY c.serviceName, c.name;

/*
Output (All functions with order_type parameter):
Function                    | Service              | Parameters                                      | Caller Count
----------------------------|----------------------|-------------------------------------------------|-------------
calculate_pricing           | pricing_pnl_service  | symbol, quantity, price, order_type, ...       | 2
calculate_estimated_pnl     | pricing_pnl_service  | symbol, quantity, price, order_type, ...       | 1
get_market_price            | pricing_pnl_service  | symbol, order_type, trace_id, order_id         | 1
validate_account_balance    | trade_service        | quantity, price, symbol, order_type, ...       | 1
validate_order_requirements | trade_service        | symbol, quantity, price, order_type, ...       | 1
*/


-- ============================================================================
-- 7. COMPLETE EXECUTION FLOW WITH RELATIONSHIP TYPES AND CALL ORDER
-- Shows flow with relationship metadata
-- ============================================================================

;WITH DetailedFlow AS (
    SELECT 
        1 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        CAST('1' AS NVARCHAR(MAX)) AS sort_path,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        CAST('' AS NVARCHAR(100)) AS rel_type,
        CAST(NULL AS INT) AS call_order,
        CAST(NULL AS INT) AS line_number,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.api_endpoint = '/orders'

    UNION ALL

    SELECT 
        flow.level + 1,
        target.name,
        target.serviceName,
        CAST(flow.sort_path + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        CAST(flow.path + ' -[' + r.relationshipType + ']-> ' + target.name AS NVARCHAR(MAX)),
        CAST(r.relationshipType AS NVARCHAR(100)),
        r.call_order,
        r.line_number,
        target.$node_id
    FROM DetailedFlow flow
    INNER JOIN Relationships r ON r.$from_id = flow.current_node_id
    INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
    WHERE flow.level < 10
      AND r.relationshipType IN ('CALLS', 'API_CALLS', 'EXPOSES')
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sort_path) AS step,
    level,
    function_name,
    service,
    rel_type AS relationship,
    call_order AS seq,
    line_number AS line,
    path
FROM DetailedFlow
ORDER BY sort_path;

/*
Output (Flow with relationship types):
Step | Level | Function              | Service      | Relationship | Seq | Line | Path
-----|-------|-----------------------|--------------|--------------|-----|------|--------------------------------------
1    | 1     | place_order           | orchestrator |              |     |      | place_order
2    | 2     | validate_trade        | trade_service| API_CALLS    | 1   | 52   | place_order -[API_CALLS]-> validate_trade
3    | 3     | check_symbol_tradeable| trade_service| CALLS        | 1   | 85   | place_order -[API_CALLS]-> validate_trade -[CALLS]-> check_symbol_tradeable
*/


-- ============================================================================
-- 8. FIND LEAF FUNCTIONS (Functions that don't call anything else)
-- These are typically where actual business logic happens
-- ============================================================================

SELECT 
    c.name AS leaf_function,
    c.serviceName AS service,
    c.parameters,
    c.type,
    -- Count how many functions call this leaf
    (SELECT COUNT(*) 
     FROM Relationships r 
     WHERE r.$to_id = c.$node_id 
       AND r.relationshipType IN ('CALLS', 'API_CALLS')) AS called_by_count
FROM CodeNodes c
WHERE c.type IN ('function', 'method')
  AND NOT EXISTS (
      SELECT 1 
      FROM Relationships r 
      WHERE r.$from_id = c.$node_id
        AND r.relationshipType IN ('CALLS', 'API_CALLS')
  )
ORDER BY called_by_count DESC, c.serviceName, c.name;

/*
Output (Leaf functions - no downstream calls):
Function                     | Service              | Parameters                    | Called By
-----------------------------|----------------------|-------------------------------|----------
get_symbol_metadata          | trade_service        | symbol                        | 3
verify_market_conditions     | pricing_pnl_service  | symbol, price, trace_id, ... | 1
get_cost_basis               | pricing_pnl_service  | symbol                        | 1
is_market_open               | trade_service        | NULL                          | 1
calculate_volatility_multiplier| risk_service       | symbol                        | 1
...
*/


-- ============================================================================
-- 9. FIND ROOT FUNCTIONS (Functions that are never called by others)
-- These are API entry points
-- ============================================================================

SELECT 
    c.name AS root_function,
    c.api_method AS method,
    c.api_endpoint AS endpoint,
    c.serviceName AS service,
    c.parameters,
    -- Count how many functions this root calls
    (SELECT COUNT(*) 
     FROM Relationships r 
     WHERE r.$from_id = c.$node_id 
       AND r.relationshipType IN ('CALLS', 'API_CALLS')) AS calls_count
FROM CodeNodes c
WHERE c.type IN ('function', 'method')
  AND c.api_endpoint IS NOT NULL  -- Only API endpoints
  AND NOT EXISTS (
      SELECT 1 
      FROM Relationships r 
      WHERE r.$to_id = c.$node_id
        AND r.relationshipType IN ('CALLS', 'API_CALLS')
  )
ORDER BY c.serviceName, c.api_endpoint;

/*
Output (Root functions - API entry points):
Function         | Method | Endpoint            | Service              | Calls
-----------------|--------|---------------------|----------------------|------
root             | GET    | /                   | orchestrator         | 0
health_check     | GET    | /health             | orchestrator         | 0
place_order      | POST   | /orders             | orchestrator         | 5
get_order_status | GET    | /orders/{order_id}  | orchestrator         | 1
calculate_pricing| POST   | /pricing/calculate  | pricing_pnl_service  | 3
...
*/


-- ============================================================================
-- 10. MAP LOG LINE TO COMPLETE FLOW
-- Given a function name from a log, find which flow it belongs to
-- ============================================================================

DECLARE @LogFunction NVARCHAR(255) = 'validate_account_balance';

;WITH BackwardToRoot AS (
    SELECT 
        0 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        c.api_endpoint,
        c.api_method,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        CAST('0' AS NVARCHAR(MAX)) AS sort_path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.name = @LogFunction

    UNION ALL

    SELECT 
        flow.level - 1,
        caller.name,
        caller.serviceName,
        caller.api_endpoint,
        caller.api_method,
        CAST(caller.name + ' → ' + flow.path AS NVARCHAR(MAX)),
        CAST(CAST(flow.level - 1 AS VARCHAR) + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        caller.$node_id
    FROM BackwardToRoot flow
    INNER JOIN Relationships r ON r.$to_id = flow.current_node_id
    INNER JOIN CodeNodes caller ON caller.$node_id = r.$from_id
    WHERE flow.level > -20
)
-- Find the root API endpoint
SELECT TOP 1
    'LOG CONTEXT' AS analysis_type,
    @LogFunction AS log_function,
    function_name AS root_function,
    api_method AS method,
    api_endpoint AS endpoint,
    service AS root_service,
    ABS(level) AS call_depth,
    path AS full_execution_path
FROM BackwardToRoot
WHERE api_endpoint IS NOT NULL
ORDER BY level ASC;

/*
Output (Given log from validate_account_balance):
Analysis Type | Log Function              | Root Function | Method | Endpoint | Root Service | Depth | Full Path
--------------|---------------------------|---------------|--------|----------|--------------|-------|------------------------------------------
LOG CONTEXT   | validate_account_balance  | place_order   | POST   | /orders  | orchestrator | 4     | place_order → validate_trade → validate_order_requirements → validate_account_balance

This tells you:
- The log is from a POST /orders request
- It's 4 levels deep in the call stack
- Complete path: place_order → validate_trade → validate_order_requirements → validate_account_balance
*/


-- ============================================================================
-- 11. FIND ALL VALIDATION CHAINS
-- Functions with "validate", "check", "verify" in sequential order
-- ============================================================================

;WITH ValidationFlow AS (
    SELECT 
        1 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        CAST('1' AS NVARCHAR(MAX)) AS sort_path,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.api_endpoint = '/orders'

    UNION ALL

    SELECT 
        flow.level + 1,
        target.name,
        target.serviceName,
        CAST(flow.sort_path + '.' + 
             RIGHT('000' + CAST(ISNULL(r.call_order, 999) AS VARCHAR), 3) AS NVARCHAR(MAX)),
        CAST(flow.path + ' → ' + target.name AS NVARCHAR(MAX)),
        target.$node_id
    FROM ValidationFlow flow
    INNER JOIN Relationships r ON r.$from_id = flow.current_node_id
    INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
    WHERE flow.level < 15
      AND r.relationshipType IN ('CALLS', 'API_CALLS')
      AND (target.name LIKE '%validate%' 
           OR target.name LIKE '%check%' 
           OR target.name LIKE '%verify%'
           OR target.name LIKE '%assess%')
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sort_path) AS validation_step,
    level,
    REPLICATE('  ', level - 1) + function_name AS validation_chain,
    service,
    path
FROM ValidationFlow
ORDER BY sort_path;

/*
Output (All validation functions in order):
Step | Level | Validation Chain                         | Service              | Path
-----|-------|------------------------------------------|----------------------|------------------------------------------
1    | 2     |   validate_trade                         | trade_service        | place_order → validate_trade
2    | 3     |     check_symbol_tradeable               | trade_service        | place_order → validate_trade → check_symbol_tradeable
3    | 3     |     validate_order_requirements          | trade_service        | place_order → validate_trade → validate_order_requirements
4    | 4     |       validate_account_balance           | trade_service        | ...
5    | 3     |     check_order_limits                   | trade_service        | ...
6    | 4     |       validate_price_components          | pricing_pnl_service  | ...
7    | 5     |         check_price_range_validity       | pricing_pnl_service  | ...
8    | 6     |           verify_market_conditions       | pricing_pnl_service  | ...
*/


-- ============================================================================
-- 12. PERFORMANCE ANALYSIS: Find longest call chains
-- ============================================================================

;WITH CallChainDepth AS (
    SELECT 
        1 AS level,
        c.name AS function_name,
        c.serviceName AS service,
        CAST(c.name AS NVARCHAR(MAX)) AS path,
        c.$node_id AS current_node_id
    FROM CodeNodes c
    WHERE c.api_endpoint = '/orders'

    UNION ALL

    SELECT 
        flow.level + 1,
        target.name,
        target.serviceName,
        CAST(flow.path + ' → ' + target.name AS NVARCHAR(MAX)),
        target.$node_id
    FROM CallChainDepth flow
    INNER JOIN Relationships r ON r.$from_id = flow.current_node_id
    INNER JOIN CodeNodes target ON target.$node_id = r.$to_id
    WHERE flow.level < 20
      AND r.relationshipType IN ('CALLS', 'API_CALLS')
)
SELECT TOP 10
    level AS max_depth,
    function_name AS leaf_function,
    service,
    path AS complete_call_chain
FROM CallChainDepth
WHERE NOT EXISTS (
    SELECT 1 
    FROM Relationships r 
    WHERE r.$from_id = current_node_id
      AND r.relationshipType IN ('CALLS', 'API_CALLS')
)
ORDER BY level DESC, path;

/*
Output (Deepest call chains - potential performance issues):
Depth | Leaf Function               | Service              | Complete Call Chain
------|-----------------------------|-----------------------|------------------------------------------------
6     | verify_market_conditions    | pricing_pnl_service  | place_order → calculate_pricing → get_market_price → validate_price_components → check_price_range_validity → verify_market_conditions
5     | audit_commission_rate       | pricing_pnl_service  | place_order → calculate_pricing → calculate_total_cost → validate_cost_breakdown → audit_commission_rate
4     | get_symbol_metadata         | trade_service        | place_order → validate_trade → check_symbol_tradeable → get_symbol_metadata
...
*/
