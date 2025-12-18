# Knowledge Graph Ingestion Pipeline

Simple pipeline to parse microservices code and logs into a graph database for RCA (Root Cause Analysis).

---

## Overview

This pipeline extracts code structure and runtime logs, then connects them to enable questions like:
- "Why did this order fail?"
- "Which function caused this error?"
- "Show me the execution path for trace XYZ"

---

## Components

### 1. Code Parser (`code_parser.py`)

**What it does:**
- Reads Python files from microservices
- Extracts functions, classes, and methods using Python AST
- Identifies function calls (who calls whom)

**Output Schema:**

**CodeNodes:**
```json
{
  "name": "validate_quantity",
  "type": "function",
  "serviceName": "trade_service",
  "filePath": "trade_service/src/app.py",
  "summary": "Validates order quantity",
  "snippet": "def validate_quantity(quantity: int):\n    if quantity < 0:\n        return 0\n    ..."
}
```

**Code Relationships:**
```json
{
  "sourceName": "place_order",
  "targetName": "validate_quantity",
  "relationshipType": "CALLS",
  "description": "place_order calls validate_quantity"
}
```

**Relationship Types:**
- `CALLS`: Function A calls Function B
- `CONTAINS`: Class contains Method

---

### 2. Log Parser (`log_parser.py`)

**What it does:**
- Reads trace-specific log files (`logs/trace_*.log`)
- Parses JSON log entries
- Creates temporal sequence (what happened after what)

**Output Schema:**

**LogEvents:**
```json
{
  "timestamp": "2025-12-17T23:40:18.123Z",
  "service": "trade_service",
  "level": "ERROR",
  "traceId": "abc-123-def-456",
  "message": "Invalid quantity: -50",
  "errorCode": "INVALID_QUANTITY"
}
```

**Log Relationships:**
```json
{
  "source": "abc-123_2025-12-17T23:40:18.123Z_trade_service",
  "target": "abc-123_2025-12-17T23:40:18.456Z_orchestrator",
  "type": "next_log",
  "description": "Temporal sequence in trace abc-123"
}
```

**Relationship Types:**
- `next_log`: Event A happened before Event B (in same trace)

---

### 3. Code-Log Linker (`link_code_logs.py`)

**What it does:**
Connects code and logs using 3 strategies:

**Strategy 1: Function Name Matching**
- Finds logs that mention function names
- Example: Log "Validating quantity: -50" → Links to `validate_quantity()` function

**Strategy 2: Error Pattern Matching**
- Links error logs to error-handling functions
- Example: "Unknown symbol" error → Links to `get_market_price()` function

**Strategy 3: Service Context**
- Links functions to logs from same service
- Creates context relationships

**Output Schema:**

**Code-to-Log Relationships:**
```json
{
  "type": "executed_in",
  "description": "validate_quantity executed and logged",
  "from": "validate_quantity CodeNode",
  "to": "LogEvent"
}
```

**Relationship Types:**
- `executed_in`: Code was executed and produced this log
- `logged_error`: Code logged this error
- `service_context`: Code and log share same service context

---

## Complete Graph Structure

```
CodeNodes (57 nodes)
├── Functions (validate_quantity, calculate_pnl, etc.)
├── Classes (OrderRequest, RiskAssessmentResponse, etc.)
└── Methods (JsonFormatter.format, etc.)
     │
     │ CALLS relationships
     ├──> Function calls another function
     │
     │ CONTAINS relationships  
     └──> Class contains methods

LogEvents (varies by runtime)
├── timestamp
├── service
├── level (INFO/ERROR/WARNING)
├── traceId
└── message
     │
     │ next_log relationships
     └──> Temporal sequence within trace

Code ←→ Log Connections
├── executed_in: Function → Log
├── logged_error: Function → Error Log
└── service_context: Function ↔ Service Logs
```

---

## Database Tables (SQL Server Graph)

### CodeNodes (AS NODE)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| name | NVARCHAR | Function/class name |
| type | NVARCHAR | function/class/method |
| serviceName | NVARCHAR | orchestrator/trade_service/etc |
| filePath | NVARCHAR | Source file path |
| summary | NVARCHAR | First line of docstring |
| snippet | NVARCHAR | Code snippet |

### LogEvents (AS NODE)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| timestamp | NVARCHAR | ISO timestamp |
| service | NVARCHAR | Service name |
| level | NVARCHAR | INFO/ERROR/WARNING/DEBUG |
| traceId | NVARCHAR | Distributed trace ID |
| message | NVARCHAR | Log message |
| errorCode | NVARCHAR | Error code if applicable |

### Relationships (AS EDGE)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| relationshipType | NVARCHAR | CALLS/next_log/executed_in/etc |
| description | NVARCHAR | Human-readable description |
| $from_id | NODE_ID | Source node |
| $to_id | NODE_ID | Target node |

---

## Usage

**Generate JSON files only (no database):**
```bash
python ingest_pipeline.py
```
Creates:
- `code_graph.json` - All code nodes and relationships
- `log_graph.json` - All log events and temporal relationships

**Store in database:**
```bash
python ingest_pipeline.py --use-database
```

**Clean database:**
```bash
python cleanup_database.py
```

---

## Configuration

Edit `.env` file:
```env
DB_SERVER=NAGARAJ-08
DB_DATABASE=trade_kg_db
DB_DRIVER={ODBC Driver 17 for SQL Server}
```

---

## Understanding the Graph (Step by Step)

### What's in Each Table?

**CodeNodes** = All your functions and classes (from Python files)
**LogEvents** = All log messages (from runtime execution)
**Relationships** = Connections between everything (edges)

---

### Step 1: See Your Code Functions

```sql
-- View all functions in your system
SELECT TOP 10 id, name, [type], serviceName 
FROM CodeNodes 
WHERE [type] = 'function'
ORDER BY serviceName, name;
```

**You'll see:** validate_quantity, place_order, get_market_price, etc.

---

### Step 2: See Your Runtime Logs

```sql
-- View logs from one trace (replace with actual trace ID)
SELECT timestamp, service, level, message 
FROM LogEvents 
WHERE traceId = 'YOUR_TRACE_ID_HERE'
ORDER BY timestamp;
```

**You'll see:** The timeline of what happened during one order execution.

---

### Step 3: See Code-to-Code Relationships (CALLS)

```sql
-- Which functions call which other functions
SELECT TOP 10
    cn1.name as CallerFunction, 
    cn1.serviceName as CallerService,
    cn2.name as CalledFunction,
    cn2.serviceName as CalledService
FROM CodeNodes cn1
JOIN Relationships r ON cn1.$node_id = r.$from_id
JOIN CodeNodes cn2 ON r.$to_id = cn2.$node_id
WHERE r.relationshipType = 'CALLS';
```

**You'll see:** place_order → validate_quantity, place_order → get_market_price, etc.

---

### Step 4: See Log-to-Log Relationships (next_log - Timeline)

```sql
-- Show event sequence: what happened after what
SELECT TOP 10
    le1.timestamp as FirstEventTime,
    le1.service as FirstService,
    LEFT(le1.message, 40) as FirstEvent,
    le2.timestamp as NextEventTime,
    le2.service as NextService,
    LEFT(le2.message, 40) as NextEvent
FROM LogEvents le1
JOIN Relationships r ON le1.$node_id = r.$from_id
JOIN LogEvents le2 ON r.$to_id = le2.$node_id
WHERE r.relationshipType = 'next_log'
ORDER BY le1.timestamp;
```

**You'll see:** Timeline showing Log1 → Log2 → Log3 in chronological order.

---

### Step 5: See Code-to-Log Relationships (executed_in)

```sql
-- Which function created which log
SELECT TOP 10
    cn.name as FunctionName,
    cn.serviceName as Service,
    LEFT(le.message, 60) as LogMessage,
    le.timestamp as When,
    r.relationshipType as LinkType
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE r.relationshipType IN ('executed_in', 'logged_error')
ORDER BY le.timestamp;
```

**You'll see:** validate_quantity function created "Validating quantity: -50" log.

---

### Complete Trace Analysis Query

```sql
-- Replace YOUR_TRACE_ID with actual trace ID from your logs
DECLARE @TraceId NVARCHAR(200) = 'YOUR_TRACE_ID_HERE';

-- 1. Show what code was involved
PRINT '========== CODE FUNCTIONS INVOLVED ==========';
SELECT DISTINCT cn.name as FunctionName, cn.serviceName as Service
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @TraceId;

-- 2. Show timeline of events
PRINT '========== EXECUTION TIMELINE ==========';
SELECT 
    ROW_NUMBER() OVER (ORDER BY timestamp) as Step,
    timestamp, 
    service, 
    level, 
    message
FROM LogEvents
WHERE traceId = @TraceId
ORDER BY timestamp;

-- 3. Show which code created which log
PRINT '========== CODE-TO-LOG CONNECTIONS ==========';
SELECT 
    cn.name as FunctionName,
    cn.serviceName as Service,
    le.message as LogCreated,
    r.relationshipType as LinkType,
    le.timestamp
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @TraceId
  AND r.relationshipType IN ('executed_in', 'logged_error')
ORDER BY le.timestamp;
```

---

## Quick Verification Queries

### View All Nodes (Code + Logs)
```sql
-- See sample of everything in the graph
SELECT 'CODE' as NodeType, name as NodeName, [type] as SubType, serviceName as Service, NULL as TraceId
FROM CodeNodes
UNION ALL
SELECT 'LOG' as NodeType, LEFT(message, 50) as NodeName, level as SubType, service as Service, traceId as TraceId
FROM LogEvents
ORDER BY NodeType, Service;
```

### View All Relationships
```sql
-- See all connections in the graph
SELECT 
    relationshipType as RelationType,
    COUNT(*) as Count,
    CASE 
        WHEN relationshipType IN ('CALLS', 'CONTAINS') THEN 'Code-to-Code'
        WHEN relationshipType = 'next_log' THEN 'Log-to-Log'
        ELSE 'Code-to-Log'
    END as Category
FROM Relationships
GROUP BY relationshipType
ORDER BY Count DESC;
```

### Detailed Relationship View
```sql
-- See actual node connections (first 20)
SELECT TOP 20
    r.relationshipType,
    COALESCE(cn1.name, le1.service + ': ' + LEFT(le1.message, 30)) as FromNode,
    COALESCE(cn2.name, le2.service + ': ' + LEFT(le2.message, 30)) as ToNode,
    r.description
FROM Relationships r
LEFT JOIN CodeNodes cn1 ON r.$from_id = cn1.$node_id
LEFT JOIN CodeNodes cn2 ON r.$to_id = cn2.$node_id
LEFT JOIN LogEvents le1 ON r.$from_id = le1.$node_id
LEFT JOIN LogEvents le2 ON r.$to_id = le2.$node_id;
```

---

## Example Queries

**Find function and its logs:**
```sql
SELECT cn.name, le.message, le.timestamp
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE cn.name = 'validate_quantity';
```

**Trace execution path:**
```sql
SELECT timestamp, service, message
FROM LogEvents
WHERE traceId = 'abc-123'
ORDER BY timestamp;
```

**Find what function calls what:**
```sql
SELECT cn1.name as Caller, cn2.name as Callee
FROM CodeNodes cn1
JOIN Relationships r ON cn1.$node_id = r.$from_id
JOIN CodeNodes cn2 ON r.$to_id = cn2.$node_id
WHERE r.relationshipType = 'CALLS';
```

---

## Files

- `code_parser.py` - Parse Python code using AST
- `log_parser.py` - Parse JSON log files
- `link_code_logs.py` - Connect code to logs
- `ingest_pipeline.py` - Orchestrate full pipeline
- `db_config.py` - Database connection
- `cleanup_database.py` - Delete all data
- `create_schema.sql` - SQL Server schema

----------------------
-- ============================================================
-- TRACE RELATIONSHIP ANALYSIS QUERY
-- ============================================================
-- Instructions: Replace the trace ID below with your actual trace ID
-- Then execute this entire script in SQL Server Management Studio
-- ============================================================

USE trade_kg_db;
GO

-- SET YOUR TRACE ID HERE:
DECLARE @traceId VARCHAR(100) = '61ca2bc2-527c-438d-87a3-e208ad998984';

PRINT '';
PRINT '======================================================================';
PRINT 'TRACE ANALYSIS FOR: ' + @traceId;
PRINT '======================================================================';
PRINT '';

-- ============================================================
-- 1. LOG EVENTS IN THIS TRACE (temporal sequence)
-- ============================================================
PRINT '1. LOG EVENTS IN THIS TRACE (temporal sequence):';
PRINT '----------------------------------------------------------------------';

SELECT 
    le.id,
    FORMAT(CAST(le.timestamp AS datetime2), 'HH:mm:ss.fff') as time,
    le.service,
    le.level,
    SUBSTRING(le.message, 1, 100) as message
FROM LogEvents le
WHERE le.traceId = @traceId
ORDER BY le.timestamp;

PRINT '';

-- 2. Show all code functions involved (via executed_in relationships)
PRINT '';
PRINT '2. CODE FUNCTIONS EXECUTED (via executed_in relationships):';
PRINT '------------------------------------------------------------';
SELECT 
    cn.id as node_id,
    cn.name as function_name,
    cn.serviceName,
    cn.[type],
    COUNT(*) as num_logs_created
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
GROUP BY cn.id, cn.name, cn.serviceName, cn.[type]
ORDER BY cn.serviceName, cn.name;

-- 3. Show executed_in relationship details
PRINT '';
PRINT '3. EXECUTED_IN RELATIONSHIPS (Function → Log):';
PRINT '------------------------------------------------------------';
SELECT 
    cn.name as function_name,
    cn.serviceName,
    r.relationshipType,
    SUBSTRING(le.message, 1, 100) as log_message,
    le.level,
    le.timestamp
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
ORDER BY le.timestamp;

-- 4. Show error relationships if any
PRINT '';
PRINT '4. ERROR RELATIONSHIPS (logged_error):';
PRINT '------------------------------------------------------------';
SELECT 
    cn.name as function_name,
    cn.serviceName,
    r.relationshipType,
    SUBSTRING(le.message, 1, 100) as error_message,
    le.level
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'logged_error'
ORDER BY le.timestamp;

-- 5. Show temporal flow (next_log relationships)
PRINT '';
PRINT '5. TEMPORAL LOG FLOW (next_log relationships):';
PRINT '------------------------------------------------------------';
SELECT 
    le1.service as from_service,
    SUBSTRING(le1.message, 1, 50) as from_message,
    r.relationshipType,
    le2.service as to_service,
    SUBSTRING(le2.message, 1, 50) as to_message
FROM LogEvents le1
JOIN Relationships r ON le1.$node_id = r.$from_id
JOIN LogEvents le2 ON r.$to_id = le2.$node_id
WHERE le1.traceId = @traceId 
  AND r.relationshipType = 'next_log'
ORDER BY le1.timestamp;

-- 6. Summary statistics
PRINT '';
PRINT '6. SUMMARY STATISTICS:';
PRINT '------------------------------------------------------------';
SELECT 
    'Total Log Events' as metric,
    COUNT(*) as count
FROM LogEvents 
WHERE traceId = @traceId

UNION ALL

SELECT 
    'Unique Functions Executed' as metric,
    COUNT(DISTINCT cn.name) as count
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'

UNION ALL

SELECT 
    'executed_in links' as metric,
    COUNT(*) as count
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'

UNION ALL

SELECT 
    'logged_error links' as metric,
    COUNT(*) as count
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'logged_error'

UNION ALL

SELECT 
    'next_log links' as metric,
    COUNT(*) as count
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'next_log';

-- 7. Show service call graph
PRINT '';
PRINT '7. SERVICE INTERACTION GRAPH:';
PRINT '------------------------------------------------------------';
SELECT DISTINCT
    le1.service as from_service,
    le2.service as to_service,
    COUNT(*) as interaction_count
FROM LogEvents le1
JOIN Relationships r ON le1.$node_id = r.$from_id
JOIN LogEvents le2 ON r.$to_id = le2.$node_id
WHERE le1.traceId = @traceId 
  AND r.relationshipType = 'next_log'
  AND le1.service != le2.service
GROUP BY le1.service, le2.service
ORDER BY interaction_count DESC;

PRINT '';
PRINT '============================================================';
PRINT 'END OF TRACE ANALYSIS';
PRINT '============================================================';




----------------------------------

```

-- ============================================================
-- TRACE RELATIONSHIP ANALYSIS QUERY
-- ============================================================
-- Instructions: Replace the trace ID below with your actual trace ID
-- Then execute this entire script in SQL Server Management Studio
-- ============================================================

USE trade_kg_db;
GO

-- SET YOUR TRACE ID HERE:
DECLARE @traceId VARCHAR(100) = '61ca2bc2-527c-438d-87a3-e208ad998984';

PRINT '';
PRINT '======================================================================';
PRINT 'TRACE ANALYSIS FOR: ' + @traceId;
PRINT '======================================================================';
PRINT '';

-- ============================================================
-- 1. LOG EVENTS IN THIS TRACE (temporal sequence)
-- ============================================================
PRINT '1. LOG EVENTS IN THIS TRACE (temporal sequence):';
PRINT '----------------------------------------------------------------------';

SELECT 
    le.id,
    FORMAT(CAST(le.timestamp AS datetime2), 'HH:mm:ss.fff') as time,
    le.service,
    le.level,
    SUBSTRING(le.message, 1, 100) as message
FROM LogEvents le
WHERE le.traceId = @traceId
ORDER BY le.timestamp;

PRINT '';

PRINT '';

-- ============================================================
-- 2. CODE FUNCTIONS EXECUTED (via executed_in relationships)
-- ============================================================
PRINT '2. CODE FUNCTIONS EXECUTED (via executed_in relationships):';
PRINT '----------------------------------------------------------------------';

SELECT 
    cn.serviceName,
    cn.name as function_name,
    cn.[type],
    COUNT(*) as num_logs_created
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
GROUP BY cn.serviceName, cn.name, cn.[type]
ORDER BY cn.serviceName, cn.name;

PRINT '';

-- ============================================================
-- 3. EXECUTED_IN RELATIONSHIPS DETAIL (Function → Log)
-- ============================================================
PRINT '3. EXECUTED_IN RELATIONSHIPS DETAIL (Function → Log):';
PRINT '----------------------------------------------------------------------';

SELECT 
    cn.name as function_name,
    cn.serviceName,
    '--executed_in-->' as relationship,
    le.level,
    SUBSTRING(le.message, 1, 80) as log_message,
    FORMAT(CAST(le.timestamp AS datetime2), 'HH:mm:ss.fff') as time
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
ORDER BY le.timestamp;

PRINT '';

-- ============================================================
-- 4. ERROR RELATIONSHIPS (logged_error)
-- ============================================================
PRINT '4. ERROR RELATIONSHIPS (logged_error):';
PRINT '----------------------------------------------------------------------';

SELECT 
    cn.name as function_name,
    cn.serviceName,
    '--logged_error-->' as relationship,
    le.level,
    SUBSTRING(le.message, 1, 100) as error_message
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'logged_error'
ORDER BY le.timestamp;

-- Show message if no errors
IF NOT EXISTS (
    SELECT 1 
    FROM CodeNodes cn
    JOIN Relationships r ON cn.$node_id = r.$from_id
    JOIN LogEvents le ON r.$to_id = le.$node_id
    WHERE le.traceId = @traceId AND r.relationshipType = 'logged_error'
)
BEGIN
    PRINT '  ✓ No error relationships found (no errors in this trace)';
END

PRINT '';

-- ============================================================
-- 5. SUMMARY STATISTICS
-- ============================================================
PRINT '5. SUMMARY STATISTICS:';
PRINT '----------------------------------------------------------------------';

DECLARE @totalLogs INT, @executedInCount INT, @loggedErrorCount INT, @nextLogCount INT, @uniqueFunctions INT;

SELECT @totalLogs = COUNT(*) FROM LogEvents WHERE traceId = @traceId;

SELECT @executedInCount = COUNT(*) 
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId AND r.relationshipType = 'executed_in';

SELECT @loggedErrorCount = COUNT(*) 
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId AND r.relationshipType = 'logged_error';

SELECT @nextLogCount = COUNT(*) 
FROM Relationships r
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId AND r.relationshipType = 'next_log';

SELECT @uniqueFunctions = COUNT(DISTINCT cn.name) 
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId AND r.relationshipType = 'executed_in';

SELECT 
    'Total Log Events' as Metric,
    @totalLogs as Count,
    '' as Percentage
UNION ALL
SELECT 
    'Unique Functions Executed',
    @uniqueFunctions,
    ''
UNION ALL
SELECT 
    'executed_in links',
    @executedInCount,
    CAST(CAST(@executedInCount * 100.0 / NULLIF(@totalLogs, 0) AS DECIMAL(5,1)) AS VARCHAR) + '%'
UNION ALL
SELECT 
    'logged_error links',
    @loggedErrorCount,
    ''
UNION ALL
SELECT 
    'next_log links',
    @nextLogCount,
    '';

PRINT '';
PRINT '  Code-to-Log Coverage: ' + CAST(CAST(@executedInCount * 100.0 / NULLIF(@totalLogs, 0) AS DECIMAL(5,1)) AS VARCHAR) + '%' 
    + ' (' + CAST(@executedInCount AS VARCHAR) + '/' + CAST(@totalLogs AS VARCHAR) + ' logs linked)';

PRINT '';

-- ============================================================
-- 6. SERVICE INTERACTION FLOW
-- ============================================================
PRINT '6. SERVICE INTERACTION FLOW:';
PRINT '----------------------------------------------------------------------';

SELECT DISTINCT
    le1.service as from_service,
    '-->' as flow,
    le2.service as to_service,
    COUNT(*) as transitions
FROM LogEvents le1
JOIN Relationships r ON le1.$node_id = r.$from_id
JOIN LogEvents le2 ON r.$to_id = le2.$node_id
WHERE le1.traceId = @traceId 
  AND r.relationshipType = 'next_log'
  AND le1.service != le2.service
GROUP BY le1.service, le2.service
ORDER BY transitions DESC;

PRINT '';

-- ============================================================
-- 7. TEMPORAL FLOW (next_log chain)
-- ============================================================
PRINT '7. TEMPORAL LOG FLOW (showing service transitions):';
PRINT '----------------------------------------------------------------------';

SELECT TOP 20
    FORMAT(CAST(le1.timestamp AS datetime2), 'HH:mm:ss.fff') as time,
    le1.service as from_service,
    SUBSTRING(le1.message, 1, 40) as from_message,
    '--next-->' as flow,
    le2.service as to_service,
    SUBSTRING(le2.message, 1, 40) as to_message
FROM LogEvents le1
JOIN Relationships r ON le1.$node_id = r.$from_id
JOIN LogEvents le2 ON r.$to_id = le2.$node_id
WHERE le1.traceId = @traceId 
  AND r.relationshipType = 'next_log'
ORDER BY le1.timestamp;

PRINT '';
PRINT '======================================================================';
PRINT 'END OF TRACE ANALYSIS';
PRINT '======================================================================';
PRINT '';

-- ============================================================
-- BONUS: List all available trace IDs
-- ============================================================
PRINT '';
PRINT 'OTHER AVAILABLE TRACE IDs:';
PRINT '----------------------------------------------------------------------';

SELECT DISTINCT 
    le.traceId,
    MIN(le.timestamp) as first_log,
    MAX(le.timestamp) as last_log,
    COUNT(*) as log_count,
    COUNT(DISTINCT le.service) as services_involved
FROM LogEvents le
GROUP BY le.traceId
ORDER BY MIN(le.timestamp) DESC;

GO

```

---------

```
-- ============================================================
-- SIMPLE TRACE FLOW ANALYSIS
-- ============================================================
-- Shows the end-to-end execution flow for a trace ID
-- Easy to understand visualization of the Knowledge Graph
-- ============================================================

USE trade_kg_db;
GO

-- SET YOUR TRACE ID HERE:
DECLARE @traceId VARCHAR(100) = '61ca2bc2-527c-438d-87a3-e208ad998984';

PRINT '';
PRINT '======================================================================';
PRINT 'END-TO-END TRACE FLOW: ' + @traceId;
PRINT '======================================================================';
PRINT '';

-- ============================================================
-- 1. EXECUTION TIMELINE - What happened and when?
-- ============================================================
PRINT '1. EXECUTION TIMELINE';
PRINT '----------------------------------------------------------------------';

SELECT 
    ROW_NUMBER() OVER (ORDER BY le.timestamp) as Step,
    FORMAT(CAST(le.timestamp AS datetime2), 'HH:mm:ss.fff') as Time,
    le.service as Service,
    SUBSTRING(le.message, 1, 70) as Action
FROM LogEvents le
WHERE le.traceId = @traceId
ORDER BY le.timestamp;

PRINT '';

-- ============================================================
-- 2. FUNCTION EXECUTION MAP - Which code functions ran?
-- ============================================================
PRINT '2. FUNCTION EXECUTION MAP (Code → Logs)';
PRINT '----------------------------------------------------------------------';

SELECT 
    cn.serviceName as Service,
    cn.name as [Function],
    COUNT(*) as [Logs Created],
    STRING_AGG(CAST(le.level AS VARCHAR), ', ') as [Log Levels]
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
GROUP BY cn.serviceName, cn.name
ORDER BY cn.serviceName;

PRINT '';

-- ============================================================
-- 3. SERVICE JOURNEY - How did the request flow?
-- ============================================================
PRINT '3. SERVICE JOURNEY';
PRINT '----------------------------------------------------------------------';

WITH ServiceFlow AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY MIN(le.timestamp)) as StepNum,
        le.service,
        MIN(le.timestamp) as FirstLog,
        MAX(le.timestamp) as LastLog,
        COUNT(*) as LogCount
    FROM LogEvents le
    WHERE le.traceId = @traceId
    GROUP BY le.service
)
SELECT 
    StepNum as [Step],
    service as [Service],
    FORMAT(CAST(FirstLog AS datetime2), 'HH:mm:ss.fff') as [Started],
    FORMAT(CAST(LastLog AS datetime2), 'HH:mm:ss.fff') as [Ended],
    LogCount as [Activities]
FROM ServiceFlow
ORDER BY StepNum;

PRINT '';

-- ============================================================
-- 4. KEY FUNCTIONS EXECUTED - The main story
-- ============================================================
PRINT '4. KEY FUNCTIONS EXECUTED (In Order)';
PRINT '----------------------------------------------------------------------';

SELECT DISTINCT
    FORMAT(CAST(le.timestamp AS datetime2), 'HH:mm:ss.fff') as Time,
    cn.serviceName as Service,
    cn.name as [Function Called],
    SUBSTRING(le.message, 1, 60) as [What It Did]
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId 
  AND r.relationshipType = 'executed_in'
  AND le.message LIKE '%[[]%'  -- Only show logs with [function_name] prefix
ORDER BY le.timestamp;

PRINT '';

-- ============================================================
-- 5. VISUAL FLOW DIAGRAM
-- ============================================================
PRINT '5. VISUAL SERVICE FLOW';
PRINT '----------------------------------------------------------------------';

WITH ServiceTransitions AS (
    SELECT DISTINCT
        le1.service as FromService,
        le2.service as ToService,
        COUNT(*) as Calls
    FROM LogEvents le1
    JOIN Relationships r ON le1.$node_id = r.$from_id
    JOIN LogEvents le2 ON r.$to_id = le2.$node_id
    WHERE le1.traceId = @traceId 
      AND r.relationshipType = 'next_log'
      AND le1.service != le2.service
    GROUP BY le1.service, le2.service
)
SELECT 
    FromService as [From],
    '───►' as [Flow],
    ToService as [To],
    Calls as [Times]
FROM ServiceTransitions
ORDER BY Calls DESC;

PRINT '';

-- ============================================================
-- 6. QUICK SUMMARY
-- ============================================================
PRINT '6. SUMMARY';
PRINT '----------------------------------------------------------------------';

DECLARE @totalLogs INT, @executedFunctions INT, @servicesInvolved INT, @duration VARCHAR(20);

SELECT @totalLogs = COUNT(*) FROM LogEvents WHERE traceId = @traceId;

SELECT @executedFunctions = COUNT(DISTINCT cn.name)
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = @traceId AND r.relationshipType = 'executed_in';

SELECT @servicesInvolved = COUNT(DISTINCT service)
FROM LogEvents WHERE traceId = @traceId;

SELECT @duration = 
    CAST(
        DATEDIFF(MILLISECOND, MIN(timestamp), MAX(timestamp)) 
        AS VARCHAR
    ) + ' ms'
FROM LogEvents WHERE traceId = @traceId;

SELECT 
    @totalLogs as [Total Log Events],
    @executedFunctions as [Functions Executed],
    @servicesInvolved as [Services Involved],
    @duration as [Duration];

PRINT '';
PRINT '======================================================================';
PRINT 'This trace involved ' + CAST(@servicesInvolved AS VARCHAR) + ' services, ';
PRINT 'executed ' + CAST(@executedFunctions AS VARCHAR) + ' functions, ';
PRINT 'and completed in ' + @duration;
PRINT '======================================================================';
PRINT '';

GO


```
