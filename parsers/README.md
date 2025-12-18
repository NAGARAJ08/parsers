# Trade Platform Knowledge Graph Ingestion Pipeline

This directory contains the complete ingestion pipeline for building a knowledge graph from microservices code and runtime logs.

## Overview

The pipeline parses:
- **Code Structure**: Functions, classes, methods, and their relationships (calls, contains)
- **Log Events**: Runtime logs from trace-specific log files
- **Code-Log Links**: Relationships between code execution and log events

All data is stored in SQL Server Graph database for efficient traversal and querying.

## Architecture

```
┌─────────────────────┐
│  Trade Platform     │
│  Microservices      │
│  (4 services)       │
└──────┬──────────────┘
       │
       ├─── Code (.py files)
       │         │
       │         ▼
       │    code_parser.py ──┐
       │                     │
       └─── Logs (trace_*.log)│
                 │            │
                 ▼            │
            log_parser.py ────┤
                              │
                              ▼
                    ┌─────────────────┐
                    │  SQL Server     │
                    │  Graph Database │
                    └─────────────────┘
                              │
                              ▼
                    link_code_logs.py
                              │
                              ▼
                    ┌─────────────────┐
                    │ Unified Knowledge│
                    │      Graph      │
                    └─────────────────┘
```

## Files

### Core Components

- **`db_config.py`**: Database connection configuration
- **`code_parser.py`**: Parse Python code using AST module
- **`log_parser.py`**: Parse JSON log files from trace logs
- **`link_code_logs.py`**: Create relationships between code and logs
- **`ingest_pipeline.py`**: Orchestrate full ingestion process

### Database

- **`create_schema.sql`**: SQL Server Graph schema (CodeNodes, LogEvents, Relationships)

## Database Schema

### Tables

1. **CodeNodes** (AS NODE)
   - `id`: Primary key
   - `name`: Function/class/method name
   - `type`: 'function', 'class', 'method', 'module'
   - `summary`: LLM-generated summary (future)
   - `snippet`: Code snippet
   - `filePath`: Source file path
   - `serviceName`: Service name (orchestrator, trade_service, etc.)
   - `embedding`: Vector embedding (future)

2. **LogEvents** (AS NODE)
   - `id`: Primary key
   - `timestamp`: ISO format timestamp
   - `service`: Service name
   - `level`: Log level (INFO, ERROR, WARNING, DEBUG)
   - `traceId`: Distributed trace ID
   - `message`: Log message
   - `errorCode`: Error code if applicable
   - `embedding`: Vector embedding (future)

3. **Relationships** (AS EDGE)
   - `id`: Primary key
   - `relationshipType`: Type of relationship
     - `calls`: Function A calls Function B
     - `contains`: Class contains Method
     - `next_log`: Temporal sequence of log events
     - `executed_in`: Code executed and logged
     - `logged_error`: Code logged an error
     - `service_context`: Code and logs in same service
   - `description`: Human-readable description
   - `timestamp`: When relationship was created
   - `$from_id`: Source node
   - `$to_id`: Target node

## Installation

### Prerequisites

1. **SQL Server** with Graph capabilities (SQL Server 2017+)
2. **Python 3.8+**
3. **ODBC Driver 17 for SQL Server**

### Python Dependencies

```bash
pip install pyodbc
```

### Database Setup

1. Create database:
```sql
CREATE DATABASE trade_kg_db;
```

2. Run schema creation:
```bash
sqlcmd -S localhost -d trade_kg_db -U sa -P YourPassword -i create_schema.sql
```

3. Update connection string in `db_config.py`:
```python
server = "localhost"
database = "trade_kg_db"
username = "sa"
password = "YourPassword"
```

## Usage

### Option 1: Run Full Pipeline (Database)

```bash
python ingest_pipeline.py --platform-dir ../trade-platform
```

This will:
1. Parse all code from trade-platform services
2. Parse all trace logs
3. Store everything in SQL Server
4. Create code-to-log relationships
5. Verify graph structure

### Option 2: Export to JSON Only (No Database)

```bash
python ingest_pipeline.py --platform-dir ../trade-platform --json-only
```

This creates:
- `output/code_graph.json`: Code nodes and relationships
- `output/log_graph.json`: Log events and temporal relationships

### Option 3: Run Individual Components

**Parse code only:**
```python
from code_parser import CodeParser

parser = CodeParser(use_database=True)
parser.parse_microservices("../trade-platform")
```

**Parse logs only:**
```python
from log_parser import LogParser

parser = LogParser(use_database=True)
parser.parse_trace_logs("../trade-platform")
```

**Link code and logs:**
```python
from link_code_logs import CodeLogLinker

linker = CodeLogLinker()
linker.link_code_and_logs()
linker.verify_graph_structure()
```

## Example Queries

### Find all functions that logged errors
```sql
SELECT 
    cn.name as FunctionName,
    le.message as ErrorMessage,
    le.timestamp
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE r.relationshipType = 'logged_error'
ORDER BY le.timestamp;
```

### Trace execution path for a specific trace
```sql
SELECT 
    le.timestamp,
    le.service,
    le.message
FROM LogEvents le
WHERE le.traceId = 'abc-123'
ORDER BY le.timestamp;
```

### Find all code involved in a failed order
```sql
SELECT DISTINCT
    cn.name as CodeElement,
    cn.serviceName as Service,
    le.message as LogMessage
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = 'failed-trace-id'
AND le.level = 'ERROR';
```

### Find function call chains
```sql
SELECT 
    cn1.name as Caller,
    cn2.name as Callee,
    cn1.serviceName as CallerService,
    cn2.serviceName as CalleeService
FROM CodeNodes cn1
JOIN Relationships r ON cn1.$node_id = r.$from_id
JOIN CodeNodes cn2 ON r.$to_id = cn2.$node_id
WHERE r.relationshipType = 'calls';
```

## Workflow

### 1. Generate Test Data

First, run the trade platform to generate trace logs:

```bash
cd ../trade-platform
.\start-all-services.ps1

# In another terminal, send test orders
curl -X POST http://localhost:8000/order \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","quantity":-50,"action":"BUY","price":150.0}'
```

This creates trace logs in each service's `logs/` directory.

### 2. Run Ingestion

```bash
cd parsers
python ingest_pipeline.py
```

### 3. Query the Graph

Connect to SQL Server and run queries to explore relationships.

### 4. Future: Add LLM Enrichment

- Generate summaries for each function
- Create embeddings for semantic search
- Enable RAG-based RCA queries

## Troubleshooting

### Connection Issues

If you get connection errors:
1. Verify SQL Server is running
2. Check ODBC driver is installed: `odbcinst -q -d`
3. Test connection string manually
4. Ensure TCP/IP is enabled in SQL Server Configuration Manager

### Parsing Issues

If code parsing fails:
1. Check file encoding (should be UTF-8)
2. Verify Python syntax is valid
3. Look for syntax errors in service files

### Log Parsing Issues

If log parsing fails:
1. Ensure trace logs exist in `logs/` directories
2. Verify JSON format is valid
3. Check log file permissions

## Next Steps

1. **LLM Enrichment**: Add function summaries and embeddings
2. **RAG Agent**: Build query agent with LangChain
3. **UI Layer**: Create chatbot interface for RCA queries
4. **Real-time Ingestion**: Stream logs to graph in real-time
5. **Advanced Queries**: Multi-hop graph traversals for root cause analysis

## License

Internal use only

------------------
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

