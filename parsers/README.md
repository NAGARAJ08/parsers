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
