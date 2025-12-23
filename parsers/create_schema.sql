-- SQL Server Graph Database Schema for Trade Platform Knowledge Graph
-- This script creates the tables and indexes needed for the RCA knowledge graph

-- Enable SQL Server Graph features
USE trade_kg_db;
GO

-- Drop existing tables if they exist (for fresh setup)
IF OBJECT_ID('dbo.Relationships', 'U') IS NOT NULL
    DROP TABLE dbo.Relationships;
IF OBJECT_ID('dbo.LogEvents', 'U') IS NOT NULL
    DROP TABLE dbo.LogEvents;
IF OBJECT_ID('dbo.CodeNodes', 'U') IS NOT NULL
    DROP TABLE dbo.CodeNodes;
GO

-- Create CodeNodes table (AS NODE)
CREATE TABLE CodeNodes (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(500) NOT NULL,
    type NVARCHAR(50) NOT NULL, -- 'function', 'class', 'method', 'module', 'endpoint'
    parameters NVARCHAR(1000), -- Function parameters: symbol, quantity, order_type
    api_endpoint NVARCHAR(500), -- API endpoint if exposed: /orders, /trades/validate
    api_method NVARCHAR(10), -- HTTP method: GET, POST, PUT, DELETE
    summary NVARCHAR(MAX),
    snippet NVARCHAR(MAX),
    filePath NVARCHAR(1000),
    serviceName NVARCHAR(200),
    embedding VARBINARY(MAX), -- Store vector embeddings as binary
    created_at DATETIME DEFAULT GETDATE()
) AS NODE;
GO

-- Create LogEvents table (AS NODE)
CREATE TABLE LogEvents (
    id INT IDENTITY(1,1) PRIMARY KEY,
    timestamp NVARCHAR(100) NOT NULL,
    service NVARCHAR(200) NOT NULL,
    level NVARCHAR(50) NOT NULL, -- 'INFO', 'ERROR', 'WARNING', 'DEBUG'
    traceId NVARCHAR(200) NOT NULL,
    orderId NVARCHAR(200), -- NEW: Order ID for tracking
    functionName NVARCHAR(500), -- NEW: Function that logged this event
    message NVARCHAR(MAX),
    errorCode NVARCHAR(100),
    errorType NVARCHAR(200), -- NEW: Type of exception (ValueError, HTTPException, etc.)
    exception NVARCHAR(MAX), -- NEW: Full stack trace
    durationMs INT, -- NEW: Performance duration in milliseconds
    metadata NVARCHAR(MAX), -- JSON metadata
    embedding VARBINARY(MAX),
    created_at DATETIME DEFAULT GETDATE()
) AS NODE;
GO

-- Create Relationships table (AS EDGE)
CREATE TABLE Relationships (
    id INT IDENTITY(1,1) PRIMARY KEY,
    relationshipType NVARCHAR(100) NOT NULL, -- 'calls', 'contains', 'next_log', 'executed_in', 'logged_error'
    description NVARCHAR(MAX),
    call_order INT, -- NEW: Order of function calls (1st, 2nd, 3rd...)
    line_number INT, -- NEW: Line number in source code where call happens
    timestamp NVARCHAR(100),
    created_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT EC_Relationships CONNECTION (CodeNodes TO CodeNodes, CodeNodes TO LogEvents, LogEvents TO LogEvents) ON DELETE NO ACTION
) AS EDGE;
GO

-- Create indexes for better query performance
CREATE INDEX IX_CodeNodes_Name ON CodeNodes(name);
CREATE INDEX IX_CodeNodes_Type ON CodeNodes(type);
CREATE INDEX IX_CodeNodes_ServiceName ON CodeNodes(serviceName);
CREATE INDEX IX_LogEvents_TraceId ON LogEvents(traceId);
CREATE INDEX IX_LogEvents_Service ON LogEvents(service);
CREATE INDEX IX_LogEvents_Level ON LogEvents(level);
CREATE INDEX IX_Relationships_Type ON Relationships(relationshipType);
GO

-- Sample queries to verify the schema

-- Query 1: Count nodes by type
SELECT 
    'CodeNodes' as TableName,
    type,
    COUNT(*) as Count
FROM CodeNodes
GROUP BY type
UNION ALL
SELECT 
    'LogEvents' as TableName,
    level as type,
    COUNT(*) as Count
FROM LogEvents
GROUP BY level;
GO

-- Query 2: Find all functions that logged errors
SELECT 
    cn.name as FunctionName,
    cn.serviceName as Service,
    le.message as ErrorMessage,
    le.timestamp as ErrorTime,
    r.relationshipType
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE r.relationshipType = 'logged_error'
ORDER BY le.timestamp;
GO

-- Query 3: Trace execution path for a specific trace_id
SELECT 
    le.timestamp,
    le.service,
    le.level,
    le.message
FROM LogEvents le
WHERE le.traceId = 'example-trace-id'
ORDER BY le.timestamp;
GO

-- Query 4: Find all code involved in a trace
SELECT DISTINCT
    cn.name as CodeElement,
    cn.type as ElementType,
    cn.serviceName as Service,
    le.message as LogMessage,
    r.relationshipType
FROM CodeNodes cn
JOIN Relationships r ON cn.$node_id = r.$from_id
JOIN LogEvents le ON r.$to_id = le.$node_id
WHERE le.traceId = 'example-trace-id';
GO

-- Query 5: Find functions that call other functions
SELECT 
    cn1.name as Caller,
    cn2.name as Callee,
    r.description
FROM CodeNodes cn1
JOIN Relationships r ON cn1.$node_id = r.$from_id
JOIN CodeNodes cn2 ON r.$to_id = cn2.$node_id
WHERE r.relationshipType = 'calls';
GO

PRINT 'Trade Platform Knowledge Graph schema created successfully!';
