USE trade_kg_db;
GO

-- Drop existing tables if they exist
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
    type NVARCHAR(50) NOT NULL,
    summary NVARCHAR(MAX),
    snippet NVARCHAR(MAX),
    filePath NVARCHAR(1000),
    serviceName NVARCHAR(200),
    embedding VARBINARY(MAX),
    created_at DATETIME DEFAULT GETDATE()
) AS NODE;
GO

-- Create LogEvents table (AS NODE)
CREATE TABLE LogEvents (
    id INT IDENTITY(1,1) PRIMARY KEY,
    timestamp NVARCHAR(100) NOT NULL,
    service NVARCHAR(200) NOT NULL,
    level NVARCHAR(50) NOT NULL,
    traceId NVARCHAR(200) NOT NULL,
    message NVARCHAR(MAX),
    errorCode NVARCHAR(100),
    embedding VARBINARY(MAX),
    created_at DATETIME DEFAULT GETDATE()
) AS NODE;
GO

-- Create Relationships table (AS EDGE)
CREATE TABLE Relationships (
    id INT IDENTITY(1,1) PRIMARY KEY,
    relationshipType NVARCHAR(100) NOT NULL,
    description NVARCHAR(MAX),
    timestamp NVARCHAR(100),
    created_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT EC_Relationships CONNECTION (CodeNodes TO CodeNodes, CodeNodes TO LogEvents, LogEvents TO LogEvents) ON DELETE NO ACTION
) AS EDGE;
GO

PRINT 'Tables created successfully!';
