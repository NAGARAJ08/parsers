-- Create table to store pre-computed workflows with routes and summaries
-- This supports RCA by providing immediate access to workflow context without graph traversal

-- Drop existing table if it exists
IF OBJECT_ID('WorkflowCatalog', 'U') IS NOT NULL
    DROP TABLE WorkflowCatalog;
GO

-- Create WorkflowCatalog table
CREATE TABLE WorkflowCatalog (
    workflow_id INT IDENTITY(1,1) PRIMARY KEY,
    entry_point_name NVARCHAR(255) NOT NULL,
    workflow_type NVARCHAR(100) NOT NULL,  -- e.g., 'retail', 'institutional', 'algo'
    full_route NVARCHAR(MAX) NOT NULL,     -- JSON array of function names in order
    workflow_summary NVARCHAR(MAX),         -- Aggregated summary of the entire workflow
    total_steps INT NOT NULL,               -- Number of functions in the workflow
    services_involved NVARCHAR(500),        -- Comma-separated list of services
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
) AS NODE;
GO

-- Create index for faster lookups
CREATE INDEX idx_entry_point ON WorkflowCatalog(entry_point_name);
CREATE INDEX idx_workflow_type ON WorkflowCatalog(workflow_type);
GO

-- Create table to track which functions belong to which workflows
-- This enables quick RCA: "Which workflows include function X?"
IF OBJECT_ID('WorkflowFunctions', 'U') IS NOT NULL
    DROP TABLE WorkflowFunctions;
GO

CREATE TABLE WorkflowFunctions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    workflow_id INT NOT NULL,
    function_name NVARCHAR(255) NOT NULL,
    step_order INT NOT NULL,                -- Position in the workflow (1-based)
    service_name NVARCHAR(100) NOT NULL,
    function_summary NVARCHAR(MAX),         -- Individual function summary
    data_contracts NVARCHAR(MAX),           -- JSON with params, returns, fields
    FOREIGN KEY (workflow_id) REFERENCES WorkflowCatalog(workflow_id) ON DELETE CASCADE
) AS NODE;
GO

-- Create indexes for RCA queries
CREATE INDEX idx_workflow_id ON WorkflowFunctions(workflow_id);
CREATE INDEX idx_function_name ON WorkflowFunctions(function_name);
GO
