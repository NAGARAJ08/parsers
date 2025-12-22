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
