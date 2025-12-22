"""
Analyze relationships for a specific trace ID in the Knowledge Graph
"""
import sys
from db_config import DatabaseConfig
from datetime import datetime


def analyze_trace(trace_id):
    """Analyze all relationships for a given trace ID"""
    db = DatabaseConfig()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("\n" + "="*70)
    print(f"TRACE ANALYSIS FOR: {trace_id}")
    print("="*70)
    
    try:
        # 1. Show all log events in this trace
        print("\n1. LOG EVENTS IN THIS TRACE (temporal sequence):")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                le.id,
                le.timestamp,
                le.service,
                le.level,
                SUBSTRING(le.message, 1, 100) as message_preview
            FROM LogEvents le
            WHERE le.traceId = ?
            ORDER BY le.timestamp
        """, (trace_id,))
        
        log_events = cursor.fetchall()
        if log_events:
            for row in log_events:
                print(f"  [{row[3]}] {row[2]:20s} | {row[4]}")
        else:
            print("  No log events found for this trace ID")
            return
        
        # 2. Show all code functions involved (via executed_in relationships)
        print("\n2. CODE FUNCTIONS EXECUTED (via executed_in relationships):")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                cn.name as function_name,
                cn.serviceName,
                cn.[type],
                COUNT(*) as num_logs_created
            FROM CodeNodes cn
            JOIN Relationships r ON cn.$node_id = r.$from_id
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? 
              AND r.relationshipType = 'executed_in'
            GROUP BY cn.name, cn.serviceName, cn.[type]
            ORDER BY cn.serviceName, cn.name
        """, (trace_id,))
        
        functions = cursor.fetchall()
        if functions:
            for row in functions:
                print(f"  {row[1]:20s} | {row[0]:30s} ({row[2]}) → {row[3]} logs")
        else:
            print("  ⚠️  No executed_in relationships found")
            print("  This means the linker couldn't match function names to log messages")
        
        # 3. Show executed_in relationship details
        print("\n3. EXECUTED_IN RELATIONSHIPS DETAIL (Function → Log):")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                cn.name as function_name,
                cn.serviceName,
                SUBSTRING(le.message, 1, 80) as log_message,
                le.level,
                le.timestamp
            FROM CodeNodes cn
            JOIN Relationships r ON cn.$node_id = r.$from_id
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? 
              AND r.relationshipType = 'executed_in'
            ORDER BY le.timestamp
        """, (trace_id,))
        
        details = cursor.fetchall()
        if details:
            for row in details:
                print(f"  {row[0]:25s} --executed_in--> [{row[3]}] {row[2]}")
        else:
            print("  No executed_in relationships found")
        
        # 4. Show error relationships if any
        print("\n4. ERROR RELATIONSHIPS (logged_error):")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                cn.name as function_name,
                cn.serviceName,
                SUBSTRING(le.message, 1, 80) as error_message,
                le.level
            FROM CodeNodes cn
            JOIN Relationships r ON cn.$node_id = r.$from_id
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? 
              AND r.relationshipType = 'logged_error'
            ORDER BY le.timestamp
        """, (trace_id,))
        
        errors = cursor.fetchall()
        if errors:
            for row in errors:
                print(f"  {row[0]:25s} --logged_error--> [{row[3]}] {row[2]}")
        else:
            print("  No error relationships found (no errors in this trace)")
        
        # 5. Summary statistics
        print("\n5. SUMMARY STATISTICS:")
        print("-" * 70)
        
        # Total log events
        cursor.execute("SELECT COUNT(*) FROM LogEvents WHERE traceId = ?", (trace_id,))
        total_logs = cursor.fetchone()[0]
        
        # executed_in count
        cursor.execute("""
            SELECT COUNT(*) 
            FROM Relationships r
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? AND r.relationshipType = 'executed_in'
        """, (trace_id,))
        executed_in_count = cursor.fetchone()[0]
        
        # logged_error count
        cursor.execute("""
            SELECT COUNT(*) 
            FROM Relationships r
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? AND r.relationshipType = 'logged_error'
        """, (trace_id,))
        logged_error_count = cursor.fetchone()[0]
        
        # next_log count
        cursor.execute("""
            SELECT COUNT(*) 
            FROM Relationships r
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? AND r.relationshipType = 'next_log'
        """, (trace_id,))
        next_log_count = cursor.fetchone()[0]
        
        # Unique functions
        cursor.execute("""
            SELECT COUNT(DISTINCT cn.name) 
            FROM CodeNodes cn
            JOIN Relationships r ON cn.$node_id = r.$from_id
            JOIN LogEvents le ON r.$to_id = le.$node_id
            WHERE le.traceId = ? AND r.relationshipType = 'executed_in'
        """, (trace_id,))
        unique_functions = cursor.fetchone()[0]
        
        print(f"  Total Log Events:        {total_logs}")
        print(f"  Unique Functions:        {unique_functions}")
        print(f"  executed_in links:       {executed_in_count}")
        print(f"  logged_error links:      {logged_error_count}")
        print(f"  next_log links:          {next_log_count}")
        
        # Calculate coverage percentage
        if total_logs > 0:
            coverage = (executed_in_count / total_logs) * 100
            print(f"\n  Code-to-Log Coverage:    {coverage:.1f}% ({executed_in_count}/{total_logs} logs linked)")
            
            if coverage < 10:
                print("\n  ⚠️  WARNING: Very low coverage!")
                print("     - Check if log messages contain [function_name] prefix")
                print("     - Services may need to be restarted with updated code")
        
        # 6. Service interaction graph
        print("\n6. SERVICE INTERACTION FLOW:")
        print("-" * 70)
        cursor.execute("""
            SELECT DISTINCT
                le1.service as from_service,
                le2.service as to_service,
                COUNT(*) as transition_count
            FROM LogEvents le1
            JOIN Relationships r ON le1.$node_id = r.$from_id
            JOIN LogEvents le2 ON r.$to_id = le2.$node_id
            WHERE le1.traceId = ? 
              AND r.relationshipType = 'next_log'
              AND le1.service != le2.service
            GROUP BY le1.service, le2.service
            ORDER BY transition_count DESC
        """, (trace_id,))
        
        interactions = cursor.fetchall()
        if interactions:
            for row in interactions:
                print(f"  {row[0]:25s} → {row[1]:25s} ({row[2]} transitions)")
        else:
            print("  No cross-service interactions found")
        
    except Exception as e:
        print(f"\n❌ Error analyzing trace: {e}")
    finally:
        cursor.close()
        conn.close()
    
    print("\n" + "="*70)
    print("END OF TRACE ANALYSIS")
    print("="*70 + "\n")


def list_available_traces():
    """List all available trace IDs"""
    db = DatabaseConfig()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("\nAvailable Trace IDs:")
    print("-" * 70)
    
    cursor.execute("""
        SELECT DISTINCT 
            le.traceId,
            MIN(le.timestamp) as first_log,
            MAX(le.timestamp) as last_log,
            COUNT(*) as log_count
        FROM LogEvents le
        GROUP BY le.traceId
        ORDER BY MIN(le.timestamp) DESC
    """)
    
    traces = cursor.fetchall()
    for i, row in enumerate(traces, 1):
        print(f"{i}. {row[0]}")
        print(f"   First: {row[1]} | Last: {row[2]} | Logs: {row[3]}")
    
    cursor.close()
    conn.close()
    
    return [row[0] for row in traces]


if __name__ == "__main__":
    if len(sys.argv) > 1:
        trace_id = sys.argv[1]
        analyze_trace(trace_id)
    else:
        print("\n" + "="*70)
        print("TRACE RELATIONSHIP ANALYZER")
        print("="*70)
        
        traces = list_available_traces()
        
        if not traces:
            print("\n❌ No traces found in database")
            print("   Run some test orders first to generate traces")
        else:
            print("\nUsage:")
            print(f"  python analyze_trace.py <trace_id>")
            print(f"\nExample:")
            print(f"  python analyze_trace.py {traces[0]}")
