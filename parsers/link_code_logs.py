from datetime import datetime
from db_config import DatabaseConfig


class CodeLogLinker:
    
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.code_to_log_links = []
        
    def link_code_and_logs(self):
        """Create relationships between CodeNodes and LogEvents"""
        conn = self.db_config.get_connection()
        cursor = conn.cursor()
        
        try:
            print("Creating code-to-log relationships...")
            
            # Strategy 1: Link functions to logs by matching function names in log messages
            self.link_by_function_name(cursor)
            
            # Strategy 2: Link error codes to specific functions
            self.link_by_error_handling(cursor)
            
            # Strategy 3: Link services (module level) to all logs from that service
            self.link_service_to_logs(cursor)
            
            conn.commit()
            print(f"\n✅ Successfully created code-to-log relationships")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error linking code and logs: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def link_by_function_name(self, cursor):
        """Link functions to log events by matching function names in messages"""
        print("\n1. Linking functions to logs by name matching...")
        
        # Get all function nodes
        cursor.execute("""
            SELECT $node_id, name, serviceName
            FROM CodeNodes
            WHERE type = 'function'
        """)
        functions = cursor.fetchall()
        
        links_created = 0
        for func_node_id, func_name, service_name in functions:
            # Find log events that mention this function name
            cursor.execute("""
                SELECT $node_id, message, service
                FROM LogEvents
                WHERE message LIKE ?
                AND service = ?
            """, (f'%{func_name}%', service_name))
            
            matching_logs = cursor.fetchall()
            
            for log_node_id, message, log_service in matching_logs:
                # Create relationship: CodeNode -> LogEvent (executed_in)
                cursor.execute("""
                    INSERT INTO Relationships (relationshipType, description, timestamp, $from_id, $to_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    'executed_in',
                    f"Function {func_name} executed and logged",
                    datetime.now().isoformat(),
                    func_node_id,
                    log_node_id
                ))
                links_created += 1
        
        print(f"  Created {links_created} function-to-log links")
    
    def link_by_error_handling(self, cursor):
        """Link functions that raise errors to error log events"""
        print("\n2. Linking error-handling code to error logs...")
        
        # Define error patterns and their likely source functions
        error_patterns = {
            'Unknown symbol': 'get_market_price',
            'Invalid quantity': 'validate_quantity',
            'Risk assessment failed': 'assess_risk',
            'PnL integrity check failed': 'assess_risk',
            'execution timed out': 'assess_risk'
        }
        
        links_created = 0
        for error_pattern, func_name in error_patterns.items():
            # Get function node
            cursor.execute("""
                SELECT $node_id, serviceName
                FROM CodeNodes
                WHERE name = ? AND type = 'function'
            """, (func_name,))
            
            func_result = cursor.fetchone()
            if not func_result:
                continue
            
            func_node_id, service_name = func_result
            
            # Get error log events matching this pattern
            cursor.execute("""
                SELECT $node_id, message
                FROM LogEvents
                WHERE message LIKE ?
                AND level = 'ERROR'
                AND service = ?
            """, (f'%{error_pattern}%', service_name))
            
            error_logs = cursor.fetchall()
            
            for log_node_id, message in error_logs:
                # Create relationship: CodeNode -> LogEvent (logged_error)
                cursor.execute("""
                    INSERT INTO Relationships (relationshipType, description, timestamp, $from_id, $to_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    'logged_error',
                    f"Function {func_name} logged error: {error_pattern}",
                    datetime.now().isoformat(),
                    func_node_id,
                    log_node_id
                ))
                links_created += 1
        
        print(f"  Created {links_created} error-handling links")
    
    def link_service_to_logs(self, cursor):
        """Link service modules to all logs from that service"""
        print("\n3. Linking services to their log events...")
        
        # Get all service names from CodeNodes
        cursor.execute("""
            SELECT DISTINCT serviceName
            FROM CodeNodes
            WHERE serviceName IS NOT NULL
        """)
        services = cursor.fetchall()
        
        links_created = 0
        for (service_name,) in services:
            # Get all function nodes from this service
            cursor.execute("""
                SELECT $node_id, name
                FROM CodeNodes
                WHERE serviceName = ? AND type = 'function'
            """, (service_name,))
            
            functions = cursor.fetchall()
            
            # Get all log events from this service
            cursor.execute("""
                SELECT $node_id
                FROM LogEvents
                WHERE service = ?
            """, (service_name,))
            
            logs = cursor.fetchall()
            
            # Create relationships between functions and logs within same trace
            for func_node_id, func_name in functions:
                # Link to logs from same service (first 10 to avoid explosion)
                for i, (log_node_id,) in enumerate(logs[:10]):
                    cursor.execute("""
                        INSERT INTO Relationships (relationshipType, description, timestamp, $from_id, $to_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        'service_context',
                        f"Function {func_name} in service context",
                        datetime.now().isoformat(),
                        func_node_id,
                        log_node_id
                    ))
                    links_created += 1
        
        print(f"  Created {links_created} service-context links")
    
    def verify_graph_structure(self):
        """Run queries to verify the graph structure"""
        conn = self.db_config.get_connection()
        cursor = conn.cursor()
        
        try:
            print("\n" + "="*60)
            print("GRAPH STRUCTURE VERIFICATION")
            print("="*60)
            
            # Count nodes
            cursor.execute("SELECT COUNT(*) FROM CodeNodes")
            code_count = cursor.fetchone()[0]
            print(f"\nCodeNodes: {code_count}")
            
            cursor.execute("SELECT COUNT(*) FROM LogEvents")
            log_count = cursor.fetchone()[0]
            print(f"LogEvents: {log_count}")
            
            # Count relationships
            cursor.execute("SELECT COUNT(*) FROM Relationships")
            rel_count = cursor.fetchone()[0]
            print(f"Total Relationships: {rel_count}")
            
            # Count by relationship type
            cursor.execute("""
                SELECT relationshipType, COUNT(*) as count
                FROM Relationships
                GROUP BY relationshipType
                ORDER BY count DESC
            """)
            print("\nRelationships by type:")
            for rel_type, count in cursor.fetchall():
                print(f"  {rel_type}: {count}")
            
            # Sample traversal: Find logs related to a specific function
            print("\n" + "-"*60)
            print("Sample Query: Find logs for 'validate_quantity' function")
            print("-"*60)
            cursor.execute("""
                SELECT TOP 5
                    cn.name as function_name,
                    le.message as log_message,
                    le.level as log_level,
                    r.relationshipType as relationship
                FROM CodeNodes cn
                JOIN Relationships r ON cn.$node_id = r.$from_id
                JOIN LogEvents le ON r.$to_id = le.$node_id
                WHERE cn.name = 'validate_quantity'
                ORDER BY le.timestamp
            """)
            
            results = cursor.fetchall()
            if results:
                for func, msg, level, rel in results:
                    print(f"  [{level}] {func} --{rel}--> {msg[:80]}...")
            else:
                print("  No results found")
            
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    linker = CodeLogLinker()
    
    print("Starting code-to-log linking process...")
    linker.link_code_and_logs()
    
    print("\nVerifying graph structure...")
    linker.verify_graph_structure()
    
    print("\n✅ Code-log linking complete!")
