"""
Cleanup script to remove all data from knowledge graph database tables.
Run this before fresh ingestion to start with clean tables.
"""

from db_config import DatabaseConfig


def cleanup_all_data():
    """Delete all data from CodeNodes, LogEvents, and Relationships tables"""
    
    print("="*70)
    print(" DATABASE CLEANUP")
    print("="*70)
    
    
    
    
    db_config = DatabaseConfig()
    conn = db_config.get_connection()
    cursor = conn.cursor()
    
    try:
        # Delete in correct order: edges first, then nodes
        print("\n1. Deleting all relationships (edges)...")
        cursor.execute("DELETE FROM Relationships")
        relationships_deleted = cursor.rowcount
        print(f"   ✅ Deleted {relationships_deleted} relationships")
        
        print("\n2. Deleting all log events...")
        cursor.execute("DELETE FROM LogEvents")
        logs_deleted = cursor.rowcount
        print(f"   ✅ Deleted {logs_deleted} log events")
        
        print("\n3. Deleting all code nodes...")
        cursor.execute("DELETE FROM CodeNodes")
        nodes_deleted = cursor.rowcount
        print(f"   ✅ Deleted {nodes_deleted} code nodes")
        
        # Commit all changes
        conn.commit()
        
        print("\n" + "="*70)
        print(" CLEANUP SUMMARY")
        print("="*70)
        print(f"Total deleted:")
        print(f"  - Code nodes: {nodes_deleted}")
        print(f"  - Log events: {logs_deleted}")
        print(f"  - Relationships: {relationships_deleted}")
        print("\n✅ Database cleanup complete! Ready for fresh ingestion.")
        print("="*70)
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error during cleanup: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def verify_cleanup():
    """Verify that all tables are empty"""
    
    print("\n" + "="*70)
    print(" VERIFICATION")
    print("="*70)
    
    db_config = DatabaseConfig()
    conn = db_config.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM CodeNodes")
        code_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM LogEvents")
        log_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Relationships")
        rel_count = cursor.fetchone()[0]
        
        print(f"CodeNodes: {code_count}")
        print(f"LogEvents: {log_count}")
        print(f"Relationships: {rel_count}")
        
        if code_count == 0 and log_count == 0 and rel_count == 0:
            print("\n✅ All tables are empty!")
        else:
            print("\n⚠️ Warning: Some tables still contain data")
        
        print("="*70)
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys
    
    # Confirm before deleting
    print("\n⚠️  WARNING: This will DELETE ALL DATA from the knowledge graph!")
    print("Tables affected: CodeNodes, LogEvents, Relationships")
    
    response = input("\nAre you sure you want to continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        cleanup_all_data()
        verify_cleanup()
    else:
        print("\n❌ Cleanup cancelled.")
        sys.exit(0)
