"""
Update CodeNode summaries in database from enhanced JSON file
"""
import json
from db_config import DatabaseConfig


def update_summaries(json_file='code_nodes_enhanced.json'):
    """Update summaries in database from JSON file"""
    
    # Load enhanced summaries
    print(f"Loading summaries from {json_file}...")
    with open(json_file, 'r', encoding='utf-8') as f:
        nodes = json.load(f)
    
    # Connect to database
    db_config = DatabaseConfig()
    conn = db_config.get_connection()
    cursor = conn.cursor()
    
    try:
        updated = 0
        skipped = 0
        
        print(f"\nUpdating {len(nodes)} CodeNodes...")
        
        for node in nodes:
            node_id = node['id']
            new_summary = node.get('new_summary', '').strip()
            
            if not new_summary:
                skipped += 1
                continue
            
            # Update summary in database
            cursor.execute("""
                UPDATE CodeNodes
                SET summary = ?
                WHERE id = ?
            """, (new_summary, node_id))
            
            updated += 1
            print(f"  Updated {node['name']} ({node['type']}): {new_summary[:60]}...")
        
        conn.commit()
        
        print(f"\n✅ Summary update complete!")
        print(f"   Updated: {updated}")
        print(f"   Skipped: {skipped}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error updating summaries: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        json_file = 'code_nodes_enhanced.json'
    
    print("="*60)
    print("CODE NODE SUMMARY UPDATER")
    print("="*60)
    
    update_summaries(json_file)
