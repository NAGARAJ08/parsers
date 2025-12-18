"""
Extract all CodeNodes from database to JSON for summary enhancement
"""
import json
from db_config import DatabaseConfig


def extract_code_nodes():
    """Extract all CodeNodes from database"""
    db_config = DatabaseConfig()
    conn = db_config.get_connection()
    cursor = conn.cursor()
    
    try:
        print("Extracting CodeNodes from database...")
        
        cursor.execute("""
            SELECT 
                id,
                name,
                [type],
                summary,
                snippet,
                filePath,
                serviceName
            FROM CodeNodes
            ORDER BY serviceName, [type], name
        """)
        
        nodes = []
        for row in cursor.fetchall():
            node = {
                'id': row[0],
                'name': row[1],
                'type': row[2],
                'current_summary': row[3],
                'snippet': row[4],
                'filePath': row[5],
                'serviceName': row[6],
                'new_summary': ''  # To be filled by AI/human
            }
            nodes.append(node)
        
        # Save to JSON
        output_file = 'code_nodes_for_summary.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(nodes, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Extracted {len(nodes)} CodeNodes to {output_file}")
        print("\nBreakdown by type:")
        
        type_counts = {}
        service_counts = {}
        for node in nodes:
            node_type = node['type']
            service = node['serviceName']
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
            service_counts[service] = service_counts.get(service, 0) + 1
        
        for node_type, count in sorted(type_counts.items()):
            print(f"  {node_type}: {count}")
        
        print("\nBreakdown by service:")
        for service, count in sorted(service_counts.items()):
            print(f"  {service}: {count}")
        
        return nodes
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    nodes = extract_code_nodes()
    print("\n📝 Next steps:")
    print("1. Review 'code_nodes_for_summary.json'")
    print("2. Fill in 'new_summary' field for each node")
    print("3. Run 'python update_summaries.py' to update database")
