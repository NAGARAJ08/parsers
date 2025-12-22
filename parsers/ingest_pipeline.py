"""
Complete ingestion pipeline for Trade Platform Knowledge Graph

This script orchestrates the full ingestion process:
1. Parse code structure from microservices
2. Parse log events from trace logs
3. Link code and logs together
4. Create unified knowledge graph in SQL Server
"""

from code_parser import CodeParser
from log_parser import LogParser
from link_code_logs import CodeLogLinker
from datetime import datetime


def run_full_ingestion(trade_platform_dir, use_database=True):
    """
    Run complete ingestion pipeline
    
    Args:
        trade_platform_dir: Path to trade-platform directory
        use_database: If True, store in database. If False, export to JSON only.
    """
    
    print("="*70)
    print(" TRADE PLATFORM KNOWLEDGE GRAPH INGESTION")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source directory: {trade_platform_dir}")
    print(f"Storage mode: {'DATABASE' if use_database else 'JSON FILES ONLY'}")
    print("="*70)
    
    # Step 1: Parse code structure
    print("\n" + "="*70)
    print("STEP 1: PARSING CODE STRUCTURE")
    print("="*70)
    
    code_parser = CodeParser(use_database=use_database)
    code_parser.parse_microservices(trade_platform_dir)
    
    # Always export to JSON for inspection
    code_parser.export_to_json_for_debugging("code_graph.json")
    print(f"   📄 Exported to: code_graph.json")
    
    print(f"\n✅ Code parsing complete:")
    print(f"   - {len(code_parser.code_nodes)} code nodes extracted")
    print(f"   - {len(code_parser.relationships)} code relationships created")
    
    # Step 2: Parse log events
    print("\n" + "="*70)
    print("STEP 2: PARSING LOG EVENTS")
    print("="*70)
    
    log_parser = LogParser(use_database=use_database)
    log_parser.parse_trace_logs(trade_platform_dir)
    
    # Always export to JSON for inspection
    log_parser.export_to_json_for_debugging("log_graph.json")
    print(f"   📄 Exported to: log_graph.json")
    
    print(f"\n✅ Log parsing complete:")
    print(f"   - {len(log_parser.log_events)} log events extracted")
    print(f"   - {len(log_parser.relationships)} temporal relationships created")
    
    # Step 3: Link code and logs (only if database is enabled)
    if use_database:
        print("\n" + "="*70)
        print("STEP 3: LINKING CODE AND LOGS")
        print("="*70)
        
        linker = CodeLogLinker()
        linker.link_code_and_logs()
        
        print("\n✅ Code-log linking complete")
        
        # Step 4: Verify graph structure
        print("\n" + "="*70)
        print("STEP 4: VERIFYING GRAPH STRUCTURE")
        print("="*70)
        
        linker.verify_graph_structure()
    
    # Summary
    print("\n" + "="*70)
    print("INGESTION SUMMARY")
    print("="*70)
    print(f"✅ Code nodes: {len(code_parser.code_nodes)}")
    print(f"✅ Log events: {len(log_parser.log_events)}")
    print(f"✅ Total relationships: {len(code_parser.relationships) + len(log_parser.relationships)}")
    if use_database:
        print(f"✅ Knowledge graph stored in SQL Server database")
    else:
        print(f"✅ JSON files created:")
        print(f"   - code_graph.json (code nodes & relationships)")
        print(f"   - log_graph.json (log events & temporal relationships)")
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest trade platform code and logs into knowledge graph")
    parser.add_argument(
        "--platform-dir",
        default="../trade-platform",
        help="Path to trade-platform directory (default: ../trade-platform)"
    )
    parser.add_argument(
        "--use-database",
        action="store_true",
        help="Store in database (default: export to JSON files only)"
    )
    
    args = parser.parse_args()
    
    # Run ingestion - default to JSON export, use database only if flag is set
    run_full_ingestion(
        trade_platform_dir=args.platform_dir,
        use_database=True
    )
