"""
Pre-compute and persist all workflows to database for RCA.
This script:
1. Discovers all entry points (workflows)
2. Traverses complete workflow paths
3. Aggregates node summaries into workflow summaries
4. Persists to WorkflowCatalog and WorkflowFunctions tables

Usage: python precompute_workflows.py
"""

import pyodbc
import json
from typing import List, Dict, Tuple, Optional
from db_config import DatabaseConfig

def get_connection():
    """Get database connection"""
    config = DatabaseConfig()
    return config.get_connection()

def find_entry_points_dynamic() -> List[Dict]:
    """Find all functions in orchestrator with no incoming callers (entry points)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT 
        c.id,
        c.name,
        c.type,
        c.serviceName
    FROM CodeNodes c
    WHERE c.serviceName = 'orchestrator'
    AND c.type = 'function'
    AND NOT EXISTS (
        SELECT 1 
        FROM Relationships r
        WHERE r.$to_id = c.$node_id
        AND r.relationshipType = 'CALLS'
    )
    ORDER BY c.name
    """
    
    cursor.execute(query)
    entry_points = []
    for row in cursor.fetchall():
        entry_points.append({
            'node_id': row[0],
            'name': row[1],
            'type': row[2],
            'service': row[3]
        })
    
    cursor.close()
    conn.close()
    
    return entry_points


def get_workflow_path(entry_point_name: str, max_depth: int = 10) -> List[Dict]:
    """
    Get the complete sequential workflow path starting from an entry point.
    Returns list of functions in order with their details.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get the full path using recursive traversal across all services
    query = f"""
    WITH WorkflowPath AS (
        -- Base case: Start with the entry point
        SELECT 
            c.id,
            c.$node_id as node_id_json,
            c.name,
            c.type,
            c.serviceName,
            c.summary as current_summary,
            CAST(c.name AS NVARCHAR(MAX)) as path,
            1 as depth
        FROM CodeNodes c
        WHERE c.name = ?
        
        UNION ALL
        
        -- Recursive case: Follow both CALLS and API_CALLS relationships
        SELECT 
            called.id,
            called.$node_id,
            called.name,
            called.type,
            called.serviceName,
            called.summary,
            CAST(wp.path + ' -> ' + called.name AS NVARCHAR(MAX)),
            wp.depth + 1
        FROM WorkflowPath wp
        JOIN Relationships r ON r.$from_id = wp.node_id_json AND r.relationshipType IN ('CALLS', 'API_CALLS')
        JOIN CodeNodes called ON r.$to_id = called.$node_id
        WHERE wp.depth < {max_depth}
    )
    SELECT DISTINCT 
        id,
        name,
        type,
        serviceName,
        current_summary,
        depth
    FROM WorkflowPath
    ORDER BY depth, name
    """
    
    cursor.execute(query, (entry_point_name,))
    
    workflow_functions = []
    seen_functions = set()
    
    for row in cursor.fetchall():
        func_name = row[1]
        if func_name not in seen_functions:
            workflow_functions.append({
                'node_id': row[0],
                'name': func_name,
                'type': row[2],
                'service': row[3],
                'summary': row[4] or '',
                'depth': row[5]
            })
            seen_functions.add(func_name)
    
    cursor.close()
    conn.close()
    
    return workflow_functions


def extract_data_contracts(function_name: str) -> Dict:
    """
    Extract data contracts (parameters, return types, fields) from function.
    This supports point 4: data lineage tracking.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT snippet, summary
    FROM CodeNodes
    WHERE name = ?
    """
    
    cursor.execute(query, (function_name,))
    row = cursor.fetchone()
    
    if not row:
        cursor.close()
        conn.close()
        return {}
    
    snippet = row[0] or ''
    
    # Parse function signature to extract parameters
    params = []
    returns = ''
    
    # Simple extraction from snippet (can be enhanced with AST parsing)
    if 'def ' in snippet:
        # Extract function signature
        sig_start = snippet.find('def ' + function_name)
        if sig_start != -1:
            sig_end = snippet.find(':', sig_start)
            if sig_end != -1:
                signature = snippet[sig_start:sig_end]
                
                # Extract parameters between parentheses
                paren_start = signature.find('(')
                paren_end = signature.rfind(')')
                if paren_start != -1 and paren_end != -1:
                    param_str = signature[paren_start+1:paren_end]
                    if param_str.strip():
                        for param in param_str.split(','):
                            param = param.strip()
                            if param and param != 'self':
                                params.append(param)
                
                # Check for return type annotation
                if '->' in signature:
                    returns = signature.split('->')[-1].strip()
    
    # Extract fields accessed (simple regex - can be enhanced)
    fields = []
    if '.get(' in snippet:
        import re
        field_matches = re.findall(r"\.get\(['\"](\w+)['\"]", snippet)
        fields = list(set(field_matches))[:10]  # Limit to top 10
    
    cursor.close()
    conn.close()
    
    return {
        'parameters': params,
        'return_type': returns,
        'fields_accessed': fields
    }


def aggregate_workflow_summary(workflow_functions: List[Dict]) -> str:
    """
    Aggregate individual function summaries into a workflow-level summary.
    This supports point 3: workflow summary generation.
    """
    if not workflow_functions:
        return "Empty workflow"
    
    # Build a narrative summary from function summaries
    summaries = [f['summary'] for f in workflow_functions if f['summary']]
    
    if not summaries:
        return f"Workflow with {len(workflow_functions)} steps"
    
    # Create a high-level workflow description
    entry_point = workflow_functions[0]['name']
    services = list(set([f['service'] for f in workflow_functions]))
    
    workflow_summary = f"Workflow '{entry_point}': "
    workflow_summary += " → ".join(summaries[:5])  # First 5 steps
    
    if len(summaries) > 5:
        workflow_summary += f" ... and {len(summaries) - 5} more steps"
    
    workflow_summary += f". Involves {len(services)} service(s): {', '.join(services)}"
    
    return workflow_summary


def determine_workflow_type(entry_point_name: str) -> str:
    """Determine workflow type from entry point name."""
    name_lower = entry_point_name.lower()
    if 'institutional' in name_lower:
        return 'institutional'
    elif 'algo' in name_lower:
        return 'algo'
    elif 'retail' in name_lower or 'place_order' == name_lower:
        return 'retail'
    else:
        return 'common'


def persist_workflow(entry_point: Dict, workflow_functions: List[Dict]) -> int:
    """
    Persist a workflow to WorkflowCatalog and WorkflowFunctions tables.
    Returns the workflow_id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Aggregate workflow summary
    workflow_summary = aggregate_workflow_summary(workflow_functions)
    workflow_type = determine_workflow_type(entry_point['name'])
    
    # Build route as JSON array
    route = [f['name'] for f in workflow_functions]
    route_json = json.dumps(route)
    
    # Get unique services involved
    services = list(set([f['service'] for f in workflow_functions]))
    services_str = ', '.join(services)
    
    # Insert into WorkflowCatalog
    insert_workflow = """
    INSERT INTO WorkflowCatalog (entry_point_name, workflow_type, full_route, workflow_summary, total_steps, services_involved)
    OUTPUT INSERTED.workflow_id
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    cursor.execute(insert_workflow, (
        entry_point['name'],
        workflow_type,
        route_json,
        workflow_summary,
        len(workflow_functions),
        services_str
    ))
    
    workflow_id = cursor.fetchone()[0]
    
    # Insert each function into WorkflowFunctions
    for idx, func in enumerate(workflow_functions, start=1):
        # Extract data contracts for this function
        data_contracts = extract_data_contracts(func['name'])
        data_contracts_json = json.dumps(data_contracts)
        
        insert_function = """
        INSERT INTO WorkflowFunctions (workflow_id, function_name, step_order, service_name, function_summary, data_contracts)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_function, (
            workflow_id,
            func['name'],
            idx,
            func['service'],
            func['summary'],
            data_contracts_json
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return workflow_id


def clear_existing_workflows():
    """Clear existing workflow data before recomputing."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM WorkflowFunctions")
    cursor.execute("DELETE FROM WorkflowCatalog")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✓ Cleared existing workflow data")


def precompute_all_workflows():
    """
    Main function to pre-compute and persist all workflows.
    Implements points 2, 3, and 4 from mentor feedback.
    """
    print("=" * 80)
    print("PRE-COMPUTING AND PERSISTING WORKFLOWS")
    print("=" * 80)
    
    # Clear existing data
    clear_existing_workflows()
    
    # Step 1: Find all entry points (point 1 - already implemented)
    print("\n[Step 1] Discovering entry points...")
    entry_points = find_entry_points_dynamic()
    print(f"✓ Found {len(entry_points)} entry points")
    
    # Step 2 & 3: Traverse and persist each workflow
    print("\n[Step 2] Computing and persisting workflows...")
    
    for idx, entry_point in enumerate(entry_points, start=1):
        print(f"\n  [{idx}/{len(entry_points)}] Processing: {entry_point['name']}")
        
        # Get complete workflow path
        workflow_functions = get_workflow_path(entry_point['name'])
        
        if not workflow_functions:
            print(f"    ⚠ No functions found for {entry_point['name']}")
            continue
        
        # Persist workflow with aggregated summary
        workflow_id = persist_workflow(entry_point, workflow_functions)
        
        workflow_type = determine_workflow_type(entry_point['name'])
        print(f"    ✓ Persisted workflow_id={workflow_id}, type={workflow_type}, steps={len(workflow_functions)}")
    
    print("\n" + "=" * 80)
    print("✅ PRE-COMPUTATION COMPLETE")
    print("=" * 80)
    
    # Summary statistics
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM WorkflowCatalog")
    workflow_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM WorkflowFunctions")
    function_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    print(f"\nStatistics:")
    print(f"  - Total Workflows: {workflow_count}")
    print(f"  - Total Function Entries: {function_count}")
    print(f"\nWorkflows are now ready for RCA queries!")


def query_workflows_for_function(function_name: str):
    """
    Demo: Query which workflows contain a specific function.
    This is the RCA use case.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT
        wc.workflow_id,
        wc.entry_point_name,
        wc.workflow_type,
        wc.workflow_summary,
        wf.step_order
    FROM WorkflowCatalog wc
    JOIN WorkflowFunctions wf ON wc.workflow_id = wf.workflow_id
    WHERE wf.function_name = ?
    ORDER BY wc.workflow_type, wc.entry_point_name
    """
    
    cursor.execute(query, (function_name,))
    
    print(f"\n{'='*80}")
    print(f"WORKFLOWS CONTAINING: {function_name}")
    print(f"{'='*80}\n")
    
    results = cursor.fetchall()
    
    if not results:
        print(f"⚠ No workflows found containing '{function_name}'")
    else:
        for row in results:
            workflow_id, entry_point, wf_type, summary, step = row
            print(f"[Workflow #{workflow_id}] {entry_point} (Type: {wf_type})")
            print(f"  Step: {step}")
            print(f"  Summary: {summary[:150]}...")
            print()
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # Pre-compute and persist all workflows
    precompute_all_workflows()
    
    # Demo: Show which workflows contain specific functions
    print("\n" + "="*80)
    print("DEMO: RCA Query Examples")
    print("="*80)
    
    test_functions = ['check_order_velocity', 'calculate_retail_pricing', 'call_service']
    
    for func in test_functions:
        query_workflows_for_function(func)
