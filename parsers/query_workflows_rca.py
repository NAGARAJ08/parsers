"""
Query pre-computed workflows for RCA (Root Cause Analysis).
This script demonstrates how to use the pre-computed workflow data for fast RCA.

Usage examples:
    python query_workflows_rca.py --function check_order_velocity
    python query_workflows_rca.py --workflow place_order
    python query_workflows_rca.py --service risk_service
"""

import pyodbc
import json
import argparse
from typing import List, Dict
from db_config import DatabaseConfig

def get_connection():
    """Get database connection"""
    config = DatabaseConfig()
    return config.get_connection()


def query_workflows_by_function(function_name: str):
    """
    RCA Query 1: Find all workflows that contain a specific function.
    Use case: "check_order_velocity is failing, which workflows are affected?"
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT
        wc.workflow_id,
        wc.entry_point_name,
        wc.workflow_type,
        wc.total_steps,
        wc.services_involved,
        wf.step_order,
        wf.function_summary,
        wf.data_contracts
    FROM WorkflowCatalog wc
    JOIN WorkflowFunctions wf ON wc.workflow_id = wf.workflow_id
    WHERE wf.function_name = ?
    ORDER BY wc.workflow_type, wc.entry_point_name
    """
    
    cursor.execute(query, (function_name,))
    results = cursor.fetchall()
    
    print(f"\n{'='*100}")
    print(f"RCA QUERY: Workflows containing function '{function_name}'")
    print(f"{'='*100}\n")
    
    if not results:
        print(f"⚠ No workflows found containing '{function_name}'\n")
        cursor.close()
        conn.close()
        return
    
    print(f"✓ Found {len(results)} workflow(s) affected\n")
    
    for row in results:
        workflow_id, entry_point, wf_type, total_steps, services, step_order, summary, data_contracts_json = row
        
        print(f"[Workflow #{workflow_id}] {entry_point}")
        print(f"  Type: {wf_type}")
        print(f"  Total Steps: {total_steps}")
        print(f"  Services Involved: {services}")
        print(f"  '{function_name}' at Step: {step_order}/{total_steps}")
        print(f"  Function Summary: {summary}")
        
        # Parse and display data contracts (point 4 implementation)
        if data_contracts_json:
            try:
                contracts = json.loads(data_contracts_json)
                if contracts.get('parameters'):
                    print(f"  Parameters: {', '.join(contracts['parameters'][:5])}")  # First 5 params
                if contracts.get('fields_accessed'):
                    print(f"  Fields Accessed: {', '.join(contracts['fields_accessed'][:5])}")
            except:
                pass
        
        print()
    
    cursor.close()
    conn.close()


def query_workflow_details(function_name: str):
    """
    RCA Query 2: Get complete workflow details for ANY function (entry point, middle, or end).
    Use case: "Show me the complete workflow containing check_order_velocity"
    
    If the function is an entry point, shows that workflow.
    If the function is in the middle/end, finds all workflows containing it and shows them.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # First, try to find if this function is an entry point
    workflow_query = """
    SELECT 
        workflow_id,
        entry_point_name,
        workflow_type,
        full_route,
        workflow_summary,
        total_steps,
        services_involved
    FROM WorkflowCatalog
    WHERE entry_point_name = ?
    """
    
    cursor.execute(workflow_query, (function_name,))
    workflow = cursor.fetchone()
    
    # If not an entry point, find workflows containing this function
    if not workflow:
        print(f"\n'{function_name}' is not an entry point. Searching for workflows containing it...\n")
        
        # Find all workflows that contain this function
        find_workflows_query = """
        SELECT DISTINCT
            wc.workflow_id,
            wc.entry_point_name,
            wc.workflow_type,
            wc.full_route,
            wc.workflow_summary,
            wc.total_steps,
            wc.services_involved
        FROM WorkflowCatalog wc
        JOIN WorkflowFunctions wf ON wc.workflow_id = wf.workflow_id
        WHERE wf.function_name = ?
        ORDER BY wc.workflow_type, wc.entry_point_name
        """
        
        cursor.execute(find_workflows_query, (function_name,))
        workflows = cursor.fetchall()
        
        if not workflows:
            print(f"⚠ No workflows found containing '{function_name}'\n")
            cursor.close()
            conn.close()
            return
        
        print(f"✓ Found {len(workflows)} workflow(s) containing '{function_name}'\n")
        
        # Show details for each workflow
        for workflow in workflows:
            workflow_id, name, wf_type, route_json, summary, total_steps, services = workflow
            route = json.loads(route_json)
            _display_workflow_details(cursor, workflow_id, name, wf_type, route, summary, total_steps, services, function_name)
        
        cursor.close()
        conn.close()
        return
    
    # If it IS an entry point, show that workflow
    workflow_id, name, wf_type, route_json, summary, total_steps, services = workflow
    route = json.loads(route_json)
    
    _display_workflow_details(cursor, workflow_id, name, wf_type, route, summary, total_steps, services, function_name)
    
    cursor.close()
    conn.close()


def _display_workflow_details(cursor, workflow_id, name, wf_type, route, summary, total_steps, services, highlight_function=None):
    """Helper function to display workflow details"""
    
    print(f"\n{'='*100}")
    print(f"WORKFLOW DETAILS: {name} (Type: {wf_type})")
    print(f"{'='*100}\n")
    
    print(f"Summary: {summary}")
    print(f"Total Steps: {total_steps}")
    print(f"Services Involved: {services}\n")
    
    # Get all functions in the workflow
    functions_query = """
    SELECT 
        step_order,
        function_name,
        service_name,
        function_summary,
        data_contracts
    FROM WorkflowFunctions
    WHERE workflow_id = ?
    ORDER BY step_order
    """
    
    cursor.execute(functions_query, (workflow_id,))
    functions = cursor.fetchall()
    
    print(f"Execution Path ({len(functions)} steps):")
    print("-" * 100)
    
    for row in functions:
        step, func_name, service, func_summary, data_contracts_json = row
        
        # Highlight if this is the target function
        highlight = " ⭐ [TARGET]" if func_name == highlight_function else ""
        
        print(f"\n[Step {step}] {func_name} ({service}){highlight}")
        print(f"  Summary: {func_summary}")
        
        # Show data contracts
        if data_contracts_json:
            try:
                contracts = json.loads(data_contracts_json)
                if contracts.get('parameters'):
                    params = contracts['parameters'][:3]  # First 3 params
                    if params:
                        print(f"  Params: {', '.join(params)}")
                if contracts.get('return_type') and contracts['return_type']:
                    print(f"  Returns: {contracts['return_type']}")
                if contracts.get('fields_accessed'):
                    fields = contracts['fields_accessed'][:5]
                    if fields:
                        print(f"  Accesses Fields: {', '.join(fields)}")
            except:
                pass
    
    print()


def query_workflows_by_service(service_name: str):
    """
    RCA Query 3: Find all workflows that involve a specific service.
    Use case: "risk_service is down, which workflows are impacted?"
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT
        wc.workflow_id,
        wc.entry_point_name,
        wc.workflow_type,
        wc.total_steps,
        wc.services_involved
    FROM WorkflowCatalog wc
    WHERE wc.services_involved LIKE '%' + ? + '%'
    ORDER BY wc.workflow_type, wc.entry_point_name
    """
    
    cursor.execute(query, (service_name,))
    results = cursor.fetchall()
    
    print(f"\n{'='*100}")
    print(f"RCA QUERY: Workflows involving service '{service_name}'")
    print(f"{'='*100}\n")
    
    if not results:
        print(f"⚠ No workflows found involving '{service_name}'\n")
        cursor.close()
        conn.close()
        return
    
    print(f"✓ Found {len(results)} workflow(s) impacted\n")
    
    for row in results:
        workflow_id, entry_point, wf_type, total_steps, services = row
        print(f"[Workflow #{workflow_id}] {entry_point} (Type: {wf_type})")
        print(f"  Total Steps: {total_steps}")
        print(f"  All Services: {services}\n")
    
    cursor.close()
    conn.close()


def list_all_workflows():
    """List all pre-computed workflows in the catalog."""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        workflow_id,
        entry_point_name,
        workflow_type,
        total_steps,
        services_involved
    FROM WorkflowCatalog
    ORDER BY workflow_type, entry_point_name
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\n{'='*100}")
    print(f"ALL PRE-COMPUTED WORKFLOWS")
    print(f"{'='*100}\n")
    
    for row in results:
        workflow_id, entry_point, wf_type, total_steps, services = row
        print(f"[#{workflow_id}] {entry_point} (Type: {wf_type})")
        print(f"  Steps: {total_steps}, Services: {services}\n")
    
    print(f"Total Workflows: {len(results)}\n")
    
    cursor.close()
    conn.close()


def get_context_for_copilot(function_name: str) -> str:
    """
    Generate context for GitHub Copilot (or any LLM) for RCA.
    This is the key use case: provide pre-computed context without API calls.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get workflows containing the function
    query = """
    SELECT 
        wc.entry_point_name,
        wc.workflow_type,
        wc.workflow_summary,
        wc.full_route,
        wf.step_order,
        wf.function_summary,
        wf.data_contracts
    FROM WorkflowCatalog wc
    JOIN WorkflowFunctions wf ON wc.workflow_id = wf.workflow_id
    WHERE wf.function_name = ?
    ORDER BY wc.workflow_type
    """
    
    cursor.execute(query, (function_name,))
    results = cursor.fetchall()
    
    if not results:
        cursor.close()
        conn.close()
        return f"No workflows found containing function '{function_name}'"
    
    # Build context string (plain text, no markdown)
    context = f"RCA CONTEXT FOR FUNCTION: {function_name}\n"
    context += "="*80 + "\n\n"
    context += f"AFFECTED WORKFLOWS: {len(results)}\n\n"
    
    for idx, row in enumerate(results, 1):
        entry_point, wf_type, summary, route_json, step_order, func_summary, data_contracts_json = row
        route = json.loads(route_json)
        
        context += f"[{idx}] Workflow: {entry_point} (Type: {wf_type})\n"
        context += f"    Summary: {summary[:200]}...\n"
        context += f"    Function Position: Step {step_order}/{len(route)}\n"
        context += f"    Function Role: {func_summary}\n"
        
        # Add data contract info
        if data_contracts_json:
            try:
                contracts = json.loads(data_contracts_json)
                if contracts.get('parameters'):
                    context += f"    Parameters: {', '.join(contracts['parameters'][:5])}\n"
                if contracts.get('fields_accessed'):
                    context += f"    Fields Accessed: {', '.join(contracts['fields_accessed'][:5])}\n"
            except:
                pass
        
        # Show 2 steps before and after (for context)
        start_idx = max(0, step_order - 3)
        end_idx = min(len(route), step_order + 2)
        context_path = route[start_idx:end_idx]
        
        context += f"    Execution Context: {' -> '.join(context_path)}\n"
        context += "\n"
    
    cursor.close()
    conn.close()
    
    return context


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Query pre-computed workflows for RCA')
    parser.add_argument('--function', type=str, help='Find workflows containing this function')
    parser.add_argument('--workflow', type=str, help='Get complete details of this workflow')
    parser.add_argument('--service', type=str, help='Find workflows involving this service')
    parser.add_argument('--list', action='store_true', help='List all workflows')
    parser.add_argument('--copilot', type=str, help='Generate GitHub Copilot context for this function')
    
    args = parser.parse_args()
    
    if args.function:
        query_workflows_by_function(args.function)
    elif args.workflow:
        query_workflow_details(args.workflow)
    elif args.service:
        query_workflows_by_service(args.service)
    elif args.list:
        list_all_workflows()
    elif args.copilot:
        context = get_context_for_copilot(args.copilot)
        print(context)
        
        # Save to file for easy copy-paste to GitHub Copilot
        with open(f'copilot_context_{args.copilot}.txt', 'w', encoding='utf-8') as f:
            f.write(context)
        print(f"\n✓ Context saved to: copilot_context_{args.copilot}.txt")
    else:
        # Default: show examples
        print("\n" + "="*100)
        print("RCA WORKFLOW QUERY SYSTEM - Usage Examples")
        print("="*100 + "\n")
        
        print("1. Find workflows containing a function (RCA for function failures):")
        print("   python query_workflows_rca.py --function check_order_velocity\n")
        
        print("2. Get complete workflow details (works with ANY function - entry, middle, or end):")
        print("   python query_workflows_rca.py --workflow place_order")
        print("   python query_workflows_rca.py --workflow check_order_velocity\n")
        
        print("3. Find workflows impacted by a service outage:")
        print("   python query_workflows_rca.py --service risk_service\n")
        
        print("4. List all pre-computed workflows:")
        print("   python query_workflows_rca.py --list\n")
        
        print("5. Generate GitHub Copilot context for RCA:")
        print("   python query_workflows_rca.py --copilot check_order_velocity\n")
        
        print("\nRunning demo queries...\n")
        
        # Demo queries (commented out - user will run with specific args)
        # query_workflows_by_function('check_order_velocity')
        # query_workflow_details('check_order_velocity')
