"""
Show COMPLETE END-TO-END workflows for ANY function (beginning, middle, or end)

Given a function name:
1. Find which workflows include it (backward traversal to entry points)
2. Show complete sequential flow for each workflow (forward from entry point)
"""
import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from db_config import DatabaseConfig
from typing import List, Set

db_config = DatabaseConfig()
conn = db_config.get_connection()

def get_main_workflow(entry_function: str):
    """Get the main sequential workflow based on call_order"""
    
    query = """
    SELECT 
        caller.name as from_func,
        caller.serviceName as from_service,
        r.relationshipType,
        r.call_order,
        callee.name as to_func,
        callee.serviceName as to_service
    FROM CodeNodes AS caller,
         Relationships AS r,
         CodeNodes AS callee
    WHERE MATCH(caller-(r)->callee)
      AND caller.name = ?
    ORDER BY r.call_order
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (entry_function,))
    return cursor.fetchall()

def find_entry_points_dynamic() -> List[str]:
    """Dynamically find all entry points (functions with no callers) in orchestrator"""
    
    query = """
    SELECT DISTINCT n.name
    FROM CodeNodes n
    WHERE n.serviceName = 'orchestrator'
      AND n.type = 'function'
      AND NOT EXISTS (
          SELECT 1 
          FROM CodeNodes AS caller,
               Relationships AS r,
               CodeNodes AS callee
          WHERE MATCH(caller-(r)->callee)
            AND callee.id = n.id
      )
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def find_workflows_containing_function(target_function: str) -> List[str]:
    """Find all entry points (workflows) that eventually call the target function"""
    
    # Get all entry points dynamically
    entry_points = find_entry_points_dynamic()
    
    matching = []
    for entry_point in entry_points:
        if check_if_function_in_workflow(entry_point, target_function):
            matching.append(entry_point)
    
    return matching

def check_if_function_in_workflow(workflow_entry: str, target_function: str) -> bool:
    """Check if target function appears anywhere in this workflow (up to 3-hop depth)"""
    
    # Get all functions called by workflow (1-hop, 2-hop, and 3-hop)
    query = """
    SELECT DISTINCT callee.name
    FROM CodeNodes AS caller,
         Relationships AS r,
         CodeNodes AS callee
    WHERE MATCH(caller-(r)->callee)
      AND caller.name = ?
    
    UNION
    
    SELECT DISTINCT c3.name
    FROM CodeNodes AS c1,
         Relationships AS r1,
         CodeNodes AS c2,
         Relationships AS r2,
         CodeNodes AS c3
    WHERE MATCH(c1-(r1)->c2-(r2)->c3)
      AND c1.name = ?
    
    UNION
    
    SELECT DISTINCT c4.name
    FROM CodeNodes AS c1,
         Relationships AS r1,
         CodeNodes AS c2,
         Relationships AS r2,
         CodeNodes AS c3,
         Relationships AS r3,
         CodeNodes AS c4
    WHERE MATCH(c1-(r1)->c2-(r2)->c3-(r3)->c4)
      AND c1.name = ?
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (workflow_entry, workflow_entry, workflow_entry))
    functions_in_workflow = [row[0] for row in cursor.fetchall()]
    return target_function in functions_in_workflow

def print_main_workflow(entry_point: str, highlight_function: str = None):
    print(f"\n🚀 {entry_point}")
    
    direct_calls = get_main_workflow(entry_point)
    
    if not direct_calls:
        print(f"   (No calls found)")
        return
    
    found_target = False
    
    for idx, call in enumerate(direct_calls, 1):
        indent = "  "
        relationship = call.relationshipType
        symbol = "📡" if relationship == "API_CALLS" else "↳"
        
        # Highlight if this is the target function
        highlight = " ⭐ [TARGET]" if call.to_func == highlight_function else ""
        found_target = found_target or (call.to_func == highlight_function)
        
        print(f"{indent}{idx}. {symbol} {call.to_func} [{call.to_service}]{highlight}")
        
        # If it's an API call to another service, show what that service does
        if relationship == "API_CALLS":
            sub_calls = get_main_workflow(call.to_func)
            for sub_idx, sub_call in enumerate(sub_calls, 1):
                sub_indent = "      "
                sub_highlight = " ⭐ [TARGET]" if sub_call.to_func == highlight_function else ""
                found_target = found_target or (sub_call.to_func == highlight_function)
                print(f"{sub_indent}{idx}.{sub_idx} ↳ {sub_call.to_func} [{sub_call.to_service}]{sub_highlight}")
    
    return found_target

# Get target function from command line or use default
import sys
if len(sys.argv) > 1:
    target_function = sys.argv[1]
else:
    target_function = "check_order_velocity"  # Default: common function

print("\n" + "="*100)
print(f"COMPLETE END-TO-END WORKFLOW DISCOVERY")
print("="*100)
print(f"Target Function: {target_function}")
print("Goal: Find ALL workflows that execute this function")
print("="*100)

# Dynamically discover all entry points
print("\n[Step 1] Discovering entry points from database...")
all_entry_points = find_entry_points_dynamic()
print(f"✓ Found {len(all_entry_points)} entry point(s): {', '.join(all_entry_points)}")

# Find which workflows include the target function
print(f"\n[Step 2] Checking which workflows execute '{target_function}'...")
matching_workflows = find_workflows_containing_function(target_function)

print(f"\n✓ Found {len(matching_workflows)} workflow(s) that execute '{target_function}':")
for wf in matching_workflows:
    print(f"  - {wf}")

# Show complete end-to-end flow for each matching workflow
for workflow in matching_workflows:
    print("\n" + "="*100)
    print(f"COMPLETE END-TO-END FLOW: {workflow}")
    print("="*100)
    print_main_workflow(workflow, highlight_function=target_function)

conn.close()

print("\n" + "="*100)
print("SUMMARY")
print("="*100)
print(f"✓ Function '{target_function}' found in {len(matching_workflows)} workflow(s)")
print("✓ Complete sequential execution paths shown above")
print("✓ API calls (📡) trigger sub-workflows in other microservices")
print("✓ Target function marked with ⭐ [TARGET]")
print("\nUsage: python show_sequential_workflows.py <function_name>")
print("Example: python show_sequential_workflows.py calculate_institutional_pricing")
print("="*100)
