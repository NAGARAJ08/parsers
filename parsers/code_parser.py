import ast
import json
from pathlib import Path
from datetime import datetime
from db_config import DatabaseConfig


class CodeParser:

    def __init__(self, use_database=True):
        self.code_nodes = []
        self.relationships = []
        self.use_database = use_database
        self.db_config = DatabaseConfig() if use_database else None
        self.node_id_map = {}  # Map node names to database IDs

    def parse_microservices(self, services_dir):
        service_path = Path(services_dir)
        for service_dir in service_path.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith('.'):
                service_name = service_dir.name
                self.parse_service(str(service_dir), service_name)
        
        # Store in database if enabled
        if self.use_database:
            self.store_to_database()

    def parse_service(self, service_dir, service_name):
        service_path = Path(service_dir)
        src_dir = service_path / 'src'
        if src_dir.exists():
            for py_file in src_dir.rglob("*.py"):
                self.parse_file(py_file, service_name)

    def parse_file(self, filepath, service_name):
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                source = file.read()

            tree = ast.parse(source, filename=str(filepath))

            # track current class context for methods
            current_class = None

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    self.extract_function(node, filepath, service_name, source, current_class)
                elif isinstance(node, ast.ClassDef):
                    current_class = node.name
                    self.extract_class(node, filepath, service_name, source)
                    current_class = None

        except Exception:
            pass  # Ignore files that can't be parsed

    def extract_function(self, node, filepath, service_name, source, class_name):
        function_name = node.name
        
        # Skip utility/infrastructure functions - only parse business logic
        utility_functions = {
            'JsonFormatter', 'format', 'get_trace_logger', 'get_trace_id',
            'TraceFilter', 'filter', '__init__', '__str__', '__repr__',
            'health_check', 'root'
        }
        if function_name in utility_functions:
            return

        if class_name:
            full_name = f"{class_name}.{function_name}"
            code_type = "method"
        else:
            full_name = function_name
            code_type = "function"

        summary = ast.get_docstring(node) or f"{code_type.capitalize()} in {service_name}"
        if summary:
            summary = summary.split('\n')[0]

        try:
            start_line = node.lineno
            end_line = node.end_lineno
            lines = source.split('\n')
            snippet = "\n".join(lines[start_line - 1:end_line])
        except Exception:
            snippet = f"def {function_name}(...): pass"

        code_node = {
            "name": full_name,
            "type": code_type,
            "serviceName": service_name,
            "filePath": str(filepath),
            "summary": summary,
            "snippet": snippet
        }
        self.code_nodes.append(code_node)

        if class_name:
            relationship = {
                "sourceName": class_name,
                "sourceType": "class",
                "sourceService": service_name,
                "targetName": full_name,
                "targetType": code_type,
                "targetService": service_name,
                "relationshipType": "CONTAINS",
                "description": f"{class_name} contains {full_name}",
                "timestamp": datetime.now()
            }
            self.relationships.append(relationship)

        self.extract_function_calls(node, full_name, service_name)

    def extract_class(self, node, filepath, service_name, source):
        class_name = node.name
        
        # Skip utility/infrastructure classes - only parse business domain classes
        utility_classes = {
            'JsonFormatter', 'TraceFilter'
        }
        if class_name in utility_classes:
            return
        
        summary = ast.get_docstring(node) or f"Class in {service_name}"

        if summary:
            summary = summary.split('\n')[0]

        try:
            start_line = node.lineno
            end_line = node.end_lineno
            lines = source.split('\n')
            snippet = "\n".join(lines[start_line - 1:end_line])
        except Exception:
            snippet = f"class {class_name}(...): pass"

        code_node = {
            "name": class_name,
            "type": "class",
            "serviceName": service_name,
            "filePath": str(filepath),
            "summary": summary,
            "snippet": snippet
        }
        self.code_nodes.append(code_node)

    def extract_function_calls(self, node, source_name, source_service):
        for node in ast.walk(node):
            if isinstance(node, ast.Call):
                callee_name = None

                if isinstance(node.func, ast.Name):
                    callee_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    callee_name = node.func.attr

                if callee_name:
                    relationship = {
                        "sourceName": source_name,
                        "sourceType": "function",
                        "sourceService": source_service,
                        "targetName": callee_name,
                        "targetType": "function",
                        "targetService": source_service,
                        "relationshipType": "CALLS",
                        "description": f"{source_name} calls {callee_name}",
                        "timestamp": datetime.now()
                    }
                    self.relationships.append(relationship)

    def export_to_json_for_debugging(self, json_path="code_data.json"):
        data = {
            "codeNodes": self.code_nodes,
            "relationships": self.relationships
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, indent=4)

    def store_to_database(self):
        """Store parsed code nodes and relationships to SQL Server Graph database"""
        if not self.use_database or not self.db_config:
            print("Database storage not enabled")
            return

        conn = self.db_config.get_connection()
        cursor = conn.cursor()

        try:
            # Insert CodeNodes
            print(f"Inserting {len(self.code_nodes)} code nodes...")
            for node in self.code_nodes:
                # Insert node (wrap 'type' in brackets as it's a reserved keyword)
                cursor.execute("""
                    INSERT INTO CodeNodes (name, [type], summary, snippet, filePath, serviceName)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    node['name'],
                    node['type'],
                    node.get('summary', ''),
                    node.get('snippet', ''),
                    node.get('filePath', ''),
                    node.get('serviceName', '')
                ))
                
                # Get the $node_id after insert
                cursor.execute("""
                    SELECT TOP 1 $node_id, id 
                    FROM CodeNodes 
                    WHERE name = ? AND [type] = ? AND serviceName = ?
                    ORDER BY id DESC
                """, (node['name'], node['type'], node.get('serviceName', '')))
                
                result = cursor.fetchone()
                if result:
                    node_id_db, id_value = result
                    # Store mapping from node name to database $node_id
                    self.node_id_map[node['name']] = node_id_db
                    print(f"  Inserted {node['type']}: {node['name']} (id={id_value})")

            # Insert Relationships
            print(f"\nInserting {len(self.relationships)} relationships...")
            for rel in self.relationships:
                source_name = rel.get('sourceName') or rel.get('source')
                target_name = rel.get('targetName') or rel.get('target')
                rel_type = rel.get('relationshipType') or rel.get('type')
                
                if source_name not in self.node_id_map or target_name not in self.node_id_map:
                    print(f"  Skipping relationship {source_name} -> {target_name}: Node not found")
                    continue

                source_node_id = self.node_id_map[source_name]
                target_node_id = self.node_id_map[target_name]

                cursor.execute("""
                    INSERT INTO Relationships (relationshipType, description, timestamp, $from_id, $to_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    rel_type,
                    rel.get('description', ''),
                    datetime.now().isoformat(),
                    source_node_id,
                    target_node_id
                ))
                print(f"  Created {rel_type}: {source_name} -> {target_name}")

            conn.commit()
            print(f"\n✅ Successfully stored {len(self.code_nodes)} nodes and {len(self.relationships)} relationships")

        except Exception as e:
            conn.rollback()
            print(f"❌ Error storing to database: {e}")
            raise
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    parser = CodeParser()
    parser.parse_microservices(
        "C:/Users/K197227/Downloads/sample_services/services/services"
    )
    print("Code Nodes:")
