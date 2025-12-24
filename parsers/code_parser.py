import ast
import json
import re
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any
from db_config import DatabaseConfig


class CodeParser:

    def __init__(self, config_path: str = "parser_config.yaml", use_database: bool = True):
        self.config = self.load_config(config_path)
        self.code_nodes = []
        self.relationships = []
        self.use_database = use_database
        self.db_config = DatabaseConfig() if use_database else None
        self.node_id_map = {}
        
        self.service_urls = {}
        self.api_endpoints = {}
        
        self.skip_functions = set(self.config['parsing']['skip_functions'])
        self.skip_classes = set(self.config['parsing']['skip_classes'])
        self.skip_base_classes = set(self.config['parsing']['skip_base_classes'])
        
        self.web_decorators = self._build_web_decorators()
        
        self.http_client_functions = set(
            self.config['service_communication']['http_client_functions']['function_names']
        ) if self.config['service_communication']['http_client_functions']['enabled'] else set()

    def load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Config file '{config_path}' not found. Using defaults.")
            return self._default_config()

    def _default_config(self) -> Dict[str, Any]:
        return {
            'parsing': {
                'skip_functions': ['__init__', '__str__', '__repr__'],
                'skip_classes': [],
                'skip_base_classes': ['Enum', 'BaseModel']
            },
            'web_frameworks': {
                'fastapi': {'enabled': True, 'decorators': ['app.get', 'app.post', 'app.put', 'app.delete']},
                'flask': {'enabled': True, 'decorators': ['app.route']},
            },
            'service_communication': {
                'url_constants': {'enabled': True, 'patterns': ['*_SERVICE_URL', '*_URL']},
                'http_client_functions': {'enabled': True, 'function_names': ['call_service', 'requests.get', 'requests.post']}
            },
            'output': {'verbose': True}
        }

    def _build_web_decorators(self) -> Set[str]:
        decorators = set()
        frameworks = self.config['web_frameworks']
        
        for framework, settings in frameworks.items():
            if settings.get('enabled', False):
                decorators.update(settings.get('decorators', []))
        
        return decorators

    def parse_project(self, project_dir: str):
        project_path = Path(project_dir)
        
        if not project_path.exists():
            raise ValueError(f"Project directory not found: {project_dir}")
        
        print(f"\n{'='*80}")
        print(f" Parsing Project: {project_path.name}")
        print(f"{'='*80}\n")
        
        print("=== PASS 1: Parsing code and building endpoint registry ===")
        for py_file in self._find_python_files(project_path):
            service_name = self._extract_service_name(py_file, project_path)
            if self.config['output'].get('verbose'):
                print(f"\n📄 {py_file.name} ({service_name})")
            self.parse_file(py_file, service_name)
        
        print(f"\n Found {len(self.code_nodes)} code nodes")
        print(f" Found {len(self.api_endpoints)} API endpoints")
        
        print("\n=== PASS 2: Mapping API calls to endpoints ===")
        self.map_api_calls_to_endpoints()
        
        print("\n=== PASS 3: Cleaning up relationships ===")
        self.cleanup_relationships()
        
        print("\n=== PASS 4: Validating relationships ===")
        self.validate_relationships()
        
        if self.use_database:
            self.store_to_database()

    def _find_python_files(self, project_path: Path) -> List[Path]:
        include_patterns = self.config['parsing'].get('include_patterns', ['**/*.py'])
        exclude_patterns = self.config['parsing'].get('exclude_patterns', [])
        
        all_files = []
        for pattern in include_patterns:
            all_files.extend(project_path.glob(pattern))
        
        filtered_files = []
        for file in all_files:
            exclude = False
            for exclude_pattern in exclude_patterns:
                if file.match(exclude_pattern):
                    exclude = True
                    break
            if not exclude:
                filtered_files.append(file)
        
        return filtered_files

    def _extract_service_name(self, filepath: Path, project_root: Path) -> str:
        try:
            rel_path = filepath.relative_to(project_root)
            return rel_path.parts[0] if len(rel_path.parts) > 1 else "main"
        except:
            return "unknown"

    def parse_file(self, filepath: Path, service_name: str):
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                source = file.read()

            tree = ast.parse(source, filename=str(filepath))

            if self.config['service_communication']['url_constants']['enabled']:
                self.extract_service_urls(tree, service_name)

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    self.extract_function(node, filepath, service_name, source, tree, None)
                elif isinstance(node, ast.ClassDef):
                    if not self._should_skip_class(node):
                        self.extract_class(node, filepath, service_name, source)

        except Exception as e:
            if self.config['output'].get('verbose'):
                print(f"  Warning: Could not parse {filepath.name}: {e}")

    def _should_skip_class(self, node: ast.ClassDef) -> bool:
        if node.name in self.skip_classes:
            return True
        
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id in self.skip_base_classes:
                return True
        
        return False

    def extract_service_urls(self, tree: ast.AST, service_name: str):
        url_patterns = self.config['service_communication']['url_constants']['patterns']
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_name = target.id
                        
                        for pattern in url_patterns:
                            if self._matches_pattern(var_name, pattern):
                                if isinstance(node.value, ast.Constant):
                                    url = node.value.value
                                    service_key = var_name.lower()
                                    for suffix in ['_service_url', '_url', '_endpoint', '_base_url']:
                                        service_key = service_key.replace(suffix, '')
                                    
                                    self.service_urls[service_key] = url
                                    if self.config['output'].get('verbose'):
                                        print(f"  Found service URL: {service_key} = {url}")
                                break

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        regex_pattern = pattern.replace('*', '.*')
        return re.match(f"^{regex_pattern}$", name) is not None

    def extract_function_parameters(self, node: ast.FunctionDef) -> Optional[str]:
        params = []
        for arg in node.args.args:
            if arg.arg not in {'self', 'request', 'x_trace_id', 'cls'}:
                params.append(arg.arg)
        return ', '.join(params) if params else None

    def extract_api_decorator(self, node: ast.FunctionDef) -> tuple:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Attribute):
                    decorator_name = f"{decorator.func.value.id}.{decorator.func.attr}" if isinstance(decorator.func.value, ast.Name) else None
                    
                    if decorator_name and any(decorator_name.startswith(pattern.split('.')[0]) for pattern in self.web_decorators):
                        method = decorator.func.attr.upper()
                        
                        if decorator.args and isinstance(decorator.args[0], ast.Constant):
                            endpoint = decorator.args[0].value
                            return method, endpoint
        
        return None, None

    def extract_function(self, node: ast.FunctionDef, filepath: Path, service_name: str, 
                        source: str, tree: ast.AST, class_name: Optional[str]):
        function_name = node.name
        
        if function_name in self.skip_functions:
            return

        parameters = self.extract_function_parameters(node)

        api_method, api_endpoint = self.extract_api_decorator(node)

        full_name = f"{class_name}.{function_name}" if class_name else function_name
        code_type = "method" if class_name else "function"

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
            "parameters": parameters,
            "api_endpoint": api_endpoint,
            "api_method": api_method,
            "serviceName": service_name,
            "filePath": str(filepath),
            "summary": summary,
            "snippet": snippet
        }
        self.code_nodes.append(code_node)

        if api_endpoint:
            self.api_endpoints[api_endpoint] = {
                'service': service_name,
                'function': full_name,
                'method': api_method
            }
            if self.config['output'].get('verbose'):
                print(f"  Registered API: {api_method} {api_endpoint} -> {service_name}.{full_name}")

        if api_endpoint:
            self.relationships.append({
                "sourceName": api_endpoint,
                "sourceType": "endpoint",
                "sourceService": service_name,
                "targetName": full_name,
                "targetType": code_type,
                "targetService": service_name,
                "relationshipType": "EXPOSES",
                "description": f"Endpoint {api_endpoint} exposes {full_name}",
                "timestamp": datetime.now()
            })

        self.extract_local_calls(node, full_name, service_name)
        self.extract_api_calls(node, full_name, service_name)

    def extract_class(self, node: ast.ClassDef, filepath: Path, service_name: str, source: str):
        summary = ast.get_docstring(node) or f"Class in {service_name}"
        if summary:
            summary = summary.split('\n')[0]

        try:
            start_line = node.lineno
            end_line = node.end_lineno
            lines = source.split('\n')
            snippet = "\n".join(lines[start_line - 1:end_line])
        except Exception:
            snippet = f"class {node.name}(...): pass"

        code_node = {
            "name": node.name,
            "type": "class",
            "parameters": None,
            "api_endpoint": None,
            "api_method": None,
            "serviceName": service_name,
            "filePath": str(filepath),
            "summary": summary,
            "snippet": snippet
        }
        self.code_nodes.append(code_node)

    def extract_local_calls(self, node: ast.FunctionDef, source_name: str, source_service: str):
        call_order = 0
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                callee_name = None

                if isinstance(child.func, ast.Name):
                    callee_name = child.func.id
                elif isinstance(child.func, ast.Attribute):
                    callee_name = child.func.attr

                skip_functions = self.http_client_functions | {
                    'get_trace_id', 'get_trace_logger', 'info', 'debug', 'warning', 
                    'error', 'exception', 'log', 'append', 'get', 'format', 'dumps',
                    'loads', 'isoformat', 'now', 'sleep', 'round', 'abs', 'min', 'max'
                }
                
                if callee_name and callee_name not in skip_functions:
                    call_order += 1
                    self.relationships.append({
                        "sourceName": source_name,
                        "sourceType": "function",
                        "sourceService": source_service,
                        "targetName": callee_name,
                        "targetType": "function",
                        "targetService": source_service,
                        "relationshipType": "CALLS",
                        "description": f"{source_name} calls {callee_name}",
                        "call_order": call_order,
                        "line_number": child.lineno if hasattr(child, 'lineno') else None,
                        "timestamp": datetime.now()
                    })

    def extract_api_calls(self, node: ast.FunctionDef, source_name: str, source_service: str):
        if not self.config['service_communication']['http_client_functions']['enabled']:
            return

        call_order = 0
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                func_name = self._get_func_name(child)
                
                if func_name == 'call_service':
                    call_order += 1
                    url_arg = child.args[0] if child.args else None
                    
                    if url_arg:
                        url_pattern = self._extract_url_from_arg(url_arg)
                        
                        if url_pattern:
                            service_key, endpoint = self._parse_service_url(url_pattern)
                            
                            if self.config['output'].get('verbose'):
                                print(f"     API Call: {source_name} -> {endpoint}")
                            
                            self.relationships.append({
                                "sourceName": source_name,
                                "sourceType": "function",
                                "sourceService": source_service,
                                "targetName": None,
                                "targetType": "function",
                                "targetService": service_key,
                                "relationshipType": "API_CALLS",
                                "description": f"{source_name} makes API call to {endpoint}",
                                "url_pattern": url_pattern,
                                "endpoint": endpoint,
                                "call_order": call_order,
                                "line_number": child.lineno if hasattr(child, 'lineno') else None,
                                "timestamp": datetime.now()
                            })

    def _get_func_name(self, call_node: ast.Call) -> Optional[str]:
        if isinstance(call_node.func, ast.Name):
            return call_node.func.id
        elif isinstance(call_node.func, ast.Attribute):
            if isinstance(call_node.func.value, ast.Name):
                return f"{call_node.func.value.id}.{call_node.func.attr}"
            return call_node.func.attr
        return None

    def _extract_url_from_arg(self, url_arg) -> Optional[str]:
        if isinstance(url_arg, ast.JoinedStr):
            url_parts = []
            for value in url_arg.values:
                if isinstance(value, ast.Constant):
                    url_parts.append(value.value)
                elif isinstance(value, ast.FormattedValue):
                    if isinstance(value.value, ast.Name):
                        url_parts.append(f"{{{value.value.id}}}")
            return ''.join(url_parts)
        
        elif isinstance(url_arg, ast.Constant):
            return url_arg.value
        
        elif isinstance(url_arg, ast.BinOp) and isinstance(url_arg.op, ast.Add):
            left = self._extract_url_from_arg(url_arg.left)
            right = self._extract_url_from_arg(url_arg.right)
            if left and right:
                return left + right
        
        return None

    def _parse_service_url(self, url_pattern: str) -> tuple[Optional[str], Optional[str]]:
        match = re.match(r'\{(\w+)_SERVICE_URL\}(/.*)', url_pattern)
        if match:
            service_var = match.group(1).lower()
            endpoint = match.group(2)
            return service_var, endpoint
        
        match = re.match(r'\{(\w+)_URL\}(/.*)', url_pattern)
        if match:
            service_var = match.group(1).lower()
            endpoint = match.group(2)
            return service_var, endpoint
        
        match = re.match(r'https?://[^/]+(/.*)', url_pattern)
        if match:
            endpoint = match.group(1)
            for service_key, url in self.service_urls.items():
                if url in url_pattern:
                    return service_key, endpoint
            return None, endpoint
        
        return None, None

    def map_api_calls_to_endpoints(self):
        api_call_count = sum(1 for r in self.relationships if r.get('relationshipType') == 'API_CALLS')
        print(f"\nResolving {api_call_count} API calls...")
        
        mapped_count = 0
        unmapped_count = 0
        
        for rel in self.relationships:
            if rel.get('relationshipType') == 'API_CALLS':
                endpoint = rel.get('endpoint')
                
                if endpoint and endpoint in self.api_endpoints:
                    target_info = self.api_endpoints[endpoint]
                    rel['targetName'] = target_info['function']
                    rel['targetService'] = target_info['service']
                    rel['description'] = f"{rel['sourceName']} calls {endpoint} -> {target_info['function']}"
                    mapped_count += 1
                    
                    if self.config['output'].get('verbose'):
                        print(f"   {rel['sourceName']} -> {endpoint} -> {target_info['function']}")
                else:
                    unmapped_count += 1
                    if self.config['output'].get('verbose'):
                        print(f"   {rel['sourceName']} -> {endpoint} (NOT FOUND)")
        
        print(f"  Mapped: {mapped_count}, Unmapped: {unmapped_count}")

    def cleanup_relationships(self):
        valid_functions = {node['name'] for node in self.code_nodes if node['type'] in ['function', 'method']}
        
        utility_functions = {
            'get_trace_id', 'get_trace_logger', 'TraceFilter', 'filter',
            'info', 'debug', 'warning', 'error', 'exception', 'log', 'critical',
            'format', 'JsonFormatter', 'dumps', 'loads', 'json',
            'get_connection', 'cursor', 'commit', 'rollback', 'close', 'execute', 'fetchone',
            'now', 'isoformat', 'strftime', 'strptime', 'utcnow',
            'str', 'int', 'float', 'len', 'type', 'isinstance', 'append', 'get', 
            'split', 'join', 'replace', 'lower', 'upper', 'strip',
            'round', 'abs', 'min', 'max', 'sum', 'sorted',
            'HTTPException', 'raise_for_status', 'post', 'put', 'delete',
            'BaseModel', 'Field', 'OrderResponse', 'OrderRequest',
            'TradeValidationResponse', 'TradeExecutionResponse',
            'RiskAssessmentResponse', 'PricingResponse',
            'OrderType', 'RiskLevel', 'Enum',
            'sleep', 'time',
            'ast', 'walk', 'parse'
        }
        
        cleaned_relationships = []
        seen = set()
        removed_utility = 0
        removed_invalid = 0
        removed_duplicate = 0
        
        for rel in self.relationships:
            target_name = rel.get('targetName')
            source_name = rel.get('sourceName')
            rel_type = rel.get('relationshipType')
            
            if not target_name:
                continue
            
            if target_name in utility_functions:
                removed_utility += 1
                continue
            
            if rel_type == 'CALLS':
                if target_name not in valid_functions:
                    removed_invalid += 1
                    continue
            
            key = (source_name, target_name, rel_type, rel.get('sourceService'), rel.get('targetService'))
            if key in seen:
                removed_duplicate += 1
                continue
            seen.add(key)
            
            cleaned_relationships.append(rel)
        
        old_count = len(self.relationships)
        self.relationships = cleaned_relationships
        
        print(f"  Cleaned relationships: {old_count} → {len(cleaned_relationships)}")
        print(f"    - Removed {removed_utility} utility function calls")
        print(f"    - Removed {removed_invalid} invalid targets")
        print(f"    - Removed {removed_duplicate} duplicates")

    def validate_relationships(self):
        print("\n=== Relationship Validation ===")
        
        valid_nodes = {node['name'] for node in self.code_nodes}
        
        by_type = {}
        invalid = []
        
        for rel in self.relationships:
            rel_type = rel.get('relationshipType')
            by_type[rel_type] = by_type.get(rel_type, 0) + 1
            
            target = rel.get('targetName')
            source = rel.get('sourceName')
            
            if source not in valid_nodes and not rel.get('sourceType') == 'endpoint':
                invalid.append(f"Source not found: {source}")
            
            if target and target not in valid_nodes:
                invalid.append(f"Target not found: {target} (from {source})")
        
        print(f"\nRelationship breakdown:")
        for rel_type, count in sorted(by_type.items()):
            print(f"  {rel_type}: {count}")
        
        if invalid:
            print(f"\n  Found {len(invalid)} invalid relationships:")
            for msg in invalid[:10]:
                print(f"  - {msg}")
        else:
            print(f"\n All relationships valid!")

    def export_to_json(self, json_path: Optional[str] = None):
        if not self.config['output'].get('json_export', True):
            return
        
        json_path = json_path or self.config['output'].get('json_filename', 'code_graph.json')
        
        data = {
            "project": self.config['project'],
            "codeNodes": self.code_nodes,
            "relationships": self.relationships,
            "serviceUrls": self.service_urls,
            "apiEndpoints": self.api_endpoints
        }
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, indent=4)
        
        if self.config['output'].get('verbose'):
            print(f"\n[OK] Exported to {json_path}")

    def store_to_database(self):
        if not self.use_database or not self.db_config:
            print("Database storage not enabled")
            return

        conn = self.db_config.get_connection()
        cursor = conn.cursor()

        try:
            print(f"\n=== Inserting {len(self.code_nodes)} code nodes ===")
            for node in self.code_nodes:
                cursor.execute("""
                    INSERT INTO CodeNodes (name, [type], parameters, api_endpoint, api_method, summary, snippet, filePath, serviceName)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node['name'],
                    node['type'],
                    node.get('parameters'),
                    node.get('api_endpoint'),
                    node.get('api_method'),
                    node.get('summary', ''),
                    node.get('snippet', ''),
                    node.get('filePath', ''),
                    node.get('serviceName', '')
                ))
                
                cursor.execute("""
                    SELECT TOP 1 $node_id, id 
                    FROM CodeNodes 
                    WHERE name = ? AND [type] = ? AND serviceName = ?
                    ORDER BY id DESC
                """, (node['name'], node['type'], node.get('serviceName', '')))
                
                result = cursor.fetchone()
                if result:
                    node_id_db, id_value = result
                    self.node_id_map[node['name']] = node_id_db
                    
                    if self.config['output'].get('verbose'):
                        endpoint_info = f" [{node.get('api_method')} {node.get('api_endpoint')}]" if node.get('api_endpoint') else ""
                        params_info = f" ({node.get('parameters')})" if node.get('parameters') else ""
                        print(f"  [OK] {node['type']}: {node['name']}{params_info}{endpoint_info}")

            print(f"\n=== Inserting {len(self.relationships)} relationships ===")
            inserted = 0
            for rel in self.relationships:
                source_name = rel.get('sourceName')
                target_name = rel.get('targetName')
                rel_type = rel.get('relationshipType')
                
                if not target_name or source_name not in self.node_id_map or target_name not in self.node_id_map:
                    continue

                cursor.execute("""
                    INSERT INTO Relationships (relationshipType, description, call_order, line_number, timestamp, $from_id, $to_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    rel_type,
                    rel.get('description', ''),
                    rel.get('call_order'),
                    rel.get('line_number'),
                    datetime.now().isoformat(),
                    self.node_id_map[source_name],
                    self.node_id_map[target_name]
                ))
                inserted += 1

            conn.commit()
            print(f"\n[SUCCESS] Successfully stored {len(self.code_nodes)} nodes and {inserted} relationships")

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Error storing to database: {e}")
            raise
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    parser = CodeParser(config_path="parser_config.yaml", use_database=True)
    parser.parse_project("E:/learn_GENAI/git_repo_sample/trade-platform")
    parser.export_to_json("generic_code_graph.json")
    
    print("\n" + "="*80)
    print(" PARSING COMPLETE")
    print("="*80)
    print(f"Total Nodes: {len(parser.code_nodes)}")
    print(f"Total Relationships: {len(parser.relationships)}")
    print(f"API Endpoints: {len(parser.api_endpoints)}")