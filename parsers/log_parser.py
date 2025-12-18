import json
from pathlib import Path
from datetime import datetime
from db_config import DatabaseConfig


class LogParser:
    
    def __init__(self, use_database=False):
        self.log_events = []
        self.relationships = []
        self.use_database = use_database
        self.db_config = DatabaseConfig() if use_database else None
        self.event_id_map = {}  # Map (traceId, timestamp, service) to database $node_id
        
    def parse_trace_logs(self, trade_platform_dir):
        """Parse all trace logs from all services"""
        platform_path = Path(trade_platform_dir)
        
        # Parse logs from each service
        for service_dir in platform_path.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith('.'):
                service_name = service_dir.name
                logs_dir = service_dir / 'logs'
                
                if logs_dir.exists():
                    self.parse_service_logs(logs_dir, service_name)
        
        # Create temporal relationships
        self.create_temporal_relationships()
        
        # Store in database if enabled
        if self.use_database:
            self.store_to_database()
    
    def parse_service_logs(self, logs_dir, service_name):
        """Parse all trace log files in a service's logs directory"""
        # Support both trace_*.log and UUID.log formats
        for log_file in logs_dir.glob('*.log'):
            # Skip non-trace files
            if log_file.name in ['orchestrator.log', 'trade_service.log', 'pricing_service.log', 'risk_service.log']:
                continue
            
            # Extract trace ID from filename
            if log_file.stem.startswith('trace_'):
                trace_id = log_file.stem.replace('trace_', '')
            else:
                # Assume the filename itself is the trace ID
                trace_id = log_file.stem
            
            self.parse_log_file(log_file, service_name, trace_id)
    
    def parse_log_file(self, log_file, service_name, trace_id):
        """Parse a single trace log file (JSON lines format)"""
        print(f"Parsing {log_file.name} from {service_name}...")
        
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    log_entry = json.loads(line)
                    
                    # Extract log event data
                    event = {
                        'timestamp': log_entry.get('timestamp', ''),
                        'service': service_name,
                        'level': log_entry.get('level', ''),
                        'traceId': trace_id,
                        'message': log_entry.get('message', ''),
                        'errorCode': log_entry.get('extra_data', {}).get('error_code', ''),
                        'metadata': json.dumps(log_entry.get('extra_data', {}))
                    }
                    
                    self.log_events.append(event)
                    
                except json.JSONDecodeError as e:
                    print(f"  Warning: Could not parse line in {log_file}: {e}")
    
    def create_temporal_relationships(self):
        """Create temporal sequence relationships between log events"""
        # Group events by trace_id
        events_by_trace = {}
        for event in self.log_events:
            trace_id = event['traceId']
            if trace_id not in events_by_trace:
                events_by_trace[trace_id] = []
            events_by_trace[trace_id].append(event)
        
        # Sort events by timestamp and create next_log relationships
        for trace_id, events in events_by_trace.items():
            sorted_events = sorted(events, key=lambda e: e['timestamp'])
            
            for i in range(len(sorted_events) - 1):
                current = sorted_events[i]
                next_event = sorted_events[i + 1]
                
                # Create unique keys for events
                current_key = f"{current['traceId']}_{current['timestamp']}_{current['service']}"
                next_key = f"{next_event['traceId']}_{next_event['timestamp']}_{next_event['service']}"
                
                relationship = {
                    'source': current_key,
                    'target': next_key,
                    'type': 'next_log',
                    'description': f"Temporal sequence in trace {trace_id}"
                }
                self.relationships.append(relationship)
    
    def store_to_database(self):
        """Store parsed log events and relationships to SQL Server Graph database"""
        if not self.use_database or not self.db_config:
            print("Database storage not enabled")
            return
        
        conn = self.db_config.get_connection()
        cursor = conn.cursor()
        
        try:
            # Insert LogEvents
            print(f"\nInserting {len(self.log_events)} log events...")
            for event in self.log_events:
                # Insert event
                cursor.execute("""
                    INSERT INTO LogEvents (timestamp, service, level, traceId, message, errorCode)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    event['timestamp'],
                    event['service'],
                    event['level'],
                    event['traceId'],
                    event['message'],
                    event['errorCode']
                ))
                
                # Get the $node_id after insert
                cursor.execute("""
                    SELECT TOP 1 $node_id, id 
                    FROM LogEvents 
                    WHERE timestamp = ? AND service = ? AND traceId = ?
                    ORDER BY id DESC
                """, (event['timestamp'], event['service'], event['traceId']))
                
                result = cursor.fetchone()
                if result:
                    node_id_db, id_value = result
                    # Store mapping
                    event_key = f"{event['traceId']}_{event['timestamp']}_{event['service']}"
                    self.event_id_map[event_key] = node_id_db
                    print(f"  Inserted LogEvent: {event['service']} - {event['message'][:50]}... (id={id_value})")
            
            # Insert Relationships
            print(f"\nInserting {len(self.relationships)} log relationships...")
            for rel in self.relationships:
                source_key = rel['source']
                target_key = rel['target']
                
                if source_key not in self.event_id_map or target_key not in self.event_id_map:
                    print(f"  Skipping relationship: Event not found")
                    continue
                
                source_node_id = self.event_id_map[source_key]
                target_node_id = self.event_id_map[target_key]
                
                cursor.execute("""
                    INSERT INTO Relationships (relationshipType, description, timestamp, $from_id, $to_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    rel['type'],
                    rel.get('description', ''),
                    datetime.now().isoformat(),
                    source_node_id,
                    target_node_id
                ))
                print(f"  Created {rel['type']} relationship")
            
            conn.commit()
            print(f"\n✅ Successfully stored {len(self.log_events)} events and {len(self.relationships)} relationships")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error storing to database: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def export_to_json_for_debugging(self, json_path="log_data.json"):
        """Export log data to JSON for debugging"""
        data = {
            "logEvents": self.log_events,
            "relationships": self.relationships,
            "metadata": {
                "total_events": len(self.log_events),
                "total_relationships": len(self.relationships),
                "timestamp": datetime.now().isoformat()
            }
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"Exported {len(self.log_events)} events to {json_path}")


if __name__ == "__main__":
    # Example usage
    parser = LogParser(use_database=False)
    parser.parse_trace_logs("../trade-platform")
    parser.export_to_json_for_debugging()
    
    print(f"\nParsed {len(parser.log_events)} log events")
    print(f"Created {len(parser.relationships)} temporal relationships")
