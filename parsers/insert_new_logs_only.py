"""
Insert only NEW log files without touching existing CodeNodes or their summaries.

This script:
1. Checks which trace log files are already in the database
2. Parses only NEW log files that haven't been inserted yet
3. Inserts only the new log events and their relationships
4. Does NOT touch CodeNodes, Summaries, or Workflows

Usage: python insert_new_logs_only.py
"""

import json
from pathlib import Path
from datetime import datetime
from db_config import DatabaseConfig


class IncrementalLogParser:
    
    def __init__(self):
        self.log_events = []
        self.relationships = []
        self.db_config = DatabaseConfig()
        self.event_id_map = {}
        self.existing_trace_ids = set()
        
    def get_existing_trace_ids(self):
        """Get list of trace IDs already in the database"""
        conn = self.db_config.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT traceId FROM LogEvents WHERE traceId IS NOT NULL AND traceId != ''")
        rows = cursor.fetchall()
        
        self.existing_trace_ids = {row[0] for row in rows}
        
        cursor.close()
        conn.close()
        
        print(f"Found {len(self.existing_trace_ids)} existing trace IDs in database")
        return self.existing_trace_ids
    
    def parse_new_trace_logs(self, trade_platform_dir):
        """Parse only NEW trace logs that are not in the database"""
        platform_path = Path(trade_platform_dir)
        
        # Get existing trace IDs from database
        self.get_existing_trace_ids()
        
        new_logs_count = 0
        skipped_logs_count = 0
        
        # Parse logs from each service
        for service_dir in platform_path.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith('.'):
                service_name = service_dir.name
                logs_dir = service_dir / 'logs'
                
                if logs_dir.exists():
                    new_count, skipped_count = self.parse_service_logs(logs_dir, service_name)
                    new_logs_count += new_count
                    skipped_logs_count += skipped_count
        
        print(f"\n📊 Summary:")
        print(f"  ✅ New log files parsed: {new_logs_count}")
        print(f"  ⏭️  Skipped existing log files: {skipped_logs_count}")
        
        if new_logs_count > 0:
            # Create temporal relationships for new logs
            self.create_temporal_relationships()
            
            # Store in database
            self.store_to_database()
        else:
            print("\n✅ No new logs to insert. All logs are already in the database.")
    
    def parse_service_logs(self, logs_dir, service_name):
        """Parse only NEW trace log files in a service's logs directory"""
        new_count = 0
        skipped_count = 0
        
        for log_file in logs_dir.glob('*.log'):
            # Skip non-trace files
            if log_file.name in ['orchestrator.log', 'trade_service.log', 'pricing_service.log', 'risk_service.log']:
                continue
            
            # Extract trace ID from filename
            if log_file.stem.startswith('trace_'):
                trace_id = log_file.stem.replace('trace_', '')
            else:
                trace_id = log_file.stem
            
            # Check if this trace_id is already in the database
            if trace_id in self.existing_trace_ids:
                print(f"  ⏭️  Skipping {log_file.name} (already exists)")
                skipped_count += 1
                continue
            
            # This is a new log file
            print(f"  ✅ Parsing NEW log: {log_file.name} from {service_name}")
            self.parse_log_file(log_file, service_name, trace_id)
            new_count += 1
        
        return new_count, skipped_count
    
    def parse_log_file(self, log_file, service_name, trace_id):
        """Parse a single trace log file (JSON lines format)"""
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    log_entry = json.loads(line)
                    
                    # Extract function from the log message pattern [function_name]
                    function_name = ''
                    message = log_entry.get('message', '')
                    if message.startswith('[') and ']' in message:
                        function_name = message[1:message.index(']')]
                    
                    # Extract extra_data for easier access
                    extra_data = log_entry.get('extra_data', {})
                    
                    # Extract log event data
                    event = {
                        'timestamp': log_entry.get('timestamp', ''),
                        'service': service_name,
                        'level': log_entry.get('level', ''),
                        'traceId': log_entry.get('trace_id', trace_id),
                        'orderId': log_entry.get('order_id', ''),
                        'function': function_name,
                        'message': log_entry.get('message', ''),
                        'errorCode': extra_data.get('error_code', ''),
                        'errorType': extra_data.get('error_type', ''),
                        'exception': log_entry.get('exception', ''),
                        'durationMs': extra_data.get('duration_ms', None),
                        'metadata': json.dumps(extra_data)
                    }
                    
                    self.log_events.append(event)
                    
                except json.JSONDecodeError as e:
                    print(f"  ⚠️  Warning: Could not parse line in {log_file}: {e}")
    
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
            # Sort by timestamp
            sorted_events = sorted(events, key=lambda e: e['timestamp'])
            
            # Create next_log relationships
            for i in range(len(sorted_events) - 1):
                current_event = sorted_events[i]
                next_event = sorted_events[i + 1]
                
                current_key = f"{current_event['traceId']}_{current_event['timestamp']}_{current_event['service']}"
                next_key = f"{next_event['traceId']}_{next_event['timestamp']}_{next_event['service']}"
                
                relationship = {
                    'source': current_key,
                    'target': next_key,
                    'type': 'next_log',
                    'description': f"Next log in trace {trace_id}"
                }
                self.relationships.append(relationship)
    
    def store_to_database(self):
        """Store parsed log events and relationships to SQL Server Graph database"""
        conn = self.db_config.get_connection()
        cursor = conn.cursor()
        
        try:
            # Insert LogEvents
            print(f"\n💾 Inserting {len(self.log_events)} new log events...")
            for idx, event in enumerate(self.log_events, 1):
                cursor.execute("""
                    INSERT INTO LogEvents (timestamp, service, level, traceId, orderId, functionName, message, errorCode, errorType, exception, durationMs, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['timestamp'],
                    event['service'],
                    event['level'],
                    event['traceId'],
                    event.get('orderId', ''),
                    event.get('function', ''),
                    event['message'],
                    event.get('errorCode', ''),
                    event.get('errorType', ''),
                    event.get('exception', ''),
                    event.get('durationMs'),
                    event['metadata']
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
                    
                    if idx % 10 == 0:
                        print(f"  Progress: {idx}/{len(self.log_events)} events inserted")
            
            print(f"  ✅ Inserted all {len(self.log_events)} log events")
            
            # Insert Relationships
            print(f"\n💾 Inserting {len(self.relationships)} new log relationships...")
            inserted_rels = 0
            for rel in self.relationships:
                source_key = rel['source']
                target_key = rel['target']
                
                if source_key not in self.event_id_map or target_key not in self.event_id_map:
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
                inserted_rels += 1
            
            print(f"  ✅ Inserted {inserted_rels} relationships")
            
            conn.commit()
            
            print(f"\n✅ Successfully inserted {len(self.log_events)} new log events and {inserted_rels} relationships")
            print(f"✅ CodeNodes and their summaries were NOT touched")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error storing to database: {e}")
            raise
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    print("="*80)
    print(" INCREMENTAL LOG INSERTION - PRESERVES EXISTING DATA")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("⚠️  This script will:")
    print("   ✅ Insert ONLY new log files")
    print("   ✅ Preserve all existing CodeNodes")
    print("   ✅ Preserve all GenAI-generated summaries")
    print("   ✅ Preserve all existing logs")
    print("="*80)
    
    parser = IncrementalLogParser()
    parser.parse_new_trace_logs("../trade-platform")
    
    print("\n" + "="*80)
    print("✅ INCREMENTAL LOG INSERTION COMPLETE")
    print("="*80)
