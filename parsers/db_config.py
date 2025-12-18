"""Database configuration for SQL Server connection"""
import pyodbc

class DatabaseConfig:
    def __init__(self):
        self.server = 'NAGARAJ-08'  # Your SQL Server instance name
        self.database = 'trade_kg_db'
        self.driver = '{ODBC Driver 17 for SQL Server}'
        
    def get_connection_string(self):
        """Get SQL Server connection string (Windows Authentication)"""
        return (
            f'DRIVER={self.driver};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'Trusted_Connection=yes;'
        )
    
    def get_connection(self):
        """Create and return database connection"""
        try:
            conn = pyodbc.connect(self.get_connection_string())
            return conn
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

# Test connection
if __name__ == "__main__":
    config = DatabaseConfig()
    try:
        conn = config.get_connection()
        print("✓ Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
