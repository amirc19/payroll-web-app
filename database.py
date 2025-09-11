import os
import psycopg
import json
from urllib.parse import urlparse

def get_db_connection():
    """Get database connection using environment variable or local fallback"""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Use the DATABASE_URL directly with psycopg3
        return psycopg.connect(database_url)
    else:
        # Local development fallback
        return psycopg.connect(
            "dbname=payroll_db user=postgres password=password host=localhost port=5432"
        )

def init_database():
    """Initialize the database table if it doesn't exist"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Create drivers table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS drivers (
                        id SERIAL PRIMARY KEY,
                        driver_name VARCHAR(255) UNIQUE NOT NULL,
                        config JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
        
        print("Database initialized successfully")
        return True
    except Exception as e:
        print(f"Database initialization error: {e}")
        return False

def save_driver_to_db(driver_name, config):
    """Save or update driver configuration in database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Use UPSERT (INSERT ... ON CONFLICT)
                cur.execute("""
                    INSERT INTO drivers (driver_name, config, updated_at) 
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (driver_name) 
                    DO UPDATE SET 
                        config = EXCLUDED.config,
                        updated_at = CURRENT_TIMESTAMP
                """, (driver_name, json.dumps(config)))
                conn.commit()
        
        return True
    except Exception as e:
        print(f"Error saving driver {driver_name}: {e}")
        return False

def load_all_drivers_from_db():
    """Load all driver configurations from database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT driver_name, config FROM drivers ORDER BY driver_name")
                rows = cur.fetchall()
                
                drivers = {}
                for row in rows:
                    driver_name, config = row
                    # config is returned as a string from JSONB, so we need to parse it
                    if isinstance(config, str):
                        drivers[driver_name] = json.loads(config)
                    else:
                        drivers[driver_name] = config
                
                return drivers
    except Exception as e:
        print(f"Error loading drivers: {e}")
        return {}

def delete_driver_from_db(driver_name):
    """Delete driver from database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM drivers WHERE driver_name = %s", (driver_name,))
                rows_deleted = cur.rowcount
                conn.commit()
                return rows_deleted > 0
    except Exception as e:
        print(f"Error deleting driver {driver_name}: {e}")
        return False

def get_driver_count():
    """Get total number of drivers in database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM drivers")
                count = cur.fetchone()[0]
                return count
    except Exception as e:
        print(f"Error getting driver count: {e}")
        return 0
