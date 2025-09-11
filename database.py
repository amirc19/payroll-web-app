import os
import psycopg2
import json
from urllib.parse import urlparse

def get_db_connection():
    """Get database connection using environment variable or local fallback"""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Parse the DATABASE_URL (Render format)
        url = urlparse(database_url)
        return psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
    else:
        # Local development fallback (you can use SQLite locally if preferred)
        return psycopg2.connect(
            database="payroll_db",
            user="postgres", 
            password="password",
            host="localhost",
            port="5432"
        )

def init_database():
    """Initialize the database table if it doesn't exist"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
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
        cur.close()
        conn.close()
        print("Database initialized successfully")
        return True
    except Exception as e:
        print(f"Database initialization error: {e}")
        return False

def save_driver_to_db(driver_name, config):
    """Save or update driver configuration in database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
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
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving driver {driver_name}: {e}")
        return False

def load_all_drivers_from_db():
    """Load all driver configurations from database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT driver_name, config FROM drivers ORDER BY driver_name")
        rows = cur.fetchall()
        
        drivers = {}
        for row in rows:
            driver_name, config = row
            drivers[driver_name] = config  # config is already parsed from JSONB
        
        cur.close()
        conn.close()
        return drivers
    except Exception as e:
        print(f"Error loading drivers: {e}")
        return {}

def delete_driver_from_db(driver_name):
    """Delete driver from database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("DELETE FROM drivers WHERE driver_name = %s", (driver_name,))
        rows_deleted = cur.rowcount
        
        conn.commit()
        cur.close()
        conn.close()
        return rows_deleted > 0
    except Exception as e:
        print(f"Error deleting driver {driver_name}: {e}")
        return False

def get_driver_count():
    """Get total number of drivers in database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM drivers")
        count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error getting driver count: {e}")
        return 0
