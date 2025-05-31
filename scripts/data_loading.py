# scripts/data_loading.py
import logging
import sqlite3
import pandas as pd
from utils import get_db_path, get_data_path, setup_logging

print("FILE RAN")

# Initialize logging
setup_logging("processed/loading.log")

def create_database():
    """Create database tables if needed"""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS air_quality (
        timestamp DATETIME PRIMARY KEY,
        pm2_5 REAL,
        pm10 REAL,
        temperature REAL,
        humidity REAL,
        pm2_5_h1_avg REAL,
        pm10_h1_avg REAL,
        temperature_h1_avg REAL,
        humidity_h1_avg REAL,
        aqi_category TEXT
    )""")
    
    conn.commit()
    conn.close()
    logging.info(f"Database table created/verified at {db_path}")

def load_to_database(df):
    """Load transformed data into database"""
    try:
        create_database()
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        
        # Load data
        df.to_sql("air_quality", conn, if_exists="replace", index=False)
        
        conn.commit()
        conn.close()
        logging.info(f"Successfully loaded {len(df)} rows into database")
    except Exception as e:
        logging.error(f"Database loading failed: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting database loading pipeline")
    try:
        # Load transformed data
        input_path = get_data_path("processed", "transformed_data.csv")
        df = pd.read_csv(input_path, parse_dates=["timestamp"])
        load_to_database(df)
    except Exception as e:
        logging.error(f"Loading process failed: {str(e)}")