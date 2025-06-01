# scripts/data_acquisition.py
import os
import logging
import pandas as pd
from utils import get_data_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def ensure_directories():
    for folder in ["raw", "processed"]:
        os.makedirs(get_data_path(folder), exist_ok=True)

def fetch_sample_data():
    """Generate sample data for testing (matches pipeline.py expectations)"""
    logging.info("Generating sample data")
    return pd.DataFrame({
        "Timestamp": pd.date_range(start="2023-01-01", periods=100, freq="H"),
        "PM2_5": [10 + i % 20 for i in range(100)],
        "PM10": [20 + i % 30 for i in range(100)],
        "Temperature": [25 + i % 5 for i in range(100)]
    })

def save_raw_data(df):
    output_path = get_data_path("processed", "combined_raw_data.csv")
    df.to_csv(output_path, index=False)
    logging.info(f"Saved raw data to {output_path}")

def acquire_raw_data():
    """
    Original data acquisition function (keep for backward compatibility)
    """
    ensure_directories()
    sample_data = fetch_sample_data()
    save_raw_data(sample_data)
    return sample_data

if __name__ == "__main__":
    acquire_raw_data()