# scripts/data_cleaning.py
import os
import logging
import pandas as pd
from dateutil import parser
from utils import get_data_path  # Shared path helper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(get_data_path("processed", "cleaning.log"))
    ]
)

def load_combined_raw():
    """Load combined raw data from acquisition step"""
    input_path = get_data_path("processed", "combined_raw_data.csv")
    
    if not os.path.exists(input_path):
        logging.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Run data_acquisition.py first to generate {input_path}")

    try:
        df = pd.read_csv(input_path)
        logging.info(f"Loaded {len(df)} rows from {input_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read {input_path}: {str(e)}")
        raise

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main cleaning pipeline"""
    if df.empty:
        logging.warning("Received empty DataFrame")
        return df

    # 1. Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    logging.info("Standardized column names")

    # 2. Handle timestamps
    if "timestamp" not in df.columns:
        raise ValueError("DataFrame missing required 'timestamp' column")

    original_count = len(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    logging.info(f"Removed {original_count - len(df)} rows with invalid timestamps")

    # 3. Deduplicate
    df = df.drop_duplicates(subset=["timestamp"])
    logging.info(f"Removed {original_count - len(df)} duplicate timestamps")

    # 4. Numeric columns conversion
    numeric_cols = ["pm2_5", "pm10", "aqi", "temperature", "humidity"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            na_count = df[col].isna().sum()
            if na_count > 0:
                logging.warning(f"Converted {na_count} non-numeric values in {col} to NaN")

    # 5. Drop rows missing all sensor readings
    sensor_cols = [c for c in numeric_cols if c in df.columns]
    if sensor_cols:
        initial_rows = len(df)
        df = df.dropna(subset=sensor_cols, how="all")
        removed = initial_rows - len(df)
        if removed > 0:
            logging.info(f"Removed {removed} rows missing all sensor readings")

    logging.info(f"Cleaning complete. Final shape: {df.shape}")
    return df

def save_cleaned(df: pd.DataFrame):
    """Save cleaned data to CSV"""
    output_path = get_data_path("processed", "cleaned_data.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        df.to_csv(output_path, index=False)
        logging.info(f"Saved cleaned data to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save cleaned data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        raw_df = load_combined_raw()
        cleaned_df = clean_data(raw_df)
        save_cleaned(cleaned_df)
    except Exception as e:
        logging.error(f"Cleaning pipeline failed: {str(e)}", exc_info=True)
        raise