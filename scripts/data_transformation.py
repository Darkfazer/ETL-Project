# scripts/data_transformation.py
import logging
import pandas as pd
from utils import get_data_path, setup_logging

# Initialize logging
setup_logging("processed/transformation.log")

def load_cleaned_data():
    """Load cleaned data from previous step"""
    input_path = get_data_path("processed", "cleaned_data.csv")
    try:
        df = pd.read_csv(input_path, parse_dates=["timestamp"])
        logging.info(f"Successfully loaded {len(df)} rows from {input_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading cleaned data: {str(e)}")
        raise

def transform_data(df):
    logging.info("Starting data transformations")
    
    # 1. Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    # 2. Resample to hourly and calculate averages
    df = df.set_index("timestamp")
    hourly = df.resample("1H").mean().add_suffix("_h1_avg")
    df = df.join(hourly, how="left")
    
    # 3. Add AQI category
    def aqi_category(aqi_val):
        if pd.isna(aqi_val): return None
        if aqi_val <= 50: return "Good"
        if aqi_val <= 100: return "Moderate"
        if aqi_val <= 150: return "Unhealthy for Sensitive Groups"
        if aqi_val <= 200: return "Unhealthy"
        if aqi_val <= 300: return "Very Unhealthy"
        return "Hazardous"
    
    if "aqi" in df.columns:
        df["aqi_category"] = df["aqi"].apply(aqi_category)
        logging.info("Added AQI category column")
    
    df = df.reset_index()
    logging.info(f"Transformation complete. Final shape: {df.shape}")
    return df

def save_transformed(df):
    """Save transformed data"""
    output_path = get_data_path("processed", "transformed_data.csv")
    try:
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved transformed data to {output_path}")
    except Exception as e:
        logging.error(f"Error saving transformed data: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting transformation pipeline")
    try:
        cleaned_df = load_cleaned_data()
        transformed_df = transform_data(cleaned_df)
        save_transformed(transformed_df)
    except Exception as e:
        logging.error(f"Transformation pipeline failed: {str(e)}")
