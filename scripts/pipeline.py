# scripts/pipeline.py
import logging
import pandas as pd
from data_cleaning import clean_data, save_cleaned
from data_loading import load_to_database
from data_transformation import transform_data
from utils import setup_logging
from data_acquisition import (
    fetch_openweathermap_air_quality,
    save_raw_data,
    fetch_uk_carbon_intensity,
    fetch_worldbank_sustainability,
)

def run_pipeline():
    """Main ETL pipeline function."""
    # Initialize logging (FIXED: Added closing parenthesis)
    setup_logging("processed/pipeline.log")

    try:
        # 1. Data Acquisition
        logging.info("Fetching data from APIs")
        weather_df = fetch_openweathermap_air_quality(lat=51.5074, lon=-0.1278, api_key="YOUR_KEY")
        carbon_df = fetch_uk_carbon_intensity()
        worldbank_df = fetch_worldbank_sustainability()

        # Combine data (example: merge on timestamp)
        combined_df = pd.merge(weather_df, carbon_df, on="timestamp", how="outer")
        save_raw_data(combined_df)  # Save to CSV

        # 2. Cleaning & Transformation
        logging.info("Cleaning data...")
        cleaned_df = clean_data(combined_df)
        save_cleaned(cleaned_df)

        # 3. Transformation
        logging.info("Transforming data...")
        transformed_df = transform_data(cleaned_df)

        # 4. Database Loading
        logging.info("Loading data to database...")
        load_to_database(transformed_df)

        logging.info("ETL pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_pipeline()