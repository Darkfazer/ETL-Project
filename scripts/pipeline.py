# scripts/pipeline.py
import logging
from data_acquisition import fetch_sample_data, save_raw_data
from data_cleaning import load_combined_raw, clean_data, save_cleaned
from data_transformation import transform_data  # You'll create this
from data_loading import load_to_database      # You'll create this
from utils import get_data_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run_pipeline():
    """Full ETL pipeline execution"""
    try:
        # 1. Acquisition
        logging.info("Starting data acquisition")
        raw_df = fetch_sample_data()
        save_raw_data(raw_df)

        # 2. Cleaning
        logging.info("Starting data cleaning")
        combined_df = load_combined_raw()
        cleaned_df = clean_data(combined_df)
        save_cleaned(cleaned_df)

        # 3. Transformation
        logging.info("Starting data transformation")
        transformed_df = transform_data(cleaned_df)

        # 4. Loading
        logging.info("Starting database loading")
        load_to_database(transformed_df)

        logging.info("ETL pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_pipeline()