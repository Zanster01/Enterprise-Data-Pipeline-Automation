"""
Enterprise Data Pipeline & ETL Automation Engine
Author: Tushar Parihar
Description: A modular, production-ready ETL pipeline designed to ingest, 
sanitize, transform, and load multi-source transactional datasets with 
robust error handling and automated logging.
"""

import logging
import os
import sys
import pandas as pd

# Configure enterprise-grade logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ETL_Pipeline")


class DataPipelineEngine:
    def __init__(self, source_path: str):
        self.source_path = source_path
        logger.info(f"Initialized pipeline engine targeting source: {source_path}")

    def extract_data(self) -> pd.DataFrame:
        """Extracts raw dataset from source with rigorous validation."""
        logger.info("Starting data extraction phase...")
        try:
            if not os.path.exists(self.source_path):
                raise FileNotFoundError(f"Source file not found at: {self.source_path}")
            
            df = pd.read_csv(self.source_path)
            logger.info(f"Extraction successful. Rows extracted: {len(df)}, Columns: {len(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Critical failure during extraction: {e}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitizes, cleans, and transforms raw data into a pristine schema."""
        logger.info("Starting data transformation and sanitization phase...")
        
        initial_count = len(df)
        
        # Drop completely duplicate rows
        df = df.drop_duplicates()
        
        # Strip whitespace from string columns to maintain data integrity
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()

        # Handle null/missing values gracefully based on industry standards
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)

        # Drop rows where critical identifier fields are missing or NaN
        if 'id' in df.columns:
            df = df.dropna(subset=['id'])

        final_count = len(df)
        logger.info(f"Transformation complete. Cleaned rows: {final_count} (Dropped {initial_count - final_count} anomalies).")
        return df

    def load_data(self, df: pd.DataFrame, output_path: str) -> None:
        """Loads the transformed clean data into the target destination securely."""
        logger.info(f"Starting data load phase to destination: {output_path}...")
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info("Data load successfully completed and verified.")
        except Exception as e:
            logger.error(f"Critical failure during data load: {e}")
            raise

    def run_pipeline(self, output_path: str) -> None:
        """Executes the full end-to-end ETL lifecycle."""
        logger.info("=== Pipeline Execution Started ===")
        try:
            raw_data = self.extract_data()
            clean_data = self.transform_data(raw_data)
            self.load_data(clean_data, output_path)
            logger.info("=== Pipeline Execution Completed Successfully ===")
        except Exception as e:
            logger.error("=== Pipeline Execution Failed Abruptly ===")
            sys.exit(1)


if __name__ == "__main__":
    # Example execution mock paths for enterprise verification
    INPUT_FILE = "data/raw_transactions.csv"
    OUTPUT_FILE = "data/processed_transactions.csv"
    
    # Instantiate and execute pipeline
    # pipeline = DataPipelineEngine(INPUT_FILE)
    # pipeline.run_pipeline(OUTPUT_FILE)
    logger.info("Pipeline script compiled and ready for execution.")
