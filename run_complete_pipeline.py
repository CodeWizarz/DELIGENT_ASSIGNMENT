# run_complete_pipeline.py
"""
Complete Pipeline Execution Script
One-command execution of the entire A-SDLC process
"""

import subprocess
import sys
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command: str, description: str) -> bool:
    """Execute a shell command with error handling"""
    logger.info(f"🚀 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def main():
    """Execute the complete A-SDLC pipeline"""
    logger.info("🎯 Starting Complete E-commerce Analytics Pipeline")
    
    # Step 1: Generate synthetic data
    if not run_command(
        "python scripts/advanced_data_generator.py",
        "Generating advanced synthetic data"
    ):
        sys.exit(1)
    
    # Step 2: Ingest data into SQLite
    if not run_command(
        "python scripts/advanced_ingestion.py",
        "Ingesting data into SQLite database"
    ):
        sys.exit(1)
    
    # Step 3: Run data quality tests
    if not run_command(
        "python tests/test_data_generation.py",
        "Running data quality tests"
    ):
        sys.exit(1)
    
    # Step 4: Execute SQL queries validation
    if not run_command(
        "python tests/test_queries.py", 
        "Validating SQL queries"
    ):
        sys.exit(1)
    
    logger.info("🎉 Pipeline completed successfully!")
    logger.info("📊 Your advanced e-commerce analytics platform is ready!")
    logger.info("💡 Next steps:")
    logger.info("   - Explore the SQL queries in sql/ directory")
    logger.info("   - Check the database at database/ecommerce_analytics.sqlite")
    logger.info("   - Review reports in reports/ directory")

if __name__ == "__main__":
    main()