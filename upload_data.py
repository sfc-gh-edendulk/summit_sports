"""
Data Upload Script for Summit Sports Reviews Analytics
This script uploads the intersport_reviews.csv to Snowflake table
"""

import pandas as pd
from snowflake.snowpark import Session
import os
from typing import Dict, Any

def create_snowflake_session(connection_params: Dict[str, Any]) -> Session:
    """Create Snowflake session"""
    return Session.builder.configs(connection_params).create()

def upload_reviews_data(session: Session, csv_file_path: str) -> None:
    """Upload CSV data to Snowflake table"""
    
    # Read CSV file
    print(f"Reading CSV file: {csv_file_path}")
    df = pd.read_csv(csv_file_path)
    print(f"Loaded {len(df)} records from CSV")
    
    # Create schema if not exists
    session.sql("CREATE SCHEMA IF NOT EXISTS SS_101.RAW_CUSTOMER").collect()
    session.use_schema("SS_101.RAW_CUSTOMER")
    
    # Create table
    create_table_sql = """
    CREATE OR REPLACE TABLE INTERSPORT_REVIEWS (
        CUSTOMER_NAME STRING,
        RATING INTEGER,
        REVIEW_TEXT STRING,
        DATE STRING,
        STORE_LOCATION STRING,
        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    print("Creating table...")
    session.sql(create_table_sql).collect()
    
    # Upload data
    print("Uploading data to Snowflake...")
    session.write_pandas(
        df, 
        'INTERSPORT_REVIEWS', 
        schema='SS_101.RAW_CUSTOMER',
        overwrite=True,
        auto_create_table=False
    )
    
    # Verify upload
    count_result = session.sql("SELECT COUNT(*) as count FROM INTERSPORT_REVIEWS").collect()
    record_count = count_result[0]['COUNT']
    print(f"✅ Successfully uploaded {record_count} records to Snowflake!")
    
    # Create the clean view
    create_view_sql = """
    CREATE OR REPLACE VIEW CLEAN_REVIEWS AS
    SELECT 
        CUSTOMER_NAME,
        RATING,
        REVIEW_TEXT,
        TRY_TO_DATE(DATE, 'DD/MM/YYYY') as REVIEW_DATE,
        CASE 
            WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Online Order'
            ELSE STORE_LOCATION 
        END as STORE_LOCATION,
        LENGTH(REVIEW_TEXT) as REVIEW_LENGTH,
        CASE 
            WHEN RATING >= 4 THEN 'Positive'
            WHEN RATING = 3 THEN 'Neutral'
            WHEN RATING <= 2 THEN 'Negative'
            ELSE 'Unrated'
        END as SENTIMENT_CATEGORY
    FROM INTERSPORT_REVIEWS
    WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';
    """
    
    session.sql(create_view_sql).collect()
    print("✅ Created CLEAN_REVIEWS view")

def main():
    """Main function"""
    # Snowflake connection parameters
    # Update these with your actual Snowflake credentials
    connection_params = {
        "account": "YOUR_ACCOUNT",  # e.g., "abc12345.snowflakecomputing.com"
        "user": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",  # or use key_pair authentication
        "role": "YOUR_ROLE",  # e.g., "ACCOUNTADMIN"
        "warehouse": "YOUR_WAREHOUSE",  # e.g., "COMPUTE_WH"
        "database": "YOUR_DATABASE",  # e.g., "SUMMIT_SPORTS"
    }
    
    # Check if CSV file exists
    csv_file = "social_listening/review_collection/intersport_reviews.csv"
    if not os.path.exists(csv_file):
        print(f"❌ Error: {csv_file} not found in current directory")
        return
    
    try:
        # Create session and upload data
        session = create_snowflake_session(connection_params)
        upload_reviews_data(session, csv_file)
        
        # Close session
        session.close()
        
        print("\n🎉 Data upload completed successfully!")
        print("You can now run your Streamlit app to analyze the reviews.")
        
    except Exception as e:
        print(f"❌ Error during upload: {str(e)}")
        print("Please check your connection parameters and try again.")

# This script provides utility functions for data upload
# Use the functions within the Streamlit app for data loading 