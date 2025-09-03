-- Summit Sports Review Analytics Setup Script
-- This script sets up the required schema and table structure

-- Create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS SS_101.RAW_CUSTOMER;

-- Use the schema
USE SCHEMA SS_101.RAW_CUSTOMER;

-- Create the reviews table
CREATE OR REPLACE TABLE INTERSPORT_REVIEWS (
    CUSTOMER_NAME STRING,
    RATING INTEGER,
    REVIEW_TEXT STRING,
    DATE STRING,
    STORE_LOCATION STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create a view for clean data analysis
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

-- Grant necessary permissions (adjust as needed for your role)
-- GRANT SELECT ON ALL TABLES IN SCHEMA SS_101.RAW_CUSTOMER TO ROLE PUBLIC;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA SS_101.RAW_CUSTOMER TO ROLE PUBLIC;

-- Sample AI_AGG queries you can run manually for testing
/*
-- Overall sentiment analysis
SELECT AI_AGG(
    REVIEW_TEXT, 
    'Summarize customer sentiment and key themes in these sports retailer reviews'
) as overall_summary
FROM SS_101.RAW_CUSTOMER.CLEAN_REVIEWS;

-- Positive feedback analysis
SELECT AI_AGG(
    REVIEW_TEXT,
    'Identify the top strengths that customers appreciate most about this retailer'
) as strengths
FROM SS_101.RAW_CUSTOMER.CLEAN_REVIEWS
WHERE SENTIMENT_CATEGORY = 'Positive';

-- Areas for improvement
SELECT AI_AGG(
    REVIEW_TEXT,
    'Identify main customer complaints and provide actionable improvement recommendations'
) as improvement_areas
FROM SS_101.RAW_CUSTOMER.CLEAN_REVIEWS
WHERE SENTIMENT_CATEGORY = 'Negative';
*/ 