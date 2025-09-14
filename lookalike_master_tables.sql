-- Master Tables for Lookalike Audience Creation
-- Creates feature-rich customer profiles for Summit Sports and Crocevia
-- to enable lookalike modeling and cross-brand audience targeting

-- =============================================================================
-- SUMMIT SPORTS MASTER TABLE: High-Value Customer Features
-- =============================================================================

CREATE OR REPLACE TABLE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES AS
WITH 
-- Latest customer demographics (one row per customer)
latest_demo AS (
  SELECT 
    CUSTOMER_ID,
    FIRST_NAME, LAST_NAME, GENDER, BIRTH_DATE, 
    CUSTOMER_POSTCODE AS POSTAL_CODE, LATITUDE, LONGITUDE
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) = 1
),

-- Purchase behavior aggregations
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    SUM(SALES_PRICE_EURO) AS total_spend,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    COUNT(*) AS total_items,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(DISTINCT ORDER_ID), 2) AS avg_basket_size,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(*), 2) AS avg_item_price,
    
    -- Recency
    MAX(SALE_DATE) AS last_purchase_date,
    DATEDIFF(day, MAX(SALE_DATE), CURRENT_DATE) AS days_since_last_purchase,
    
    -- Frequency patterns
    DATEDIFF(day, MIN(SALE_DATE), MAX(SALE_DATE)) / NULLIF(COUNT(DISTINCT ORDER_ID) - 1, 0) AS avg_days_between_orders,
    
    -- Seasonality (quarters)
    SUM(CASE WHEN QUARTER(SALE_DATE) = 1 THEN SALES_PRICE_EURO ELSE 0 END) AS q1_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 2 THEN SALES_PRICE_EURO ELSE 0 END) AS q2_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 3 THEN SALES_PRICE_EURO ELSE 0 END) AS q3_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 4 THEN SALES_PRICE_EURO ELSE 0 END) AS q4_spend,
    
    -- Store and brand diversity
    COUNT(DISTINCT STOREID) AS stores_visited,
    COUNT(DISTINCT BRAND) AS brands_purchased,
    
    -- Price sensitivity
    AVG(CASE WHEN SALES_PRICE_EURO < 50 THEN 1 ELSE 0 END) AS low_price_preference,
    AVG(CASE WHEN SALES_PRICE_EURO > 200 THEN 1 ELSE 0 END) AS premium_preference
    
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sport category affinity scores
sports_behavior AS (
  SELECT 
    CUSTOMER_ID,
    -- Sport-specific spend shares
    SUM(CASE WHEN ILIKE(COALESCE(SPORT, ''), '%running%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS running_affinity,
    SUM(CASE WHEN ILIKE(COALESCE(SPORT, ''), '%cycling%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS cycling_affinity,
    SUM(CASE WHEN ILIKE(COALESCE(SPORT, ''), '%winter%') OR ILIKE(COALESCE(SPORT, ''), '%ski%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS winter_sports_affinity,
    SUM(CASE WHEN ILIKE(COALESCE(SPORT, ''), '%team%') OR ILIKE(COALESCE(SPORT, ''), '%football%') OR ILIKE(COALESCE(SPORT, ''), '%basketball%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS team_sports_affinity,
    SUM(CASE WHEN ILIKE(COALESCE(SPORT, ''), '%outdoor%') OR ILIKE(COALESCE(SPORT, ''), '%hiking%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS outdoor_affinity,
    
    -- Category breadth
    COUNT(DISTINCT SPORT) AS sports_breadth,
    COUNT(DISTINCT PRODUCT_CATEGORY) AS category_breadth
    
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Payment method preferences
payment_prefs AS (
  SELECT 
    CUSTOMER_ID,
    MODE(PAYMENT_METHOD) AS preferred_payment_method,
    COUNT(DISTINCT PAYMENT_METHOD) AS payment_methods_used
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Final enrichment
enriched AS (
  SELECT 
    ld.*,
    cm.total_spend, cm.total_orders, cm.total_items, cm.avg_basket_size, cm.avg_item_price,
    cm.last_purchase_date, cm.days_since_last_purchase, cm.avg_days_between_orders,
    cm.q1_spend, cm.q2_spend, cm.q3_spend, cm.q4_spend,
    cm.stores_visited, cm.brands_purchased, cm.low_price_preference, cm.premium_preference,
    
    COALESCE(sb.running_affinity, 0) AS running_affinity,
    COALESCE(sb.cycling_affinity, 0) AS cycling_affinity,
    COALESCE(sb.winter_sports_affinity, 0) AS winter_sports_affinity,
    COALESCE(sb.team_sports_affinity, 0) AS team_sports_affinity,
    COALESCE(sb.outdoor_affinity, 0) AS outdoor_affinity,
    COALESCE(sb.sports_breadth, 0) AS sports_breadth,
    COALESCE(sb.category_breadth, 0) AS category_breadth,
    
    pp.preferred_payment_method, pp.payment_methods_used,
    
    -- Age band
    CASE
      WHEN ld.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
      ELSE '55+'
    END AS age_band,
    
    -- Value tier
    CASE
      WHEN cm.total_spend >= 2000 THEN 'VIP'
      WHEN cm.total_spend >= 500 THEN 'High Value'
      WHEN cm.total_spend >= 100 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_tier,
    
    -- Engagement level
    CASE
      WHEN cm.days_since_last_purchase <= 30 THEN 'Highly Active'
      WHEN cm.days_since_last_purchase <= 90 THEN 'Active'
      WHEN cm.days_since_last_purchase <= 180 THEN 'At Risk'
      ELSE 'Lapsed'
    END AS engagement_level,
    
    -- Sports engagement composite score
    ROUND((COALESCE(sb.running_affinity, 0) + COALESCE(sb.cycling_affinity, 0) + COALESCE(sb.winter_sports_affinity, 0) + 
           COALESCE(sb.team_sports_affinity, 0) + COALESCE(sb.outdoor_affinity, 0)), 4) AS sports_engagement_score
    
  FROM latest_demo ld
  LEFT JOIN customer_metrics cm USING (CUSTOMER_ID)
    LEFT JOIN sports_behavior sb USING (CUSTOMER_ID)
  LEFT JOIN payment_prefs pp USING (CUSTOMER_ID)
)

SELECT * FROM enriched;

-- =============================================================================
-- CROCEVIA MASTER TABLE: Sports-Adjacent Customer Features
-- =============================================================================

CREATE OR REPLACE TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES AS
WITH
-- Latest customer demographics
latest_demo AS (
  SELECT 
    CUSTOMER_ID,
    FIRST_NAME, LAST_NAME, GENDER, BIRTH_DATE,
    CUSTOMER_POSTCODE AS POSTAL_CODE, LATITUDE, LONGITUDE
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) = 1
),

-- Base customer metrics
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    SUM(SALES_PRICE_EURO) AS total_spend,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    COUNT(*) AS total_items,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(DISTINCT ORDER_ID), 2) AS avg_basket_size,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(*), 2) AS avg_item_price,
    
    -- Recency
    MAX(SALE_DATE) AS last_purchase_date,
    DATEDIFF(day, MAX(SALE_DATE), CURRENT_DATE) AS days_since_last_purchase,
    
    -- Frequency
    DATEDIFF(day, MIN(SALE_DATE), MAX(SALE_DATE)) / NULLIF(COUNT(DISTINCT ORDER_ID) - 1, 0) AS avg_days_between_orders,
    
    -- Store diversity
    COUNT(DISTINCT STORE_ID) AS stores_visited
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sports and fitness category analysis (critical for lookalike)
sports_behavior AS (
  SELECT 
    CUSTOMER_ID,
    -- Sports category spend
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%fitness%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%fitness%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS fitness_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%outdoor%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%outdoor%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS outdoor_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%nutrition%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%protein%') 
             OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%energy%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%vitamin%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS nutrition_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%apparel%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%clothing%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS apparel_spend,
    
    -- Sports brand affinity
    SUM(CASE WHEN ILIKE(COALESCE(BRAND, ''), '%nike%') OR ILIKE(COALESCE(BRAND, ''), '%adidas%') OR ILIKE(COALESCE(BRAND, ''), '%decathlon%') 
             OR ILIKE(COALESCE(BRAND, ''), '%salomon%') OR ILIKE(COALESCE(BRAND, ''), '%columbia%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_brand_spend,
    
    -- Sports item frequency
    COUNT(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%') 
               THEN 1 END) AS sports_items,
    COUNT(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%fitness%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%fitness%') 
               THEN 1 END) AS fitness_items,
    
    -- Seasonal sports patterns
    SUM(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (1,4) THEN SALES_PRICE_EURO ELSE 0 END) AS winter_sports_spend,
    SUM(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (2,3) THEN SALES_PRICE_EURO ELSE 0 END) AS summer_sports_spend,
    
    -- Premium sports purchasing
    AVG(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND SALES_PRICE_EURO > 100 THEN 1 ELSE 0 END) AS premium_sports_preference
    
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Payment preferences
payment_prefs AS (
  SELECT 
    CUSTOMER_ID,
    MODE(PAYMENT_METHOD) AS preferred_payment_method,
    COUNT(DISTINCT PAYMENT_METHOD) AS payment_methods_used
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Final enrichment
enriched AS (
  SELECT 
    ld.*,
    cm.total_spend, cm.total_orders, cm.total_items, cm.avg_basket_size, cm.avg_item_price,
    cm.last_purchase_date, cm.days_since_last_purchase, cm.avg_days_between_orders,
    cm.q1_spend, cm.q2_spend, cm.q3_spend, cm.q4_spend,
    
    COALESCE(sb.sports_spend, 0) AS sports_spend,
    COALESCE(sb.fitness_spend, 0) AS fitness_spend,
    COALESCE(sb.outdoor_spend, 0) AS outdoor_spend,
    COALESCE(sb.nutrition_spend, 0) AS nutrition_spend,
    COALESCE(sb.apparel_spend, 0) AS apparel_spend,
    COALESCE(sb.sports_brand_spend, 0) AS sports_brand_spend,
    COALESCE(sb.sports_items, 0) AS sports_items,
    COALESCE(sb.fitness_items, 0) AS fitness_items,
    COALESCE(sb.winter_sports_spend, 0) AS winter_sports_spend,
    COALESCE(sb.summer_sports_spend, 0) AS summer_sports_spend,
    COALESCE(sb.premium_sports_preference, 0) AS premium_sports_preference,
    
    pp.preferred_payment_method, pp.payment_methods_used,
    
    -- Sports affinity ratios (key lookalike features)
    ROUND(COALESCE(sb.sports_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS sports_spend_ratio,
    ROUND(COALESCE(sb.fitness_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS fitness_spend_ratio,
    ROUND(COALESCE(sb.outdoor_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS outdoor_spend_ratio,
    ROUND(COALESCE(sb.nutrition_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS nutrition_spend_ratio,
    ROUND(COALESCE(sb.sports_brand_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS sports_brand_ratio,
    
    -- Composite sports engagement score
    ROUND((COALESCE(sb.sports_spend, 0) + COALESCE(sb.fitness_spend, 0) + COALESCE(sb.outdoor_spend, 0)) / NULLIF(cm.total_spend, 0), 4) AS sports_engagement_score,
    
    -- Age band
    CASE
      WHEN ld.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
      ELSE '55+'
    END AS age_band,
    
    -- Value tier
    CASE
      WHEN cm.total_spend >= 2000 THEN 'VIP'
      WHEN cm.total_spend >= 500 THEN 'High Value'
      WHEN cm.total_spend >= 100 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_tier,
    
    -- Engagement level
    CASE
      WHEN cm.days_since_last_purchase <= 30 THEN 'Highly Active'
      WHEN cm.days_since_last_purchase <= 90 THEN 'Active'
      WHEN cm.days_since_last_purchase <= 180 THEN 'At Risk'
      ELSE 'Lapsed'
    END AS engagement_level
    
  FROM latest_demo ld
  LEFT JOIN customer_metrics cm USING (CUSTOMER_ID)
  LEFT JOIN sports_behavior sb USING (CUSTOMER_ID)
  LEFT JOIN payment_prefs pp USING (CUSTOMER_ID)
)

SELECT * FROM enriched;

-- =============================================================================
-- CROCEVIA MASTER TABLE: Sports-Adjacent Customer Features  
-- =============================================================================

CREATE OR REPLACE TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES AS
WITH
-- Latest customer demographics
latest_demo AS (
  SELECT 
    CUSTOMER_ID,
    FIRST_NAME, LAST_NAME, GENDER, BIRTH_DATE,
    CUSTOMER_POSTCODE AS POSTAL_CODE, LATITUDE, LONGITUDE
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) = 1
),

-- Base customer metrics
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    SUM(SALES_PRICE_EURO) AS total_spend,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    COUNT(*) AS total_items,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(DISTINCT ORDER_ID), 2) AS avg_basket_size,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(*), 2) AS avg_item_price,
    
    -- Recency
    MAX(SALE_DATE) AS last_purchase_date,
    DATEDIFF(day, MAX(SALE_DATE), CURRENT_DATE) AS days_since_last_purchase,
    
    -- Frequency
    DATEDIFF(day, MIN(SALE_DATE), MAX(SALE_DATE)) / NULLIF(COUNT(DISTINCT ORDER_ID) - 1, 0) AS avg_days_between_orders,
    
    -- Store diversity
    COUNT(DISTINCT STORE_ID) AS stores_visited,
    
    -- Seasonality
    SUM(CASE WHEN QUARTER(SALE_DATE) = 1 THEN SALES_PRICE_EURO ELSE 0 END) AS q1_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 2 THEN SALES_PRICE_EURO ELSE 0 END) AS q2_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 3 THEN SALES_PRICE_EURO ELSE 0 END) AS q3_spend,
    SUM(CASE WHEN QUARTER(SALE_DATE) = 4 THEN SALES_PRICE_EURO ELSE 0 END) AS q4_spend
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sports and fitness category analysis (critical for lookalike)
sports_behavior AS (
  SELECT 
    CUSTOMER_ID,
    -- Sports category spend
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%fitness%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%fitness%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS fitness_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%outdoor%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%outdoor%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS outdoor_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%nutrition%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%protein%') 
             OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%energy%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%vitamin%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS nutrition_spend,
    SUM(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%apparel%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%clothing%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS apparel_spend,
    
    -- Sports brand affinity
    SUM(CASE WHEN ILIKE(COALESCE(BRAND, ''), '%nike%') OR ILIKE(COALESCE(BRAND, ''), '%adidas%') OR ILIKE(COALESCE(BRAND, ''), '%decathlon%') 
             OR ILIKE(COALESCE(BRAND, ''), '%salomon%') OR ILIKE(COALESCE(BRAND, ''), '%columbia%') OR ILIKE(COALESCE(BRAND, ''), '%quechua%')
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_brand_spend,
    
    -- Sports item frequency
    COUNT(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%') 
               THEN 1 END) AS sports_items,
    COUNT(CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%fitness%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%fitness%') 
               THEN 1 END) AS fitness_items,
    
    -- Seasonal sports patterns
    SUM(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (1,4) THEN SALES_PRICE_EURO ELSE 0 END) AS winter_sports_spend,
    SUM(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (2,3) THEN SALES_PRICE_EURO ELSE 0 END) AS summer_sports_spend,
    
    -- Premium sports purchasing
    AVG(CASE WHEN (ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%')) 
                  AND SALES_PRICE_EURO > 100 THEN 1 ELSE 0 END) AS premium_sports_preference,
    
    -- Category diversity in sports
    COUNT(DISTINCT CASE WHEN ILIKE(COALESCE(PRODUCT_CATEGORY, ''), '%sport%') OR ILIKE(COALESCE(PRODUCT_SUBCATEGORY, ''), '%sport%') 
                        THEN PRODUCT_SUBCATEGORY END) AS sports_category_breadth
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Payment preferences
payment_prefs AS (
  SELECT 
    CUSTOMER_ID,
    MODE(PAYMENT_METHOD) AS preferred_payment_method,
    COUNT(DISTINCT PAYMENT_METHOD) AS payment_methods_used
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Enriched with calculated features
enriched AS (
  SELECT 
    ld.*,
    cm.total_spend, cm.total_orders, cm.total_items, cm.avg_basket_size, cm.avg_item_price,
    cm.last_purchase_date, cm.days_since_last_purchase, cm.avg_days_between_orders,
    cm.stores_visited, cm.q1_spend, cm.q2_spend, cm.q3_spend, cm.q4_spend,
    
    COALESCE(sb.sports_spend, 0) AS sports_spend,
    COALESCE(sb.fitness_spend, 0) AS fitness_spend,
    COALESCE(sb.outdoor_spend, 0) AS outdoor_spend,
    COALESCE(sb.nutrition_spend, 0) AS nutrition_spend,
    COALESCE(sb.apparel_spend, 0) AS apparel_spend,
    COALESCE(sb.sports_brand_spend, 0) AS sports_brand_spend,
    COALESCE(sb.sports_items, 0) AS sports_items,
    COALESCE(sb.fitness_items, 0) AS fitness_items,
    COALESCE(sb.winter_sports_spend, 0) AS winter_sports_spend,
    COALESCE(sb.summer_sports_spend, 0) AS summer_sports_spend,
    COALESCE(sb.premium_sports_preference, 0) AS premium_sports_preference,
    COALESCE(sb.sports_category_breadth, 0) AS sports_category_breadth,
    
    pp.preferred_payment_method, pp.payment_methods_used,
    
    -- Sports affinity ratios (key lookalike features)
    ROUND(COALESCE(sb.sports_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS sports_spend_ratio,
    ROUND(COALESCE(sb.fitness_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS fitness_spend_ratio,
    ROUND(COALESCE(sb.outdoor_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS outdoor_spend_ratio,
    ROUND(COALESCE(sb.nutrition_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS nutrition_spend_ratio,
    ROUND(COALESCE(sb.sports_brand_spend, 0) / NULLIF(cm.total_spend, 0), 4) AS sports_brand_ratio,
    
    -- Composite sports engagement score
    ROUND((COALESCE(sb.sports_spend, 0) + COALESCE(sb.fitness_spend, 0) + COALESCE(sb.outdoor_spend, 0) + COALESCE(sb.nutrition_spend, 0)) / NULLIF(cm.total_spend, 0), 4) AS sports_engagement_score,
    
    -- Age band
    CASE
      WHEN ld.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, ld.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
      ELSE '55+'
    END AS age_band,
    
    -- Value tier (hypermarket scale)
    CASE
      WHEN cm.total_spend >= 5000 THEN 'VIP'
      WHEN cm.total_spend >= 2000 THEN 'High Value'
      WHEN cm.total_spend >= 500 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_tier,
    
    -- Sports customer classification
    CASE
      WHEN COALESCE(sb.sports_spend, 0) + COALESCE(sb.fitness_spend, 0) + COALESCE(sb.outdoor_spend, 0) >= 200 THEN 'High Sports Affinity'
      WHEN COALESCE(sb.sports_spend, 0) + COALESCE(sb.fitness_spend, 0) + COALESCE(sb.outdoor_spend, 0) >= 50 THEN 'Medium Sports Affinity'
      WHEN COALESCE(sb.sports_spend, 0) + COALESCE(sb.fitness_spend, 0) + COALESCE(sb.outdoor_spend, 0) > 0 THEN 'Low Sports Affinity'
      ELSE 'No Sports Affinity'
    END AS sports_affinity_tier
    
  FROM latest_demo ld
  LEFT JOIN customer_metrics cm USING (CUSTOMER_ID)
  LEFT JOIN sports_behavior sb USING (CUSTOMER_ID)
  LEFT JOIN payment_prefs pp USING (CUSTOMER_ID)
)

SELECT * FROM enriched;

-- =============================================================================
-- LOOKALIKE SEED AND TARGET VIEWS
-- =============================================================================

-- Summit high-value seed audience
CREATE OR REPLACE VIEW SS_101.HARMONIZED.SUMMIT_LOOKALIKE_SEED AS
SELECT *
FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES
WHERE value_tier IN ('VIP', 'High Value')
  AND engagement_level IN ('Highly Active', 'Active')
  AND sports_engagement_score > 0.3;

-- Crocevia sports-affinity target pool  
CREATE OR REPLACE VIEW CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_TARGETS AS
SELECT *
FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES
WHERE sports_affinity_tier IN ('High Sports Affinity', 'Medium Sports Affinity')
  AND value_tier IN ('VIP', 'High Value', 'Medium Value')
  AND days_since_last_purchase <= 180;

-- =============================================================================
-- LOOKALIKE SCORING EXAMPLE
-- =============================================================================

/*
Example lookalike audience creation:

WITH summit_seed_profile AS (
  SELECT 
    AVG(sports_engagement_score) AS avg_sports_engagement,
    AVG(total_spend) AS avg_spend,
    AVG(avg_basket_size) AS avg_basket,
    AVG(stores_visited) AS avg_stores,
    MODE(age_band) AS typical_age_band,
    MODE(gender) AS typical_gender,
    AVG(premium_sports_preference) AS avg_premium_pref
  FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_SEED
),
crocevia_scored AS (
  SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.POSTAL_CODE,
    c.age_band,
    c.value_tier,
    c.sports_engagement_score,
    c.total_spend,
    
    -- Similarity scoring (0-1, higher = more similar to Summit high-value customers)
    ROUND(
      (1 - ABS(c.sports_engagement_score - s.avg_sports_engagement)) * 0.35 +
      (1 - ABS(c.total_spend - s.avg_spend) / GREATEST(c.total_spend, s.avg_spend, 1)) * 0.25 +
      (1 - ABS(c.avg_basket_size - s.avg_basket) / GREATEST(c.avg_basket_size, s.avg_basket, 1)) * 0.20 +
      (CASE WHEN c.age_band = s.typical_age_band THEN 1 ELSE 0 END) * 0.10 +
      (CASE WHEN c.gender = s.typical_gender THEN 1 ELSE 0 END) * 0.10,
      4
    ) AS lookalike_score
    
  FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_TARGETS c
  CROSS JOIN summit_seed_profile s
)
SELECT * FROM crocevia_scored
WHERE lookalike_score >= 0.75
ORDER BY lookalike_score DESC
LIMIT 50000;
*/
