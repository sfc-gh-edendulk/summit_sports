-- Master Tables for Lookalike Audience Creation
-- Creates feature-rich customer profiles for Summit Sports and Crocevia
-- to enable lookalike modeling and cross-brand audience targeting

-- =============================================================================
-- SUMMIT SPORTS MASTER TABLE: High-Value Customer Features
-- =============================================================================

CREATE OR REPLACE TABLE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES AS
WITH 
-- Base customer metrics from orders
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    -- Demographics (from most recent order)
    FIRST_VALUE(FIRST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS FIRST_NAME,
    FIRST_VALUE(LAST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LAST_NAME,
    FIRST_VALUE(GENDER) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS GENDER,
    FIRST_VALUE(BIRTH_DATE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS BIRTH_DATE,
    FIRST_VALUE(CUSTOMER_POSTCODE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS POSTAL_CODE,
    FIRST_VALUE(LATITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LATITUDE,
    FIRST_VALUE(LONGITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LONGITUDE,
    
    -- Purchase behavior
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
    
    -- Store diversity
    COUNT(DISTINCT STOREID) AS stores_visited,
    
    -- Brand diversity
    COUNT(DISTINCT BRAND) AS brands_purchased,
    
    -- Payment preferences
    MODE(PAYMENT_METHOD) AS preferred_payment_method,
    
    -- Price sensitivity (discount vs full price)
    AVG(CASE WHEN SALES_PRICE_EURO < 50 THEN 1 ELSE 0 END) AS low_price_preference,
    AVG(CASE WHEN SALES_PRICE_EURO > 200 THEN 1 ELSE 0 END) AS premium_preference
    
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sport category affinity scores
sport_affinity AS (
  SELECT 
    CUSTOMER_ID,
    -- Sport-specific spend shares
    SUM(CASE WHEN ILIKE(SPORT, '%running%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS running_affinity,
    SUM(CASE WHEN ILIKE(SPORT, '%cycling%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS cycling_affinity,
    SUM(CASE WHEN ILIKE(SPORT, '%winter%') OR ILIKE(SPORT, '%ski%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS winter_sports_affinity,
    SUM(CASE WHEN ILIKE(SPORT, '%team%') OR ILIKE(SPORT, '%football%') OR ILIKE(SPORT, '%basketball%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS team_sports_affinity,
    SUM(CASE WHEN ILIKE(SPORT, '%outdoor%') OR ILIKE(SPORT, '%hiking%') THEN SALES_PRICE_EURO ELSE 0 END) / SUM(SALES_PRICE_EURO) AS outdoor_affinity,
    
    -- Category breadth
    COUNT(DISTINCT SPORT) AS sports_breadth,
    COUNT(DISTINCT PRODUCT_CATEGORY) AS category_breadth
    
  FROM SS_101.HARMONIZED.ORDERS_DT
  WHERE CUSTOMER_ID IS NOT NULL AND SPORT IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Age bands and segments
enriched AS (
  SELECT 
    cm.*,
    sa.running_affinity, sa.cycling_affinity, sa.winter_sports_affinity, 
    sa.team_sports_affinity, sa.outdoor_affinity, sa.sports_breadth, sa.category_breadth,
    
    -- Age band
    CASE
      WHEN cm.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
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
    
  FROM customer_metrics cm
  LEFT JOIN sport_affinity sa USING (CUSTOMER_ID)
)

SELECT * FROM enriched;

-- =============================================================================
-- CROCEVIA MASTER TABLE: Sports-Affinity Customer Features
-- =============================================================================

CREATE OR REPLACE TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES AS
WITH
-- Base customer metrics
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    -- Demographics
    FIRST_VALUE(FIRST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS FIRST_NAME,
    FIRST_VALUE(LAST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LAST_NAME,
    FIRST_VALUE(GENDER) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS GENDER,
    FIRST_VALUE(BIRTH_DATE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS BIRTH_DATE,
    FIRST_VALUE(CUSTOMER_POSTCODE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS POSTAL_CODE,
    FIRST_VALUE(LATITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LATITUDE,
    FIRST_VALUE(LONGITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LONGITUDE,
    
    -- Overall purchase behavior
    SUM(SALES_PRICE_EURO) AS total_spend,
    COUNT(DISTINCT ORDER_ID) AS total_orders,
    COUNT(*) AS total_items,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(DISTINCT ORDER_ID), 2) AS avg_basket_size,
    ROUND(SUM(SALES_PRICE_EURO) / COUNT(*), 2) AS avg_item_price,
    
    -- Recency
    MAX(SALE_DATE) AS last_purchase_date,
    DATEDIFF(day, MAX(SALE_DATE), CURRENT_DATE) AS days_since_last_purchase,
    
    -- Store diversity
    COUNT(DISTINCT STORE_ID) AS stores_visited,
    
    -- Payment preferences
    MODE(PAYMENT_METHOD) AS preferred_payment_method
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sports affinity scoring (key for lookalike)
sports_affinity AS (
  SELECT 
    CUSTOMER_ID,
    -- Sports category spend
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%fitness%') OR ILIKE(PRODUCT_SUBCATEGORY, '%fitness%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS fitness_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%outdoor%') OR ILIKE(PRODUCT_SUBCATEGORY, '%outdoor%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS outdoor_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%nutrition%') OR ILIKE(PRODUCT_SUBCATEGORY, '%protein%') 
             OR ILIKE(PRODUCT_SUBCATEGORY, '%energy%') THEN SALES_PRICE_EURO ELSE 0 END) AS nutrition_spend,
    
    -- Sports category frequency
    COUNT(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%') 
               THEN 1 END) AS sports_items,
    COUNT(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%fitness%') OR ILIKE(PRODUCT_SUBCATEGORY, '%fitness%') 
               THEN 1 END) AS fitness_items,
    
    -- Sports brand affinity (assuming premium sports brands)
    SUM(CASE WHEN ILIKE(BRAND, '%nike%') OR ILIKE(BRAND, '%adidas%') OR ILIKE(BRAND, '%decathlon%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_brand_spend,
    
    -- Seasonality for sports purchases
    SUM(CASE WHEN (ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (1,4) THEN SALES_PRICE_EURO ELSE 0 END) AS winter_sports_spend,
    SUM(CASE WHEN (ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (2,3) THEN SALES_PRICE_EURO ELSE 0 END) AS summer_sports_spend
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Enriched with calculated features
enriched AS (
  SELECT 
    cm.*,
    sa.sports_spend, sa.fitness_spend, sa.outdoor_spend, sa.nutrition_spend,
    sa.sports_items, sa.fitness_items, sa.sports_brand_spend,
    sa.winter_sports_spend, sa.summer_sports_spend,
    
    -- Sports affinity ratios (key lookalike features)
    ROUND(sa.sports_spend / NULLIF(cm.total_spend, 0), 4) AS sports_spend_ratio,
    ROUND(sa.fitness_spend / NULLIF(cm.total_spend, 0), 4) AS fitness_spend_ratio,
    ROUND(sa.outdoor_spend / NULLIF(cm.total_spend, 0), 4) AS outdoor_spend_ratio,
    ROUND(sa.nutrition_spend / NULLIF(cm.total_spend, 0), 4) AS nutrition_spend_ratio,
    ROUND(sa.sports_brand_spend / NULLIF(cm.total_spend, 0), 4) AS sports_brand_ratio,
    
    -- Sports engagement score (composite)
    ROUND((sa.sports_spend + sa.fitness_spend + sa.outdoor_spend) / NULLIF(cm.total_spend, 0), 4) AS sports_engagement_score,
    
    -- Age band
    CASE
      WHEN cm.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
      ELSE '55+'
    END AS age_band,
    
    -- Value tier
    CASE
      WHEN cm.total_spend >= 2000 THEN 'VIP'
      WHEN cm.total_spend >= 500 THEN 'High Value'
      WHEN cm.total_spend >= 100 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_tier,
    
    -- Sports customer flag (for seed audience)
    CASE 
      WHEN sa.sports_spend > 0 OR sa.fitness_spend > 0 OR sa.outdoor_spend > 0 THEN TRUE 
      ELSE FALSE 
    END AS has_sports_affinity,
    
    -- Premium customer flag
    CASE 
      WHEN cm.avg_item_price > 100 AND cm.total_spend > 1000 THEN TRUE 
      ELSE FALSE 
    END AS is_premium_customer
    
  FROM customer_metrics cm
  LEFT JOIN sports_affinity sa USING (CUSTOMER_ID)
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY total_spend DESC) = 1;

-- =============================================================================
-- CROCEVIA MASTER TABLE: Sports-Adjacent Customer Features  
-- =============================================================================

CREATE OR REPLACE TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES AS
WITH
-- Base customer metrics
customer_metrics AS (
  SELECT 
    CUSTOMER_ID,
    -- Demographics
    FIRST_VALUE(FIRST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS FIRST_NAME,
    FIRST_VALUE(LAST_NAME) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LAST_NAME,
    FIRST_VALUE(GENDER) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS GENDER,
    FIRST_VALUE(BIRTH_DATE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS BIRTH_DATE,
    FIRST_VALUE(CUSTOMER_POSTCODE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS POSTAL_CODE,
    FIRST_VALUE(LATITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LATITUDE,
    FIRST_VALUE(LONGITUDE) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE DESC) AS LONGITUDE,
    
    -- Purchase behavior
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
    
    -- Payment preferences
    MODE(PAYMENT_METHOD) AS preferred_payment_method
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Sports and fitness category analysis (critical for lookalike)
sports_behavior AS (
  SELECT 
    CUSTOMER_ID,
    -- Sports category spend
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%fitness%') OR ILIKE(PRODUCT_SUBCATEGORY, '%fitness%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS fitness_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%outdoor%') OR ILIKE(PRODUCT_SUBCATEGORY, '%outdoor%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS outdoor_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%nutrition%') OR ILIKE(PRODUCT_SUBCATEGORY, '%protein%') 
             OR ILIKE(PRODUCT_SUBCATEGORY, '%energy%') OR ILIKE(PRODUCT_SUBCATEGORY, '%vitamin%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS nutrition_spend,
    SUM(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%apparel%') OR ILIKE(PRODUCT_SUBCATEGORY, '%clothing%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS apparel_spend,
    
    -- Sports brand affinity
    SUM(CASE WHEN ILIKE(BRAND, '%nike%') OR ILIKE(BRAND, '%adidas%') OR ILIKE(BRAND, '%decathlon%') 
             OR ILIKE(BRAND, '%salomon%') OR ILIKE(BRAND, '%columbia%') 
             THEN SALES_PRICE_EURO ELSE 0 END) AS sports_brand_spend,
    
    -- Sports item frequency
    COUNT(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%') 
               THEN 1 END) AS sports_items,
    COUNT(CASE WHEN ILIKE(PRODUCT_CATEGORY, '%fitness%') OR ILIKE(PRODUCT_SUBCATEGORY, '%fitness%') 
               THEN 1 END) AS fitness_items,
    
    -- Seasonal sports patterns
    SUM(CASE WHEN (ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (1,4) THEN SALES_PRICE_EURO ELSE 0 END) AS winter_sports_spend,
    SUM(CASE WHEN (ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%')) 
                  AND QUARTER(SALE_DATE) IN (2,3) THEN SALES_PRICE_EURO ELSE 0 END) AS summer_sports_spend,
    
    -- Premium sports purchasing
    AVG(CASE WHEN (ILIKE(PRODUCT_CATEGORY, '%sport%') OR ILIKE(PRODUCT_SUBCATEGORY, '%sport%')) 
                  AND SALES_PRICE_EURO > 100 THEN 1 ELSE 0 END) AS premium_sports_preference
    
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS
  WHERE CUSTOMER_ID IS NOT NULL
  GROUP BY CUSTOMER_ID
),

-- Enriched with calculated features
enriched AS (
  SELECT 
    cm.*,
    sb.sports_spend, sb.fitness_spend, sb.outdoor_spend, sb.nutrition_spend, sb.apparel_spend,
    sb.sports_brand_spend, sb.sports_items, sb.fitness_items,
    sb.winter_sports_spend, sb.summer_sports_spend, sb.premium_sports_preference,
    
    -- Sports affinity ratios (key lookalike features)
    ROUND(sb.sports_spend / NULLIF(cm.total_spend, 0), 4) AS sports_spend_ratio,
    ROUND(sb.fitness_spend / NULLIF(cm.total_spend, 0), 4) AS fitness_spend_ratio,
    ROUND(sb.outdoor_spend / NULLIF(cm.total_spend, 0), 4) AS outdoor_spend_ratio,
    ROUND(sb.nutrition_spend / NULLIF(cm.total_spend, 0), 4) AS nutrition_spend_ratio,
    ROUND(sb.sports_brand_spend / NULLIF(cm.total_spend, 0), 4) AS sports_brand_ratio,
    
    -- Composite sports engagement score
    ROUND((sb.sports_spend + sb.fitness_spend + sb.outdoor_spend + sb.nutrition_spend) / NULLIF(cm.total_spend, 0), 4) AS sports_engagement_score,
    
    -- Age band
    CASE
      WHEN cm.BIRTH_DATE IS NULL THEN 'Unknown'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
      WHEN DATEDIFF(year, cm.BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
      ELSE '55+'
    END AS age_band,
    
    -- Value tier
    CASE
      WHEN cm.total_spend >= 5000 THEN 'VIP'
      WHEN cm.total_spend >= 2000 THEN 'High Value'
      WHEN cm.total_spend >= 500 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_tier,
    
    -- Sports customer classification
    CASE
      WHEN sb.sports_engagement_score >= 0.15 THEN 'High Sports Affinity'
      WHEN sb.sports_engagement_score >= 0.05 THEN 'Medium Sports Affinity'
      WHEN sb.sports_engagement_score > 0 THEN 'Low Sports Affinity'
      ELSE 'No Sports Affinity'
    END AS sports_affinity_tier
    
  FROM customer_metrics cm
  LEFT JOIN sports_behavior sb USING (CUSTOMER_ID)
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY total_spend DESC) = 1;

-- =============================================================================
-- LOOKALIKE SEED AUDIENCE: Summit High-Value Customers
-- =============================================================================

CREATE OR REPLACE VIEW SS_101.HARMONIZED.SUMMIT_LOOKALIKE_SEED AS
SELECT *
FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES
WHERE value_tier IN ('VIP', 'High Value')
  AND engagement_level IN ('Highly Active', 'Active')
  AND sports_engagement_score > 0.5;

-- =============================================================================
-- LOOKALIKE TARGET POOL: Crocevia Sports-Affinity Customers
-- =============================================================================

CREATE OR REPLACE VIEW CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_TARGETS AS
SELECT *
FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES
WHERE sports_affinity_tier IN ('High Sports Affinity', 'Medium Sports Affinity')
  AND value_tier IN ('VIP', 'High Value', 'Medium Value')
  AND days_since_last_purchase <= 180;

-- =============================================================================
-- LOOKALIKE SCORING QUERY (Example)
-- =============================================================================

/*
Example lookalike scoring query - compares Crocevia customers to Summit seed audience
on key behavioral and demographic features:

WITH summit_seed_profile AS (
  SELECT 
    AVG(sports_engagement_score) AS avg_sports_engagement,
    AVG(total_spend) AS avg_spend,
    AVG(avg_basket_size) AS avg_basket,
    AVG(stores_visited) AS avg_stores,
    MODE(age_band) AS typical_age_band,
    MODE(gender) AS typical_gender,
    AVG(premium_preference) AS avg_premium_pref
  FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_SEED
),
crocevia_scored AS (
  SELECT 
    c.*,
    -- Similarity scoring (0-1, higher = more similar)
    (1 - ABS(c.sports_engagement_score - s.avg_sports_engagement)) * 0.3 +
    (1 - ABS(c.total_spend - s.avg_spend) / GREATEST(c.total_spend, s.avg_spend)) * 0.2 +
    (1 - ABS(c.avg_basket_size - s.avg_basket) / GREATEST(c.avg_basket_size, s.avg_basket)) * 0.2 +
    (CASE WHEN c.age_band = s.typical_age_band THEN 1 ELSE 0 END) * 0.15 +
    (CASE WHEN c.gender = s.typical_gender THEN 1 ELSE 0 END) * 0.15 AS lookalike_score
  FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_TARGETS c
  CROSS JOIN summit_seed_profile s
)
SELECT * FROM crocevia_scored
WHERE lookalike_score >= 0.7
ORDER BY lookalike_score DESC
LIMIT 10000;
*/
