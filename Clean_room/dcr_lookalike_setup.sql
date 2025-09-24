-- Data Clean Room Lookalike Setup for Summit Sports x Crocevia
-- Adds required email columns and DCR lookalike configuration

-- =============================================================================
-- 1. ADD EMAIL COLUMNS TO EXISTING LOOKALIKE TABLES
-- =============================================================================

-- Add email to Summit lookalike table
ALTER TABLE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES 
ADD COLUMN EMAIL STRING;

ALTER TABLE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES 
ADD COLUMN HASHED_EMAIL STRING;

-- Populate emails from orders table (latest email per customer)
UPDATE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES 
SET EMAIL = (
  SELECT EMAIL 
  FROM SS_101.HARMONIZED.ORDERS_DT o
  WHERE o.CUSTOMER_ID = SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES.CUSTOMER_ID
    AND o.EMAIL IS NOT NULL
  ORDER BY o.SALE_DATE DESC
  LIMIT 1
);

-- Create hashed email for DCR
UPDATE SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES 
SET HASHED_EMAIL = SHA2(LOWER(TRIM(EMAIL)), 256)
WHERE EMAIL IS NOT NULL;

-- Add email to Crocevia lookalike table
ALTER TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES 
ADD COLUMN EMAIL STRING;

ALTER TABLE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES 
ADD COLUMN HASHED_EMAIL STRING;

-- Populate emails from orders table
UPDATE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES 
SET EMAIL = (
  SELECT EMAIL 
  FROM CROCEVIA_DB.GOLD_DATA.CC_ORDERS o
  WHERE o.CUSTOMER_ID = CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES.CUSTOMER_ID
    AND o.EMAIL IS NOT NULL
  ORDER BY o.SALE_DATE DESC
  LIMIT 1
);

-- Create hashed email for DCR
UPDATE CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES 
SET HASHED_EMAIL = SHA2(LOWER(TRIM(EMAIL)), 256)
WHERE EMAIL IS NOT NULL;

-- =============================================================================
-- 2. DCR LOOKALIKE TEMPLATE SETUP
-- =============================================================================

-- Provider (Summit Sports) adds the lookalike template
CALL samooha_by_snowflake_local_db.provider.add_custom_sql_template(
    'summit_crocevia_cleanroom', 
    'sports_customer_lookalike', 
    $$
    WITH
    -- Training features from provider (Summit) high-value customers
    provider_features AS (
        SELECT
            p.hashed_email,
            array_construct(
                p.sports_engagement_score::float,
                ln(greatest(p.total_spend, 1))::float,  -- log-transform spend
                p.avg_basket_size::float,
                hash(p.age_band)::float,  -- categorical to numeric
                p.premium_sports_preference::float,
                p.sports_category_breadth::float
            ) as features
        FROM identifier({{ source_table[0] }}) as p
        WHERE p.value_tier IN ('VIP', 'High Value')
          AND p.engagement_level IN ('Highly Active', 'Active')
          AND p.hashed_email IS NOT NULL
    ),
    
    -- Labels: 1 for high-value Summit customers, 0 for others
    provider_labels AS (
        SELECT
            p.hashed_email,
            1 as label_value
        FROM identifier({{ source_table[0] }}) as p
        WHERE p.value_tier IN ('VIP', 'High Value')
          AND p.sports_engagement_score > 0.3
          AND p.hashed_email IS NOT NULL
    ),
    
    -- Consumer (Crocevia) features for scoring
    consumer_features AS (
        SELECT
            c.hashed_email,
            array_construct(
                c.sports_engagement_score::float,
                ln(greatest(c.total_spend, 1))::float,
                c.avg_basket_size::float,
                hash(c.age_band)::float,
                c.premium_sports_preference::float,
                c.sports_category_breadth::float
            ) as features
        FROM identifier({{ my_table[0] }}) as c
        WHERE c.sports_engagement_score > 0
          AND c.hashed_email IS NOT NULL
    ),
    
    -- Train lookalike model
    trained_model AS (
        SELECT
            cleanroom.lookalike_train(
                array_agg(pf.features), 
                array_agg(pl.label_value)
            ) as train_result
        FROM provider_features pf
        JOIN provider_labels pl USING (hashed_email)
    ),
    
    -- Score consumer prospects
    scored_prospects AS (
        SELECT
            cleanroom.lookalike_score(
                (SELECT train_result FROM trained_model),
                array_agg(cf.hashed_email),
                array_agg(cf.features)
            ) as score_result
        FROM consumer_features cf
        WHERE cf.hashed_email NOT IN (SELECT hashed_email FROM provider_labels)
    ),
    
    -- Parse results
    final_scores AS (
        SELECT 
            value:email::string as hashed_email,
            value:score::float as lookalike_score
        FROM scored_prospects,
             LATERAL FLATTEN(input => parse_json(score_result))
        WHERE value:score::float >= 0.6  -- minimum similarity threshold
    )
    
    SELECT 
        COUNT(*) as qualified_prospects,
        ROUND(AVG(lookalike_score), 3) as avg_score,
        ROUND(MIN(lookalike_score), 3) as min_score,
        ROUND(MAX(lookalike_score), 3) as max_score
    FROM final_scores;
    $$
);

-- =============================================================================
-- 3. RUN THE LOOKALIKE ANALYSIS
-- =============================================================================

-- Consumer (Crocevia) runs the analysis
CALL samooha_by_snowflake_local_db.consumer.run_analysis(
    'summit_crocevia_cleanroom',
    'sports_customer_lookalike',
    
    -- Consumer table (Crocevia customers with sports affinity)
    ['CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES'],
    
    -- Provider table (Summit high-value customers)
    ['SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES'],
    
    -- No additional parameters needed - logic is in template
    object_construct()
);

-- =============================================================================
-- 4. VALIDATION QUERIES
-- =============================================================================

-- Check email overlap before running DCR
SELECT 
  'Summit' AS source, 
  COUNT(*) AS total_customers,
  COUNT(CASE WHEN HASHED_EMAIL IS NOT NULL THEN 1 END) AS with_email
FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES

UNION ALL

SELECT 
  'Crocevia',
  COUNT(*),
  COUNT(CASE WHEN HASHED_EMAIL IS NOT NULL THEN 1 END)
FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES;

-- Check actual email overlap
SELECT COUNT(*) AS overlapping_emails
FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES s
JOIN CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES c 
  ON s.HASHED_EMAIL = c.HASHED_EMAIL
WHERE s.HASHED_EMAIL IS NOT NULL;

-- Check feature distributions (ensure they're reasonable for ML)
SELECT 
  'Summit' AS source,
  ROUND(AVG(sports_engagement_score), 3) AS avg_sports_score,
  ROUND(AVG(total_spend), 0) AS avg_spend,
  ROUND(AVG(avg_basket_size), 0) AS avg_basket
FROM SS_101.HARMONIZED.SUMMIT_LOOKALIKE_FEATURES
WHERE value_tier IN ('VIP', 'High Value')

UNION ALL

SELECT 
  'Crocevia',
  ROUND(AVG(sports_engagement_score), 3),
  ROUND(AVG(total_spend), 0),
  ROUND(AVG(avg_basket_size), 0)
FROM CROCEVIA_DB.GOLD_DATA.CROCEVIA_LOOKALIKE_FEATURES
WHERE sports_engagement_score > 0;
