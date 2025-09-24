-- Demo AI_AGG Queries for Summit Sports Reviews Analytics
-- These queries demonstrate the power of Snowflake's AI_AGG function
-- Run these after setting up your data to see AI insights in action

-- Set your context
USE SCHEMA SS_101.RAW_CUSTOMER;

-- Query 1: Overall Customer Sentiment Analysis
-- This provides a comprehensive overview of customer experiences
SELECT AI_AGG(
    REVIEW_TEXT, 
    'Analyze customer sentiment for this sports retailer. Provide a comprehensive summary highlighting key themes, overall satisfaction levels, and main customer experiences mentioned in the reviews.'
) as overall_sentiment_analysis
FROM INTERSPORT_REVIEWS
WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 2: Positive Reviews - What Customers Love
-- Identifies the retailer's main strengths
SELECT AI_AGG(
    REVIEW_TEXT,
    'Focus on positive reviews only. Identify the top 5 strengths that customers appreciate most about this sports retailer. Include specific mentions of services, products, or experiences that delight customers.'
) as customer_loves
FROM INTERSPORT_REVIEWS
WHERE RATING >= 4 AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 3: Areas for Improvement
-- Provides actionable insights from negative feedback
SELECT AI_AGG(
    REVIEW_TEXT,
    'Analyze negative reviews to identify the top 3 main issues customers complain about. Provide specific, actionable recommendations for improvement that the business could implement.'
) as improvement_recommendations
FROM INTERSPORT_REVIEWS
WHERE RATING <= 2 AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 4: Delivery and Logistics Analysis
-- Focuses specifically on shipping and delivery experiences
SELECT AI_AGG(
    REVIEW_TEXT,
    'Focus specifically on delivery, shipping, logistics, and order fulfillment experiences. Summarize customer satisfaction with delivery services, speed, packaging, and any delivery-related issues.'
) as delivery_analysis
FROM INTERSPORT_REVIEWS
WHERE (LOWER(REVIEW_TEXT) LIKE '%livraison%' 
    OR LOWER(REVIEW_TEXT) LIKE '%delivery%' 
    OR LOWER(REVIEW_TEXT) LIKE '%envoi%'
    OR LOWER(REVIEW_TEXT) LIKE '%colis%'
    OR LOWER(REVIEW_TEXT) LIKE '%expedition%')
AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 5: Store vs Online Experience Comparison
-- Compares in-store vs online shopping experiences
SELECT 
    CASE 
        WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Online Shopping'
        ELSE 'Physical Store'
    END as channel_type,
    AI_AGG(
        REVIEW_TEXT,
        'Summarize the customer experience for this sales channel. Focus on service quality, convenience, product availability, and overall satisfaction. Compare strengths and weaknesses.'
    ) as channel_experience_summary
FROM INTERSPORT_REVIEWS
WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != ''
GROUP BY channel_type;

-- Query 6: Product-Related Insights
-- Analyzes feedback specifically about products
SELECT AI_AGG(
    REVIEW_TEXT,
    'Focus on product-related feedback. Analyze mentions of product quality, variety, pricing, sizing, and availability. Identify which product categories receive the most positive and negative feedback.'
) as product_insights
FROM INTERSPORT_REVIEWS
WHERE (LOWER(REVIEW_TEXT) LIKE '%produit%' 
    OR LOWER(REVIEW_TEXT) LIKE '%article%'
    OR LOWER(REVIEW_TEXT) LIKE '%qualité%'
    OR LOWER(REVIEW_TEXT) LIKE '%prix%'
    OR LOWER(REVIEW_TEXT) LIKE '%taille%'
    OR LOWER(REVIEW_TEXT) LIKE '%chaussure%'
    OR LOWER(REVIEW_TEXT) LIKE '%vetement%')
AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 7: Customer Service Analysis
-- Focuses on customer service experiences
SELECT AI_AGG(
    REVIEW_TEXT,
    'Analyze customer service experiences mentioned in the reviews. Focus on staff helpfulness, responsiveness, professionalism, and problem resolution. Identify both excellent service examples and areas needing improvement.'
) as customer_service_analysis
FROM INTERSPORT_REVIEWS
WHERE (LOWER(REVIEW_TEXT) LIKE '%service%' 
    OR LOWER(REVIEW_TEXT) LIKE '%accueil%'
    OR LOWER(REVIEW_TEXT) LIKE '%personnel%'
    OR LOWER(REVIEW_TEXT) LIKE '%conseill%'
    OR LOWER(REVIEW_TEXT) LIKE '%staff%')
AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 8: Seasonal/Promotional Insights
-- Analyzes feedback related to sales, promotions, and seasonal shopping
SELECT AI_AGG(
    REVIEW_TEXT,
    'Focus on mentions of sales, promotions, discounts, and seasonal shopping experiences. Analyze customer satisfaction with pricing strategies, sale events, and promotional offers.'
) as promotional_insights
FROM INTERSPORT_REVIEWS
WHERE (LOWER(REVIEW_TEXT) LIKE '%solde%' 
    OR LOWER(REVIEW_TEXT) LIKE '%promotion%'
    OR LOWER(REVIEW_TEXT) LIKE '%reduction%'
    OR LOWER(REVIEW_TEXT) LIKE '%remise%'
    OR LOWER(REVIEW_TEXT) LIKE '%prix%'
    OR LOWER(REVIEW_TEXT) LIKE '%promo%')
AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Query 9: Top Performing Store Locations
-- Analyzes the best performing physical store locations
SELECT 
    STORE_LOCATION,
    COUNT(*) as review_count,
    AVG(RATING) as avg_rating,
    AI_AGG(
        REVIEW_TEXT,
        'Summarize what makes this store location successful. Highlight specific strengths, customer experiences, and factors that contribute to customer satisfaction at this location.'
    ) as success_factors
FROM INTERSPORT_REVIEWS
WHERE STORE_LOCATION IS NOT NULL 
    AND STORE_LOCATION != ''
    AND REVIEW_TEXT IS NOT NULL 
    AND REVIEW_TEXT != ''
    AND RATING >= 4
GROUP BY STORE_LOCATION
HAVING COUNT(*) >= 3  -- Only locations with multiple positive reviews
ORDER BY avg_rating DESC, review_count DESC
LIMIT 5;

-- Query 10: Competitive Analysis Insights
-- Identifies what customers compare this retailer to
SELECT AI_AGG(
    REVIEW_TEXT,
    'Identify any mentions of competitors or comparisons to other retailers. Analyze what customers say about how this sports retailer compares to alternatives in terms of pricing, service, product selection, and overall value.'
) as competitive_insights
FROM INTERSPORT_REVIEWS
WHERE (LOWER(REVIEW_TEXT) LIKE '%decathlon%' 
    OR LOWER(REVIEW_TEXT) LIKE '%nike%'
    OR LOWER(REVIEW_TEXT) LIKE '%adidas%'
    OR LOWER(REVIEW_TEXT) LIKE '%sport 2000%'
    OR LOWER(REVIEW_TEXT) LIKE '%go sport%'
    OR LOWER(REVIEW_TEXT) LIKE '%concurrent%'
    OR LOWER(REVIEW_TEXT) LIKE '%comparison%'
    OR LOWER(REVIEW_TEXT) LIKE '%moins cher%'
    OR LOWER(REVIEW_TEXT) LIKE '%plus cher%')
AND REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != '';

-- Bonus Query: Sentiment by Rating Analysis
-- Validates how well ratings align with actual text sentiment
SELECT 
    RATING,
    COUNT(*) as review_count,
    AI_AGG(
        REVIEW_TEXT,
        'Analyze the sentiment and tone of these reviews. Determine if the written feedback aligns with the numerical rating given. Identify any discrepancies or surprising insights.'
    ) as sentiment_analysis
FROM INTERSPORT_REVIEWS
WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != ''
GROUP BY RATING
ORDER BY RATING DESC; 