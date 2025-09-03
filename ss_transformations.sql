-- add lat and long to sports stores
-- verify poscode
--





---- STORE CATALOGUE ----------

---PREVIEW -----

select * from SPORTS_DB.SPORTS_DATA.SPORTS_STORES limit 3;

-- CHANGE STORE NAMES
UPDATE SPORTS_DB.SPORTS_DATA.SPORTS_STORE_CATALOGUE 
SET STORE_NAME = REPLACE(STORE_NAME, 'INTERSPORT', 'SUMMITSPORT')
WHERE STORE_NAME LIKE '%INTERSPORT%';

--- ADD STORE ID
ALTER TABLE SPORTS_DB.SPORTS_DATA.SPORTS_STORES ADD COLUMN STOREID VARCHAR;

UPDATE SPORTS_DB.SPORTS_DATA.SPORTS_STORES
SET STOREID = SUBSTR(STORE_TYPE, 1, 1) || POSTCODE || SUBSTR(REGEXP_SUBSTR(ADDRESS, '\\d{5}\\s+([A-Za-z])'), 7, 1);

SELECT
    REGEXP_SUBSTR(ADDRESS, '\\d{5}') AS POSTCODE,
    IFF(REGEXP_SUBSTR(ADDRESS, '\\d{5}\\s+([A-Za-z])') IS NULL, '', SUBSTR(REGEXP_SUBSTR(ADDRESS, '\\d{5}\\s+([A-Za-z])'), 7, 1)) AS NEXT_LETTER,
    POSTCODE || NEXT_LETTER AS POSTCODE_AND_NEXT_LETTER,
    SUBSTR(STORE_TYPE, 1, 1) AS STORE_TYPE_FIRST_LETTER,
    POSTCODE || NEXT_LETTER || SUBSTR(STORE_TYPE, 1, 1) AS FINAL_CONCATENATED_STRING
FROM
    SPORTS_DB.SPORTS_DATA.SPORTS_STORES;

SELECT  SUBSTR(STORE_TYPE, 1, 1) || POSTCODE || SUBSTR(REGEXP_SUBSTR(ADDRESS, '\\d{5}\\s+([A-Za-z])'), 7, 1)
FROM
    SPORTS_DB.SPORTS_DATA.SPORTS_STORES;

--- ADD POSTCODE
ALTER TABLE SPORTS_DB.SPORTS_DATA.SPORTS_STORES ADD COLUMN POSTCODE VARCHAR;

UPDATE SPORTS_DB.SPORTS_DATA.SPORTS_STORES
SET POSTCODE = REGEXP_SUBSTR(ADDRESS, '\\d{5}');

SELECT REGEXP_SUBSTR(ADDRESS, '\\d{5}') AS POSTCODE
FROM SPORTS_DB.SPORTS_DATA.SPORTS_STORES;


---- PRODUCT CATALOGUE ----------

---PREVIEW -----

select * from SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE limit 15;

--- ADD CATEGORY

ALTER TABLE SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE ADD COLUMN PRODUCT_CATEGORY VARCHAR;
ALTER TABLE SPORTS_DB.SPORTS_DATA.SPORTS_STORES drop COLUMN PRODUCT_CATEGORY;

SELECT DESCRIPTION, SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
        CONCAT('''give me the category from this product, respond only with the category for example Men''s Jackets or Women''s Pants''', DESCRIPTION)
) FROM SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE
limit 15;

UPDATE SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE
SET PRODUCT_CATEGORY = SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
CONCAT('''give me the category from this product, respond only with the category for example Men''s Jackets or Women''s Pants''', DESCRIPTION));

--- ADD PRODUCT CATEGORY ID

SELECT product_name,
    UPPER(
        ARRAY_TO_STRING(
            ARRAY_CAT(
                ARRAY_AGG(LEFT(VALUE, 1)), 
                ARRAY_CONSTRUCT(
                    LEFT(SPLIT(product_name, ' ')[ARRAY_SIZE(SPLIT(product_name, ' ')) - 1], 
                    GREATEST(0, 3 - COUNT(*)))
                )
            ), ''
        )
    ) AS short_code, 
    LEFT(
        RPAD(UPPER(short_code), 10, CAST(UNIFORM(0, 9, RANDOM()) AS STRING)),
        10
    ) AS padded_string
FROM SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE , LATERAL FLATTEN(INPUT => SPLIT(product_name, ' ')) 
GROUP BY product_name;

SELECT product_name,
    UPPER(
        ARRAY_TO_STRING(
            ARRAY_CAT(
                ARRAY_AGG(LEFT(VALUE, 1)), 
                ARRAY_CONSTRUCT(
                    LEFT(SPLIT(product_name, ' ')[ARRAY_SIZE(SPLIT(product_name, ' ')) - 1], 
                    GREATEST(0, 3 - COUNT(*)))
                )
            ), ''
        )
    ) AS raw_short_code, 
    
    -- Remove any special characters to keep only A-Z and 0-9
    REGEXP_REPLACE(
        UPPER(
            ARRAY_TO_STRING(
                ARRAY_CAT(
                    ARRAY_AGG(LEFT(VALUE, 1)), 
                    ARRAY_CONSTRUCT(
                        LEFT(SPLIT(product_name, ' ')[ARRAY_SIZE(SPLIT(product_name, ' ')) - 1], 
                        GREATEST(0, 3 - COUNT(*)))
                    )
                ), ''
            )
        ), '[^A-Z0-9]', '', 1, 0, 'i'
    ) AS short_code, 
    
    -- Pad short_code with random digits to ensure a length of 10
    LEFT(
        RPAD(
            REGEXP_REPLACE(
                UPPER(
                    ARRAY_TO_STRING(
                        ARRAY_CAT(
                            ARRAY_AGG(LEFT(VALUE, 1)), 
                            ARRAY_CONSTRUCT(
                                LEFT(SPLIT(product_name, ' ')[ARRAY_SIZE(SPLIT(product_name, ' ')) - 1], 
                                GREATEST(0, 3 - COUNT(*)))
                            )
                        ), ''
                    )
                ), '[^A-Z0-9]', '', 1, 0, 'i'
            ), 
            10, 
            CAST(UNIFORM(0, 9, RANDOM()) AS STRING)
        ), 
        10
    ) AS padded_string

FROM SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE
LATERAL FLATTEN(INPUT => SPLIT(product_name, ' ')) 
GROUP BY product_name;

--- ADD PRODUCT ID

ALTER TABLE SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE ADD COLUMN PRODUCTID VARCHAR;
SELECT 
    1000 + ROW_NUMBER() OVER (ORDER BY RANDOM()) as unique_random_number
FROM SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE;

select FLOOR(power(RANDom()/1000000000000, 2));

SELECT CONCAT(
    UPPER(SUBSTRING(REPLACE(PRODUCT_CATEGORY, ' ', ''), 1, 3)),
    '-',
    LPAD(FLOOR(power(RANDom()/1000000000000, 2)), 4, '0')
) as id FROM SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE;

UPDATE SPORTS_DB.SPORTS_DATA.SPORTS_PRODUCT_CATALOGUE
SET productid = CONCAT(
    UPPER(SUBSTRING(REPLACE(PRODUCT_CATEGORY, ' ', ''), 1, 3)),
    '-',
    LPAD(FLOOR(power(RANDom()/1000000000000, 2)), 4, '0')
);

------

select count(distinct customer_id), count(distinct order_id)  from SPORTS_DB.SPORTS_DATA.instore_sales_data_france;

select sale_date, sum(sales_price_euro) as totalsales from SPORTS_DB.SPORTS_DATA.instore_sales_data_INDEXED
group by sale_date;


UPDATE SPORTS_DB.SPORTS_DATA.CHDRY0120_0325
SET DATE = 
    CASE 
        WHEN LEFT(DATE, 1) = '0' 
        THEN CONCAT('2', RIGHT(DATE, LENGTH(DATE)-1))
        ELSE DATE
    END;
    
---ADDING MISSING DATA

CREATE OR REPLACE TABLE SPORTS_DB.SPORTS_DATA.CHDRY0120_0325_COMPLETE AS
WITH date_sequence AS (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
        (SELECT MIN(DATE) FROM SPORTS_DB.SPORTS_DATA.CHDRY0120_0325)) as date_column
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
    QUALIFY date_column <= (SELECT MAX(DATE) FROM SPORTS_DB.SPORTS_DATA.CHDRY0120_0325)
),
averages AS (
    SELECT 
        AVG(OPEN) as avg_open,
        AVG(HIGH) as avg_high,
        AVG(LOW) as avg_low,
        AVG(CLOSE) as avg_close,
        AVG(VOLUME) as avg_volume
    FROM SPORTS_DB.SPORTS_DATA.CHDRY0120_0325
)
SELECT 
    d.date_column as DATE,
    COALESCE(t.OPEN, avg_open * (1 + (UNIFORM(0.10, 0.50, RANDOM()))) ) as OPEN,
    COALESCE(t.HIGH, avg_high * (1 + (UNIFORM(0.10, 0.40, RANDOM()))) ) as HIGH,
    COALESCE(t.LOW, avg_low * (1 + (UNIFORM(0.10, 0.30, RANDOM()))) ) as LOW,
    COALESCE(t.CLOSE, avg_close * (1 + (UNIFORM(0.10, 0.50, RANDOM()))) ) as CLOSE,
    COALESCE(t.VOLUME, avg_volume * (1 + (UNIFORM(0.10, 0.30, RANDOM()))) ) as VOLUME
FROM date_sequence d
LEFT JOIN SPORTS_DB.SPORTS_DATA.CHDRY0120_0325 t ON d.date_column = t.DATE
CROSS JOIN averages a
ORDER BY d.date_column;

----- generate data stored procedure
call SPORTS_DB.SPORTS_DATA.SPORTS_RETAILER_SALES_GENERATOR_CRM_V2('2024-2-28'::DATE, 58, 'append');

select * from snowflake.telemetry.events order by timestamp desc limit 10;

---- ingestion

create or replace schema sports_db.sports_ingestion;

COPY INTO "SPORTS_DB"."SPORTS_INGESTION"."sports_ingestion3a"
FROM (
    SELECT $1:_COMPUTATION_DATETIME::TIMESTAMP_NTZ, $1:_IMPORT_DATETIME::TIMESTAMP_NTZ, $1:COLUMN_FAMILY::VARCHAR, $1:METRIC::VARCHAR, $1:ROWKEY::VARCHAR, $1:VALUE::NUMBER(38, 0), $1:APP_ID_FIRSTCHAR::VARCHAR, $1:APPLICATION_ID::VARCHAR, $1:INDEX_NAME::VARCHAR, $1:YEAR::NUMBER(38, 0), $1:MONTH::NUMBER(38, 0), $1:DAY::NUMBER(38, 0), $1:METRIC_DATETIME::TIMESTAMP_NTZ, $1:YEAR_MONTH_DAY::VARCHAR
    FROM '@"S3_DB"."PURCHASE_DUMPS3"."DUMP_PARQUET_STAGES3"/TEST251024.snappy.parquet/'
)
PATTERN = '.*.parquet'
FILE_FORMAT = (
    TYPE=PARQUET,
    REPLACE_INVALID_CHARACTERS=TRUE,
    BINARY_AS_TEXT=FALSE
)
ON_ERROR=CONTINUE;

CREATE OR REPLACE TABLE "SPORTS_DB"."SPORTS_INGESTION"."sports_ingestion3a" ( _COMPUTATION_DATETIME TIMESTAMP_NTZ , _IMPORT_DATETIME TIMESTAMP_NTZ , COLUMN_FAMILY VARCHAR , METRIC VARCHAR , ROWKEY VARCHAR , VALUE NUMBER(38, 0) , APP_ID_FIRSTCHAR VARCHAR , APPLICATION_ID VARCHAR , INDEX_NAME VARCHAR , YEAR NUMBER(38, 0) , MONTH NUMBER(38, 0) , DAY NUMBER(38, 0) , METRIC_DATETIME TIMESTAMP_NTZ , YEAR_MONTH_DAY VARCHAR ); 

----- transformation

create or replace schema sports_db.sports_transformation;

create or replace materialized view instore_sales_france_daily_aggregated_crm3 as 
select SALE_DATE, sum(SALES_PRICE_EURO) as daily_sales
from SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_INDEXED
group by SALE_DATE
ORDER BY SALE_DATE DESC;

create or replace table sports_db.sports_transformation.crm_indexed_aggregated as 
SELECT 
    c.*,
    COALESCE(s.number_of_purchases, 0) as number_of_purchases,
    COALESCE(s.total_purchase_value, 0) as total_purchase_value,
    COALESCE(s.avg_purchase_value, 0) as avg_purchase_value,
    s.last_purchase_date,
    s.top_product
FROM SPORTS_DB.SPORTS_DATA.CUSTOMERS c
LEFT JOIN (
    SELECT 
        CUSTOMER_ID,
        COUNT(DISTINCT ORDER_ID) as number_of_purchases,
        SUM(SALES_PRICE_EURO - COALESCE(DISCOUNT_AMOUNT_EURO, 0)) as total_purchase_value,
        SUM(SALES_PRICE_EURO - COALESCE(DISCOUNT_AMOUNT_EURO, 0)) / COUNT(DISTINCT ORDER_ID) as avg_purchase_value,
        MAX(SALE_DATE) as last_purchase_date,
        FIRST_VALUE(PRODUCT_ID) OVER (
            PARTITION BY CUSTOMER_ID 
            ORDER BY COUNT(*) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as top_product
    FROM SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_CRM3
    GROUP BY CUSTOMER_ID
) s ON c.CUSTOMER_ID = s.CUSTOMER_ID;


select distinct sale_date from SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_CRM3 order by sale_date;



select * from sports_db.sports_transformation.crm_indexed_aggregated limit 10000;