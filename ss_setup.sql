CREATE OR REPLACE DATABASE SS_DEV;

CREATE OR REPLACE SCHEMA SS_NOTEBOOKS;


USE ROLE sysadmin;

-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"ss_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "intro"}}';

/*--
 • database, schema and warehouse creation
--*/

-- create ss_101 database
CREATE if not exists DATABASE ss_101;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA ss_101.raw_pos;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA ss_101.raw_customer;

-- create harmonized schema
CREATE OR REPLACE SCHEMA ss_101.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA ss_101.analytics;

-- create warehouses
CREATE OR REPLACE WAREHOUSE ss_de_wh
    WAREHOUSE_SIZE = 'large' -- Large for initial data load - scaled down to XSmall at end of this scripts
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for summit sports';

CREATE OR REPLACE WAREHOUSE ss_dev_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for summit sports';

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS ss_admin
    COMMENT = 'admin for summit sports';
    
CREATE ROLE IF NOT EXISTS ss_data_engineer
    COMMENT = 'data engineer for summit sports';
    
CREATE ROLE IF NOT EXISTS ss_dev
    COMMENT = 'developer for summit sports';
    
-- role hierarchy
GRANT ROLE ss_admin TO ROLE sysadmin;
GRANT ROLE ss_data_engineer TO ROLE ss_admin;
GRANT ROLE ss_dev TO ROLE ss_data_engineer;

-- privilege grants
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE ss_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE ss_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE ss_101 TO ROLE ss_admin;
GRANT USAGE ON DATABASE ss_101 TO ROLE ss_data_engineer;
GRANT USAGE ON DATABASE ss_101 TO ROLE ss_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_dev;

GRANT ALL ON SCHEMA ss_101.raw_pos TO ROLE ss_admin;
GRANT ALL ON SCHEMA ss_101.raw_pos TO ROLE ss_data_engineer;
GRANT ALL ON SCHEMA ss_101.raw_pos TO ROLE ss_dev;

GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_admin;
GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_data_engineer;
GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_dev;

GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_admin;
GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_data_engineer;
GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_dev;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE ss_de_wh TO ROLE ss_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE ss_de_wh TO ROLE ss_admin;
GRANT ALL ON WAREHOUSE ss_de_wh TO ROLE ss_data_engineer;

GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_admin;
GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_data_engineer;
GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_dev;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_pos TO ROLE ss_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_pos TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_pos TO ROLE ss_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_customer TO ROLE ss_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_customer TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA ss_101.raw_customer TO ROLE ss_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_dev;

-- Apply Masking Policy Grants
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE ss_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE ss_data_engineer;
  
-- raw_pos table build
USE ROLE sysadmin;
USE WAREHOUSE ss_de_wh;

/*--
 • file format and stage creation
--*/


CREATE or replace TABLE "SS_RAW"."SS_RAW_SALES"."ss_raw_sales" ( ORDER_ID VARCHAR , STOREID VARCHAR , SALE_DATE DATE , PRODUCT_ID VARCHAR , QUANTITY NUMBER(38, 0) , SALES_PRICE_EURO NUMBER(38, 2) , DISCOUNT_AMOUNT_EURO NUMBER(38, 2) , PAYMENT_METHOD VARCHAR , SALES_ASSISTANT_ID VARCHAR , CUSTOMER_ID VARCHAR , CARD_ID VARCHAR ); 

CREATE TEMP FILE FORMAT "SS_RAW"."SS_RAW_SALES"."temp_file_format_2025-06-13T13:45:40.650Z"
	TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=','
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO; 

COPY INTO "SS_RAW"."SS_RAW_SALES"."ss_raw_sales" 
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
	FROM '@"AZURE_DB"."AZURE_SCHEMA"."AZURE_STAGE"/summitsports/Summit Sports - French Retail Data Set - Examples.csv') 
FILE_FORMAT = '"SS_RAW"."SS_RAW_SALES"."temp_file_format_2025-06-13T13:45:40.650Z"' 
ON_ERROR=ABORT_STATEMENT 
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
/*--
 raw zone table build 
--*/

-- store table build = tb_101.raw_pos.franchise

create or replace table ss_101.raw_pos.magasins as 
    select * except latitude, longitude from "ORGDATACLOUD$INTERNAL$SUMMIT_SPORTS_-_FRENCH_RETAIL_DATA_SET".SPORTS_DATA.SPORTS_STORES;


-- products table build = tb_101.raw_pos.menu
CREATE OR REPLACE TABLE ss_101.raw_pos.referentiels_produit as 
select * except product_url from "ORGDATACLOUD$INTERNAL$SUMMIT_SPORTS_-_FRENCH_RETAIL_DATA_SET".SPORTS_PRODUCT_CATALOGUE;


-- orders table build = tb_101.raw_pos.order_detail 
CREATE OR REPLACE TABLE ss_101.raw_pos.order_detail as 
select * except store_name from "ORGDATACLOUD$INTERNAL$SUMMIT_SPORTS_-_FRENCH_RETAIL_DATA_SET".INSTORE_SALES_DATA_CRM3;


-- customer loyalty table build = tb_101.raw_customer.customer_loyalty

CREATE OR REPLACE TABLE ss_101.raw_customer.customer_loyalty as 
select * except store_name from "ORGDATACLOUD$INTERNAL$SUMMIT_SPORTS_-_FRENCH_RETAIL_DATA_SET".CUSTOMERS;


/*--
 • harmonized view creation
--*/

CREATE OR REPLACE VIEW ss_101.harmonized.orders_v AS
SELECT 
    od.*,
    cl.*,
    m.*,
    rp.*
FROM SS_DEV.SS_NOTEBOOKS.order_detail od
LEFT JOIN SS_DEV.SS_NOTEBOOKS.customer_loyalty cl ON od.customer_id = cl.customer_id
LEFT JOIN SS_DEV.SS_NOTEBOOKS.magasins m ON od.store_id = m.store_id
LEFT JOIN SS_DEV.SS_NOTEBOOKS.referentiels_produit rp ON od.product_id = rp.product_id;

-- orders_v view
CREATE OR REPLACE VIEW ss_101.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM ss_101.raw_pos.order_detail od
JOIN ss_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN ss_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN ss_101.raw_pos.referentiels_produit rp
    ON od.menu_item_id = rp.
JOIN ss_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN ss_101.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN ss_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v view
--créer une vue qui joint à gauche les champs suivants calculés à partir de INSTORE_SALES_DATA_CRM3 sur toute la table customer_loyalty: taille moyenne du panier de fidélité client (somme de (sales price - discount amount) divisée par le nombre d'order id uniques), les dépenses totales (somme de (sales price - discount amount)), le nombre total d'achats (nombre d'order id uniques), la date du dernier achat (sale_date maximale)

CREATE OR REPLACE VIEW SS_DEV.SS_NOTEBOOKS.customer_loyalty_metrics_v AS
SELECT 
    c.*,
    SUM(i.sales_price - i.discount_amount) / COUNT(DISTINCT i.order_id) AS avg_basket_size,
    SUM(i.sales_price - i.discount_amount) AS total_spend,
    COUNT(DISTINCT i.order_id) AS total_purchases,
    MAX(i.sale_date) AS last_purchase_date
FROM SS_DEV.SS_NOTEBOOKS.customer_loyalty c
LEFT JOIN SS_DEV.SS_NOTEBOOKS.INSTORE_SALES_DATA_CRM3 i
    ON c.customer_id = i.customer_id
GROUP BY c.customer_id;

CREATE OR REPLACE DYNAMIC TABLE SS_DEV.SS_NOTEBOOKS.CUSTOMER_LOYALTY_WITH_METRICS_DT
TARGET_LAG = '1 day'
WAREHOUSE = ss_de_wh
AS
SELECT 
    cl.*,
    COALESCE(SUM(isd.sales_price - isd.discount_amount) / COUNT(DISTINCT isd.order_id), 0) AS avg_basket_size,
    COALESCE(SUM(isd.sales_price - isd.discount_amount), 0) AS total_spend,
    COALESCE(COUNT(DISTINCT isd.order_id), 0) AS total_purchases,
    MAX(isd.sale_date) AS last_purchase_date,
    ARRAY_AGG(DISTINCT rp.product_name) WITHIN GROUP (ORDER BY rp.product_name) AS purchased_products
FROM SS_DEV.SS_NOTEBOOKS.customer_loyalty cl
LEFT JOIN SS_DEV.SS_NOTEBOOKS.INSTORE_SALES_DATA_CRM3 isd
    ON cl.customer_id = isd.customer_id
LEFT JOIN SS_DEV.SS_NOTEBOOKS.referentiels_produit rp
    ON isd.product_id = rp.product_id
GROUP BY cl.customer_id;

CREATE OR REPLACE VIEW ss_101.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM ss_101.raw_customer.customer_loyalty cl
JOIN ss_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

/*--
 • analytics view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW ss_101.analytics.orders_v
COMMENT = 'summit sports Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM ss_101.harmonized.orders_v o;

-- customer_loyalty_metrics_v view
CREATE OR REPLACE VIEW ss_101.analytics.customer_loyalty_metrics_v
COMMENT = 'summit sports Customer Loyalty Member Metrics View'
    AS
SELECT * FROM ss_101.harmonized.customer_loyalty_metrics_v;

/*--
 raw zone table load 
--*/

-- country table load
COPY INTO ss_101.raw_pos.country
FROM @ss_101.public.s3load/raw_pos/country/;

-- franchise table load
COPY INTO ss_101.raw_pos.franchise
FROM @ss_101.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO ss_101.raw_pos.location
FROM @ss_101.public.s3load/raw_pos/location/;

-- menu table load
COPY INTO ss_101.raw_pos.menu
FROM @ss_101.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO ss_101.raw_pos.truck
FROM @ss_101.public.s3load/raw_pos/truck/;

-- customer_loyalty table load
COPY INTO ss_101.raw_customer.customer_loyalty
FROM @ss_101.public.s3load/raw_customer/customer_loyalty/;

-- order_header table load
COPY INTO ss_101.raw_pos.order_header
FROM @ss_101.public.s3load/raw_pos/order_header/;

-- order_detail table load
COPY INTO ss_101.raw_pos.order_detail
FROM @ss_101.public.s3load/raw_pos/order_detail/;

ALTER WAREHOUSE ss_de_wh SET WAREHOUSE_SIZE = 'XSmall';

-- setup completion note
SELECT 'ss_101 setup is now complete' AS note;