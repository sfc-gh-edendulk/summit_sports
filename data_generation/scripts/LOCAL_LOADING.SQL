
create or replace schema ss_101.source_data;

create or replace stage ss_101.source_data.source_data_stage;

REMOVE @ss_101.source_data.source_data_stage/adresses-france.csv.gz ;

put file:///Users/edendulk/code/crocevia/data/adresses-france.csv.gz @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/webscraping/output/french_death_records.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/carrefour_stores_france.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/carrefour_products.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/decathlon_stores_france.csv @ss_101.source_data.source_data_stage;

USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  PUT file:///Users/edendulk/code/summit_sports/data_generation/scripts/dual_crm_generator.py
      @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE
      OVERWRITE=TRUE
      AUTO_COMPRESS=FALSE;

   USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  CREATE OR REPLACE PROCEDURE SS_101.SOURCE_DATA.DUAL_CRM_RUN(CROCEVIA_ROWS NUMBER, SUMMIT_ROWS NUMBER)
  RETURNS TABLE (
    SOURCE STRING,
    CUSTOMER_ID STRING,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    GENDER STRING,
    BIRTH_DATE DATE,
    EMAIL STRING,
    PHONE STRING,
    STREET STRING,
    POSTAL_CODE STRING,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    OVERLAP_TYPE STRING
  )
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
  IMPORTS = ('@SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/dual_crm_generator.py')
  HANDLER = 'dual_crm_generator.run'
  EXECUTE AS OWNER;
  USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  CALL SS_101.SOURCE_DATA.DUAL_CRM_RUN(10000, 5000);   

  USE ROLE ss_admin;
  USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  CALL SS_101.SOURCE_DATA.DUAL_CRM_RUN(10000, 5000);

USE DATABASE SS_101;
  USE SCHEMA SS_101.SOURCE_DATA;
ALTER TABLE IF EXISTS "SS_101.SOURCE_DATA.CROCEVIA_CRM" RENAME TO CROCEVIA_CRM;
  ALTER TABLE IF EXISTS "SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM" RENAME TO SUMMIT_SPORTS_CRM;
  SELECT COUNT(*) AS CROCEVIA_COUNT FROM SS_101.SOURCE_DATA.CROCEVIA_CRM;
  SELECT COUNT(*) AS SUMMIT_COUNT   FROM SS_101.SOURCE_DATA.SUMMIT_SPORTS_CRM;

  select * from ss_101.source_data.crocevia_crm limit 1000;
  select * from ss_101.source_data.summit_sports_crm limit 1000;

    USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  CALL SS_101.SOURCE_DATA.DUAL_CRM_RUN(6000000, 2000000);

  -- Download CROCEVIA_CRM table to CSV file
  USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  USE DATABASE SS_101;
  USE SCHEMA SOURCE_DATA;
  
  -- Create a file format for CSV export
  CREATE OR REPLACE FILE FORMAT csv_export_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;
  
  -- Export CROCEVIA_CRM table to CSV
  COPY INTO @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/crocevia_crm_export.csv
  FROM (
    SELECT 
      SOURCE,
      CUSTOMER_ID,
      FIRST_NAME,
      LAST_NAME,
      GENDER,
      BIRTH_DATE,
      EMAIL,
      PHONE,
      STREET,
      POSTAL_CODE,
      LATITUDE,
      LONGITUDE,
      OVERLAP_TYPE
    FROM SS_101.SOURCE_DATA.CROCEVIA_CRM
  )
  FILE_FORMAT = (FORMAT_NAME = 'csv_export_format')
  HEADER = TRUE
  OVERWRITE = TRUE;
  
  -- Download the file to local directory
  GET @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/crocevia_crm_export.csv 
    file:///Users/edendulk/code/crocevia/data/;


USE WAREHOUSE SS_DATAINGEST_GEN_SPO;
  PUT file:///Users/edendulk/code/summit_sports/data_generation/scripts/sales_sp_generator.py
      @SS_101.SOURCE_DATA.SOURCE_DATA_STAGE
      OVERWRITE=TRUE
      AUTO_COMPRESS=FALSE;

CREATE OR REPLACE PROCEDURE SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(
  START_YEAR NUMBER, END_YEAR NUMBER, START_MONTH NUMBER, END_MONTH NUMBER
)
RETURNS TABLE (
  ORDER_ID STRING,
  STOREID STRING,
  SALE_DATE DATE,
  PRODUCT_ID STRING,
  QUANTITY NUMBER,
  SALES_PRICE_EURO FLOAT,
  PAYMENT_METHOD STRING,
  CUSTOMER_ID STRING
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
IMPORTS = ('@SS_101.SOURCE_DATA.SOURCE_DATA_STAGE/sales_sp_generator.py')
HANDLER = 'sales_sp_generator.run'
EXECUTE AS OWNER;

call ss_101.source_data.ss_new_sales_gen_run(2025, 2025, 4, 5);

SELECT COUNT(*) FROM SS_101.SOURCE_DATA.SUMMIT_SPORTS_SALES;

SELECT COUNT(*) 
FROM SS_101.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='SOURCE_DATA' AND TABLE_NAME='SUMMIT_SPORTS_SALES';

CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 1,2);

CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2025, 2025, 6, 6);

CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 12, 12);
CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 11, 11);
CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 10, 10);
CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 9, 9);
CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 8, 8);
CALL SS_101.SOURCE_DATA.SS_NEW_SALES_GEN_RUN(2024, 2024, 7, 7);
