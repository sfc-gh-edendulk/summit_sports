--------------tasty bytes transfo

--Step 1 - Create a Clone of Production
USE ROLE tb_dev;
USE DATABASE tb_101;

CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck;
--Step 1 - Querying our Cloned Table
USE WAREHOUSE tb_dev_wh;

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM raw_pos.truck_dev t
ORDER BY t.truck_id;

--Step 2 - Using Persisted Query Results
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model, --> Snowflake supports Trailing Comma's in SELECT clauses
FROM raw_pos.truck_dev t
ORDER BY t.truck_id;

--Step 1 - Updating Incorrect Values in a Column

UPDATE raw_pos.truck_dev 
    SET make = 'Ford' WHERE make = 'Ford_';

--Step 2 - Constructing our Truck Type Column

SELECT
    truck_id,
    year,
    make,
    model,
    CONCAT(year,' ',make,' ',REPLACE(model,' ','_')) AS truck_type
FROM raw_pos.truck_dev;

ALTER TABLE raw_pos.truck_dev 
    ADD COLUMN truck_type VARCHAR(100);

UPDATE raw_pos.truck_dev
    SET truck_type =  CONCAT(year,make,' ',REPLACE(model,' ','_'));

--Step 5 - Querying our new Column
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;

--Step 1 - Leveraging Query History

SELECT
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(information_schema.query_history())
WHERE 1=1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%raw_pos.truck_dev%'
ORDER BY start_time DESC;

--Step 2 - Setting a SQL Variable

SET query_id =
    (
    SELECT TOP 1
        query_id
    FROM TABLE(information_schema.query_history())
    WHERE 1=1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_type =%'
    ORDER BY start_time DESC
    );

    --Step 3 - Leveraging Time-Travel to Revert our Table

    SELECT 
    truck_id,
    make,
    truck_type
FROM raw_pos.truck_dev
BEFORE(STATEMENT => $query_id)
ORDER BY truck_id;

CREATE OR REPLACE TABLE raw_pos.truck_dev
    AS
SELECT * FROM raw_pos.truck_dev
BEFORE(STATEMENT => $query_id); -- revert to before a specified Query ID ran

UPDATE raw_pos.truck_dev t
    SET truck_type = CONCAT(t.year,' ',t.make,' ',REPLACE(t.model,' ','_'));

--Step 1 - Table Swap

USE ROLE accountadmin;

ALTER TABLE raw_pos.truck_dev 
    SWAP WITH raw_pos.truck;

--Step 2 - Validate Production

SELECT
    t.truck_id,
    t.truck_type
FROM raw_pos.truck t
WHERE t.make = 'Ford';

--Step 4 - Dropping a Table

DROP TABLE raw_pos.truck;

--Uh oh!! That result set shows that even our accountadmin can make mistakes. We incorrectly dropped production truck and not development truck_dev! Thankfully, Snowflake's Time-Travel can come to the rescue again.

UNDROP TABLE raw_pos.truck;

DROP TABLE raw_pos.truck_dev;