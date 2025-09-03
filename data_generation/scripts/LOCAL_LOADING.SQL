
create or replace schema ss_101.source_data;

create or replace stage ss_101.source_data.source_data_stage;

REMOVE @ss_101.source_data.source_data_stage/adresses-france.csv.gz ;

put file:///Users/edendulk/code/crocevia/data/adresses-france.csv.gz @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/webscraping/output/french_death_records.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/carrefour_stores_france.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/carrefour_products.csv @ss_101.source_data.source_data_stage;

put file:///Users/edendulk/code/crocevia/data/decathlon_stores_france.csv @ss_101.source_data.source_data_stage;