# Project Purpose: 

Demonstrate adeptness in designing Airflow DAGs, custom Airflow operators, and general 
SQL knowledge.

## Scenario: 

Sparkify's analysts wish to run an hourly scan of their log files to answer some questions regarding the songs that
are listened to, the metadata surrounding the artists, and the users listening to those songs. 
It is up to the engineer to design and rationalize their decisions regarding both schema and methodologies. 

Since one of the most popular schemas for data warehouses is a star schema, it seemed to be the most logical choice 
considering how little information is being collected to justify a snowflake schema. Airflow was literally built for 
situations like this, with the ability to adjust the DAG to backfill time data to better enable tasks to run 
in parallel across the cluster. Although the analysts did not specify backfill, this would be a great addition to 
implement in the future. 

Note, although we could have used subDAGs to bundle in all of the loads from the fact table to the corresponding 
dimension tables, the programmer deemed that to be unnecessary, as doing so would compromise data lineage thanks to 
abstraction. 


### What's in the directories: 
- dags/
    - Contains the DAG that will run through Airflow.
- plugins/helpers
    - Contains a file that will help the custom built operators: sql_queries.py. This file contains all the SQL 
    queries to populate the fact and dimension table. 
- plugins/operators
    - All of the custom operators that are in use to stage the data to Redshift, load the lact and dimension tables, 
    and asserts that valid data had been entered into the tables.