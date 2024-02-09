-- Synapse Analytics notebook source

-- METADATA ********************

-- META {
-- META   "synapse": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "ea4e2bd3-9dbb-4f0a-bbcc-de9329421a9c",
-- META       "default_lakehouse_name": "UPBI_INEP_LakeHouse_Fabricators",
-- META       "default_lakehouse_workspace_id": "bfbe47b0-7617-4e7f-a7ea-2a3a52c91046",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "ea4e2bd3-9dbb-4f0a-bbcc-de9329421a9c"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

select  max(cast(f.qt_doc_total as decimal)) as qttot 
from    fat_cadastrosxies f 
-- limit   3 

-- CELL ********************

show create table fat_cadastrosxies

-- CELL ********************

select * from information_schema.columns

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("SHOW CREATE TABLE dim_curso").display()
-- MAGIC 
-- MAGIC 
-- MAGIC #abfss://bfbe47b0-7617-4e7f-a7ea-2a3a52c91046@onelake.dfs.fabric.microsoft.com/ea4e2bd3-9dbb-4f0a-bbcc-de9329421a9c/Tables/dim_curso

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.sql("SELECT * FROM UPBI_INEP_LakeHouse_Fabricators.dim_curso LIMIT 1000")
-- MAGIC display(df)

-- CELL ********************

-- show tables
show columns in dim_curso
