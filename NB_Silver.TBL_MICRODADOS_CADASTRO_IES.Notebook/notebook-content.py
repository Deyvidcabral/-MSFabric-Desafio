# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "ea4e2bd3-9dbb-4f0a-bbcc-de9329421a9c",
# META       "default_lakehouse_name": "UPBI_INEP_LakeHouse_Fabricators",
# META       "default_lakehouse_workspace_id": "bfbe47b0-7617-4e7f-a7ea-2a3a52c91046",
# META       "known_lakehouses": [
# META         {
# META           "id": "ea4e2bd3-9dbb-4f0a-bbcc-de9329421a9c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("Delta").load("Files/1_BRONZE/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")
# df now is a Spark DataFrame containing CSV data from "Files/landing/microdados_censo_da_educacao_superior/2021/Microdados do Censo da Educaç╞o Superior 2021/dados/MICRODADOS_CADASTRO_IES_2021.CSV".

# CELL ********************

df = df.dropDuplicates()

# CELL ********************

df.write.format("delta").mode("Overwrite").save("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

# CELL ********************

