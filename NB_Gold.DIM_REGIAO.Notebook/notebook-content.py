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

CaminhoDestino = "Files/3_GOLD/DIM_REGIAO"
NomeTabelaDestino = "DIM_REGIAO"

# CELL ********************

df_cursos = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

df_ies = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

# CELL ********************

df_save1 = df_cursos.select([ "CO_REGIAO" ,"NO_REGIAO" ]).filter("CO_REGIAO is not null").dropDuplicates().orderBy("CO_REGIAO")
df_save2 = df_ies.select([ "CO_REGIAO_IES" ,"NO_REGIAO_IES" ]).filter("CO_REGIAO_IES is not null").dropDuplicates().orderBy("CO_REGIAO_IES")

# CELL ********************

df_save1.createOrReplaceTempView("Regiao_Cursos")
df_save2.createOrReplaceTempView("Regiao_Ies")

# CELL ********************

df_save = spark.sql("""
                    SELECT
                   CO_REGIAO  ,NO_REGIAO 
                    from (
                            select 
                                *,
                                row_number() over(partition by CO_REGIAO order by CO_REGIAO desc  ) rk
                                from
                                    (select  CO_REGIAO as CO_REGIAO ,NO_REGIAO from Regiao_Cursos
                                    union  
                                    select CO_REGIAO_IES ,NO_REGIAO_IES from Regiao_Ies)X
                    )y
                    WHERE RK = 1""")

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)

# CELL ********************

