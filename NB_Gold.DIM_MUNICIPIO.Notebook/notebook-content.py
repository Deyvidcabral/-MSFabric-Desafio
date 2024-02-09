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

CaminhoDestino = "Files/3_GOLD/DIM_MUNICIPIO"
NomeTabelaDestino = "DIM_MUNICIPIO"

# CELL ********************

df_cursos = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

df_ies = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

# CELL ********************

df_save1 = df_cursos.select(['CO_MUNICIPIO','NO_MUNICIPIO','CO_UF','CO_REGIAO']).dropDuplicates().filter("CO_MUNICIPIO is not null").orderBy("CO_MUNICIPIO")
df_save2 = df_ies.select(['CO_MUNICIPIO_IES','NO_MUNICIPIO_IES','CO_UF_IES','CO_REGIAO_IES']).dropDuplicates().filter("CO_MUNICIPIO_IES is not null").orderBy("CO_MUNICIPIO_IES")

# CELL ********************

df_save1.createOrReplaceTempView("Municipio_Cursos")
df_save2.createOrReplaceTempView("Municipio_Ies")

# CELL ********************

df_save = spark.sql("""
                    SELECT
                    CO_MUNICIPIO,NO_MUNICIPIO,CO_UF,CO_REGIAO
                    from (
                            select 
                                *,
                                row_number() over(partition by CO_MUNICIPIO order by CO_UF desc  ) rk
                                from
                                    (select  CO_MUNICIPIO,NO_MUNICIPIO,CO_UF,CO_REGIAO as CO_REGIAO from Municipio_Cursos
                                    union all 
                                    select CO_MUNICIPIO_IES,NO_MUNICIPIO_IES,CO_UF_IES,CO_REGIAO_IES from Municipio_Ies)X
                    )y
                    WHERE RK = 1""")

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)
