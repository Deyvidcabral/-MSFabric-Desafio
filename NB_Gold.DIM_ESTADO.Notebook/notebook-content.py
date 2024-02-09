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

CaminhoDestino = "Files/3_GOLD/DIM_ESTADO"
NomeTabelaDestino = "DIM_ESTADO"

# CELL ********************

df_cursos = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

df_ies = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

# CELL ********************

df_save1 = df_cursos.select(['CO_UF','SG_UF','NO_UF']).dropDuplicates().filter("CO_UF is not null").orderBy("CO_UF")
df_save2 = df_ies.select(['CO_UF_IES','SG_UF_IES','NO_UF_IES']).dropDuplicates().filter("CO_UF_IES is not null").orderBy("CO_UF_IES")

# CELL ********************

df_save1.createOrReplaceTempView("ESTADOS_Cursos")
df_save2.createOrReplaceTempView("ESTADOS_Ies")

# CELL ********************

# MAGIC %%sql
# MAGIC   SELECT
# MAGIC                     CO_UF,SG_UF,NO_UF
# MAGIC                     from (
# MAGIC                             select 
# MAGIC                                 *,
# MAGIC                                 row_number() over(partition by CO_UF order by NO_UF desc  ) rk
# MAGIC                                 from
# MAGIC                                     (select  CO_UF,SG_UF,NO_UF from ESTADOS_Cursos
# MAGIC                                     union all 
# MAGIC                                     select CO_UF_IES,SG_UF_IES,NO_UF_IES from ESTADOS_Ies)X
# MAGIC                     )y
# MAGIC                     WHERE RK = 1

# CELL ********************

df_save = spark.sql("""
                    SELECT
                    CO_UF,SG_UF,NO_UF
                    from (
                            select 
                                *,
                                row_number() over(partition by CO_UF order by NO_UF desc  ) rk
                                from
                                    (select  CO_UF,SG_UF,NO_UF from ESTADOS_Cursos
                                    union all 
                                    select CO_UF_IES,SG_UF_IES,NO_UF_IES from ESTADOS_Ies)X
                    )y
                    WHERE RK = 1""")

# CELL ********************

#df.write.format("delta").partitionBy("NU_ANO_CENSO").mode("Overwrite").save("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)

# CELL ********************

display(df_ies)

# CELL ********************

