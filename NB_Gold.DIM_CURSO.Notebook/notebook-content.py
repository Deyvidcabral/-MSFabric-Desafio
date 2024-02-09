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

CaminhoDestino = "Files/3_GOLD/DIM_CURSO"
NomeTabelaDestino = "DIM_CURSO"

# CELL ********************

df = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

# CELL ********************

# df.columns

# CELL ********************

df_save = df.select([
"CO_CURSO",
"NO_CURSO",
"NO_CINE_ROTULO",
"CO_CINE_ROTULO",
"CO_CINE_AREA_GERAL",
"NO_CINE_AREA_GERAL",
"CO_CINE_AREA_ESPECIFICA",
"NO_CINE_AREA_ESPECIFICA",
"CO_CINE_AREA_DETALHADA",
"NO_CINE_AREA_DETALHADA",
"TP_GRAU_ACADEMICO",
"IN_GRATUITO",
"TP_MODALIDADE_ENSINO",
"TP_DIMENSAO",
"TP_NIVEL_ACADEMICO","CO_IES"]).filter("CO_CURSO is not null").dropDuplicates().orderBy("CO_CURSO")

# CELL ********************

df_save.createOrReplaceTempView("temp_dim_curso")

# CELL ********************

df_save = spark.sql("""select
                            CO_CURSO,
                            NO_CURSO NM_CURSO,
                            NO_CINE_ROTULO,
                            CO_CINE_ROTULO,
                            CO_CINE_AREA_GERAL,
                            NO_CINE_AREA_GERAL,
                            CO_CINE_AREA_ESPECIFICA,
                            NO_CINE_AREA_ESPECIFICA,
                            CO_CINE_AREA_DETALHADA,
                            NO_CINE_AREA_DETALHADA,
                            CASE
                                WHEN TP_GRAU_ACADEMICO = 1 THEN 'Bacharelado'
                                WHEN TP_GRAU_ACADEMICO = 2 THEN 'Licenciatura'
                                WHEN TP_GRAU_ACADEMICO = 3 THEN 'Tecnológico'
                                WHEN TP_GRAU_ACADEMICO = 4 THEN 'Bacharelado e Licenciatura'
                                ELSE '(.) Não aplicável (cursos com nível acadêmico igual a sequencial de formação específica ou cursos de área básica de Ingresso)'
                            END                                 TP_GRAU_ACADEMICO,
                            case when IN_GRATUITO = 1 then 'Sim'
                                                  when IN_GRATUITO = 0 then 'Não' 
                                                  else 'N/I'
                            END                                 IN_GRATUITO,
                            CASE 
                                WHEN TP_MODALIDADE_ENSINO = 1 THEN   'Presencial'
                                WHEN TP_MODALIDADE_ENSINO = 2 THEN   'Curso a distância'
                                ELSE 'N/I'
                            END                                 TP_MODALIDADE_ENSINO,
                            CASE 
                                WHEN TP_NIVEL_ACADEMICO = 1 THEN  'Graduação'
                                WHEN TP_NIVEL_ACADEMICO = 2 THEN  'Sequencial de Formação Específica'
                                        ELSE 'N/I'
                            END                                 TP_NIVEL_ACADEMICO,
                            CASE 
                                WHEN TP_DIMENSAO =  1 THEN  'Cursos presenciais ofertados no Brasil'
                                WHEN TP_DIMENSAO =  2 THEN  'Cursos a distância ofertados no Brasil'
                                WHEN TP_DIMENSAO =  3 THEN  'Cursos a distância com dimensão de dados somente a nível Brasil'
                                WHEN TP_DIMENSAO =  4 THEN  'Cursos a distância ofertados por instituições brasileiras no exterior'
                                ELSE 'N/I'
                                END                             TP_DIMENSAO,
                            CO_IES
                        from 
                            (
                            Select * ,row_number() over (PARTITION by CO_CURSO ORDER BY CO_CURSO) AS RK FROM temp_dim_curso
                            ) x 
                        where x.RK = 1 """)

# CELL ********************

#df.write.format("delta").partitionBy("NU_ANO_CENSO").mode("Overwrite").save("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)

# CELL ********************

# df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").save(CaminhoDestino)

# CELL ********************

# display(spark.read.format("Delta").load("Files/3_GOLD/DIM_CURSO"))

# CELL ********************

