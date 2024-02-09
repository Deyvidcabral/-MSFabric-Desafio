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

CaminhoDestino = "Files/3_GOLD/DIM_INSTITUICAO"
NomeTabelaDestino = "DIM_INSTITUICAO"

# CELL ********************

df_IES = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

df_cursos = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_CURSOS")

# CELL ********************

df_save1 = df_IES.select([
"CO_IES"
,"NO_IES"
,"SG_IES"
,"IN_CAPITAL_IES"
,"CO_MUNICIPIO_IES"
,"NO_MESORREGIAO_IES"
,"CO_MESORREGIAO_IES"
,"NO_MICRORREGIAO_IES"
,"CO_MICRORREGIAO_IES"
,"TP_ORGANIZACAO_ACADEMICA"
,"TP_CATEGORIA_ADMINISTRATIVA"
,"NO_MANTENEDORA"
,"CO_MANTENEDORA"
,"DS_ENDERECO_IES"
,"DS_NUMERO_ENDERECO_IES"
,"DS_COMPLEMENTO_ENDERECO_IES"
,"NO_BAIRRO_IES"
,"NU_CEP_IES"
,"CO_PROJETO"
,"CO_LOCAL_OFERTA"
,"NO_LOCAL_OFERTA",
"IN_ACESSO_PORTAL_CAPES",
"IN_ACESSO_OUTRAS_BASES",
"IN_ASSINA_OUTRA_BASE",
"IN_REPOSITORIO_INSTITUCIONAL",
"IN_BUSCA_INTEGRADA",
"IN_SERVICO_INTERNET",
"IN_PARTICIPA_REDE_SOCIAL",
"IN_CATALOGO_ONLINE"
]).filter("CO_IES is not null")

# CELL ********************

df_save2 = df_cursos.select([
                    "CO_IES",
                    "TP_REDE",
                    "co_municipio"
                    ]).filter("CO_IES is not null").dropDuplicates().orderBy("CO_IES")

# CELL ********************

df_save1.createOrReplaceTempView("temp_dim_INSTITUICAO")

# CELL ********************

df_save2.createOrReplaceTempView("temp_dim_rede")

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
windowSpec  = Window.partitionBy("CO_IES").orderBy(col("TP_REDE").desc())

df_save2.withColumn("RK",row_number().over(windowSpec)).filter("RK = 1").createOrReplaceTempView("temp_dim_rede")

# CELL ********************

df_save = spark.sql("""
                            with cte_ies as (select
                                                        CO_IES
                                                        ,NO_IES MM_INSTITUICAO
                                                        ,SG_IES SG_INSTITUICAO
                                                        ,IN_CAPITAL_IES
                                                        ,CO_MUNICIPIO_IES
                                                        ,CO_MESORREGIAO_IES
                                                        ,NO_MESORREGIAO_IES
                                                        ,CO_MICRORREGIAO_IES
                                                        ,NO_MICRORREGIAO_IES
                                                        ,TP_ORGANIZACAO_ACADEMICA
                                                        ,TP_CATEGORIA_ADMINISTRATIVA
                                                        ,CO_MANTENEDORA
                                                        ,NO_MANTENEDORA
                                                        ,DS_ENDERECO_IES
                                                        ,DS_NUMERO_ENDERECO_IES
                                                        ,DS_COMPLEMENTO_ENDERECO_IES
                                                        ,NO_BAIRRO_IES
                                                        ,NU_CEP_IES
                                                        ,CO_PROJETO
                                                        ,CO_LOCAL_OFERTA
                                                        ,NO_LOCAL_OFERTA
                                                        ,IN_ACESSO_PORTAL_CAPES,
                                                        IN_ACESSO_OUTRAS_BASES,
                                                        IN_ASSINA_OUTRA_BASE,
                                                        IN_REPOSITORIO_INSTITUCIONAL,
                                                        IN_BUSCA_INTEGRADA,
                                                        IN_SERVICO_INTERNET,
                                                        IN_PARTICIPA_REDE_SOCIAL,
                                                        IN_CATALOGO_ONLINE
                                                    from 
                                                        (
                                                        Select * ,row_number() over (PARTITION by CO_IES ORDER BY CO_IES) AS RK FROM temp_dim_INSTITUICAO
                                                        ) x 
                                                    where x.RK = 1) 
                                    select   coalesce(cte.CO_IES, rede.CO_IES)  CO_IES
                                            ,MM_INSTITUICAO 
                                            ,SG_INSTITUICAO
                                            ,case when CTE.IN_CAPITAL_IES = 1 then 'Sim'
                                                  when CTE.IN_CAPITAL_IES = 0 then 'Não' 
                                                  else 'N/I'
                                                 END
                                                FLG_CAPITAL_UNIDADE_FEDERACAOP
                                            ,coalesce(CTE.CO_MUNICIPIO_IES,rede.co_municipio) CO_MUNICIPIO
                                            ,CTE.CO_MESORREGIAO_IES CO_MESORREGIAO
                                            ,CTE.NO_MESORREGIAO_IES NO_MESORREGIAO
                                            ,CTE.CO_MICRORREGIAO_IES CO_MICRORREGIAO
                                            ,CTE.NO_MICRORREGIAO_IES NO_MICRORREGIAO
                                            ,CASE 
                                                WHEN CTE.TP_ORGANIZACAO_ACADEMICA = 1 THEN 'Universidade'
                                                WHEN CTE.TP_ORGANIZACAO_ACADEMICA = 2 THEN 'Centro Universitário'
                                                WHEN CTE.TP_ORGANIZACAO_ACADEMICA = 3 THEN 'Faculdade'
                                                WHEN CTE.TP_ORGANIZACAO_ACADEMICA = 4 THEN 'Instituto Federal de Educação, Ciência e Tecnologia'
                                                WHEN CTE.TP_ORGANIZACAO_ACADEMICA = 5 THEN 'Centro Federal de Educação Tecnológica'
                                                ELSE 'N/I'
                                                END TP_ORGANIZACAO_ACADEMICA
                                            ,case
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =1 THEN  'Pública Federal'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =2 THEN  'Pública Estadual'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =3 THEN  'Pública Municipal'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =4 THEN  'Privada com fins lucrativos'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =5 THEN  'Privada sem fins lucrativos'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =6 THEN  'Privada - Particular em sentido estrito'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =7 THEN  'Especial'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =8 THEN  'Privada comunitária'
                                                WHEN CTE.TP_CATEGORIA_ADMINISTRATIVA =9 THEN  'Privada confessional'
                                                ELSE 'N/I'
                                                END TP_CATEGORIA_ADMINISTRATIVA
                                            ,CTE.CO_MANTENEDORA
                                            ,CTE.NO_MANTENEDORA
                                            ,CTE.DS_ENDERECO_IES DS_ENDERECO
                                            ,CTE.DS_NUMERO_ENDERECO_IES DS_NUMERO_ENDERECO
                                            ,CTE.DS_COMPLEMENTO_ENDERECO_IES DS_COMPLEMENTO_ENDERECO
                                            ,CTE.NO_BAIRRO_IES NO_BAIRRO
                                            ,CTE.NU_CEP_IES NU_CEP
                                            ,CTE.CO_PROJETO
                                            ,CTE.CO_LOCAL_OFERTA
                                            ,CTE.NO_LOCAL_OFERTA 
                                            ,case 
                                                WHEN rede.TP_REDE  = 1 then 'Pública'
                                                WHEN rede.TP_REDE  = 2 then 'Privada' 
                                                ELSE 'N/I'
                                             END TP_REDE,
                                             CASE
                                                WHEN IN_ACESSO_PORTAL_CAPES = 0 THEN 'Não'
                                                WHEN IN_ACESSO_PORTAL_CAPES = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_ACESSO_PORTAL_CAPES,
                                            CASE
                                                WHEN IN_ACESSO_OUTRAS_BASES = 0 THEN 'Não'
                                                WHEN IN_ACESSO_OUTRAS_BASES = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_ACESSO_OUTRAS_BASES,
                                            CASE
                                                WHEN IN_ASSINA_OUTRA_BASE = 0 THEN 'Não'
                                                WHEN IN_ASSINA_OUTRA_BASE = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_ASSINA_OUTRA_BASE,
                                            CASE
                                                WHEN IN_REPOSITORIO_INSTITUCIONAL = 0 THEN 'Não'
                                                WHEN IN_REPOSITORIO_INSTITUCIONAL = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_REPOSITORIO_INSTITUCIONAL,
                                            CASE
                                                WHEN IN_BUSCA_INTEGRADA = 0 THEN 'Não'
                                                WHEN IN_BUSCA_INTEGRADA = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_BUSCA_INTEGRADA,
                                            CASE
                                                WHEN IN_SERVICO_INTERNET = 0 THEN 'Não'
                                                WHEN IN_SERVICO_INTERNET = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_SERVICO_INTERNET,
                                            CASE
                                                WHEN IN_PARTICIPA_REDE_SOCIAL = 0 THEN 'Não'
                                                WHEN IN_PARTICIPA_REDE_SOCIAL = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_PARTICIPA_REDE_SOCIAL,
                                            CASE
                                                WHEN IN_CATALOGO_ONLINE = 0 THEN 'Não'
                                                WHEN IN_CATALOGO_ONLINE = 1 THEN 'Sim'
                                                ELSE 'N/I'
                                            END AS FLG_CATALOGO_ONLINE
                                            from cte_ies CTE full outer join temp_dim_rede rede
                                            ON cte.CO_IES = rede.CO_IES
                                          
                                         """)

# CELL ********************

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# windowSpec  = Window.partitionBy("CO_IES").orderBy("CO_IES")

# df_save = df_save.withColumn("RK",row_number().over(windowSpec)).filter("RK = 1")

# CELL ********************

# df_save = spark.sql(f"""select
#                              CO_IES
#                             ,NO_IES MM_INSTITUICAO
#                             ,SG_IES SG_INSTITUICAO
#                             ,IN_CAPITAL_IES
#                             ,CO_MESORREGIAO_IES
#                             ,NO_MESORREGIAO_IES
#                             ,CO_MICRORREGIAO_IES
#                             ,NO_MICRORREGIAO_IES
#                             ,TP_ORGANIZACAO_ACADEMICA
#                             ,TP_CATEGORIA_ADMINISTRATIVA
#                             ,CO_MANTENEDORA
#                             ,NO_MANTENEDORA
#                             ,DS_ENDERECO_IES
#                             ,DS_NUMERO_ENDERECO_IES
#                             ,DS_COMPLEMENTO_ENDERECO_IES
#                             ,NO_BAIRRO_IES
#                             ,NU_CEP_IES
#                             ,CO_PROJETO
#                             ,CO_LOCAL_OFERTA
#                             ,NO_LOCAL_OFERTA
#                         from 
#                             (
#                             Select * ,row_number() over (PARTITION by CO_IES ORDER BY CO_IES) AS RK FROM temp_dim_ie_ies
#                             ) x 
#                         where x.RK = 1 """)

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)

# CELL ********************

