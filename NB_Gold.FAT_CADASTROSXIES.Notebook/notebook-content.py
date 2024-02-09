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

CaminhoDestino = "Files/3_GOLD/FAT_CADASTROXIES"
NomeTabelaDestino = "FAT_CADASTROSxIES"

# CELL ********************

df = spark.read.format("DELTA").load("Files/2_SILVER/INEP/TBL_INEP_MICRODADOS_CADASTRO_IES")

# CELL ********************

# df.columns

# CELL ********************

df_save = df.select([
                    "NU_ANO_CENSO",
                    "CO_MUNICIPIO_IES",
                    "CO_UF_IES",
                    "CO_IES",
                    "QT_TEC_TOTAL",
                    "QT_TEC_FUNDAMENTAL_INCOMP_FEM",
                    "QT_TEC_FUNDAMENTAL_INCOMP_MASC",
                    "QT_TEC_FUNDAMENTAL_COMP_FEM",
                    "QT_TEC_FUNDAMENTAL_COMP_MASC",
                    "QT_TEC_MEDIO_FEM",
                    "QT_TEC_MEDIO_MASC",
                    "QT_TEC_SUPERIOR_FEM",
                    "QT_TEC_SUPERIOR_MASC",
                    "QT_TEC_ESPECIALIZACAO_FEM",
                    "QT_TEC_ESPECIALIZACAO_MASC",
                    "QT_TEC_MESTRADO_FEM",
                    "QT_TEC_MESTRADO_MASC",
                    "QT_TEC_DOUTORADO_FEM",
                    "QT_TEC_DOUTORADO_MASC",
                    "QT_PERIODICO_ELETRONICO",
                    "QT_LIVRO_ELETRONICO",
                    "QT_DOC_TOTAL",
                    "QT_DOC_EXE",
                    "QT_DOC_EX_FEMI",
                    "QT_DOC_EX_MASC",
                    "QT_DOC_EX_SEM_GRAD",
                    "QT_DOC_EX_GRAD",
                    "QT_DOC_EX_ESP",
                    "QT_DOC_EX_MEST",
                    "QT_DOC_EX_DOUT",
                    "QT_DOC_EX_INT",
                    "QT_DOC_EX_INT_DE",
                    "QT_DOC_EX_INT_SEM_DE",
                    "QT_DOC_EX_PARC",
                    "QT_DOC_EX_HOR",
                    "QT_DOC_EX_0_29",
                    "QT_DOC_EX_30_34",
                    "QT_DOC_EX_35_39",
                    "QT_DOC_EX_40_44",
                    "QT_DOC_EX_45_49",
                    "QT_DOC_EX_50_54",
                    "QT_DOC_EX_55_59",
                    "QT_DOC_EX_60_MAIS",
                    "QT_DOC_EX_BRANCA",
                    "QT_DOC_EX_PRETA",
                    "QT_DOC_EX_PARDA",
                    "QT_DOC_EX_AMARELA",
                    "QT_DOC_EX_INDIGENA",
                    "QT_DOC_EX_COR_ND",
                    "QT_DOC_EX_BRA",
                    "QT_DOC_EX_EST",
                    "QT_DOC_EX_COM_DEFICIENCIA"
                    ]).filter("NU_ANO_CENSO is not null")\
                    .withColumnRenamed("CO_MUNICIPIO_IES","CO_MUNICIPIO")\
                    .withColumnRenamed("CO_UF_IES","CO_UF").dropDuplicates().orderBy("NU_ANO_CENSO")

# CELL ********************

df_save.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable(NomeTabelaDestino)
