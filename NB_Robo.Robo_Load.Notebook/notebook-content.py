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

# PARAMETERS CELL ********************

nomeAmbiente = ""
tipoCarga = ""
nomeTabelaFabric = ""
nomeCamada = ""
FabricCaminho = ""
FabricCaminhoDelta = ""
nomeDatabaseFabric = ""
header = ""
nomeProcesso = ""
regraIncremental = ""
nomeNotebookFabric = ""
PreCopyScript = ""
nomeOrigem = ""
queryOrigem = ""

# CELL ********************

import time
def tentativas(query):
    tentativas = 20
    i = 1

    while i <= tentativas:
        if i == tentativas:
            try:
                exec_query(query)
                break
            except:
                continue
        i = i+1

def exec_query(query):
    time.sleep(20)
    spark.sql(query)

    
query = """
    update 0_par.processos
    set idSetupStatus = 2
    ,dataInicio = current_timestamp()
    where 
    nomeAmbiente = '{}' and 
    tipoCarga = '{}' and 
    nomeProcesso = '{}' and 
    nomeCamada = '{}' and
    nomeDatabaseFabric = '{}'
    """
query = query.format(nomeAmbiente, tipoCarga, nomeProcesso, nomeCamada, nomeDatabaseFabric)

# tentativas(query)

# CELL ********************

from delta.tables import *

def get_header(tabela):
    query = spark.sql(f"""
        select 
        CONCAT('substring( Prop_0',',',START,',',LENGTH,') as ',NAME) query
        from 0_par.headers
        where FILENAME = '{tabela}'
                order by START
            """.format(tabela))
    query = query.toPandas()
    query_new = query['query'].tolist()
    query_new = str(query_new).replace("'",'').replace("[",'').replace("]",'')
    return query_new

def delete_files(env, camada, assunto, tabela):
    if camada == '1':
        camada = '1_bronze'
        
    if camada == '2':
        camada = '2_silver'

    if camada == '3':
        camada = '3_gold'

    try:
        mssparkutils.fs.rm(f"synfs:/{jobid}/mnt/{camada}/{assunto}/{tabela}/",recurse=True)
    except:
        print('caminho nao encontrado: ' + f"synfs:/{jobid}/mnt/{camada}/{assunto}/{tabela}/")
       
def carga_full(query):
    df = executeQuery(query)
    spark.sql("DROP TABLE IF EXISTS "+ table_name)
    df.write \
      .format('delta') \
      .save(save_path)

    spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

def get_delta_parquet(read_path):
    df = spark \
      .read \
      .format('delta') \
      .parquet(read_path)
    return df
    
def set_header(df):
    df.createOrReplaceTempView("TMP_HEADER")
    df = spark.sql('select '+get_header(nomeProcesso)+',ARQUIVO, current_timestamp() DT_CARGA from TMP_HEADER')
    return df

def carga_full_sem_header(read_path,save_path,table_name,query):
    df = executeQuery(query)
    df = set_header(df)
                  
    spark.sql("DROP TABLE IF EXISTS "+ table_name)
    
    df.write \
      .format('delta') \
      .save(save_path)
    spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")
    
    
def merge_data(existing_table_name, new_data, regraIncremental):
    existing_data = DeltaTable.forName(spark, existing_table_name)
    existing_data.alias('old') \
      .merge(
        new_data.alias('new'),
        str(regraIncremental) 
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
    
def deleteInsert_data(existing_table_name, new_data, regraIncremental):
    spark.sql(regraIncremental)
    new_data.write.format("delta").mode("append").saveAsTable(existing_table_name)
    
def executeSaveQuery(query, tabela, save_path):
    df = spark.sql(query)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)

def executeQuery(query):
    df = spark.sql(query)
    return df

# CELL ********************

#carga full com header
if tipoCarga=='1':
    if header=='1':
        delete_files(nomeAmbiente, nomeCamada.lower(), nomeOrigem, nomeTabelaFabric)
        carga_full(FabricCaminho,FabricCaminhoDelta,nomeDatabaseFabric+'.'+nomeTabelaFabric,queryOrigem)

# CELL ********************

#carga merge com header
if tipoCarga=='3':
    if header=='1':        
        df_etl = executeQuery(queryOrigem)
        merge_data(nomeDatabaseFabric+'.'+nomeTabelaFabric, df_etl, regraIncremental)

# CELL ********************

#carga delete-insert com header
if tipoCarga=='2':
    if header=='1':        
        df_etl = executeQuery(queryOrigem)
        deleteInsert_data(nomeDatabaseFabric+'.'+nomeTabelaFabric, df_etl, regraIncremental)

# CELL ********************

#carga de execucao de notebook
if tipoCarga=='4':
    #dbutils.notebook.run(nomeNotebookFabric, 180)
    mssparkutils.notebook.run(nomeNotebookFabric,180)

# CELL ********************

#carga de execucao de query
if tipoCarga=='5':
    executeSaveQuery(queryOrigem,nomeDatabaseFabric+'.'+nomeTabelaFabric, FabricCaminhoDelta)
