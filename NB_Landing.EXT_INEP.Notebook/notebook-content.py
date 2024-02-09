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

NomeArquivoOrigem = "MICRODADOS_CADASTRO_CURSOS_2003.zip"
CaminhoDestino = "INEP"

# CELL ********************

# NomeArquivoOrigem = 'microdados_censo_da_educacao_superior_2020.zip'
# CaminhoDestino = 'INEP'

# CELL ********************

UrlInep = "https://download.inep.gov.br/microdados/"


# CELL ********************

url_list = UrlInep + NomeArquivoOrigem

# CELL ********************

import os
import shutil
import requests
from io import BytesIO
import zipfile
import urllib3


# url_list = [
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2020.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2019.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2018.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2017.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2016.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2015.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2014.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2013.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2012.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2011.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2010.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2009.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2008.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2007.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2006.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2005.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2004.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2003.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2002.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2001.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2000.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_1999.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_1998.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_1997.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_1996.zip",
#     "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_1995.zip",
# ]

# CELL ********************

caminho_abfss = '/lakehouse/default/Files/0_LANDING'

# CELL ********************

def getFinalPath(url_zip):
    #url_zip = 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2022.zip'
    last_slash_index = url_zip.rfind("/")
    result = ""
    ano = ""
    final_path = ""

    if last_slash_index != -1:  # Verifica se a barra foi encontrada
        result = url_zip[last_slash_index + 1:].replace(".zip","")  # Obtém a substring a partir do índice seguinte à última barra
        Last_Under_index = result.rfind("_")
        #final_path = result
        if Last_Under_index != -1: 
            ano = result[Last_Under_index+1:]   
        else:
            print("A URL não contém barras.")
        result = result[:Last_Under_index]
        final_path = caminho_abfss+'/'+CaminhoDestino+'/'+result+'/'+ano
        print(caminho_abfss)
    
    #os.makedirs(final_path, exist_ok=True)
    print(final_path)
    return final_path


# CELL ********************

# for url_zip in url_list:
url_zip = url_list
print("Baixando arquivo: " +url_zip)
final_path = getFinalPath(url_zip=url_zip)
filebytes = BytesIO(requests.get(url_zip, verify=False).content)

# CELL ********************

try:    
    zipfile.ZipFile(filebytes).extractall(final_path)
    print("Extraindo arquivo para: "+ final_path)
except:
    print("Arquivo não extraido. O link pode não existir ou o formato zip está invalido.")

