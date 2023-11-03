from re import purge
import requests
import pandas as pd
import json
import io
from pandas import json_normalize

# Relatório de contacts (exportador de atendimentos).

url = 'https://api.directtalk.com.br/1.10/info/contacts?startDate=1651374000&endDate=1651460399&dateInfo=contactFinished'
usr = 'semp2cf5028b-834d-4691-b714-2996945bc936'
pwd = '79gdxgq8rnzjxhgv2e5r'
r = requests.get(url, auth=(f'{usr}', f'{pwd}'))

# FORMATO LISTA
dados = r.json()
# CONVERTE PARA DICT

#dados = r.text

dados_to_dict = { i : dados[i] for i in range(0, len(dados) ) }


# CONVERTE PARA DATAFRAME
df = pd.DataFrame.from_dict(dados_to_dict, orient='index')

df2 = pd.DataFrame(df['identifications'].values.tolist(), index=df.index)


df2.columns = df2.columns.str.replace("tb1_", "")

# CONVERTE PARA SERIES
#s  = pd.Series(dados_to_dict,index=dados_to_dict.keys())
df = pd.DataFrame([dados_to_dict], columns=dados_to_dict.keys())

#df = pd.DataFrame.from_dict(dados)
# df = pd.DataFrame(dados)
# DownloadDir = 'C://ETL/DATASCIENCE/RPA/ARQUIVOS/'
# arq = 'exp_atendimentos.csv'
# df.to_csv(DownloadDir+arq, sep=';', index=False, mode='a')
#print(dados)
