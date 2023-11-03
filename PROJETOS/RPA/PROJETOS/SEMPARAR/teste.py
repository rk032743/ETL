from ast import Global
import numpy as np
import logging
from os import path
from importlib import import_module
import sys
import datetime
from datetime import timedelta
import time
import os
import glob
from re import purge
import requests
import pandas as pd
import json
import io
from pandas import json_normalize
import csv

######################################### PARAMETROS #####################################################

d1 = datetime.timedelta(days=1)
d0 = datetime.datetime.today()
dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_hoje = d0.strftime('%d/%#m/%Y')
dt_ontem = dt_ontem.strftime('%d/%#m/%Y')
hora = d0.strftime('%H')
###############################################################################################################

def json_parse(data:str):

    try:
        js_dict = json.loads(data)

        cols= {
            'group.divisionId': 'division_id',
            'group.mediaType': 'media_type',
            'group.userId': 'user_id',
            'group.queueId': 'queue_id',
            'metrics.metric': 'metric',
            'metrics.stats.count': 'stats_count',
            'metrics.stats.max': 'stats_max',
            'metrics.stats.min': 'stats_min',
            'metrics.stats.sum': 'stats_sum'

        }


        c = pd.json_normalize(js_dict,
                                # record_path=['identifications'],
                                # meta=[['identifications','cpf']],
        errors='ignore')

        d = pd.json_normalize(js_dict,
                            record_path=['identifications'],
    
        errors='ignore')
        # print(c)

        # c = pd.json_normalize(js_dict['results'],
        #             record_path=['data'],
        #             meta=[['group','divisionId'], ['group','mediaType'], ['group','userId'], ['group','queueId']],
        #             errors='ignore')

        # hh = pd.DataFrame(c, columns=['interval', 'metrics', 'group.divisionId', 'group.mediaType', 'group.userId', 'group.queueId'])
        # print(c)

        # df1 = pd.DataFrame(c, columns=['key', 'value'])
        hh = c.explode('properties')
        df1 = pd.DataFrame(d, columns=['key', 'value'])
        print(type(df1))
        print(type(hh))
        print(df1)
        print(hh)
        # hh = c

        

        df1 = df1.pivot(columns='key', values='value')
        # print(hh)
        # hh = c.explode('properties')

        # df = df.join(df['data'].apply(pd.Series))
        # print(hh['identifications'])

        # d = hh.to_string(columns=['identifications'], index=False, header=False)
        


        # for line in d.splitlines():
        #     print(line)
        #     line = line.replace('""', '"')
        #     line = '[' + line[1:-1] + ']'
        #     line = json.loads(line)

        #     item = {}
        #     for d in line[1:]:
        #         key = d['key']
        #         val = d['value']
        #         item[key] = val

        #     rows.append( [line[0], item] )
            
        # df = pd.DataFrame(rows, columns=['id', 'data'])


        
        # ab = pd.DataFrame(hh, columns=['Consumidor_Chat', 'cpf', 'email', 'Placa', 'telefone'])
        # print('check df1')
        # print(df1)
        # df1 = df1.dropna()
        # df1.dropna(axis = 0, how = 'all', inplace = True)
        df1['Consumidor_Chat'].fillna(np.nan, inplace = True)
        df1['Placa'].fillna(np.nan, inplace = True)
        df1['botsession'].fillna(np.nan, inplace = True)
        df1['cpf'].fillna(np.nan, inplace = True)
        df1['email'].fillna(np.nan, inplace = True)
        df1['telefone'].fillna(np.nan, inplace = True)
        df1.dropna(axis = 0, how = 'all', inplace = True)
        # df1 = df1.dropna()
        # df1 = df1.dropna(how = 'all')
        # deletar todas as colunas NaN
        print(df1)
        df2 = pd.json_normalize(json.loads(hh.to_json(orient='records')))
        df3 = pd.merge(df2, df1, how='left', left_index=True, right_index=True)
        # ab = pd.json_normalize(json.loads(hh.to_json(orient='split')))

        # ab = json_normalize(c)

        return df1
    except Exception as err:
        pass
        print("EXCEPTION: It can't to continue")
        # 01. Get the exceptions
        traceback_err = err.__traceback__
        class_error = err.__class__
        line_error = traceback_err.tb_lineno
        file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]
        logging.error(f"Erro Control Data: {traceback_err}, {class_error}, {line_error}, {file_error}")

    sys.exit(0)


def busca_atendimentos():

    if hora <= '07':
        dt = datetime.datetime.today() - datetime.timedelta(days=1)
        pDestino2 = 'tb_agent_events_hist_wpp'
        pCmd2 = '-- Do Nothing'
        h1 = dt.strftime("%Y%m%d_0000")

    else:
        dt = datetime.datetime.today()
        pDestino2 = 'tb_agent_events_nrt_wpp'
        pCmd2 = 'TRUNCATE tb_agent_events_nrt_wpp;'
        h1 = dt.strftime("%Y%m%d_%H00")

    ano = dt.year
    mes = dt.month
    dia = dt.day
    
    dt1 = datetime.datetime(ano, mes, dia, 00, 00, 00)
    dt2 = datetime.datetime(ano, mes, dia, 23, 59, 59)
    # CONVERTE O TIMESTAMP EPOCH PARA O FUSO HORARIO GMT-3(USA)
    UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
    # dt1 = datetime.datetime(2022, 6, 10, 00, 00, 00)
    dt1 = dt1 - UTC_OFFSET_TIMEDELTA
    # dt2 = datetime.datetime(2022, 6, 12, 23, 59, 59)
    dt2 = dt2 - UTC_OFFSET_TIMEDELTA
    epoch1 = round(dt1.timestamp())
    epoch2 = round(dt2.timestamp())

    varDownloadDir = 'C:\ETL\DATASCIENCE\RPA\ARQUIVOS'
    DownloadDir = 'C:\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
    ######################################### APAGA ARQUIVOS ##########################################################

##################################################### LOOP PAGINAÇÃO ##################################################################

    pagina = 1
    url = f'https://api.directtalk.com.br/1.10/info/contacts?startDate={epoch1}&endDate={epoch2}&dateInfo=contactFinished'
    # url = f'https://api.directtalk.com.br/1.10/info/reports/platform/agentevents?startDate={epoch1}&endDate={epoch2}'
    paramsurl = {
                'pageNumber': pagina
        
                }
    usr = 'semp2cf5028b-834d-4691-b714-2996945bc936'
    pwd = '79gdxgq8rnzjxhgv2e5r'
    r = requests.get(url, auth=(f'{usr}', f'{pwd}'))
    results = r.json()
    df = json_parse(r.text)
    # print(df)

#     list_headers = r.headers 
#     print('Paginas: ', list_headers['X-Pagination-TotalPages'])
#     max_pages = int(list_headers['X-Pagination-TotalPages'])
#     for pagina in range(1, max_pages+1):
#         paramsurl['pageNumber'] = pagina
#         r = requests.get(url, params=paramsurl, auth=(f'{usr}', f'{pwd}'))
#         print('Página Atual: ',pagina)
#         dados = r.json()
#         for i in dados:
#             results.append(i)
#     print(results)
#     df = json_parse(results)
#     print('Request completado!')
#     print(url)
# ##################################################### LOOP PAGINAÇÃO ##################################################################

#     # df = pd.DataFrame(results)
    DownloadDir = 'C://ETL/DATASCIENCE/RPA/ARQUIVOS/'
    DownloadDir = 'E:\\OneDrive - URANET PROJETOS E SISTEMAS LTDA\\Documents - Engenharia\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
   
    arq = f'busca_atendimentos_{h1}.csv'
#     # df.sort_values(by=['date'], ascending=True, ignore_index=True, inplace=True)
#     # df['date'] = df['date'].apply(lambda x: round(datetime.datetime.utcfromtimestamp(x).timestamp()))
#     # df['dt'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
#     # df['time'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%H:%M:%S'))
    df.to_csv(DownloadDir+arq, sep=';', index=True, line_terminator= '\n')
#     param2 = varDownloadDir+"\\"+arq
#     print('Arquivo gerado!')

    return 

if __name__ == '__main__':
    busca_atendimentos()