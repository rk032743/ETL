import os, logging
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import yduqs_db as db
from sqlalchemy import text
from get_dir import get_onedrive_dirs


# PARÂMETROS DE DIRETÓRIOS.

dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'YDUQS')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))

BANCO_ORIGEM = 'bd_bi_yduqs'
BANCO_DESTINO = 'cxg_db_yduqs'
CAMPO_DATA = 'data_cri'
TABELA_ALVO = 'tb_proposta_funil'


def batch(dia)-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    global ETL_DATA
    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    # ETL_DATA = '2023-02-01'
    parametros = dict()
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA_ALVO
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_DATA} = '{ETL_DATA}'"
    WHERE = f"WHERE data_cri = '{ETL_DATA}'"
    UPDATE_STMT = f"WHERE data_atualiza = '{ETL_DATA}'"
    COLUNAS = ['*']

    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_DATA
    parametros['TABELA_ALVO'] = TABELA_ALVO
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = TABELA_DESTINO
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = PRESTMT
    parametros['WHERE'] = WHERE
    parametros['COLUNAS'] = COLUNAS

    p =prep()
    e = extract()
    t = transform(e)
    l = load(t,1)

    return l


def update(dia)-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    global ETL_DATA
    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    parametros = dict()
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    TABELA_DESTINO2 = BANCO_DESTINO + '.' + TABELA_ALVO + '_stg'
    TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA_ALVO
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    PRESTMT = f"CREATE TABLE IF NOT EXISTS {TABELA_ALVO}_stg LIKE {TABELA_DESTINO}"
    WHERE = f"WHERE data_atualiza = '{ETL_DATA}'"
    POSSTMT = f"DROP TABLE IF EXISTS {TABELA_ALVO}_stg"
    UPDATE = f"REPLACE INTO {TABELA_DESTINO} SELECT * FROM {TABELA_DESTINO2}"
    COLUNAS = ['*']

    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_DATA
    parametros['TABELA_ALVO'] = TABELA_ALVO
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = TABELA_DESTINO2
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = PRESTMT
    parametros['WHERE'] = WHERE
    parametros['POSSTMT'] = POSSTMT
    parametros['UPDATE'] = UPDATE
    parametros['COLUNAS'] = COLUNAS

    p =prep()
    e = extract()
    t = transform(e)
    l = load(t,1)
    posp()

    return l


def nrt(dia=0)-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    global ETL_DATA
    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    parametros = dict()
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA_ALVO
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_DATA} = '{ETL_DATA}'"
    WHERE = f"WHERE data_cri = '{ETL_DATA}' "
    UPDATE_STMT = f"WHERE data_atualiza = '{ETL_DATA}'"
    COLUNAS = ['*']

    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_DATA
    parametros['TABELA_ALVO'] = TABELA_ALVO
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = TABELA_DESTINO
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = PRESTMT
    parametros['WHERE'] = WHERE
    parametros['COLUNAS'] = COLUNAS

    p =prep()
    e = extract()
    t = transform(e)
    l = load(t,1)

    return l


def dump_log(data):
    # SALVA LOG DE ETL.
    tabela_alvo = parametros['TABELA_ALVO']
    file = os.path.join(dir, f'log_{tabela_alvo}_{start_process.strftime("%Y%m%d")}.json')
    with open(file, 'w') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=4)


def counter(start_time):
    # counter DE TEMPO DECORRIDO.
    tempo = (time.time() - start_time)
    return print("TEMPO DECORRIDO: %s SEGUNDOS" % round(tempo, 1))


def prep():
    # PREPARAÇÃO DAS TABELAS DE DESTINO, DELETE OU TRUNCATE.
    engine_destination = db.conn_engine(1, parametros['BANCO_DESTINO'])
    start_time = time.time()
    try:
        print("PREPARANDO AMBIENTE...")
        with engine_destination.connect() as conn:
            truncate_statement = parametros['PRESTMT']
            conn.execute(text(truncate_statement))
            conn.commit()
        counter(start_time)
    except Exception:
        print("ERRO NA PREPARAÇÃO!")
    return print("AMBIENTE PRONTO!")


def posp():
    # PREPARAÇÃO DAS TABELAS DE DESTINO, DELETE OU TRUNCATE.
    engine_destination = db.conn_engine(1, parametros['BANCO_DESTINO'])
    start_time = time.time()
    try:
        print("PREPARANDO AMBIENTE...")
        with engine_destination.connect() as conn:
            update_statement = parametros['UPDATE']
            print(update_statement)
            drop_statement = parametros['POSSTMT']
            print(drop_statement)
            conn.execute(text(update_statement))
            conn.commit()
            conn.execute(text(drop_statement))
            conn.commit()
        counter(start_time)
    except Exception:
        print("ERRO NA ATUALIZAÇÃO!")

    return print("ATUALIZAÇÃO CONCLUÍDA!")


def extract()-> pd.DataFrame:
    # EXTRAÇÃO DOS DADOS NA ORIGEM.
    global start_process

    start_process = datetime.now()
    col = ','.join(parametros['COLUNAS'])
    tabela_origem = parametros['TABELA_ORIGEM']
    where  = parametros['WHERE']
    SQL_ORIGEM = f'SELECT {col} FROM {tabela_origem} {where}'

    tabela_alvo = parametros['TABELA_ALVO']
    start_time = time.time()
    print(SQL_ORIGEM)
    print(f'{tabela_alvo.upper()} - INICIANDO REPLICAÇÃO...')

    try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(0, parametros['BANCO_ORIGEM'])
        data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        counter(start_time)
        print("DADOS EXTRAÍDOS!")
    except Exception:
        print("ERRO NA EXTRAÇÃO!")
        data = 'Erro'
        sys.exit()
    return data


def transform(df)-> pd.DataFrame:
    # CONVERSÃO DE TIMEDELTA PARA HORA.
    # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "ITERITEMS" POR "ITEMS".
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
    # REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('None', 'NULL'))
    # CRIADO_EM E ATUALIZADO EM.
    df['criado_em'] = df['data_cri'].astype(str) + ' ' + df['hora_cri'].astype(str)
    df['atualizado_em'] = df['data_cri'].astype(str) + ' ' + df['hora_cri'].astype(str)
    
    return df


def load(df, tipo)->dict:
    # CARGA DOS DADOS VIA STREAMING.
    global end_process

    engine_destination = db.conn_engine(1, parametros['BANCO_DESTINO'])
    start_time = time.time()
    if tipo == 0:
        print("INSERINDO DADOS...")
        df.to_sql(parametros['TABELA_DESTINO'], con=engine_destination,  index=False, if_exists='append')
    elif tipo == 1:
        # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "LINE_TERMINATOR" POR "LINETERMINATOR".
        df.to_csv(parametros['ARQUIVO'], sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
        print("DADOS SALVOS!")
        print("CARREGANDO DADOS...")
        print(db.bulk(parametros['ARQUIVO'], parametros['BANCO_DESTINO'], parametros['TABELA_DESTINO'], 0))

    counter(start_time)
    end_process = datetime.now()
    print("DADOS CARREGADOS!")
    metadata = meta(df)
    
    
    return metadata


def meta(df)-> dict:
    # METADADOS DO PROCESSO DE ETL.
    metadata = dict()
    metadata['Nome'] = parametros['TABELA_ALVO']
    metadata['Registros'] = int(df.shape[0])
    campo_data = parametros['CAMPO_DATA']
    data_ref = df[campo_data].max()
    metadata['data_ref'] = str(data_ref)
    if metadata['Registros'] == 0:
        status = "Erro"
        obs = "FALHA NO PROCESSO DE CARGA!"
    else:
        status = "Completo"
        obs = "PROCESSO DE CARGA CONCLUÍDO!"
    metadata['Status'] = status
    metadata['Obs'] = obs
    metadata['Data'] = start_process.strftime("%Y-%m-%d")
    metadata['Início'] = start_process.strftime("%Y-%m-%d %H:%M:%S")
    metadata['Fim'] = end_process.strftime("%Y-%m-%d %H:%M:%S")
    duration = end_process - start_process
    tempo = time.gmtime(duration.total_seconds())
    metadata['Duração'] = time.strftime("%H:%M:%S", tempo)

    return metadata


if __name__ == '__main__':
    batch(1)
