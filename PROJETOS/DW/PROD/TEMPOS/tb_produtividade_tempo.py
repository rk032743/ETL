import os, logging
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import db
from get_dir import get_onedrive_dirs


########################################### PARÂMETROS. ###########################################

dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'TEMPOS')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))



BANCO_DESTINO = 'dm_db_tempos'
TABELA_ALVO = 'tb_produtividade_tempo'
TB_A = TABELA_ALVO
TABELA_DESTINO = BANCO_DESTINO + '.' + TB_A + '_stg'
# TABELA_DESTINO = BANCO_DESTINO + '.' + TB_A
COLUNAS = ['*']
CAMPO_CHAVE = 'data'
###################################################################################################

def representante(tabela) -> str:

    loc = tabela.find('.')
    length = len(tabela)
    limit = tabela.rfind('_',0,loc)
    repre = tabela[limit+1:loc].upper()
    return repre


def main(d=1) -> list:

    global PRESTMT
    DT = (datetime.now() - timedelta(days=d)).strftime('%Y-%m-%d')
    PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_CHAVE} = '{DT}';"
    col = 'TABLE_SCHEMA'
    where  = f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{TABELA_ALVO}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise', 'bd_bi_dimep')"
    SQL = f'SELECT {col} FROM information_schema.TABLES {where}'
    engine_source = db.conn_engine(0, 'information_schema')
    data = pd.read_sql(sql=SQL, con=engine_source)
    result = data['TABLE_SCHEMA'].values.tolist()
    p =prep()

    for bd in result:

        print(bd)
        batch(d,banco=bd)

    return 


def batch(dia=1, banco='')-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    global ETL_DATA
    
    global REPRESENTANTE
    global TABELA_ORIGEM

    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    ANOMES = pd.to_datetime(ETL_DATA).strftime('%y%m')
    parametros = dict()

    BANCO_ORIGEM = banco
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}_{BANCO_ORIGEM}.csv')
    
    WHERE = f"WHERE {CAMPO_CHAVE}  = '{ETL_DATA}' "
    
    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_CHAVE
    parametros['TABELA_ALVO'] = TABELA_ALVO
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = TABELA_DESTINO
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = PRESTMT
    parametros['WHERE'] = WHERE
    parametros['COLUNAS'] = COLUNAS

    
    e = extract()
    print("EXTRAÍDO")
    print(e)
    t = transform(e)
    print("TRANSFORMADO")
    print(t)
    l = load(t,1)

    return l


def dump_log(data):
    # SALVA LOG DE ETL.
    tabela_alvo = parametros['TABELA_ALVO']
    file = os.path.join(dir, f'log_{tabela_alvo}_{ETL_DATA}.json')
    with open(file, 'w') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=4)


def counter(start_time):
    # counter DE TEMPO DECORRIDO.
    tempo = (time.time() - start_time)
    return print("TEMPO DECORRIDO: %s SEGUNDOS" % round(tempo, 1))


def prep():
    # PREPARAÇÃO DAS TABELAS DE DESTINO, DELETE OU TRUNCATE.
    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    start_time = time.time()
    try:
        print("PREPARANDO AMBIENTE...")
        with engine_destination.connect() as conn:
            truncate_statement = PRESTMT
            conn.execution_options(autocommit=True).execute(truncate_statement)
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
            conn.execution_options(autocommit=True).execute(update_statement)
            conn.execution_options(autocommit=True).execute(drop_statement)
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

    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)
    # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "ITERITEMS" POR "ITEMS".
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
    # REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))

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
        db.bulk(parametros['ARQUIVO'], parametros['BANCO_DESTINO'], parametros['TABELA_DESTINO'], 0)

    counter(start_time)
    end_process = datetime.now()
    print("DADOS CARREGADOS!")
    metadata = meta(df)
    # dump_log(metadata)
    
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

