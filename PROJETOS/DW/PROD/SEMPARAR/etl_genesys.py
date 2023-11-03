import os, logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import semparar_db as db
from sqlalchemy import text
from get_dir import get_onedrive_dirs


# PARÂMETROS DE DIRETÓRIOS.
ETL_DATA = datetime.now().strftime('%Y-%m-%d')
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'VELOE')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))


def main():
    # PROCESSO EXECUTOR DO ETL.
    tb1 = tb_lead_veloe_emrec1()
    print(tb1)
    tb2 = tb_follow_atividade_veloe()
    print(tb2)
    return 


def tb_lead_veloe_emrec1()-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    parametros = dict()
    BANCO_ORIGEM = 'bd_bi_email'
    BANCO_DESTINO = 'cxg_db_veloe'
    CAMPO_DATA = 'data_cri'
    TABELA_ALVO = 'tb_lead_veloe_emrec1'
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA_ALVO
    TABELA_DESTINO = TABELA_ALVO
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    PRESTMT = f'TRUNCATE {TABELA_DESTINO}'
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
    parametros['COLUNAS'] = COLUNAS

    p =prep()
    e = extract()
    t = transform(e)
    l = load(t,1)

    return l


def tb_follow_atividade_veloe()-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    parametros = dict()
    BANCO_ORIGEM = 'bd_bi_veloe'
    BANCO_DESTINO = 'cxg_db_veloe'
    CAMPO_DATA = 'data_cri'
    TABELA_ALVO = 'tb_follow_atividade_veloe'
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
    TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA_ALVO
    TABELA_DESTINO = TABELA_ALVO
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    PRESTMT = f'TRUNCATE {TABELA_DESTINO}'
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


def extract()-> pd.DataFrame:
    # EXTRAÇÃO DOS DADOS NA ORIGEM.
    global start_process

    start_process = datetime.now()
    col = ','.join(parametros['COLUNAS'])
    tabela_origem = parametros['TABELA_ORIGEM']
    SQL_ORIGEM = f'SELECT {col} FROM {tabela_origem}'

    tabela_alvo = parametros['TABELA_ALVO']
    start_time = time.time()
    print(f'{tabela_alvo.upper()} - INICIANDO REPLICAÇÃO...')

    try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(0, parametros['BANCO_ORIGEM'])
        data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        counter(start_time)
        print("DADOS EXTRAÍDOS!")
    except Exception:
        print("ERRO NA EXTRAÇÃO!")

    return data


def transform(df)-> pd.DataFrame:
    # CONVERSÃO DE TIMEDELTA PARA HORA.
    for colname, coltype in df.dtypes.iteritems():
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
        df.to_csv(parametros['ARQUIVO'], sep=';',index=False, line_terminator= '\r\n', encoding='utf-8')
        print("DADOS SALVOS!")
        print("CARREGANDO DADOS...")
        db.bulk(parametros['ARQUIVO'], parametros['BANCO_DESTINO'], parametros['TABELA_DESTINO'], 0)

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
    m = main()
