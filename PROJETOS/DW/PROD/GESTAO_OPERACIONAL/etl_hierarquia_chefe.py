import os, logging
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import db
from get_dir import get_onedrive_dirs


# PARÂMETROS DE DIRETÓRIOS.
ETL_DATA = datetime.now().strftime('%Y-%m-%d')
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'GESTAO_OPERACIONAL')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))

# PARÂMETROS DE TABELAS.
BANCO_ORIGEM = 'bd_bi_call_center'
BANCO_DESTINO = 'dm_db_gestao_operacional'
CAMPO_DATA = 'data_atualiza_1'
TABELA_ALVO = 'tb_hierarquia_chefe'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
TABELA_DESTINO = TABELA_ALVO + '_stg' # USADO SOMENTE NAS TABELAS STAGE.
ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
PRESTMT = f'TRUNCATE {TABELA_DESTINO}'

# COLUNAS DA TABELA.
COLUNAS = ['*']
COLUNAS = ['login_1','tp_user_1','repre_1','cargo_1','centro_custo_1','nome_1','cpf_1','data_admissao_1',
            'data_demissao_1','data_atualiza_1','login_atualiza_1','chefe_login_2','tp_user_2','repre_2','cargo_2','centro_custo_2','nome_2',
            'chefe_login_3','tp_user_3','repre_3','cargo_3','centro_custo_3','nome_3','chefe_login_4','tp_user_4','repre_4','cargo_4',
            'centro_custo_4','nome_4','chefe_login_5','tp_user_5','repre_5','cargo_5','centro_custo_5','nome_5','chefe_login_6','tp_user_6',
            'repre_6','cargo_6','centro_custo_6','nome_6','chefe_login_7','tp_user_7','repre_7','cargo_7','centro_custo_7','nome_7',
            'chefe_login_8','tp_user_8','repre_8','cargo_8','centro_custo_8','nome_8','chefe_login_9','tp_user_9','repre_9',
            'cargo_9','centro_custo_9','nome_9','chefe_login_10','tp_user_10','repre_10','cargo_10','centro_custo_10','nome_10','status','chefes'
            ]


def main():
    # PROCESSO EXECUTOR DO ETL.
    p =prep()
    e = extract()
    t = transform(e)
    l = load(t,0)

    return print(l)


def dump_log(data):
    # SALVA LOG DE ETL.
    file = os.path.join(dir, f'log_{TABELA_ALVO}_{start_process.strftime("%Y%m%d")}.json')
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


def extract()-> pd.DataFrame:
    # EXTRAÇÃO DOS DADOS NA ORIGEM.
    global start_process

    start_process = datetime.now()
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f'SELECT {col} FROM {TABELA_ORIGEM}'

    start_time = time.time()
    print(f'{TABELA_ALVO.upper()} - INICIANDO REPLICAÇÃO...')

    try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(0, BANCO_ORIGEM)
        data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        counter(start_time)
        print("DADOS EXTRAÍDOS!")
    except Exception:
        print("ERRO NA EXTRAÇÃO!")

    return data


def transform(df)-> pd.DataFrame:
    # CRIA COLUNAS AUXILIARES DO ETL.
    # df.insert(loc=len(df.columns), column='etl_data', value=ETL_DATA) INSERIR NO FIM.
    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)

    return df


def load(df, tipo)->dict:
    # CARGA DOS DADOS VIA STREAMING.
    global end_process

    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    start_time = time.time()

    if tipo == 0:
        print("INSERINDO DADOS...")
        df.to_sql(TABELA_DESTINO, con=engine_destination,  index=False, if_exists='append')
    elif tipo == 1:
        df.to_csv(ARQUIVO, sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
        print("DADOS SALVOS!")
        print("CARREGANDO DADOS...")
        db.bulk(ARQUIVO, BANCO_DESTINO, TABELA_DESTINO, 0)

    counter(start_time)
    end_process = datetime.now()
    print("DADOS CARREGADOS!")
    metadata = meta(df)
    dump_log(metadata)
    
    return metadata


def meta(df)-> dict:
    # METADADOS DO PROCESSO DE ETL.
    metadata = dict()
    metadata['Nome'] = TABELA_ALVO
    metadata['Registros'] = int(df.shape[0])
    data_ref = df[CAMPO_DATA].max()
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
