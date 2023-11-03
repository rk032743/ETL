import os, logging
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import intergrall_db as db
from sqlalchemy import text
from sqlalchemy.exc import OperationalError as erro
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from get_dir import get_onedrive_dirs


########################################### PARÂMETROS. ###########################################

dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'TALK')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    os.remove(os.path.join(dir, f))
    if not f.endswith(".csv"):
        continue
    try:
        os.remove(os.path.join(dir, f))
    except:
        pass


BANCO_ORIGEM = 'bd_bi_talk'
BANCO_DESTINO = 'dm_db_talk'
CAMPO_DATA = 'data_cri'
TABELA_MOD = 'tb_im_hw17_'
COLUNAS = ['*']
###################################################################################################

def maintance() -> str:
    
    global tabela_alvo
    
    ETL_DATA = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    ANOMES = pd.to_datetime(ETL_DATA).strftime('%y%m')
    tabela_alvo = f'{TABELA_MOD}{ANOMES}'
    CREATE = f'CREATE TABLE IF NOT EXISTS {tabela_alvo} LIKE tb_im_hw17_2309;'
    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    with engine_destination.connect() as conn:
        cmd = CREATE
        try:
            conn.execute(text(cmd))
        except erro as err:
            if err.orig.args[0]==1050:
                pass
            else:
                print("Erro:", err.orig.args[0])
                print("Desc:", err.orig.args[1])
        conn.execute(text(cmd))
        conn.commit()

    return tabela_alvo


def representante(tabela) -> str:

    loc = tabela.find('.')
    length = len(tabela)
    limit = tabela.rfind('_',-1,loc)
    repre = tabela[limit+1:loc].replace('bd_bi_', '').upper()

    return repre


def d1(dia) -> list:

    global ETL_DATA

    tb = maintance()

    TABELA_DESTINO = tb
    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    col = 'TABLE_SCHEMA'
    where  = f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{tabela_alvo}' AND TABLE_SCHEMA = 'bd_bi_talk';"
    SQL = f'SELECT {col} FROM information_schema.TABLES {where}'
    engine_source = db.conn_engine(0, 'information_schema')
    data = pd.read_sql(sql=SQL, con=engine_source)
    result = data['TABLE_SCHEMA'].values.tolist()

    
    PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_DATA} = '{ETL_DATA}'"

    p = prep(PRESTMT)

    for bd in result:

        print(bd)
        batch(dia=dia,banco=bd,tabela=tb)

    return


def d0() -> list:

    global ETL_DATA
    global TABELA_DESTINO

    tb = maintance()
    TABELA_DESTINO = tb

    ETL_DATA = datetime.now().strftime('%Y-%m-%d')
    col = 'TABLE_SCHEMA'
    where  = f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{tabela_alvo}' AND TABLE_SCHEMA = 'bd_bi_talk';"
    SQL = f'SELECT {col} FROM information_schema.TABLES {where}'
    engine_source = db.conn_engine(0, 'information_schema')
    data = pd.read_sql(sql=SQL, con=engine_source)
    result = data['TABLE_SCHEMA'].values.tolist()
    PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_DATA} = '{ETL_DATA}'"
    p =prep(PRESTMT)

    for bd in result:

        print(bd)
        nrt(banco=bd)

    return 


def batch(dia=1, banco='', tabela='')-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    ANOMES_ATUAL = datetime.now().strftime('%y%m')

    print(ANOMES_ATUAL)


    parametros = dict()

    BANCO_ORIGEM = banco
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + tabela_alvo
    
    ARQUIVO = os.path.join(dir, f'{tabela_alvo}_{BANCO_ORIGEM}_{ETL_DATA}.csv')
    WHERE = f"WHERE {CAMPO_DATA} = '{ETL_DATA}'"
    # WHERE = f"WHERE {CAMPO_DATA} >= '2023-10-01'"
    
    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_DATA
    parametros['tabela_alvo'] = tabela_alvo
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = BANCO_DESTINO+'.'+tabela
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = 'SELECT 1'
    parametros['WHERE'] = WHERE
    parametros['COLUNAS'] = COLUNAS

    
    e = extract()
    t = transform(e)
    l = load(t,1)
    # l = load(t,0)
    return l


def nrt(dia=0, banco='')-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros

    ANOMES = pd.to_datetime(ETL_DATA).strftime('%y%m')
    parametros = dict()

    BANCO_ORIGEM = banco
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + tabela_alvo
    PRESTMT = f"SELECT 1"
    ARQUIVO = os.path.join(dir, f'{tabela_alvo}_{BANCO_ORIGEM}_{ETL_DATA}.csv')
    WHERE = f"WHERE {CAMPO_DATA} = '{ETL_DATA}' "
    
    parametros['BANCO_ORIGEM'] = BANCO_ORIGEM
    parametros['BANCO_DESTINO'] = BANCO_DESTINO
    parametros['CAMPO_DATA'] = CAMPO_DATA
    parametros['tabela_alvo'] = tabela_alvo
    parametros['REPRESENTANTE'] = REPRESENTANTE
    parametros['TABELA_ORIGEM'] = TABELA_ORIGEM
    parametros['TABELA_DESTINO'] = TABELA_DESTINO
    parametros['ARQUIVO'] = ARQUIVO
    parametros['PRESTMT'] = PRESTMT
    parametros['WHERE'] = WHERE
    parametros['COLUNAS'] = COLUNAS

    
    e = extract()
    t = transform(e)
    l = load(t,1)

    return l


def dump_log(data):
    # SALVA LOG DE ETL.
    # tabela_alvo = parametros['tabela_alvo']
    # file = os.path.join(dir, f'log_{tabela_alvo}_{start_process.strftime("%Y%m%d")}.json')
    # with open(file, 'w') as fp:
    #     json.dump(data, fp, ensure_ascii=False, indent=4)
    pass


def counter(start_time):
    # counter DE TEMPO DECORRIDO.
    tempo = (time.time() - start_time)
    return print("TEMPO DECORRIDO: %s SEGUNDOS" % round(tempo, 1))


def prep(stmt):
    # PREPARAÇÃO DAS TABELAS DE DESTINO, DELETE OU TRUNCATE.

    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    start_time = time.time()
    try:
        print("PREPARANDO AMBIENTE...")
        with engine_destination.connect() as conn:
            statement1 = stmt
            conn.execute(text(statement1))
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

    tabela_alvo = parametros['tabela_alvo']
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

    df['representante'] = representante(parametros['TABELA_ORIGEM'])
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
        df.to_csv(parametros['ARQUIVO'], sep=";",index=False, lineterminator= '\r\n', encoding='utf-8')
        print("DADOS SALVOS!")
        print("CARREGANDO DADOS...")
        db.bulk(arquivo=parametros['ARQUIVO'], banco=parametros['BANCO_DESTINO'], tabela=parametros['TABELA_DESTINO'], tipo=0)

    counter(start_time)
    end_process = datetime.now()
    print("DADOS CARREGADOS!")
    metadata = meta(df)
    
    
    return metadata


def meta(df)-> dict:
    # METADADOS DO PROCESSO DE ETL.
    metadata = dict()
    metadata['Nome'] = parametros['tabela_alvo']
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
    d0()

#     for i in range(1,29):
#         # print(i)
#         ETL_DATA = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
#         print(ETL_DATA)
#         d1(i)