from msilib import sequence
import os, logging, shutil
import pandas as pd
from datetime import datetime, timedelta
import time
import re
from get_dir import get_onedrive_dirs
from mariadb import MariaDB


ETL_DATA = datetime.now().strftime('%Y-%m-%d')
DATA_REF = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# DATA_REF = '2021-01-01'
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'GESTAO_OPERACIONAL')
if not os.path.isdir(dir):
    os.makedirs(dir)

BANCO_ORIGEM = 'bd_bi_gestao'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
TABELA = 'tb_srh'
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA
BANCO_DESTINO = 'dm_db_gestao_operacional'
TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA + '_stg'
ARQUIVO = os.path.join(dir,f'{TABELA}_stg.csv')
PRESTMT = f'TRUNCATE TABLE {TABELA_DESTINO}'

COLUNAS = ['*']
CAMPO_CHAVE = 'data_solicitacao'


def main():

    print(f'{TABELA} - INICIANDO REPLICAÇÃO...')
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} WHERE {CAMPO_CHAVE} = '{DATA_REF}';"
    cnn = MariaDB(2)
    print('EXTRAINDO DADOS...')
    df = cnn.read_sql(SQL_ORIGEM)
    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)
    df = transform(df)
    print(df.head())
    print('SALVANDO ARQUIVO...')
    df.to_csv(ARQUIVO, sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
    print('ARQUIVO SALVO!')
    cnn = MariaDB(1)
    print('CARREGANDO PARA O BANCO...')
    cnn.load_data(PRESTMT, ARQUIVO, TABELA_DESTINO, 0)
    print('CARGA CONCLUÍDA!')


def update():

    TMP_TABLE = BANCO_DESTINO+'.'+TABELA + '_temp'
    STG_TABLE =  TABELA_DESTINO
    PRESTMT = f"CREATE TABLE IF NOT EXISTS {TMP_TABLE} LIKE {STG_TABLE}"
    WHERE = f"WHERE data_atualiza = '{DATA_REF}' "
    POSSTMT = f"DROP TABLE IF EXISTS {TMP_TABLE}"
    UPDATE = f"REPLACE INTO {STG_TABLE} SELECT * FROM {TMP_TABLE} {WHERE}"

    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} {WHERE};"
    cnn = MariaDB(2)
    print('EXTRAINDO DADOS...')
    df = cnn.read_sql(SQL_ORIGEM)
    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)
    df = transform(df)
    print(df.head())
    print('SALVANDO ARQUIVO...')
    df.to_csv(ARQUIVO, sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
    print('ARQUIVO SALVO!')
    cnn = MariaDB(1)
    print('CARREGANDO PARA O BANCO...')
    print(PRESTMT)
    cnn.load_data(PRESTMT, ARQUIVO, TMP_TABLE, 0)
    print('CARGA CONCLUÍDA!')
    cnn = MariaDB(1)
    cnn.execute_sql(UPDATE)
    cnn = MariaDB(1)
    cnn.execute_sql(POSSTMT)
    print('DADOS ATUALIZADOS!')


def transform(df)-> pd.DataFrame:
    # CONVERSÃO DE TIMEDELTA PARA HORA.
    # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "ITERITEMS" POR "ITEMS".
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
    # REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))


    return df


# if __name__ == '__main__':
#     # main()
#     update()