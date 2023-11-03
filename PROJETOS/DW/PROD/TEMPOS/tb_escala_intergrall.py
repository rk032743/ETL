from msilib import sequence
import os, logging, shutil
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import re
from get_dir import get_onedrive_dirs
from tempos_mariadb import MariaDB


ETL_DATA = datetime.now().strftime('%Y-%m-%d')
DATA_REF = (datetime.now() - timedelta(days=1)).strftime('%Y%m')

# DATA_REF = 202304
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'TEMPOS')
if not os.path.isdir(dir):
    os.makedirs(dir)

BANCO_ORIGEM = 'bd_bi_call_center'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + 'tb_escala_intergrall'
BANCO_DESTINO = 'dm_db_tempos'
TABELA_DESTINO = BANCO_DESTINO + '.' + 'tb_escala_intergrall_stg'
ARQUIVO = os.path.join(dir,'tb_escala_intergrall_stg.csv')
CAMPO_CHAVE = 'data_entrada'
PRESTMT = f'DELETE FROM {TABELA_DESTINO} WHERE EXTRACT(YEAR_MONTH FROM {CAMPO_CHAVE}) = {DATA_REF};'


COLUNAS = ['*']


def main():

    print('PONTO ESPELHO DSR - INICIANDO REPLICAÇÃO...')
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} WHERE EXTRACT(YEAR_MONTH FROM {CAMPO_CHAVE}) = {DATA_REF};"
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


if __name__ == '__main__':
    main()