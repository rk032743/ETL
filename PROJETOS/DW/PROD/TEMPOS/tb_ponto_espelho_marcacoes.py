from msilib import sequence
import os, logging, shutil
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import re
from get_dir import get_onedrive_dirs
from mariadb import MariaDB


ETL_DATA = datetime.now().strftime('%Y-%m-%d')
DATA_REF_FIM = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
DATA_REF_INI = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')
TABELA_REF = (datetime.now() - timedelta(days=1)).strftime('%Y')
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'TEMPOS')
if not os.path.isdir(dir):
    os.makedirs(dir)

BANCO_ORIGEM = 'bd_bi_call_center'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA = f'tb_ptoespelho_marcacoes_{TABELA_REF}'
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA
BANCO_DESTINO = 'dm_db_tempos'
TABELA = TABELA.replace(TABELA_REF, 'stg')
TABELA_DESTINO = BANCO_DESTINO + '.' + TABELA
ARQUIVO = os.path.join(dir, f'{TABELA}.csv')
COLUNAS = ['*']
CAMPO_CHAVE = 'anomes_folha'
PRESTMT = f"DELETE FROM {TABELA_DESTINO} WHERE {CAMPO_CHAVE} BETWEEN {DATA_REF_INI} AND {DATA_REF_FIM};"


print('ANOMES')
print(DATA_REF_INI)
print(DATA_REF_FIM)
print(TABELA_REF)
print(TABELA)
print(TABELA_ORIGEM)
print(TABELA_DESTINO)
print(PRESTMT)

def main():

    print('PONTO ESPELHO DSR - INICIANDO REPLICAÇÃO...')
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} WHERE {CAMPO_CHAVE} BETWEEN {DATA_REF_INI} AND {DATA_REF_FIM};"
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

    return df


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
#     print(main())