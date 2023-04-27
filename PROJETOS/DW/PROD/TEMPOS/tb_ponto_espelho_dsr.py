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
dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'TEMPOS')
if not os.path.isdir(dir):
    os.makedirs(dir)

BANCO_ORIGEM = 'bd_bi_call_center'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + 'tb_ptoespelho_dsr'
BANCO_DESTINO = 'dm_db_tempos'
TABELA_DESTINO = BANCO_DESTINO + '.' + 'tb_ptoespelho_dsr_stg'
ARQUIVO = os.path.join(dir,'tb_ptoespelho_dsr_stg.csv')
PRESTMT = f''

COLUNAS = ['flag_empresa','anomes_folha','login','data_entrada','tipo_dsr','data_cri','hora_cri']
CAMPO_CHAVE = 'data_entrada'

def main():

    print('PONTO ESPELHO DSR - INICIANDO REPLICAÇÃO...')
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} WHERE {CAMPO_CHAVE} = '{DATA_REF}';"
    cnn = MariaDB(2)
    print('EXTRAINDO DADOS...')
    df = cnn.read_sql(SQL_ORIGEM)
    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)
    print(df.head())
    print('SALVANDO ARQUIVO...')
    df.to_csv(ARQUIVO, sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
    print('ARQUIVO SALVO!')
    cnn = MariaDB(1)
    print('CARREGANDO PARA O BANCO...')
    cnn.load_data(PRESTMT, ARQUIVO, TABELA_DESTINO, 0)
    print('CARGA CONCLUÍDA!')
