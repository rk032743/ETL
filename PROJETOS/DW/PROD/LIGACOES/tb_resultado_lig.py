import os, logging
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import db
from sqlalchemy import text
from get_dir import get_onedrive_dirs


########################################### PARÂMETROS. ###########################################

dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'LIGACOES')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))



BANCO_DESTINO = 'dm_db_ligacoes'
CAMPO_DATA = 'data'
ETL_DATA = datetime.now().strftime('%Y-%m-%d')
ANOMES = pd.to_datetime(ETL_DATA).strftime('%y%m')
TABELA_ALVO = 'tb_resultado_lig'
TB_A = TABELA_ALVO.replace('tb_', 'tb_a_')
TABELA_DESTINO = BANCO_DESTINO + '.' + TB_A + '_d0'
PRESTMT = f"TRUNCATE {TABELA_DESTINO}"
COLUNAS = ['nro_discador',
            'seq',
            'seq_fone',
            'seq_resultado',
            'cod_resultado',
            'data',
            'hora',
            'login_operador',
            'id_lig',
            'bina',
            'bina_ddi',
            'campanha',
            'ano_mes',
            'tempo_tot_lig',
            'st_venda_cartao',
            'cod_recusa',
            'classe_recusa',
            'bad_fone',
            'bad_contact',
            'target_sim',
            'target_nao',
            'filtro_classe_recusa',
            'cpf_cnpj',
            'tipo_ligacao',
            'data',
            'hora_agenda',
            'grupo',
            'score_motivo',
            'no_contact'
            ]
###################################################################################################

def representante(tabela) -> str:

    loc = tabela.find('.')
    length = len(tabela)
    limit = tabela.rfind('_',0,loc)
    repre = tabela[limit+1:loc].upper()
    return repre


def main() -> list:


    col = 'TABLE_SCHEMA'
    where  = f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{TABELA_ALVO}{ANOMES}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise', 'bd_bi_dimep')"
    SQL = f'SELECT {col} FROM information_schema.TABLES {where}'
    engine_source = db.conn_engine(0, 'information_schema')
    data = pd.read_sql(sql=SQL, con=engine_source)
    result = data['TABLE_SCHEMA'].values.tolist()
    p =prep()

    for bd in result:

        print(bd)
        nrt(banco=bd)

    return 


def nrt(dia=0, banco='')-> dict:
    # PARÂMETROS DE TABELAS.
    global parametros
    global ETL_DATA
    ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    ANOMES = pd.to_datetime(ETL_DATA).strftime('%y%m')
    parametros = dict()

    BANCO_ORIGEM = banco
    CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
    REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
    TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO + ANOMES
    
    ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
    
    WHERE = f"WHERE {CAMPO_DATA} = '{ETL_DATA}' "
    
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
    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    start_time = time.time()
    try:
        print("PREPARANDO AMBIENTE...")
        with engine_destination.connect() as conn:
            truncate_statement = PRESTMT
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

    df['representante'] = representante(parametros['TABELA_ORIGEM'])

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
    dump_log(metadata)
    
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

