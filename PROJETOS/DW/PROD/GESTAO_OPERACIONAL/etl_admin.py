import os, logging
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import re
import db
from sqlalchemy import text
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
CAMPO_DATA = 'data_geracao'
TABELA_ALVO = 'tbadmin'
CHARINDEX = re.search('bd_bi_', BANCO_ORIGEM).end()
REPRESENTANTE = BANCO_ORIGEM[CHARINDEX:].upper()
TABELA_ORIGEM  = BANCO_ORIGEM + '.' + TABELA_ALVO
TABELA_DESTINO = TABELA_ALVO + '_stg' # USADO SOMENTE NAS TABELAS STAGE.
ARQUIVO = os.path.join(dir, f'{TABELA_ALVO}.csv')
PRESTMT = f'TRUNCATE {TABELA_DESTINO}'

# COLUNAS DA TABELA.

COLUNAS = ['*']
COLUNAS = ['data_geracao','login','login_ant','tp_user','tp_user_ant','nome','hora_trabalho','empresa','cpf','data_admissao','data_atualiza',
           'data_demissao','hora_trabalho_sabado','rh_cargo','sexo','data_nasc','hora_trabalho_domingo','centro_custo','contrato','ilha',
           'carga_horaria','data_ferias','data_ferias_ant','data_promocao','data_promocao_ant','cep','end_tip_log','end_nom_log','end_num',
           'end_compl','bairro','cidade','uf','microsiga_ferias_data_ini','microsiga_ferias_data_fim','dt_ini_afastamento','dt_fim_afastamento',
           'rg','rg_orgao','rg_data','rg_uf','marca_ponto','email','celula','email_uranet','cod_filial_microsiga','login_sistema_cliente',
           'equipe','grupo_atende','atende','sub_niveis','telefone','escala_horario','lanche_entrada','lanche_saida','lanche_entrada_sabado',
           'lanche_saida_sabado','lanche_entrada_domingo','lanche_saida_domingo','ramal']


def main():
    # PROCESSO EXECUTOR DO ETL.

    e = extract() # Extração dos dados na fonte.
    t = transform(e) # Transformação e limpeza dos dados.
    s = staging(t) # Amrazenamento temporário em arquivo csv.
    p = prep() # Preparação da tabela de destino Delete/Truncate.
    l = load(s,1) # Carga dos dados para a tabela de destino.
    m = meta(l) # Metadados do processo ETL, utilizado para logs.

    return print(m)


def insert_log(data):
    # SALVA LOG DE ETL.
    file = os.path.join(dir, f'log_{TABELA_ALVO}_{start_process.strftime("%Y%m%d")}.json')
    with open(file, 'w') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=4)


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
            conn.execute(text(truncate_statement))
            conn.commit()
        counter(start_time)
        msg = "AMBIENTE PRONTO!"
    except Exception as err:
        msg = "ERRO NA PREPARAÇÃO!"
        print(err)
    
    return print(msg)


def extract()-> pd.DataFrame:
    # EXTRAÇÃO DOS DADOS NA ORIGEM.
    global start_process

    start_process = datetime.now()
    col = ','.join(COLUNAS)
    SQL_ORIGEM = f"SELECT {col} FROM {TABELA_ORIGEM} WHERE rh_cargo NOT IN ('EXT') AND LEFT(login, 1) = 'r' ;"

    start_time = time.time()
    print(f'{TABELA_ALVO.upper()} - INICIANDO REPLICAÇÃO...')

    try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(0, BANCO_ORIGEM)
        df = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        counter(start_time)
        print("DADOS EXTRAÍDOS!")
    except Exception:
        print("ERRO NA EXTRAÇÃO!")

    return df


def transform(df)-> pd.DataFrame:
    # CRIA COLUNAS AUXILIARES DO ETL.
    # df.insert(loc=len(df.columns), column='etl_data', value=ETL_DATA) INSERIR NO FIM.
    df.insert(loc=0, column='etl_data', value=ETL_DATA)
    df.insert(loc=0, column='etl_empresa', value=REPRESENTANTE)
    df.insert(loc=0, column='etl_origem', value=TABELA_ORIGEM)

    # CONVERSÃO DE TIMEDELTA PARA HORA.
    # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "ITERITEMS" POR "ITEMS".
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
    # REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))

    return df


def staging(df):
    
    global end_process
    # STAGING, SALVA GRANDES VOLUMES DE DADOS TEMPORARIAMENTE EM ARQUIVO CSV.
    start_time = time.time()
    print("SALVANDO ARQUIVO...!")
    # NO PANDAS 2.0 É NECESSÁRIO SUBSTITUIR O "LINE_TERMINATOR" POR "LINETERMINATOR".
    df.to_csv(ARQUIVO, sep=';',index=False, lineterminator= '\r\n', encoding='utf-8')
    counter(start_time)
    print("DADOS SALVOS!")

    return df


def load(df, tipo)->dict:
    # CARGA DOS DADOS VIA STREAMING.
    global end_process

    engine_destination = db.conn_engine(1, BANCO_DESTINO)
    start_time = time.time()

    if tipo == 0:
        print("INSERINDO DADOS...")
        df.to_sql(TABELA_DESTINO, con=engine_destination,  index=False, if_exists='append', chunksize=10000)
    elif tipo == 1:
        print("CARREGANDO DADOS...")
        db.bulk(ARQUIVO, BANCO_DESTINO, TABELA_DESTINO, 0)

    counter(start_time)
    end_process = datetime.now()
    print("DADOS CARREGADOS!")

    return df
    

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
    dump_log(metadata)

    return metadata


if __name__ == '__main__':
    m = main()
