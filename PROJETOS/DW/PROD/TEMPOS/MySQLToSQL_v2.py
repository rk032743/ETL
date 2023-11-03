import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import sys, os, json
import tempos_db as db
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, func
from sqlalchemy.dialects import mssql


def export(dia=1) -> pd.DataFrame:

    global TB_ORIGEM
    global TB_DESTINO
    global WHERE
    global BD_0RIGEM
    global BD_DESTINO
    global SCHEMA
    global STMT
    global PREP

    DATAREF = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    DATA_REF_FIM = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
    DATA_REF_FIM = (datetime.now() + relativedelta(months=1)).strftime('%Y%m')

    DATA_REF_INI = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')
    DATA_REF_INI = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
    ANOMES = f"BETWEEN {DATA_REF_INI} AND {DATA_REF_FIM};"


    project_path = os.path.dirname(os.path.abspath(__file__))
    param_file = os.path.join(project_path, 'tempos_params.json')
    f = open(param_file)
    j = json.load(f)
    f.close()
    obj = len(j['parametros'])
    n=0
    # ESCOLHA O NUMERO DA TABELA
    # n=6
    for i in range(n,obj):
        TB_ORIGEM = j['parametros'][i]['TB_ORIGEM']
        TB_DESTINO = j['parametros'][i]['TB_DESTINO']
        WHERE = j['parametros'][i]['WHERE']
        BD_0RIGEM = j['parametros'][i]['BD_0RIGEM']
        SCHEMA = j['parametros'][i]['SCHEMA']
        BD_DESTINO = j['parametros'][i]['BD_DESTINO']
        TIPO_ATUALIZA = j['parametros'][i]['TIPO_ATUALIZA']
        PREP = j['parametros'][i]['PREP']

        if PREP == "TRUNCATE":
            STMT = f"TRUNCATE TABLE {BD_DESTINO}.{SCHEMA}.{TB_DESTINO}"

        elif PREP == "DELETE":
            
            if TIPO_ATUALIZA == 2:
                STMT = f"DELETE FROM {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} {WHERE} {ANOMES}"
            elif TIPO_ATUALIZA == 1:
                STMT = f"DELETE FROM {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} {WHERE} = '{DATAREF}'"
        else:
            STMT = f"SELECT 1"

        print("Statement")
        print(STMT)

        if TIPO_ATUALIZA == 2: # TIPO_ATUALIZA: 2 = COLUNA ANO_MES, 1 = COLUNA DE DATA
            SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM} {WHERE} {ANOMES}"
        elif TIPO_ATUALIZA == 1:
            SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM} {WHERE} = '{DATAREF}'"
        elif TIPO_ATUALIZA == 3:
        # CARGA FULL
            SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM}"

        
        print(SQL_ORIGEM)
        try:
            print("EXTRAINDO DADOS...")
            engine_source = db.conn_engine(1, BD_0RIGEM)
            data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
            print("DADOS EXTRAÍDOS!")
            importToSQL(data)
        except Exception:
            print("ERRO NA EXTRAÇÃO!")
            data = 'Erro'
            pass
        

    return data


def transform(df:pd.DataFrame)->pd.DataFrame:
    print("########## DATAFRAME ANTES ###########")
    print(df.head())
    print("######################################")
# REMOVE DAYS DO FORMATO TIMEDELTA.
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
# REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        # df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))
        # df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11', 'NULL'))
        # df[colname] = df[colname].astype(str).map(lambda x: x.replace('None', 'NULL'))

        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'None'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('0000-00-00 00:00:00', 'None'))
        # df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11', 'None'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('nan', 'None'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('NaT', 'None'))
        df[colname] = df[colname].replace('None', np.nan)
        
    print("########## DATAFRAME DEPOIS ###########")
    print(df.head())
    print("######################################")
    return df


def importToSQL(df:pd.DataFrame):

    global engine
    
    data = transform(df)

    print(data.columns)
    start_time = time.time()
    engine = db.conn_engine(2, BD_DESTINO)
    print("PREPARE TABLE...")
    with engine.connect() as conn:
        print("DATA TRUNCATED...")
        conn.execute(text(STMT))
        conn.commit()

    print("LOAD Data...")
    data.to_sql(TB_DESTINO, con=engine, schema = SCHEMA, index=False, if_exists='append', chunksize=10000)
    print("LOADED!")
    print("--- %s seconds ---" % (time.time() - start_time))


def ToMSSQL(df:pd.DataFrame):

    global engine

    
    data = transform(df)
    print(data.columns)
    start_time = time.time()
    engine = db.conn_engine(2, BD_DESTINO)
    print("PREPARE TABLE...")
    with engine.connect() as conn:
        print("DATA TRUNCATED...")
        conn.execute(text(STMT))
        conn.commit()

    print("LOAD Data...")
    data.to_sql(TB_DESTINO, con=engine, schema = SCHEMA, index=False, if_exists='append', chunksize=10000)
    print("--- %s seconds ---" % (time.time() - start_time))


def espelho(dia=1):
    
    importToSQL(export(dia))


if __name__ == '__main__':
    export(1)