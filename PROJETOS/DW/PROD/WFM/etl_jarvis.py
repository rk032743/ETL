import pandas as pd
import os
from datetime import datetime, timedelta
import time
import json
import numpy as np
import wfm_db as db
from sqlalchemy import text



def hist(dia=1) -> pd.DataFrame:

    global TB_ORIGEM
    global TB_DESTINO
    global WHERE
    global BD_0RIGEM
    global BD_DESTINO
    global SCHEMA
    global STMT
    global COLUNAS

    DATAREF = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')

    project_path = os.path.dirname(os.path.abspath(__file__))
    param_file = os.path.join(project_path, 'params.json')
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
        COLUNAS = str(j['parametros'][i]['COLUNAS'])
        WHERE = j['parametros'][i]['WHERE']
        BD_0RIGEM = j['parametros'][i]['BD_0RIGEM']
        SCHEMA = j['parametros'][i]['SCHEMA']
        BD_DESTINO = j['parametros'][i]['BD_DESTINO']
        PERIODICIDADE = j['parametros'][i]['PERIODICIDADE']
        DELETE = j['parametros'][i]['DELETE']

        # D-1

        print(TB_ORIGEM)

        if DELETE == 0:
            STMT = "SELECT 1"
        elif DELETE == 2:
            STMT = f"TRUNCATE TABLE {BD_DESTINO}.{SCHEMA}.{TB_DESTINO}"
        elif DELETE == 1:
            if WHERE == "":
                STMT = "SELECT 1"
            else:
                STMT = f"DELETE FROM {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} {WHERE} '{DATAREF}'"

        print(STMT)

        if WHERE == "":
            SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM}"
        else:
            SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM} {WHERE} '{DATAREF}'"

        print(SQL_ORIGEM)
        # try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(3, BD_0RIGEM)
        data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        print("DADOS EXTRAÍDOS!")
        df = expurga_colunas_extras(data, COLUNAS)
        importToSQL(df)
        # except Exception:
            # print("ERRO NA EXTRAÇÃO!")
            # df = 'Erro'
            # sys.exit()

    return

def nrt(dia=0) -> pd.DataFrame:

    global TB_ORIGEM
    global TB_DESTINO
    global WHERE
    global BD_0RIGEM
    global BD_DESTINO
    global SCHEMA
    global STMT
    global COLUNAS

    DATAREF = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')

    project_path = os.path.dirname(os.path.abspath(__file__))
    param_file = os.path.join(project_path, 'params.json')
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
        COLUNAS = str(j['parametros'][i]['COLUNAS'])
        WHERE = j['parametros'][i]['WHERE']
        BD_0RIGEM = j['parametros'][i]['BD_0RIGEM']
        SCHEMA = j['parametros'][i]['SCHEMA']
        BD_DESTINO = j['parametros'][i]['BD_DESTINO']
        PERIODICIDADE = j['parametros'][i]['PERIODICIDADE']
        DELETE = j['parametros'][i]['DELETE']

        # D-0
        if PERIODICIDADE == 0:

            print(TB_ORIGEM)

            if DELETE == 0:
                STMT = "SELECT 1"
            else:
                if WHERE == "":
                    STMT = "SELECT 1"
                else:
                    STMT = f"DELETE FROM {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} {WHERE} '{DATAREF}'"

            print(STMT)

            if WHERE == "":
                SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM}"
            else:
                SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM} {WHERE} '{DATAREF}'"

            print(SQL_ORIGEM)
            # try:
            print("EXTRAINDO DADOS...")
            engine_source = db.conn_engine(3, BD_0RIGEM)
            data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
            print("DADOS EXTRAÍDOS!")
            df = expurga_colunas_extras(data, COLUNAS)
            importToSQL(df)
            # except Exception:
                # print("ERRO NA EXTRAÇÃO!")
                # df = 'Erro'
                # sys.exit()
        else:
            continue
    return


def expurga_colunas_extras(df, colunas) -> pd.DataFrame:

    cols_origem = (colunas.strip('][')).split(',')
    cols_origem = [x.strip(' ') for x in cols_origem]
    df = df[cols_origem].copy()
    print("COLUNAS ORIGEM")
    print("=============================")
    for colname in cols_origem:
        print(colname)
    print("=============================")

    SQL_COLUMN_CHECK_DESTINO =  (f"SELECT name AS col_name FROM sys.columns WHERE object_id = OBJECT_ID('{BD_DESTINO}.{SCHEMA}.{TB_DESTINO}') ")
    engine_source = db.conn_engine(2, BD_DESTINO)
    data = pd.read_sql(sql=SQL_COLUMN_CHECK_DESTINO, con=engine_source)
    cols_destino  = data['col_name'].values.tolist()

    print("COLUNAS DESTINO")
    print("=============================")
    for colname in cols_destino:
        print(colname)
    print("=============================")

    non_match = []
    for i in cols_destino:
        if i not in cols_origem:
            non_match.append(i)
    print("COLUNAS EXTRAS")
    print("=============================")
    print(non_match)
    print("=============================")

    for colname in non_match:
        SQL_DROP_COLUMNS = f"ALTER TABLE {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} DROP COLUMN {colname};"
        engine_source = db.conn_engine(2, BD_DESTINO)
        with engine_source.connect() as conn:
            conn.execute(text(SQL_DROP_COLUMNS))
            conn.commit()
            print(f"COLUNA '{colname}' EXCLUÍDA!")

    return df


def transform(df:pd.DataFrame)->pd.DataFrame:

# REMOVE DAYS DO FORMATO TIMEDELTA.
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
# REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('nan', 'None'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('NaT', 'None'))
        df[colname] = df[colname].replace('None', np.nan)

    return df


def importToSQL(df:pd.DataFrame):

    global engine
    
    data = transform(df)
    
    print(data.columns)
    print(df.head())
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



if __name__ == '__main__':
    nrt()
