import pandas as pd
from datetime import datetime, timedelta
import time
import sys
import re
import tempos_db as db
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, func
from sqlalchemy.dialects import mssql

BD_0RIGEM = 'dm_db_tempos'
BD_DESTINO = 'dm_dataoffice'
SCHEMA = 'db_intergrall'
TB_ORIGEM = 'tb_produtividade_tempo_stg'
TB_DESTINO = 'tb_produtividade_tempo'
CAMPO_CHAVE = 'data'


def export(dia) -> pd.DataFrame:

    DATAREF = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    SQL_ORIGEM = f"SELECT * FROM {BD_0RIGEM}.{TB_ORIGEM} WHERE {CAMPO_CHAVE} = '{DATAREF}' ORDER BY data ASC, etl_empresa ASC"
    print(SQL_ORIGEM)
    try:
        print("EXTRAINDO DADOS...")
        engine_source = db.conn_engine(1, BD_0RIGEM)
        data = pd.read_sql(sql=SQL_ORIGEM, con=engine_source)
        print("DADOS EXTRAÍDOS!")
    except Exception:
        print("ERRO NA EXTRAÇÃO!")
        data = 'Erro'
        sys.exit()
    
    return data


def transform(df:pd.DataFrame)->pd.DataFrame:

# REMOVE DAYS DO FORMATO TIMEDELTA.
    for colname, coltype in df.dtypes.items():
        if coltype == 'timedelta64[ns]':
            df[colname] = df[colname].astype(str).map(lambda x: x[7:])
# REMOVE DADOS INVÁLIDOS.
    for colname in df.columns:
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('1111-11-11 00:00:00', 'NULL'))
        df[colname] = df[colname].astype(str).map(lambda x: x.replace('None', 'NULL'))

    return df


def importToSQL(df:pd.DataFrame):

    global engine
    
    data = transform(df)
    print(data.columns)
    start_time = time.time()
    print(data['hora_ini'])
    engine = db.conn_engine(2, BD_DESTINO)

    print("LOAD Data...")
    data.to_sql(TB_DESTINO, con=engine, schema = SCHEMA, index=False, if_exists='append', chunksize=10000)
    print("--- %s seconds ---" % (time.time() - start_time))


def espelho_d0():
    importToSQL(export(0))


if __name__ == '__main__':
    datas = (pd.date_range('2022-01-01','2022-08-16',freq='d'))
    for dias in reversed(datas):
        # 2022-08-16
        DT = (datetime.now() - dias).days
        print(DT)
        # print(DT.days)
        # print(dias)
        # print(DATA_REF.days, 'Dias')
        # dia = DT.days
        # print(dia)
        importToSQL(export(DT))
        print((datetime.now() - timedelta(days=DT)).strftime('%Y-%m-%d'))
    print(f"Dados Carregados!")

