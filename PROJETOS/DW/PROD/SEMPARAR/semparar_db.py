from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import event
from sqlalchemy.dialects import mssql, mysql
import sqlalchemy as db
import pyodbc
import pymysql
from urllib.parse import quote_plus
import time
import os
import pandas as pd
from datetime import datetime, timedelta
from get_dir import get_appdata_dir, get_onedrive_dirs

D1 = datetime.now() - timedelta(days=1)
DATA_ALVO = D1.strftime('%Y-%m-%d')
DATA_ARQUIVO = D1.strftime('%Y%m%d')

dirs = get_onedrive_dirs()
dir = os.path.join(dirs['dump_dir'], 'DW', 'SEMPARAR')
if not os.path.isdir(dir):
    os.makedirs(dir)

for f in os.listdir(dir):
    if not f.endswith(".csv"):
        continue
    os.remove(os.path.join(dir, f))


def conn_engine(id_db:int, banco:str):

    global engine
    global dbconfig
    if id_db == 0:
        # BD BI - MYSQL
        dbconfig = {
                    'username': 'report.etl',
                    'password': 'eu2czb',
                    'host': '192.168.1.10',
                    'port': '3306',
                    'db': f'{banco}'
                    }
        engine = create_engine(f"mysql+pymysql://{dbconfig['username']}:{dbconfig['password']}@{dbconfig['host']}/{dbconfig['db']}?" \
                                "charset=utf8mb4",isolation_level="READ UNCOMMITTED")
    if id_db == 1:
        # DW - MYSQL
        dbconfig = {
                    'username': 'adm_etl',
                    'password': 'Sh34d%1',
                    'host': '192.168.1.11',
                    'port': '3306',
                    'db': f'{banco}'
                    }
        engine = create_engine(f"mysql+pymysql://{dbconfig['username']}:{dbconfig['password']}@{dbconfig['host']}/{dbconfig['db']}?" \
                                "charset=utf8mb4",isolation_level="READ UNCOMMITTED")

    elif id_db == 2:
        # DW - MSSQL
        dbconfig = {'driver': quote_plus('ODBC Driver 17 for SQL Server'),
                    'username': 'etl_konecta',
                    'password': '#ETL@KonectaBI2023',
                    'host': '10.65.0.20',
                    'port': '1433',
                    'db': f'{banco}'
                    }
        engine = create_engine(f"mssql+pyodbc://{dbconfig['username']}:{dbconfig['password']}@{dbconfig['host']}:{dbconfig['port']}/{dbconfig['db']}?driver={dbconfig['driver']}")
    
    elif id_db == 3:
        # GENESYS -PGSQL
        d = get_appdata_dir()

        # dbconfig = {
        #             "host": "154.12.233.47",
        #             "user": "konecta",
        #             "dbname": "sem_parar",
        #             "port": "7432",
        #             "sslcert": d['certificate'] ,
        #             "sslkey": d['key'],
        #             "sslrootcert": "<<path to verification ca chain>>",
        #             "sslmode": "verify-full"
        #             }
        
        dbconfig = {
                    "host": "154.12.233.47",
                    "user": "konecta",
                    "dbname": "sem_parar",
                    "port": "7432",
                    "sslcert": d['certificate'] ,
                    "sslkey": d['key'],
                    "sslmode": "require"
                    }

        engine = create_engine('postgresql+psycopg2://', connect_args=dbconfig)

    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True
    return engine


Base = declarative_base()

class UserState(Base):

    __tablename__ = 'tb_a_genesys_user_state'

    id = Column(String(50), primary_key=True)
    user_id = Column(String(50))
    start_time = Column(mysql.DATETIME)
    end_time = Column(mysql.DATETIME)
    state = Column(String(50))
    state_id = Column(String(50))
    duration = Column(String(50))
    type = Column(String(50))
    _sdc_received_at = Column(mysql.DATETIME)
    _sdc_sequence = Column(mysql.BIGINT)
    _sdc_table_version = Column(mysql.BIGINT)
    _sdc_batched_at = Column(mysql.DATETIME)
    criado_em = Column(mysql.TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    atualizado_em = Column(mysql.TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())




def bulk(arquivo, banco, tabela, tipo, *args):
        # load_data(self, caminho, arquivo, tabela):
        # 1. Realiza o bulk insert de um arquivo no disco.
        # 2. Em caso de erros exibe uma mensagem.
        # arquivo = os.path.join(caminho, arquivo)
        pathfile = arquivo.replace(os.path.sep, '/')
        if tipo == 1:
            insert = "REPLACE"
        else:
            insert = "IGNORE"

        comando = rf"""LOAD DATA LOCAL INFILE '{pathfile}'
                    {insert} INTO TABLE {tabela}
                    CHARACTER SET utf8mb4 
                    FIELDS TERMINATED BY ';'
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\r\n'
                    IGNORE 1 LINES;"""
        try:

            con = pymysql.connect(host=dbconfig['host'], user=dbconfig['username'], password=dbconfig['password'],
                                  database=banco, charset='utf8mb4', autocommit=True, local_infile=1)

            cursor = con.cursor()
            cursor.execute(comando)

        except IntegrityError as err:
            print(err.detail)


if __name__ == '__main__':

    # engine = conn_engine(1, "cxg_db_semparar")
    
    TABELA_ORIGEM = 'user_state'
    ARQUIVO = os.path.join(dir, f'{TABELA_ORIGEM}_{DATA_ARQUIVO}.csv')
    sql = f"SELECT * FROM public.{TABELA_ORIGEM} WHERE start_time BETWEEN '{DATA_ALVO} 00:00:00' AND '{DATA_ALVO} 23:59:59' ORDER BY 3 ASC;"
    sql = f"SELECT * FROM public.{TABELA_ORIGEM} ORDER BY 3 ASC;"
    engine_source = conn_engine(3, "sem_parar")
    engine_destination = conn_engine(1, "cxg_db_semparar")
    data = pd.read_sql(sql=sql, con=engine_source)
    data.to_csv(ARQUIVO, sep=';',index=False, line_terminator= '\r\n', encoding='utf-8')
    print(data.head(100))
    data.to_sql(f'tb_a_genesys_{TABELA_ORIGEM}', con=engine_destination,  index=False, if_exists='append')

    print(D1)
    print(DATA_ALVO)
    print(sql)
