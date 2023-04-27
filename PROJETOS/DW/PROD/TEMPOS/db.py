from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy import event
from sqlalchemy.dialects import mssql, mysql
import pyodbc
import pymysql
from urllib.parse import quote_plus
import time
import os



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
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True
    return engine


Base = declarative_base()

class Treinamento(Base):

    __tablename__ = 'tb_treinamento'
    # __table_args__ = {"schema": "ext"}
    id = Column(Date, primary_key=True)
    repre = Column(mysql.TIME, primary_key=True)
    question = Column(String(50), primary_key=True)
    codigo = Column(String(50), primary_key=True)
    treinamento = Column(Float)
    dias = Column(Float)
    local = Column(Float)
    horas = Column(Float)
    data_inicio = Column(Float)
    data_pa = Column(Float)
    usuario = Column(Float)
    status = Column(Float)
    quantidade = Column(Float)
    data_cri = Column(Float)
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

# print("Delete Data...")
# start_time = time.time()
# engine.execute("DELETE FROM dm_latam.ext.tb_forecast WHERE data IN (SELECT DISTINCT data FROM dm_latam.ext.tb_forecast_temp);")
# print("--- %s seconds ---" % (time.time() - start_time))

# print("Insert Valid Data...")
# start_time = time.time()
# engine.execute("INSERT INTO dm_latam.ext.tb_forecast SELECT * FROM dm_latam.ext.tb_forecast_temp;")
# print("--- %s seconds ---" % (time.time() - start_time))

# print("Drop Temp Table...")
# start_time = time.time()
# engine.execute("DROP TABLE dm_latam.ext.tb_forecast_temp;")
# print("--- %s seconds ---" % (time.time() - start_time))
