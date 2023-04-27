from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, select, func
from sqlalchemy.exc import IntegrityError
import pyodbc
from urllib.parse import quote_plus
from sqlalchemy.dialects import mssql
import carga_forecast_latam as f
import time
from sqlalchemy import event



def connection_string(id_db:int, banco:str):

    if id_db == 1:

        db_auth = {
                    'username': 'adm_etl',
                    'password': 'Sh34d%1',
                    'host': '192.168.1.11',
                    'port': '3306',
                    'db': f'{banco}'
                    }
        cnn = f"mysql+pymysql://{db_auth['username']}:{db_auth['password']}@{db_auth['host']}/{db_auth['db']}?charset=utf8mb4?isolation_level=READ UNCOMMITTED"
    
    elif id_db == 2:

        db_auth = {'driver': quote_plus('ODBC Driver 17 for SQL Server'),
                    'username': 'etl_konecta',
                    'password': '#ETL@KonectaBI2023',
                    'host': '10.65.0.20',
                    'port': '1433',
                    'db': f'{banco}'
                    }
        cnn = f"mssql+pyodbc://{db_auth['username']}:{db_auth['password']}@{db_auth['host']}:{db_auth['port']}/{db_auth['db']}?driver={db_auth['driver']}"
    
    return cnn


Base = declarative_base()

class Latam_mysql():

    tb_forecast = Table(
                        "tb_forecast_temp",
                        MetaData(),
                        Column("data", Date, primary_key=True),
                        Column("hora", Time, primary_key=True),
                        Column("atendimento", String(50), primary_key=True),
                        Column("agrupamento", String(50), primary_key=True),
                        Column("recebidas", Float),
                        Column("logado", Float),
                        Column("tma", Float),
                        Column("ns", Float),
                        Column("nr17", Float),
                        Column("reforco", Float),
                        Column("dialogo", Float),
                        Column("feedback", Float),
                        Column("particular", Float),
                        Column("criado_em", TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp()),
                        Column("atualizado_em", TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
                        )
    
class Latam_mssql():
    
    bd_schema = 'ext'
    tb_forecast = Table(
                        "tb_forecast_temp",
                        MetaData(schema=bd_schema),
                        Column("data", Date, primary_key=True),
                        Column("hora", mssql.TIME(0), primary_key=True),
                        Column("atendimento", String(50), primary_key=True),
                        Column("agrupamento", String(50), primary_key=True),
                        Column("recebidas", Float),
                        Column("logado", Float),
                        Column("tma", Float),
                        Column("ns", Float),
                        Column("nr17", Float),
                        Column("reforco", Float),
                        Column("dialogo", Float),
                        Column("feedback", Float),
                        Column("particular", Float),
                        Column("criado_em", mssql.DATETIME2, server_default=func.current_timestamp(), onupdate=func.current_timestamp()),
                        Column("atualizado_em", mssql.DATETIME2, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
                        )

id= 2
# id = 1
banco = 'dm_latam'
# banco = 'dba_etl_logs'
cnn = connection_string(id, banco)

engine = create_engine(cnn,echo=True)
Latam_mssql().tb_forecast.create(engine, checkfirst=True)

tb_full_name = str(Latam_mssql().tb_forecast)
dot = tb_full_name.index('.')
tabela = tb_full_name[dot+1:]
__schema = Latam_mssql().bd_schema

@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(
    conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

data = f.read_forecast(86)

start_time = time.time()
print("Load Data...")
with engine.connect() as conn:
    truncate_statement = f"TRUNCATE TABLE {tb_full_name}"
    conn.execution_options(autocommit=True).execute(truncate_statement)

data.to_sql(f"{tabela}", con=engine, schema= __schema, index=False, if_exists='append')
print("--- %s seconds ---" % (time.time() - start_time))

print("Delete data...")
start_time = time.time()
engine.execute(f"DELETE FROM dm_latam.ext.tb_forecast WHERE data IN (SELECT DISTINCT data FROM {tb_full_name});")
print("--- %s seconds ---" % (time.time() - start_time))

print("Inserting valid data...")
start_time = time.time()
engine.execute(f"INSERT INTO dm_latam.ext.tb_forecast SELECT * FROM {tb_full_name};")
print("--- %s seconds ---" % (time.time() - start_time))

print("Drop Temp Table...")
start_time = time.time()
Latam_mssql().tb_forecast.drop(engine, checkfirst=True)
print("--- %s seconds ---" % (time.time() - start_time))

# stmt = tb_forecast.select().where(tb_forecast.c.data == '2023-02-06')
# print(stmt)

# with engine.connect() as conn:
#     for row in conn.execute(stmt):
#         pprint(row)
