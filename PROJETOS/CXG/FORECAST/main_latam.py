from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Time, DateTime, and_, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import mssql
from sqlalchemy import Table, Column, Float, String, Integer, Index, Date, Time, TIMESTAMP, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import URL
from sqlalchemy import event
import pandas as pd
from urllib.parse import quote_plus
import time
import carga_forecast_latam as f


class SQLServerDB:

    global db_url
    global connection_url
    global db_engine


    dbconfig = {'driver': '{ODBC Driver 17 for SQL Server}',
                'username': 'etl_konecta',
                'password': '#ETL@KonectaBI2023',
                'host': '10.65.0.20',
                'port': '1433',
                'db': 'dm_latam'
                    }
    
    # Configura de conexão com o banco de dados
    db_url = f"DRIVER={dbconfig['driver']};PORT={dbconfig['port']};SERVER={dbconfig['host']};DATABASE={dbconfig['db']};UID={dbconfig['username']};PWD={dbconfig['password']}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": db_url})
    print(db_url)


    engine = create_engine(connection_url, echo=False)
    # conn = db_engine.connect()
    # print(conn)
    # with engine.connect() as conn:
    #     query = text('SELECT TOP 10 * FROM db_temp.tb_presence_definitions')
    #     response = conn.execute(query).fetchall()
    #     print(response)
    # for row in response:
    #    print(row)
    Base = declarative_base()
    class Forecast(Base):

        __tablename__ = 'tb_forecast_temp'
        __table_args__ = {"schema": "ext"}
        data = Column(Date, primary_key=True)
        hora = Column(mssql.TIME(0), primary_key=True)
        atendimento = Column(String(50), primary_key=True)
        agrupamento = Column(String(50), primary_key=True)
        recebidas = Column(Float)
        logado = Column(Float)
        tma = Column(Float)
        ns = Column(Float)
        nr17 = Column(Float)
        reforco = Column(Float)
        dialogo = Column(Float)
        feedback = Column(Float)
        particular = Column(Float)
        criado_em = Column(mssql.DATETIME2, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
        atualizado_em = Column(mssql.DATETIME2, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    Base.metadata.create_all(engine)
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True

        #conn = db_engine.connect()

        #response = conn.execute('select top 3 * from db_temp.tb_presence_definitions')
        #for row in response:
        #    print(row)
        
    
    # limpa a tabela temporaria
    print("TRUNCATE Data...")
    with engine.connect() as conn:
        truncate_statement = 'TRUNCATE TABLE ext.tb_forecast_temp'
        conn.execute(text(truncate_statement))
        conn.commit()

    data = f.read_forecast(86)
    print(data.columns)
    start_time = time.time()

    print("LOAD Data...")
    data.to_sql("tb_forecast_temp", con=engine, schema = 'ext', index=False, if_exists='append')
    print("--- %s seconds ---" % (time.time() - start_time))


    start_time = time.time()
    with engine.connect() as conn:
        
        print("Delete Data...")
        delete_statement = "DELETE FROM dm_latam.ext.tb_forecast WHERE data IN (SELECT DISTINCT data FROM dm_latam.ext.tb_forecast_temp);"
        insert_statement = "INSERT INTO dm_latam.ext.tb_forecast SELECT * FROM dm_latam.ext.tb_forecast_temp;"
        drop_statement = "DROP TABLE dm_latam.ext.tb_forecast_temp;"
        conn.execute(text(delete_statement))
        conn.commit()
        conn.execute(text(insert_statement))
        conn.commit()
        conn.execute(text(drop_statement))
        conn.commit()
        print("--- %s seconds ---" % (time.time() - start_time))

    
if __name__ == '__main__':
    SQLServerDB()