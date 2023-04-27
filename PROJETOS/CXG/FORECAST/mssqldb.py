from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Float, String, Index, Date, Time, TIMESTAMP, func
from sqlalchemy.orm import sessionmaker, mapper, relationship
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as db
import carga_forecast_latam as f


# CRIA DICIONÁRIO COM CREDENCIAIS DOS BANCOS DO PROJETO.
def db_credentials() -> dict:

    banco = {
        'username': 'adm_etl',
        'password': 'Sh34d%1',
        'host': '192.168.1.11',
        'db': 'cxg_db_latam'
    }
    return banco

db_cred = db_credentials()
engine = create_engine(f"mysql+pymysql://{db_cred['username']}:{db_cred['password']}@{db_cred['host']}/{db_cred['db']}?" \
                        "charset=utf8mb4",isolation_level="READ UNCOMMITTED")
connection = engine.connect()
metadata = db.MetaData()
forecast = db.Table("tb_forecast", metadata, autoload=True, autoload_with=engine)
# print(forecast.columns.keys())

# base = declarative_base()

# mytable = Table("mytable", metadata,
#                     Column('mytable_id', Integer, primary_key=True),
#                     Column('value', String(50))
#                )

cols = [







]

tb_forecast = Table("tb_forecast_temp", metadata,
                    db.Column("data", Date, primary_key=True),
                    db.Column("hora", Time, primary_key=True),
                    db.Column("atendimento", String(100), primary_key=True),
                    db.Column("agrupamento", String(999), primary_key=True),
                    db.Column("recebidas", Float),
                    db.Column("logado", Float),
                    db.Column("tma", Float),
                    db.Column("ns", Float),
                    db.Column("nr17", Float),
                    db.Column("reforco", Float),
                    db.Column("dialogo", Float),
                    db.Column("feedback", Float),
                    db.Column("particular", Float),
                    db.Column("criado_em", TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp()),
                    db.Column("atualizado_em", TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())
                    )

# metadata.create_all(engine)

if __name__ == '__main__':
    data = f.read_forecast(86)
    print(data.columns)
    data.to_sql("tb_forecast_temp", con=engine, index=False, if_exists='replace')
    print("Concluído")

    

