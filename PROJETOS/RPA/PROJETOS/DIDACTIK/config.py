from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Index, Date, TIMESTAMP, func
import pandas as pd


# CREDENCIAIS DIDACTIK.
username = 'grace.barbosa@grupokonecta.com'
password = 'Sophia01'

# CRIA DICIONÁRIO COM O TOKEN.
def payload():

    credentials = {
        'user_password': username,
        'user_password': password
    }
    return credentials

# CRIA DICIONÁRIO COM CREDENCIAIS DOS BANCOS DO PROJETO.
def db_credentials() -> dict:

    dm_treinamento = {
        'username': 'user',
        'password': 'senha',
        'host': '192.168.1.11',
        'db': 'dm_treinamento'
    }
    return dm_treinamento

# CRIA A TABELA COD CURSO.
def didactik_cod_curso():
    metadata_obj = MetaData()
    tb_a_didactik_cod_curso = Table(
        "tb_a_didactik_cod_curso_stg",
        metadata_obj,
        Column("data", Date),
        Column("tempo", String(100)),
        Column("tipo", String(100)),
        Column("descricao", String(999)),
        Column("cod_curso", String(100)),
        Column("criado_em", TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp()),
        Index('idx_data', 'data'),
        Index('idx_cod_curso', 'cod_curso')
    )
    return metadata_obj
