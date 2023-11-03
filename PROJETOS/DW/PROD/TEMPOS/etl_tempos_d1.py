from tb_ponto_espelho_dsr import main as espelho_dsr
from tb_produtividade_tempo import d1 as prod_tempo_d1
from tb_escala_intergrall import main as escala
from tb_ponto_espelho_marcacoes import main as espelho_marcacoes
from tb_ponto_espelho_afastamentos import main as espelho_afastamentos
from tb_ponto_espelho_dados import main as espelho_dados
from tb_ponto import d1 as ponto
from MySQLToSQL_v2 import export as to_mssql
from datetime import datetime, timedelta, date
import pandas

def d1(dia=1):
    
    espelho_dsr()
    espelho_dados()
    escala()
    ponto(dia)
    espelho_marcacoes()
    espelho_afastamentos()
    prod_tempo_d1(dia)
    to_mssql(dia)


if __name__ == '__main__':
    d1()

    # for dia in reversed(range(2,4)):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     d1(dia)
