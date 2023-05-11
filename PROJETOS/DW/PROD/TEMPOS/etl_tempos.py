from tb_ponto_espelho_dsr import main as espelho_dsr
from tb_produtividade_tempo import main as prod_tempo
from tb_escala_intergrall import main as escala
from tb_ponto_espelho_marcacoes import main as espelho_marcacoes
from datetime import datetime, timedelta, date
import pandas


def d1(dia=1):
    
    prod_tempo(dia)
    espelho_dsr()
    escala()
    espelho_marcacoes()

def d0():
    
    pass
    

# if __name__ == '__main__':
#     # datas = pandas.date_range('2023-03-09',datetime.now()-timedelta(days=1),freq='d')
#     datas = pandas.date_range('2022-01-29','2022-12-31',freq='d')

#     for i in datas:

#         # DATA_REF = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
#         DATA_REF = datetime.now() - i
#         print(i)
#         print(DATA_REF.days, 'Dias')
#         dia = DATA_REF.days
        
#         d1(dia)
