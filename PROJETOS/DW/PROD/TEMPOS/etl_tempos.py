from tb_ponto_espelho_dsr import main as espelho_dsr
from tb_produtividade_tempo import d1 as prod_tempo_d1
from tb_produtividade_tempo import d0 as prod_tempo_d0
from tb_escala_intergrall import main as escala
from tb_ponto_espelho_marcacoes import main as espelho_marcacoes

from datetime import datetime, timedelta, date
import pandas


def d1(dia=1):
    
    prod_tempo_d1(dia)
    espelho_dsr()
    escala()
    espelho_marcacoes()


def d0():
    
    prod_tempo_d0()

    
    
if __name__ == '__main__':
    d0()
# #     # datas = pandas.date_range('2023-03-09',datetime.now()-timedelta(days=1),freq='d')
#     datas = reversed(pandas.date_range('2023-04-01','2023-04-30',freq='d'))

#     for i in datas:

#         # DATA_REF = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
#         DATA_REF = datetime.now() - i
#         # print(i)
#         # print(DATA_REF.days, 'Dias')
#         dia = DATA_REF.days
#         print(i)
        
#         d0(dia)