
from tb_ctr_mailing import batch as ctr_mailing
from tb_microstrategy_fila_geral import main as microstrategy_fila_geral
from tb_microstrategy_grupo_agente import main as microstrategy_grupo_agente
from tb_microstrategy_login_logout import main as microstrategy_login_logout
from tb_microstrategy_pausa import main as microstrategy_pausa
from tb_microstrategy_sessao_agente import main as microstrategy_sessao_agente
from tb_provimento_consolidado import main as provimento_consolidado
from tb_im_hw17 import d1 as im_hw17_batch, d0 as im_hw17_nrt
from tb_im_hw import batch as im_hw_batch, nrt as im_hw_nrt
from datetime import datetime, timedelta


def d1(dia=1):

    im_hw17_batch(dia)
    im_hw_batch(dia)
    ctr_mailing(dia)
    microstrategy_fila_geral()
    microstrategy_grupo_agente()
    microstrategy_login_logout()
    microstrategy_pausa()
    microstrategy_sessao_agente()
    provimento_consolidado()


def d0():

    im_hw17_nrt()
    im_hw_nrt()



if __name__ == '__main__':
    d0()
    # for dia in reversed(range(0,5)):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     im_hw_batch(dia)
