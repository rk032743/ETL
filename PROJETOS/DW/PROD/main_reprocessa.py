
from TEMPOS.etl_tempos_d1 import d1 as etl_tempos
from LIGACOES.etl_ligacoes import d1 as etl_ligacoes
from GESTAO_OPERACIONAL.etl_gestao import d1 as etl_gestao
from QUALIDADE.etl_qualidade import d1 as etl_treinamento
from VELOE.etl_veloe import d1 as etl_veloe
from BMG.etl_bmg import d1 as etl_bmg
from ORIZON.etl_orizon import d1 as etl_orizon
from INTERGRALL.etl_intergrall import d1 as etl_intergrall
from CAIXA_PRE.etl_caixa_pre import d1 as etl_caixa_pre
from TRACKER.etl_tracker import d1 as etl_tracker
from SUPERDIGITAL.etl_superdigital import d1 as etl_superdigital
from WFM.etl_wfm import d1 as etl_wfm
from datetime import datetime, timedelta

def d1(dia=1):

        etl_tempos(dia)

        etl_ligacoes(dia)

        etl_treinamento(dia)

        etl_veloe(dia)

        etl_bmg(dia)

        etl_orizon(dia)

        etl_caixa_pre(dia)

        etl_tracker(dia)

        etl_superdigital(dia)


if __name__ == '__main__':
    for dia in reversed(range(2,3)):
        ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
        print(ETL_DATA)
        etl_tempos(dia)
        etl_ligacoes(dia)
        # d1(dia)
    # etl_gestao()
    # etl_intergrall()

    # for dia in reversed(range(1,4)):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     etl_wfm()
