from GESTAO_OPERACIONAL.etl_gestao import d1 as etl_gestao
from TEMPOS.etl_tempos import d1 as etl_tempos
from QUALIDADE.etl_qualidade import d1 as etl_treinamento
from VELOE.etl_veloe import d1 as etl_veloe
from BMG.etl_bmg import d1 as etl_bmg
from ORIZON.etl_orizon import d1 as etl_orizon
# from LIGACOES.etl_ligacoes import d1 as etl_ligacoes

"""CRIAR ETL D1 DAS LIGACOES"""
if __name__ == "__main__":
    # try:
    #     etl_ligacoes()
    # except:
    #     pass
    try:
        etl_gestao()
    except:
        pass
    try:
        etl_tempos()
    except:
        pass
    try:
        etl_treinamento()
    except:
        pass
    try:
        etl_veloe()
    except:
        pass
    try:
        etl_bmg()
    except:
        pass
    try:
        etl_orizon()
    except:
        pass