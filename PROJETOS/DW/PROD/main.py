
from GESTAO_OPERACIONAL.etl_hierarquia_chefe import main as etl_hierarquia_chefe
from TEMPOS.etl_tempos import d1 as etl_tempos
from QUALIDADE.etl_qualidade import d1 as etl_treinamento
from VELOE.etl_veloe import d1 as etl_veloe
from BMG.etl_bmg import d1 as etl_bmg
from ORIZON.etl_orizon import d1 as etl_orizon


if __name__ == "__main__":
    etl_hierarquia_chefe()
    etl_tempos()
    etl_treinamento()
    etl_veloe()
    etl_bmg()
    etl_orizon()