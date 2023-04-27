
from VELOE.etl_veloe import d0 as etl_veloe
from BMG.etl_bmg import d0 as etl_bmg
from LIGACOES.etl_ligacoes import d0 as etl_ligacoes 
from ORIZON.etl_orizon import d0 as etl_orizon


if __name__ == "__main__":
    etl_veloe()
    etl_bmg()
    etl_orizon()
    etl_ligacoes()