
from VELOE.etl_veloe import d0 as etl_veloe
from BMG.etl_bmg import d0 as etl_bmg
from LIGACOES.etl_ligacoes import d0 as etl_ligacoes 
from ORIZON.etl_orizon import d0 as etl_orizon


if __name__ == "__main__":
  
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
    try:
        etl_ligacoes()
    except:
        pass