
from VELOE.etl_veloe import d0 as etl_veloe
from BMG.etl_bmg import d0 as etl_bmg
from LIGACOES.etl_ligacoes import d0 as etl_lig
from CAIXA_PRE.etl_caixa_pre import d0 as etl_caixa_pre
from TRACKER.etl_tracker import d0 as etl_tracker
from SUPERDIGITAL.etl_superdigital import d0 as etl_superdigital
from INTERGRALL.etl_intergrall import d0 as etl_intergrall_im_hw


if __name__ == "__main__":

        etl_tracker()

        etl_superdigital()

        etl_intergrall_im_hw()

        etl_veloe()

        etl_bmg()

        etl_caixa_pre()

        etl_lig()

        

