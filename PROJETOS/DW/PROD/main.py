from datetime import datetime, timedelta
from TEMPOS.etl_tempos_d1 import d1 as etl_tempos
from LIGACOES.etl_ligacoes import d1 as etl_ligacoes
from GESTAO_OPERACIONAL.etl_gestao import d1 as etl_gestao
from QUALIDADE.etl_qualidade import d1 as etl_treinamento
from INTERGRALL.etl_intergrall import d1 as etl_intergrall
from WFM.etl_wfm import d1 as etl_wfm
from YDUQS.etl_yduqs import hist as etl_ydqus_hr_reclamacao



if __name__ == '__main__':

        etl_tempos()

        etl_ligacoes()

        etl_gestao()

        etl_treinamento()

        etl_intergrall()

        etl_wfm()

        etl_ydqus_hr_reclamacao()

# if __name__ == '__main__':

#     for dia in range(0,6):
#         ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
#         print(ETL_DATA)
#         d1(dia)
