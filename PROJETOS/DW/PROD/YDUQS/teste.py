# from tb_proposta_wyden_funil import batch as batch_proposta_wyden_funil, update as update_proposta_wyden_funil, nrt as nrt_proposta_wyden_funil
# from tb_proposta_wyden_leads import batch as batch_proposta_wyden_leads, update as update_proposta_wyden_leads, nrt as nrt_proposta_wyden_leads
# from tb_prop_yduqs_estacio import batch as batch_prop_yduqs_estacio, update as update_prop_yduqs_estacio, nrt as nrt_prop_yduqs_estacio
from datetime import datetime, timedelta
from yduqs_tb_hr0d_avaliacao import batch as batch_hr0d_avaliacao, update as update_hr0d_avaliacao, nrt as nrt_hr0d_avaliacao
from yduqs_tb_hr0d_interacao import batch as batch_hr0d_interacao, update as update_hr0d_interacao, nrt as nrt_hr0d_interacao
from yduqs_tb_hr0d_reclamacao import batch as batch_hr0d_reclamacao, update as update_hr0d_reclamacao, nrt as nrt_hr0d_reclamacao

from yduqs_tb_hr0e_interacao import batch as batch_hr0e_interacao, update as update_hr0e_interacao, nrt as nrt_hr0e_interacao
from yduqs_tb_hr0e_reclamacao import batch as batch_hr0e_reclamacao, update as update_hr0e_reclamacao, nrt as nrt_hr0e_reclamacao

from yduqs_tb_hr0f_interacao import batch as batch_hr0f_interacao, update as update_hr0f_interacao, nrt as nrt_hr0f_interacao
from yduqs_tb_hr0f_reclamacao import batch as batch_hr0f_reclamacao, update as update_hr0f_reclamacao, nrt as nrt_hr0f_reclamacao

def d1(dia=1):

    batch_hr0d_avaliacao(dia)
    batch_hr0d_interacao(dia)
    batch_hr0d_reclamacao(dia)
    batch_hr0e_interacao(dia)
    batch_hr0e_reclamacao(dia)
    batch_hr0f_interacao(dia)
    batch_hr0f_reclamacao(dia)


if __name__ == '__main__':

    for dia in range(0,6):
        ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
        print(ETL_DATA)
        d1(dia)
