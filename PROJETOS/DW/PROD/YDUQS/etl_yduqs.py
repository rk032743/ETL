from datetime import datetime, timedelta
from tb_proposta_funil import batch as batch_prop_funil, update as update_prop_funil, nrt as nrt_prop_funil
from tb_proposta_leads import batch as batch_prop_leads, update as update_prop_leads, nrt as nrt_prop_leads
from tb_ativo_funil import batch as batch_ativo_funil, update as update_ativo_funil, nrt as nrt_ativo_funil
from tb_ativo_leads import batch as batch_ativo_leads, update as update_ativo_leads, nrt as nrt_ativo_leads
from tb_proposta_leaks import batch as batch_prop_leaks, update as update_prop_leaks, nrt as nrt_prop_leaks
from tb_ativo_leads_removidas import batch as batch_ativo_leads_removidas, update as update_ativo_leads_removidas, nrt as nrt_ativo_leads_removidas
from yduqs_tb_follow_atividade_sac import batch as batch_follow_atividade_sac, update as update_follow_atividade_sac, nrt as nrt_follow_atividade_sac
from yduqs_tb_follow_evento_sac import batch as batch_follow_evento_sac, update as update_follow_evento_sac, nrt as nrt_follow_evento_sac

from yduqs_tb_hr0d_avaliacao import batch as batch_hr0d_avaliacao, update as update_hr0d_avaliacao, nrt as nrt_hr0d_avaliacao
from yduqs_tb_hr0d_interacao import batch as batch_hr0d_interacao, update as update_hr0d_interacao, nrt as nrt_hr0d_interacao

from yduqs_tb_hr0e_interacao import batch as batch_hr0e_interacao, update as update_hr0e_interacao, nrt as nrt_hr0e_interacao
from yduqs_tb_hr0e_reclamacao import batch as batch_hr0e_reclamacao, update as update_hr0e_reclamacao, nrt as nrt_hr0e_reclamacao

from yduqs_tb_hr0f_interacao import batch as batch_hr0f_interacao, update as update_hr0f_interacao, nrt as nrt_hr0f_interacao
from yduqs_tb_hr0f_reclamacao import batch as batch_hr0f_reclamacao, update as update_hr0f_reclamacao, nrt as nrt_hr0f_reclamacao

from tb_proposta_wyden_funil import batch as batch_proposta_wyden_funil, update as update_proposta_wyden_funil, nrt as nrt_proposta_wyden_funil
from tb_proposta_wyden_leads import batch as batch_proposta_wyden_leads, update as update_proposta_wyden_leads, nrt as nrt_proposta_wyden_leads
from tb_prop_yduqs_estacio import batch as batch_prop_yduqs_estacio, update as update_prop_yduqs_estacio, nrt as nrt_prop_yduqs_estacio
from yduqs_tb_im_hw import batch as batch_yduqs_tb_im_hw, nrt as nrt_yduqs_tb_im_hw

from yduqs_tb_hr0d_reclamacao import reproc as reproc_hr0d_reclamacao, update as update_hr0d_reclamacao, nrt as nrt_hr0d_reclamacao


def hist():

    batch_hr0e_reclamacao()
    batch_hr0f_reclamacao()
    reproc_hr0d_reclamacao()


def d1(dia=1):

    batch_prop_funil(dia)
    batch_prop_leads(dia)
    update_prop_leads(dia)
    update_prop_funil(dia)

    batch_ativo_funil(dia)
    batch_ativo_leads(dia)
    update_ativo_funil(dia)
    update_ativo_leads(dia)
    batch_prop_leaks(dia)
    update_prop_leaks(dia)
    batch_ativo_leads_removidas(dia)
    update_ativo_leads_removidas(dia)

    batch_follow_atividade_sac(dia)
    update_follow_atividade_sac(dia)
    batch_follow_evento_sac(dia)
    update_follow_evento_sac(dia)

    batch_hr0d_avaliacao(dia)
    batch_hr0d_interacao(dia)
    batch_hr0e_interacao(dia)
    batch_hr0f_interacao(dia)

    batch_proposta_wyden_funil(dia)
    batch_proposta_wyden_leads(dia)
    batch_prop_yduqs_estacio(dia)

    update_proposta_wyden_funil(dia)
    update_proposta_wyden_leads(dia)
    update_prop_yduqs_estacio(dia)

    batch_yduqs_tb_im_hw(dia)

    
def d0():

    nrt_hr0e_interacao()
    nrt_hr0e_reclamacao()
    nrt_hr0f_interacao()
    nrt_hr0f_reclamacao()
    nrt_hr0d_avaliacao()
    nrt_hr0d_interacao()
    nrt_hr0d_reclamacao()

    nrt_prop_funil()
    nrt_prop_leads()
    nrt_ativo_funil()
    nrt_ativo_leads()
    nrt_prop_leaks()
    nrt_ativo_leads_removidas()
    nrt_follow_atividade_sac()
    nrt_follow_evento_sac()

    nrt_proposta_wyden_funil()
    nrt_proposta_wyden_leads()
    nrt_prop_yduqs_estacio()
    
    nrt_yduqs_tb_im_hw()




if __name__ == '__main__':
    d0()
    # 
    # for dia in reversed(range(0,4)):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     batch_hr0e_reclamacao(dia)
    #     batch_hr0f_reclamacao(dia)
    # reproc_hr0d_reclamacao()
#         d1(dia)
#     d0()
#     d1(3)