from tb_proposta_wyden_funil import batch as batch_proposta_wyden_funil, update as update_proposta_wyden_funil, nrt as nrt_proposta_wyden_funil
from tb_proposta_wyden_leads import batch as batch_proposta_wyden_leads, update as update_proposta_wyden_leads, nrt as nrt_proposta_wyden_leads
from tb_prop_yduqs_estacio import batch as batch_prop_yduqs_estacio, update as update_prop_yduqs_estacio, nrt as nrt_prop_yduqs_estacio
from datetime import datetime, timedelta

def d1(dia=1):

    batch_proposta_wyden_funil(dia)
    batch_proposta_wyden_leads(dia)
    batch_prop_yduqs_estacio(dia)



if __name__ == '__main__':

    for dia in range(0,6):
        ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
        print(ETL_DATA)
        d1(dia)