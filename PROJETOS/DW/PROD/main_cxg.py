import os
import logging
from datetime import datetime, timedelta

from YDUQS.etl_yduqs import d1 as etl_yduqs
from VELOE.etl_veloe import d1 as etl_veloe
from CAIXA_PRE.etl_caixa_pre import d1 as etl_caixa_pre
from TRACKER.etl_tracker import d1 as etl_tracker
from SUPERDIGITAL.etl_superdigital import d1 as etl_superdigital
from BMG.etl_bmg import d1 as etl_bmg
from ORIZON.etl_orizon import d1 as etl_orizon


FUNCS = [       
etl_yduqs,
etl_veloe,
etl_caixa_pre,
etl_tracker,
etl_superdigital,
etl_bmg,
etl_orizon
]

FUNCS_NAME = [
'etl_yduqs',
'etl_veloe',
'etl_caixa_pre',
'etl_tracker',
'etl_superdigital',
'etl_bmg',
'etl_orizon'
]

def now():
        dtt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return dtt


def caller(dia=1):

        dir = os.path.dirname(os.path.abspath(__file__))
        
        logfile = os.path.join(dir, 'log_main_cxg.log')
        with open(logfile, 'w') as fp:
                pass
        logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.DEBUG)
        logging.debug(f'{now()}: Rotina iniciada!')
        for x, function in enumerate(FUNCS):
                if x == None:
                        pass
                else:
                        try:
                                logging.debug(f'{now()}: Executando --> {FUNCS_NAME[x]}')
                                function(dia)
                                # print(function)
                                logging.debug(f'{now()}: Concluído --> {FUNCS_NAME[x]}!')
                        except:
                                logging.error(f'{now()}: Falha --> {FUNCS_NAME[x]}!')

                                pass

        logging.debug(f'{now()}: Rotina finalizada!')


def processa_dia():
        # VERIFICA SE É SEGUNDA-FEIRA E EXECUTA A ROTINA DE 3 DIAS
        d = datetime.now()
        dw = int(d.isoweekday())

        if dw == 1:
                for dia in reversed(range(1,4)):
                        ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
                        print(f"EXECUTANDO {ETL_DATA}")
                        caller(dia)
        else:
                caller(1)


if __name__ == '__main__':

        processa_dia()
