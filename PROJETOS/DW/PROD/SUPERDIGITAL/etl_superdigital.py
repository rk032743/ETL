
from superdigital_tb_follow_atividade import batch as batch_follow, update as updt_follow, nrt as nrt_follow
from superdigital_tb_follow_evento import batch as batch_evento, update as updt_evento, nrt as nrt_evento
from datetime import datetime, timedelta
import sys


def d1(dia=1):

    batch_follow(dia)
    batch_evento(dia)
    update(dia)


def update(dia=1):

    updt_follow(dia)
    updt_evento(dia)


def d0():
    
    nrt_follow()
    nrt_evento()


if __name__ == '__main__':
        d1()
    # for dia in reversed(range(1,11)):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     batch_follow(dia)