
from tb_treinamento import batch as tb1
from tb_treinamento_historico import batch as tb2

from datetime import datetime, timedelta


def d1(dia=1):

    tb1(dia)
    tb2(dia)



if __name__ == '__main__':
    d1()
# # #     for dia in range(1,19):
# # #         ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
# # #         print(ETL_DATA)
# # #         batch(dia)
#         d1()