from tb_lead_veloe_emrec1 import batch as batch_emrec1, update as update_emrec1, nrt as nrt_emrec1
from tb_follow_atividade_veloe import batch as batch_follow, update as update_follow, nrt as nrt_follow

from datetime import datetime, timedelta


def d1():
    batch_emrec1(1)
    batch_follow(1)
    update_emrec1(1)
    update_follow(1)


def d0():
    nrt_emrec1()
    nrt_follow()


# if __name__ == '__main__':
# #     for dia in range(1,19):
# #         ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
# #         print(ETL_DATA)
# #         batch(dia)
#         d1()