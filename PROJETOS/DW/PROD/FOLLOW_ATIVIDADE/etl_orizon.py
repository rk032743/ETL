
from tb_follow_atividade import batch as batch_follow, update as update_follow, nrt as nrt_follow

from datetime import datetime, timedelta


def d1(dia=1):

    batch_follow(dia)
    update_follow(dia)


def d0():
    nrt_follow()


# if __name__ == '__main__':
#     for dia in range(1,4):
#         ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
#         print(ETL_DATA)
#         batch_follow(dia)
        # d1()

if __name__ == '__main__':
    d1(2)