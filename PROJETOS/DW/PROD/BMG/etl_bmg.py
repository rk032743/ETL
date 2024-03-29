from tb_follow_atividade_cross import batch as batch_cross, update as update_cross, nrt as nrt_cross
from tb_follow_atividade_fcr import batch as batch_fcr, update as update_fcr, nrt as nrt_fcr
from datetime import datetime, timedelta
import pandas as pd

def d1(dia=1):

    batch_cross(dia)
    batch_fcr(dia)
    update_cross(dia)
    update_fcr(dia)

def d0():
    nrt_cross()
    nrt_fcr()


# if __name__ == '__main__':
#     nrt_fcr()
#     d1()
#     d0()

#     # for dia in reversed(range(0,4)):
#     #     dt = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
#     #     print(dt)
#     #     # batch_cross(dia)
#     #     batch_fcr(dia)
