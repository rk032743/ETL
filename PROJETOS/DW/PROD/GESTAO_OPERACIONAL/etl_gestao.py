
from etl_hierarquia_chefe import main as etl_hierarquia_chefe
from etl_admin import main as etl_admin
from etl_srh import main as srh_batch
from etl_srh import update as srh_update
from datetime import datetime, timedelta


def d1():

    etl_hierarquia_chefe()
    etl_admin()
    srh_batch()
    srh_update()


if __name__ == '__main__':
    d1()