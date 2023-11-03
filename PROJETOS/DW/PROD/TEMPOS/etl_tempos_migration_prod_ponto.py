from MySQLToSQL_prod_ponto import espelho as to_sql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas


if __name__ == '__main__':


    datas = pandas.date_range('2023-11-01','2023-11-03',freq='d')

    for i in datas:
        DATA_REF = datetime.now() - i
        print(i)
        print(DATA_REF.days, 'Dias')
        dia = DATA_REF.days
        to_sql(dia)
    

