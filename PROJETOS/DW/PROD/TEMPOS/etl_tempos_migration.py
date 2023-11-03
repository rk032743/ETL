from MySQLToSQL_v2 import espelho as to_sql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas


    
    

if __name__ == '__main__':

    # DATA_REF_FIM = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
    # DATA_REF_FIM = (datetime.now() + relativedelta(months=1)).strftime('%Y%m')

    # DATA_REF_INI = (datetime.now() - relativedelta(months=1)).strftime('%Y%m')
    # DATA_REF_INI = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
    # print(DATA_REF_INI)
    # print(DATA_REF_FIM)
    datas = pandas.date_range('2023-09-29','2023-10-29',freq='d')

    for i in datas:

        # DATA_REF = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        DATA_REF = datetime.now() - i
        print(i)
        print(DATA_REF.days, 'Dias')
        dia = DATA_REF.days
        to_sql(dia)
    

