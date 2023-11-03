import pandas as pd
import os
from datetime import datetime, timedelta
import time
import json
import numpy as np
import wfm_db as db
from sqlalchemy import text




project_path = os.path.dirname(os.path.abspath(__file__))
param_file = os.path.join(project_path, 'params.json')
f = open(param_file)
j = json.load(f)
f.close()
obj = len(j['parametros'])
n=0
for i in range(n,obj):
    TB_ORIGEM = j['parametros'][i]['TB_ORIGEM']
    TB_DESTINO = j['parametros'][i]['TB_DESTINO']
    COLUNAS = str(j['parametros'][i]['COLUNAS'])
    WHERE = j['parametros'][i]['WHERE']
    BD_0RIGEM = j['parametros'][i]['BD_0RIGEM']
    SCHEMA = j['parametros'][i]['SCHEMA']
    BD_DESTINO = j['parametros'][i]['BD_DESTINO']
    PERIODICIDADE = j['parametros'][i]['PERIODICIDADE']
    DELETE = j['parametros'][i]['DELETE']

    cols_origem = COLUNAS.strip('][').split(',')
    cols_origem = [x.strip(' ') for x in cols_origem]
    
    print("COLUNAS ORIGEM")
    print("=============================")
    for colname in cols_origem:
        print(colname)
    print("=============================")

    SQL_COLUMN_CHECK_DESTINO =  (f"SELECT name AS col_name FROM sys.columns WHERE object_id = OBJECT_ID('{BD_DESTINO}.{SCHEMA}.{TB_DESTINO}') ")
    engine_source = db.conn_engine(2, BD_DESTINO)
    data = pd.read_sql(sql=SQL_COLUMN_CHECK_DESTINO, con=engine_source)
    cols_destino  = data['col_name'].values.tolist()

    print("COLUNAS DESTINO")
    print("=============================")
    for colname in cols_destino:
        print(colname)
    print("=============================")

    non_match = []
    for i in cols_destino:
        if i not in cols_origem:
            non_match.append(i)
    print("COLUNAS EXTRAS")
    print("=============================")
    print(non_match)
    print("=============================")

    # for colname in non_match:
    #     SQL_DROP_COLUMNS = f"ALTER TABLE {BD_DESTINO}.{SCHEMA}.{TB_DESTINO} DROP COLUMN {colname};"
    #     engine_source = db.conn_engine(2, BD_DESTINO)
    #     with engine_source.connect() as conn:
    #         conn.execute(text(SQL_DROP_COLUMNS))
    #         conn.commit()
    #         print(f"COLUNA '{colname}' EXCLUÍDA!")

