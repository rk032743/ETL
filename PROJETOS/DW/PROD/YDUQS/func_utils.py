import pandas as pd
import yduqs_db as db
from sqlalchemy import text


def expurga_colunas_extras(df, colunas, banco, tabela, conn, *schema) -> pd.DataFrame:

    BD = banco
    TB = tabela
    SCHM = schema

    cols_origem = colunas
    df = df[cols_origem].copy()
    print("COLUNAS ORIGEM")
    print("=============================")
    for colname in cols_origem:
        print(colname)
    print("=============================")

    if conn in (0,1,3):
        SQL_COLUMN_CHECK_DESTINO = (f"SELECT COLUMN_NAME AS col_name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{BD}' AND TABLE_NAME = '{TB}'")
    elif conn ==2:
        SQL_COLUMN_CHECK_DESTINO =  (f"SELECT name AS col_name FROM sys.columns WHERE object_id = OBJECT_ID('{BD}.{SCHM}.{TB}') ")
    
    engine_source = db.conn_engine(conn, BD)
    data = pd.read_sql(sql=SQL_COLUMN_CHECK_DESTINO, con=engine_source)
    cols_destino  = data['col_name'].values.tolist()

    print("COLUNAS DESTINO")
    print("=============================")
    print(SQL_COLUMN_CHECK_DESTINO)
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

    for colname in non_match:
        SQL_DROP_COLUMNS = (f"ALTER TABLE {BD}.{TB} DROP COLUMN {colname};")
        engine_source = db.conn_engine(conn, BD)
        with engine_source.connect() as conn:
            conn.execute(text(SQL_DROP_COLUMNS))
            conn.commit()
            print(f"COLUNA '{colname}' EXCLUÍDA!")

    return df

