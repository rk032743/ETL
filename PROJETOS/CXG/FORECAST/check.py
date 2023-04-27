
import os
from pathlib import Path
from mariadb import MariaDB

cnn = MariaDB()

BANCO = 'cxg_db_semparar'
TABELA = 'tb_forecast'
ID = 65
_id = str(ID)
root = Path(__file__).parent
sqlFilePath = os.path.join(root, 'update_log_sql.sql')
f = open(sqlFilePath, 'r', encoding='utf-8')
sqlFile = f.read()
sql = sqlFile
sql = sql.replace('vID', _id)
sql = sql.replace('vBanco',BANCO)
sql = sql.replace('vTabela',TABELA)
cnn.execute_from_file(sql)


