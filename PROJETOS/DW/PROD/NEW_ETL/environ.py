import os
from dotenv import load_dotenv

load_dotenv("C:\ETL\.env")

print(os.getenv('MSSQL_DATALAKE_SERVER_USR'))
print(os.getenv('MSSQL_DATALAKE_SERVER_PWD'))

print(os.getenv('MYSQL_BDBI_USR'))
print(os.getenv('MYSQL_BDBI_PWD'))

print(os.getenv('MYSQL_DATASCIENCE_USR'))
print(os.getenv('MYSQL_DATASCIENCE_PWD'))

print(os.getenv('MARIADB_JARVIS_COSMOS_USR'))
print(os.getenv('MARIADB_JARVIS_COSMOS_PWD'))

