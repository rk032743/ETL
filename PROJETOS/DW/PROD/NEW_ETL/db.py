import os
from urllib.parse import quote_plus
import json
import pyodbc 
import pymysql

class ConfigDB:

    def __init__(self):

        project_path = os.path.dirname(os.path.abspath(__file__))
        param_file = os.path.join(project_path, 'credentials.json')
        f = open(param_file)
        self.credentials = json.load(f)
        f.close()
  

    def get_credentials(self, server)-> list:

        self.__server = server

        j_filtrado = list(filter(lambda x: x['NAME'] == self.__server, self.credentials['PARAMETROS']))

        lista = []

        for elem in j_filtrado:
            
            lista.append(elem['DRIVER'])
            lista.append(elem['USERNAME'])
            lista.append(elem['PASSWORD'])
            lista.append(elem['HOST'])
            lista.append(elem['PORT'])
            lista.append(elem['DB'])
            lista.append(elem['CHARSET'])
            lista.append(elem['EXTRA'])
            
        return lista


if __name__ == '__main__':
    db = ConfigDB()
    json_ = db.get_credentials(server="MSSQL_DATALAKE_SERVER")
    print(json_)



# print(DRIVER)
# cnn = pyodbc.connect('DRIVER={Devart ODBC Driver for SQL Server};Server=myserver;Database=mydatabase;Port=myport;User ID=myuserid;Password=mypassword')