import json
from pathlib import Path
import os
from carga_forecast import forecast
from main_latam import SQLServerDB as forecast_latam

root = Path(__file__).parent
rsp = os.path.join(root, 'forecast.json')

with open(rsp, 'r') as json_file:
    j = json_file.read()
rsp_dict = json.loads(j)


def execute_multi():

    for key in rsp_dict['LISTA']:
        print(key['ID'])
        print(key['REPRESENTANTE'])
        print(key['ARQUIVO'])
        print(key['BANCO'])
        print(key['TABELA'])
        forecast(key['REPRESENTANTE'],key['ARQUIVO'],key['BANCO'],key['TABELA'],key['ID'])
    return

def execute_one(id:int):

    result = next(k for k in rsp_dict['LISTA'] if k["ID"] == id)
    print(result['ID'])
    print(result['REPRESENTANTE'])
    print(result['ARQUIVO'])
    print(result['BANCO'])
    print(result['TABELA'])
    forecast(result['REPRESENTANTE'],result['ARQUIVO'],result['BANCO'],result['TABELA'],result['ID'])

    return

if __name__ == '__main__':
    forecast_latam()
    execute_multi()
    # execute_one(90)
    # execute_one(65)
