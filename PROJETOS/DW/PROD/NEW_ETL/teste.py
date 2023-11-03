import os
from tinydb import TinyDB
from tinydb.queries import Query
from pprint import pprint
import json
import tinydb


project_path = os.path.dirname(os.path.abspath(__file__))
storage_file = os.path.join(project_path, 'db_storage.json')
project_path = os.path.dirname(os.path.abspath(__file__))
param_file = os.path.join(project_path, 'etl_params.json')

db = TinyDB(storage_file)

with open(param_file, "r") as f:
    json_data = json.load(f)

for entry in json_data:
    for key in json_data[entry]:
        print(key)

    # print(entry)
    # db.insert(entry)

var = 'jnbkhsdkjahdskj'
var1 = var[:5]

print(var1)

# print(json_data)

# project_path = os.path.dirname(os.path.abspath(__file__))
# param_file = os.path.join(project_path, 'credentials.json')
# db = TinyDB(param_file)

# Credential = Query()

# result = db.search(Credential.NAME == "MSSQL_DATALAKE_SERVER")
# pprint(result)
