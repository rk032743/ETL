from msilib import sequence
import os, logging, shutil
import pandas as pd
from datetime import datetime, timedelta
import time
from get_dir import get_onedrive_dirs
# from telegram_bot import bot_notification, bot_notification2
from mariadb import MariaDB
import json
from pathlib import Path
# PARÂMETROS

pd.options.display.max_columns


    
DIRS = get_onedrive_dirs()
LOG_DIR = DIRS['plan_dir']
# filename_log = os.path.join(LOG_DIR, "Log","forecast.log")
# with open(filename_log, 'w') as fp:
#     pass
# logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s',
#             level=logging.INFO,
#             filename=filename_log)


hora = int(datetime.now().strftime("%H"))
year_month = str((datetime.now() - timedelta(days=0)).strftime(f'%Y%m'))

def read_forecast_param() -> dict:
    root = Path(__file__).parent
    rsp = os.path.join(root, 'forecast.json')
    with open(rsp, 'r') as json_file:
        j = json_file.read()
    rsp_dict = json.loads(j)
    return rsp_dict


def move_files():

    dest = DUMP_DIR
    source = FORECAST_DIR
    files = os.listdir(source)
    for f in files:
        __from = os.path.join(source, f)
        __to = os.path.join(dest, f)
        if f.endswith('.xlsx'):
            shutil.copy(__from, __to)
        if f.endswith('.csv'):
            shutil.move(__from, __to)


def msg(rsp, inicio, fim, report, flag):

        agora = pd.to_datetime(inicio).strftime("%Y-%m-%d")
        tempo = fim-inicio
        msg = f"{report}\n" \
            f"Data: {agora}\n"\
            f"Registros: {rsp}\n"\
            f"Status: {flag}\n"\
            f"Início: {inicio}\n"\
            f"Fim: {fim}\n"\
            f"Duração: {tempo}\n"

        return msg


def remove_csv(file):

    if os.path.exists(file):
        os.remove(file)
        print(f"Deletado: {file}")
    else:
        print("The file does not exist")


def read_forecast(id:int):

    rsp = read_forecast_param()
    result = next(k for k in rsp['LISTA'] if k["ID"] == id)
    print(result['ID'])
    print(result['REPRESENTANTE'])
    print(result['ARQUIVO'])
    print(result['BANCO'])
    print(result['TABELA'])
    df = forecast(result['REPRESENTANTE'],result['ARQUIVO'],result['BANCO'],result['TABELA'],result['ID'])
    print(df)
    return df


def forecast(REPRESENTANTE, ARQUIVO, BANCO, TABELA, ID):

    global PLAN_DIR
    global DUMP_DIR
    global FORECAST_DIR

    PLAN_DIR = os.path.join(DIRS['plan_dir'])
    DUMP_DIR = os.path.join(PLAN_DIR, REPRESENTANTE, 'Forecast', 'CARREGADOS')
    FORECAST_DIR = os.path.join(PLAN_DIR, REPRESENTANTE, 'Forecast')
    tabela = BANCO+'.'+TABELA

    order = ['DATA','INTERVALO','ATENDIMENTO','AGRUPAMENTO','VOLUME','PA','TMO','NS', 'NR17', 'REFORCO', 'DIALOGO','FEEDBACK', 'PARTICULAR']
    
    cols = {'DATA':'data',
            'INTERVALO':'hora',
            'ATENDIMENTO':'atendimento',
            'AGRUPAMENTO':'agrupamento',
            'VOLUME':'recebidas',
            'PA':'logado',
            'TMO':'tma',
            'NS':'ns',
            'NR17':'nr17',
            'REFORCO':'reforco',
            'DIALOGO':'dialogo',
            'FEEDBACK':'feedback',
            'PARTICULAR':'particular'}


    report = 'FORECAST'
    start_process = datetime.now()
    filename=os.path.join(FORECAST_DIR, ARQUIVO)
    t = os.path.getctime(filename)
    file_date = datetime.fromtimestamp(t)
    # Load the xlsx file
    excel_data = pd.read_excel(filename, sheet_name='BASE', engine='openpyxl', converters={'AGRUPAMENTO':str})

    data = pd.DataFrame(excel_data, columns=['DATA','INTERVALO','AGRUPAMENTO','VOLUME','TMO','PA',
                        'NS','ATENDIMENTO','NR17','REFORCO','DIALOGO','FEEDBACK','PARTICULAR'])
    data = data[order]
    filename = filename.replace(".xlsx", ".csv")
    data = data[data['DATA'].dt.strftime('%Y%m') == year_month]
    data['INTERVALO'] = data['INTERVALO'].apply(lambda x: x.strftime('%H:%M:%S'))
    # data.sort_values('DATA', ascending=True, inplace=True)
    data.sort_values(by=['DATA','INTERVALO'], ascending=True, inplace=True)
    data.rename(columns=cols, inplace=True)
    end_process = datetime.now()

    # ETL METADATA
    metadata = dict()
    metadata['Nome'] = report+ ' ' +REPRESENTANTE.upper()
    metadata['Registros'] = int(data.shape[0])

    if metadata['Registros'] == 0:
        status = "Erro"
        obs = "FALHA NO PROCESSO DE CARGA (SEM DADOS NA DATA ESPERADA)"
    else:
        status = "Completo"
        obs = "PROCESSO DE CARGA CONCLUÍDO"
    metadata['Status'] = status
    metadata['Obs'] = obs
    metadata['Início'] = start_process.strftime("%Y-%m-%d %H:%M:%S")
    metadata['Fim'] = end_process.strftime("%Y-%m-%d %H:%M:%S")
    duration = end_process - start_process
    tempo = time.gmtime(duration.total_seconds())
    metadata['Duração'] = time.strftime("%H:%M:%S", tempo)

    sql = "UPDATE dba_db_adm.tb_log_atualizacao "\
            f"SET registros = {metadata['Registros']}, "\
            f"status = '{metadata['Status']}', "\
            f"arquivo = '{ARQUIVO}', "\
            f"cliente = '{REPRESENTANTE}', "\
            f"banco = '{BANCO}', "\
            f"inicio = '{metadata['Início']}', "\
            f"data_postagem = '{metadata['Início']}', "\
            f"fim = '{metadata['Fim']}',"\
            f"duracao = '{metadata['Duração']}', "\
            f"observacao = '{metadata['Obs']}', "\
            f"atualizado_em = '{end_process}', "\
            f"data = '{end_process}', "\
            "responsavel = 'NULL', "\
            f"etl_data = '{end_process}' "\
            f"WHERE id = {ID}"
    cnn = MariaDB()
    cnn.execute_sql(sql)
    print("LOG ATUALIZADO!")
    logging.info(metadata)


    return data
    

if __name__ == '__main__':
    print(read_forecast(86))