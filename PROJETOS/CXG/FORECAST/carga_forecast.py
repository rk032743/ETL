from msilib import sequence
from pathlib import Path
import os, logging, shutil
import pandas as pd
from datetime import datetime, timedelta
import time
from get_dir import get_onedrive_dirs
# from telegram_bot import bot_notification, bot_notification2
from mariadb import MariaDB

# PARÂMETROS


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



def forecast(REPRESENTANTE, ARQUIVO, BANCO, TABELA, ID):

    global PLAN_DIR
    global DUMP_DIR
    global FORECAST_DIR
    global end_process
    global start_process

    PLAN_DIR = os.path.join(DIRS['plan_dir'])
    DUMP_DIR = os.path.join(PLAN_DIR, REPRESENTANTE, 'Forecast', 'CARREGADOS')
    FORECAST_DIR = os.path.join(PLAN_DIR, REPRESENTANTE, 'Forecast')
    tabela = BANCO+'.'+TABELA

    cols = ['DATA','INTERVALO','ATENDIMENTO','AGRUPAMENTO','VOLUME','PA','TMO','NS', 'NR17', 'REFORCO', 'DIALOGO','FEEDBACK', 'PARTICULAR']

    flag = 'Concluído'
    report = 'FORECAST'
    start_process = datetime.now()
    filename=os.path.join(FORECAST_DIR, ARQUIVO)
    t = os.path.getctime(filename)
    file_date = datetime.fromtimestamp(t)
    # Load the xlsx file
    excel_data = pd.read_excel(filename, sheet_name='BASE', engine='openpyxl', converters={'AGRUPAMENTO':str})

    data = pd.DataFrame(excel_data, columns=['DATA','INTERVALO','AGRUPAMENTO','VOLUME','TMO','PA',
                        'NS','ATENDIMENTO','NR17','REFORCO','DIALOGO','FEEDBACK','PARTICULAR'])
    # print(data.dtypes)
    data = data[cols]
    filename = filename.replace(".xlsx", ".csv")
    data = data[data['DATA'].dt.strftime('%Y%m') == year_month]
    # data = data[data['DATA'].dt.strftime('%Y%m') >= '202301']
    data['INTERVALO'] = data['INTERVALO'].apply(lambda x: x.strftime('%H:%M:%S'))
    # data.sort_values('DATA', ascending=True, inplace=True)
    data.sort_values(by=['DATA','INTERVALO'], ascending=True, inplace=True)
    print(data)
    data.to_csv(filename, sep=';',index=False, lineterminator= "\r\n", encoding='utf-8')
    delete = f"DELETE FROM {tabela} WHERE EXTRACT(YEAR_MONTH FROM DATA) >= {year_month};"
    logging.info(f"CSV Gerado!")
    m = MariaDB()
    # 12. Insert data
    logging.info("Iniciando a carga!")
    m.load_data(delete, filename, tabela, 0)
    logging.info("Registros carregados!")
    end_process = datetime.now()

    
    move_files()


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
            "responsavel = 'EWERTON PREDIGER', "\
            f"etl_data = '{end_process}' "\
            f"WHERE id = {ID}"
    cnn = MariaDB()
    cnn.execute_sql(sql)
    print("LOG ATUALIZADO!")
    logging.info(metadata)
    print(metadata)
    

    cnn = MariaDB()
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
    
    return data


if __name__ == '__main__':

    d = forecast()