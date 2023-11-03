from ast import Global
from importlib import import_module
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import datetime
import time
import os, sys
import glob
from re import purge
import requests
import pandas as pd
import json
import io
from pandas import json_normalize
import csv


######################################### PARAMETROS #####################################################

d1 = datetime.timedelta(days=1)
d0 = datetime.datetime.today()
dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_hoje = d0.strftime('%d/%#m/%Y')
dt_ontem = dt_ontem.strftime('%d/%#m/%Y')
hora = d0.strftime('%H')
###############################################################################################################


def report_atendimentos():
    
    varUrl = "https://www15.directtalk.com.br/static/beta/admin/login.html"
    varUsr = 'sempthiago.santana'
    varPwd = 'thiago2022'
    varDownloadDir = 'C:\ETL\DATASCIENCE\RPA\ARQUIVOS'
    DownloadDir = 'C:\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
    # varDriver = "E://OneDrive - URANET PROJETOS E SISTEMAS LTDA/Documents - Engenharia/ETL/DATASCIENCE/RPA/DRIVERS/chromedriver.exe" # Rede
    varDriver = "C://ETL/DATASCIENCE/RPA/DRIVERS/chromedriver.exe" # Local Chrome
    ######################################### APAGA ARQUIVOS ##########################################################
    padrao_arquivo = "BuscaAtendimentos*.*"
    for f in glob.glob(f"{DownloadDir}{padrao_arquivo}"):
            os.remove(f) 
    ######################################### OPTIONS ##########################################################
    chrome_options = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : f'{varDownloadDir}'}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=varDriver, chrome_options=chrome_options)
    driver.get(varUrl)

    elemLogin = 'login'
    elemSenha = 'password'
    elemBtn = 'loginButton'

    ######################################### TRATAMENTO DE DATAS #####################################################
    if hora <= '07':
        dt = dt_ontem
        pDestino1 = 'tb_busca_atendimentos_hist_wpp'
        pCmd1 = '-- Do Nothing'

    else:
        dt = dt_hoje
        pDestino1 = 'tb_busca_atendimentos_nrt_wpp'
        pCmd1 = 'TRUNCATE tb_busca_atendimentos_nrt_wpp;'

    # dt = '18/9/2022'
    # pDestino1 = 'tb_busca_atendimentos_hist_wpp'
    # pCmd1 = '-- Do Nothing'
    
    ######################################### STEP 1: AUTENTICAÇÃO #####################################################
    assert "Login" in driver.title

    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.ID, 'login')))
    elem = driver.find_element_by_id("login")
    elem.clear()
    elem.send_keys(varUsr)
    elem.send_keys(Keys.RETURN)

    elem = driver.find_element_by_id("password")
    elem.clear()
    elem.send_keys(varPwd)

    elem = driver.find_element_by_id("loginButton")
    elem.send_keys(Keys.ENTER)

    assert "Página não encontrada." not in driver.page_source

    #################################################################################################################
    ######################################### STEP 2: EXTRAÇÃO DO RELATÓRIO #####################################################
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div/div/div/div/ul/li[2]/p/a/span')))
    elem = driver.find_element_by_xpath('/html/body/div[3]/div/div/div/div/ul/li[2]/p/a/span')
    elem.click()
    # RELATÓRIO 1: EXPORTADOR DE ATENDIMENTOS
    urlrpt = 'https://www15.directtalk.com.br/static/beta/admin/main.html#!/relatorios/exportacaoatendimento?depto=-1'
    driver.get(urlrpt)
    # FILTRO 1: DATA DO INÍCIO
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dt-exportador-button"]/span')))
    elem = driver.find_element_by_xpath('//*[@id="dataInicial"]/a')
    elem.click()
    elem = driver.find_element_by_xpath('//*[@id="dataInicial"]/div/div[1]/div[1]/input')
    # elem.send_keys('24/9/2022')
    elem.send_keys(dt)
    elem.send_keys(Keys.ENTER)
    # FILTRO 2: DATA DO FIM
    elem = driver.find_element_by_xpath('//*[@id="dataFinal"]/a')
    elem.click()
    elem = driver.find_element_by_xpath('//*[@id="dataFinal"]/div/div[1]/div[1]/input')
    # elem.send_keys('24/9/2022')
    elem.send_keys(dt)
    elem.send_keys(Keys.ENTER)
    # FILTRO 3: DATA DO FIM DO ATENDIMENTO
    elem = driver.find_element_by_xpath('//*[@id="dt-exportador-select-data-1"]')
    elem.click()

    elem = driver.find_element_by_xpath('//*[@id="dt-exportador-button"]')
    elem.click()

    # FILTRAR DATA FIM DO ATENDIMENTO

    ######################################### STEP 3: AGUARDA CONCLUSÃO DO DOWNLOAD #####################################################
    #BuscaAtendimentos_20220509_200910.csv.crdownload

    tag = 1
    while 1 == tag:
        listaArquivos = []
        time.sleep(5)
        for arquivo in os.listdir(varDownloadDir):
            listaArquivos.append(arquivo)
        matching = [s for s in listaArquivos if "crdownload" in s]
        if bool(matching):
            tag = 1
        else:
            tag = 0
    driver.close()
    matching = [s for s in listaArquivos if "BuscaAtendimentos" in s]
    param1 = varDownloadDir+"\\"+matching[0]
    return param1, pDestino1, pCmd1
    sys.exit()
##########################################################################################################################


def agent_event():

    if hora <= '07':
        dt = datetime.datetime.today() - datetime.timedelta(days=1)
        pDestino2 = 'tb_agent_events_hist_wpp'
        pCmd2 = '-- Do Nothing'

    else:
        dt = datetime.datetime.today()
        pDestino2 = 'tb_agent_events_nrt_wpp'
        pCmd2 = 'TRUNCATE tb_agent_events_nrt_wpp;'

    ano = dt.year
    mes = dt.month
    dia = dt.day

    dt1 = datetime.datetime(ano, mes, dia)
    dt2 = datetime.datetime(ano, mes, dia, 23, 59)

    # dt1 = datetime.datetime(2022, 5, 1)
    # dt2 = datetime.datetime(2022, 5, 22, 23, 59)

    epoch1 = round(dt1.timestamp())
    epoch2 = round(dt2.timestamp())

    varDownloadDir = 'C:\ETL\DATASCIENCE\RPA\ARQUIVOS'
    DownloadDir = 'C:\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
    ######################################### APAGA ARQUIVOS ##########################################################
    padrao_arquivo = "agent_events*.*"
    for f in glob.glob(f"{DownloadDir}{padrao_arquivo}"):
        os.remove(f) 

    url = f'https://api.directtalk.com.br/1.10/info/reports/platform/agentevents?startDate={epoch1}&endDate={epoch2}'
    usr = 'semp2cf5028b-834d-4691-b714-2996945bc936'
    pwd = '79gdxgq8rnzjxhgv2e5r'
    r = requests.get(url, auth=(f'{usr}', f'{pwd}'))

    dados = r.json()
    df = pd.DataFrame(dados)
    DownloadDir = 'C://ETL/DATASCIENCE/RPA/ARQUIVOS/'
    arq = 'agent_events.csv'
    df.to_csv(DownloadDir+arq, sep=';', index=False, line_terminator= '\n')
    param2 = varDownloadDir+"\\"+arq

    return param2, pDestino2, pCmd2


def agent_event_range():

    if hora <= '07':
        dt = datetime.datetime.today() - datetime.timedelta(days=1)
        pDestino2 = 'tb_agent_events_hist_wpp'
        pCmd2 = '-- Do Nothing'

    else:
        dt = datetime.datetime.today()
        pDestino2 = 'tb_agent_events_nrt_wpp'
        pCmd2 = 'TRUNCATE tb_agent_events_nrt_wpp;'

    ano = dt.year
    mes = dt.month
    dia = dt.day
    
    dt1 = datetime.datetime(ano, mes, dia, 00, 00, 00)
    dt2 = datetime.datetime(ano, mes, dia, 23, 59, 59)
    # CONVERTE O TIMESTAMP EPOCH PARA O FUSO HORARIO GMT-3(USA)
    UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
    # dt1 = datetime.datetime(2022, 6, 10, 00, 00, 00)
    dt1 = dt1 - UTC_OFFSET_TIMEDELTA
    # dt2 = datetime.datetime(2022, 6, 12, 23, 59, 59)
    dt2 = dt2 - UTC_OFFSET_TIMEDELTA
    epoch1 = round(dt1.timestamp())
    epoch2 = round(dt2.timestamp())

    varDownloadDir = 'C:\ETL\DATASCIENCE\RPA\ARQUIVOS'
    DownloadDir = 'C:\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
    ######################################### APAGA ARQUIVOS ##########################################################
    padrao_arquivo = "*agent_events*.*"
    for f in glob.glob(f"{DownloadDir}{padrao_arquivo}"):
        os.remove(f) 
    dia = 2
##################################################### LOOP PAGINAÇÃO ##################################################################

    pagina = 1
    url = f'https://api.directtalk.com.br/1.10/info/reports/platform/agentevents?startDate={epoch1}&endDate={epoch2}'
    paramsurl = {'pageNumber': pagina}
    usr = 'semp2cf5028b-834d-4691-b714-2996945bc936'
    pwd = '79gdxgq8rnzjxhgv2e5r'
    r = requests.get(url, auth=(f'{usr}', f'{pwd}'))
    list_headers = r.headers 
    print('Paginas: ', list_headers['X-Pagination-TotalPages'])
    max_pages = int(list_headers['X-Pagination-TotalPages'])
    #dados = r.json()
    results = []
    for pagina in range(1, max_pages+1):
        paramsurl['pageNumber'] = pagina
        r = requests.get(url, params=paramsurl, auth=(f'{usr}', f'{pwd}'))
        print('Página Atual: ',pagina)
        dados = r.json()
        for i in dados:
            results.append(i)

    # for i in dados:
    #     results.append(i)
    # while r.status_code == 200:
    #     print('Dia Atual: ',dia)
    #     print('Página Atual: ',pagina)
    #     pagina += 1
    #     print('Dia Atual: ',dia)
    #     print('Página Atual: ',pagina)
    #     paramsurl['pageNumber'] = pagina
    #     print('status_code: ',r.status_code)
    #     r = requests.get(url, params=paramsurl, auth=(f'{usr}', f'{pwd}'))
    #     print(url, paramsurl)
    #     dados = r.json()
    #     for i in dados:
    #         results.append(i)
    #     else:
    #         break
    print('Request completado!')
    print(url)
##################################################### LOOP PAGINAÇÃO ##################################################################

    # df = pd.DataFrame(dados)
    df = pd.DataFrame(results)
    DownloadDir = 'C://ETL/DATASCIENCE/RPA/ARQUIVOS/'
    # arq = f'agent_events_{ano}{mes}{dia}.csv'
    arq = 'agent_events.csv'
    # print(df)
    df.sort_values(by=['date'], ascending=True, ignore_index=True, inplace=True)
    # CONVERTE O TIMESTAMP EPOCH PARA O FUSO HORARIO GMT+3(BRASIL)
    # epoch = round(datetime.datetime.utcfromtimestamp(epoch1).timestamp())
    # utcfromtimestamp convert para o GMT do servidor Directalk
     # fromtimestamp convert para o timezone local
    df['date'] = df['date'].apply(lambda x: round(datetime.datetime.utcfromtimestamp(x).timestamp()))
    df['dt'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
    df['time'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%H:%M:%S'))
    df.to_csv(DownloadDir+arq, sep=';', index=True, line_terminator= '\n')
    param2 = varDownloadDir+"\\"+arq
    print('Arquivo gerado!')

    return param2, pDestino2, pCmd2


if __name__ == '__main__':
    report_atendimentos()
    sys.exit()
# agent_event_range()

# report_atendimentos()