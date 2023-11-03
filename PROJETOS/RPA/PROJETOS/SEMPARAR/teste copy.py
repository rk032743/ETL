from importlib import import_module
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import datetime
import time
import os
import glob


varUrl = "https://www15.directtalk.com.br/static/beta/admin/login.html"
varUsr = 'sempthiago.santana'
varPwd = 'thiago2022'
varDownloadDir = 'C:\ETL\DATASCIENCE\RPA\ARQUIVOS'
#varDriver = "\\\\SRVUSUARIOS\\BI_Adm\\Data_Science_Team\\Ewerton\\CODE\DATASCIENCE\\RPA\\chromedriver.exe" # Rede
varDriver = "C://ETL/DATASCIENCE/RPA/DRIVERS/chromedriver.exe" # Local Chrome
######################################### APAGA ARQUIVOS ##########################################################
DownloadDir = 'C:\\ETL\\DATASCIENCE\\RPA\\ARQUIVOS\\'
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
d1 = datetime.timedelta(days=1)
d0 = datetime.datetime.today()
dt_ontem = d0 - d1
dt_hoje = d0.strftime('%d/%#m/%Y')
dt_ontem = dt_ontem.strftime('%d/%#m/%Y')
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
# RELATÓRIO 1: EVENTO DOS OPERADORES
urlrpt = 'https://www15.directtalk.com.br/static/beta/admin/main.html#!/relatorios/eventosdosoperadores?depto=-1'
driver.get(urlrpt)

# FILTRO 1: DATA DO INÍCIO
wait = WebDriverWait(driver, 10)
element = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dt-eventos-allusers"]')))
element = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[5]/div[2]/div/div[2]/div/div/div[2]/a')))





elem = driver.find_element_by_xpath('//*[@id="dataInicial"]/a')
elem.click()

elem = driver.find_element_by_class_name('quickdate-button ng-binding')
driver.execute_script("arguments[0].setAttribute('title', '12/5/2022')", elem)

elem = driver.find_element_by_xpath('//*[@id="dataInicial"]/div/table/tbody/tr[1]/td[1]')
elem.click()
elem = driver.find_element_by_xpath('//*[@id="dataInicial"]/a')
driver.execute_script("arguments[0].textContent = '13/5/2022';", elem)

# FILTRO 2: DATA DO FIM


elem = driver.find_element_by_xpath('//*[@id="dataFinal"]/a')
elem.click()
driver.execute_script("arguments[0].textContent = '13/5/2022';", elem)
elem.click()
elem.send_keys(Keys.ESCAPE)

# FILTRO 3: GERAÇÃO DO RELATÓRIO
wait = WebDriverWait(driver, 10)
element = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dt-group-modal-table"]/td[3]/i')))

elem = driver.find_element_by_xpath('//*[@id="dt-eventos-allusers"]')
elem.click()

wait = WebDriverWait(driver, 10)
element = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dt-eventos-exportar"]')))

elem = driver.find_element_by_xpath('//*[@id="dt-eventos-exportar"]')
elem.click()

######################################### STEP 3: AGUARDA CONCLUSÃO DO DOWNLOAD #####################################################
#BuscaAtendimentos_20220509_200910.csv.crdownload

# tag = 1
# while 1 == tag:
#     listaArquivos = []
#     time.sleep(5)
#     for arquivo in os.listdir(varDownloadDir):
#         listaArquivos.append(arquivo)
#     matching = [s for s in listaArquivos if "crdownload" in s]
#     if bool(matching):
#         tag = 1
#     else:
#         tag = 0
# matching = [s for s in listaArquivos if "RelatorioOperadores" in s]
# driver.close()


# parametro = varDownloadDir+"\\"+matching[0]

##########################################################################################################################
