from pprintpp import pprint
from config import payload, db_credentials, didactik_cod_curso
from utils_functions import get_html, get_log, get_root_dir
import requests
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import logging
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import Date


pd.options.display.max_columns

db_cred = db_credentials()
engine = create_engine(f"mysql+pymysql://{db_cred['username']}:{db_cred['password']}@{db_cred['host']}/{db_cred['db']}?" \
                        "charset=utf8mb4",isolation_level="READ UNCOMMITTED")


def gera_datas()-> list:

    datas = []
    m0 = pd.to_datetime(datetime.now() - timedelta(days=1))
    m1 = pd.to_datetime(m0 - relativedelta(months=1))
    dt1 = m0.strftime("%Y-%m-01 03:00:00")
    dt2 = m1.strftime("%Y-%m-01 03:00:00")
    m0 = round(pd.to_datetime(dt1).timestamp())
    m1 = round(pd.to_datetime(dt2).timestamp())
    datas.append(m0)
    datas.append(m1)

    return datas

pprint(gera_datas())
# 1. Urls do projeto.
login_url = "https://latamcargodom.zendesk.com/auth/v2/login/signin?return_to=https%3A%2F%2Flatamcargodom.zendesk.com%2Fhc%2Fpt-br&theme=hc&locale=pt-br&brand_id=360000848572&auth_origin=360000848572%2Cfalse%2Ctrue"
locals = "https://latamcargodom.zendesk.com/auth/v2/login/"
# home_url = 'https://didactik.grupokonecta.com/'
# # id = 28899 (Lista de presença), view = 3 (visão mês), curdate = epoch(primeiro dia do mês).
# lista_presenca_url = 'https://didactik.grupokonecta.com/mod/attendance/manage.php?id=28899&view=3&curdate='
# path = get_root_dir()
# filename = os.path.join(path, 'lista_presenca_')


def main():
    pass

    get_log()

# 2. Cria a sessão da chamada.
    with requests.session() as session:
        r = session.get(login_url)

# 3. Recebe o HTML e localiza o token que foi gerado.   
        # soup = BeautifulSoup(r.content, 'html.parser')

        # get_html(soup)
        # for tag in soup.find_all("input", type="email", attrs={"name": "user[email]"}):
        #     token = tag["value"]
        #     # logging.info(f"Token: {token}")

# # 4. Efetua o login com as credenciais e o token.
        r = session.post(login_url, data=payload())
        soup = BeautifulSoup(r.content, 'html.parser')
        # Para validação o Title recebido deve ser 'Brasil'. 
        logging.info(f"Title: {soup.title.text}")
        get_html(soup)
# 5. Acessando a lista de presença.
        # datas = gera_datas()
        # df_temp = pd.DataFrame()

        # for dt in datas:
        #     r = session.get(lista_presenca_url+str(dt))
        #     logging.info(r)
        #     logging.info(lista_presenca_url+str(dt))
        #     soup = BeautifulSoup(r.content, 'html.parser')
        #     # Para validação o Title recebido deve ser 'Brasil'. 
        #     logging.info(f"Title: {soup.title.text}")
        #     get_html(soup)
        #     table = pd.read_html(r.content, attrs={'class': 'generaltable'}, converters={
        #     'Ações': str,
        #     'Unnamed: 6': str,})

        #     df_temp = pd.concat([df_temp, table[0]])

        # # df = pd.DataFrame(table[0], columns=['Data', 'Tempo', 'Tipo', 'Descrição', 'Ações'])
        # df = pd.DataFrame(df_temp, columns=['Data', 'Tempo', 'Tipo', 'Descrição', 'Ações'])
        # df['Data'] = df['Data'].apply(lambda x: str(x)[:-6])
        # # print(df['Data'].unique())
        # df['Data'] = df['Data'].apply(lambda x: pd.to_datetime(x, format="%d/%m/%y", errors='coerce'))
        # df = df.dropna(subset=['Data']).reset_index(drop=True)
        # df['Data'] = df['Data'].dt.strftime("%Y-%m-%d")
        # df['cod_curso'] = df['Tipo'].apply(lambda x: x.replace('Grupo: ', '') if 'Grupo: ' in x else np.nan)
        # cols = ['Data', 'Tempo', 'Tipo', 'Descrição', 'cod_curso']
        # df = df[cols]
        # pprint(df.head(100))
        # # 6. Insere os dados na tabela stage.
        # # didactik_cod_curso().create_all(engine)
        # # df.to_sql('tb_a_didactik_cod_curso_stg', con=engine, index=False, if_exists='replace', dtype={'Data': Date})
        # # # 7. Deleta os dados existentes.
        # # engine.execute("DELETE FROM tb_a_didactik_cod_curso WHERE data IN (SELECT DISTINCT data FROM tb_a_didactik_cod_curso_stg);")
        # # # 8. Insere os novos dados na tabela de destino.
        # # engine.execute("INSERT INTO tb_a_didactik_cod_curso(data, tempo, tipo, descricao, cod_curso) SELECT * FROM tb_a_didactik_cod_curso_stg")
        # # # 9. Exclui a tabela stage.
        # # engine.execute("DROP TABLE tb_a_didactik_cod_curso_stg")
        # # print("finalizado!")
        # # fn = filename+str(dt)+'.csv'
        # # df.to_csv(fn, sep=';',index=False, line_terminator= '\r\n', encoding='utf-8')

    return "Finalizado!"


if __name__ == '__main__':
    main()

