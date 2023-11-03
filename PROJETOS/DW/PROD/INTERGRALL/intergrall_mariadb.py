import pandas as pd
import mysql.connector as mysql
import json, sys, os, logging



class MariaDB:

    def __init__(self, id):
        # 1. Realiza a conexão com o MySQL.
        global conn
        global cnx

        self.__id = id

        if self.__id == 1:
            self.db_config = {'host': '192.168.1.11','user': 'adm_etl', 'passwd': 'Sh34d%1', 'db': 'information_schema',
                                'connect_timeout': 10, 'charset': 'utf8', 'allow_local_infile': 'True'}
        elif self.__id == 2:
            self.db_config = {'host': '192.168.1.10','user': 'report.etl', 'passwd': 'eu2czb', 'db': 'information_schema',
                                'connect_timeout': 10, 'charset': 'utf8', 'allow_local_infile': 'True'}
        try:
            try:
                if conn:
                    conn.close()
            except:
                pass
            
            if self.__id == 1:
                conn = mysql.connect(user='adm_etl', password='Sh34d%1',
                                                host='192.168.1.11',
                                                database='information_schema',
                                                charset='utf8',
                                                allow_local_infile=True)
            elif self.__id == 2:
                 conn = mysql.connect(user='report.etl', password='eu2czb',
                                                host='192.168.1.10',
                                                database='information_schema',
                                                charset='utf8',
                                                allow_local_infile=True)               

            # conn = mysql.connect(**self.db_config)
            cnx = conn.cursor(dictionary=True, buffered=True)
            logging.info('Conectado ao BD!')
        except mysql.Error as err:
            class_err, err, tb = sys.exc_info()
            logging.error(f"Erro: {class_err}, {err}, {tb.tb_lineno}, {tb.tb_frame.f_code.co_filename}")


    def close(self):
        # 1. Encerra a conexão com o MySQL.
        if conn:
            conn.close()
            logging.info('Desconectado do BD!')


    def read_sql(self, query) -> pd.DataFrame:
        # 1. Realiza consultas.
        # 2. Verifica se a consulta trouxe resultados, se sim, alimenta um Dataframe com eles, se não, cria um Dataframe nulo.
        # 3. Em caso de erros exibe uma mensagem.
        global df
        self.__query = query
        try:
            for result in cnx.execute(self.__query, multi=True):
                if result.with_rows:
                    colunas = cnx.description
                    resultado = []
                    resultado = cnx.fetchall()
                    df = pd.DataFrame()
                    df = pd.DataFrame(data=resultado)
                else:
                    df = None
                self.close()
        except mysql.Error as err:
            class_err, err, tb = sys.exc_info()
            logging.error(f"Erro: {class_err}, {err}, {tb.tb_lineno}, {tb.tb_frame.f_code.co_filename}")
            df = None
            self.close()

        self.close()

        return df


    def load_data(self, cmd, arquivo, tabela, tipo, *args):
        # load_data(self, caminho, arquivo, tabela):
        # 1. Realiza o bulk insert de um arquivo no disco.
        # 2. Em caso de erros exibe uma mensagem.
        # arquivo = os.path.join(caminho, arquivo)
        # pathfile = arquivo.replace(os.path.sep, '/')
        pathfile = arquivo.replace(os.path.sep, '/')
        if tipo == 1:
            insert = "REPLACE"
        else:
            insert = "IGNORE"

        comando = rf"""LOAD DATA LOCAL INFILE '{pathfile}'
                    {insert} INTO TABLE {tabela}
                    CHARACTER SET utf8mb4 
                    FIELDS TERMINATED BY ';'
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\r\n'
                    IGNORE 1 LINES;"""
        try:
            cnx.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
            conn.commit()
            cnx.execute(cmd)
            conn.commit()
            cnx.execute(comando)
            conn.commit()
            cnx.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
            conn.commit()
            logging.info(f'Comando executado: {comando}')
            self.close()

        except mysql.Error as err:
            conn.rollback()
            class_err, err, tb = sys.exc_info()
            logging.error(f"Erro: {class_err}, {err}, {tb.tb_lineno}, {tb.tb_frame.f_code.co_filename}")
            msg = f"Comando não executado: {comando}"
            logging.error(f"{comando}")
            self.close()
        self.close()

        return


    def execute_sql(self, comando):
        # 1. Usado para Insert, Update, Delete e Truncate.
        comando
        try:
            cnx.execute(comando)
            conn.commit()
            logging.info(f'Comando executado: {comando}')
            self.close()
        except mysql.Error as err:
            conn.rollback()
            class_err, err, tb = sys.exc_info()
            logging.error(f"Erro: {class_err}, {err}, {tb.tb_lineno}, {tb.tb_frame.f_code.co_filename}") 
            self.close()

        self.close()

        return comando


