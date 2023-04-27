
import os
from datetime import date, timedelta, datetime
from time import gmtime, strftime
import pandas as pd
#from CONFIG import konnectDB
from CONFIG.ConectorMySQL import konnectDB
from FUNC.DW_FUNC_T import *


# dt_default = '2022-02-01'
dt_default = (date.today() - timedelta(days=1))

dirJob = "C:\\ETL\\DATASCIENCE\\PENTAHO\\WORKFLOW_DM_001\\"

diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
not_in = "'test', 'bd_wr_analise', 'bd_bi_dimep'"


def tb_ponto(): # OK

    dt_limite = (date.today() - timedelta(days=1))
    tabela = 'tb_ponto'
    proc = '-- N/A'
    destino = 'dm_db_tempos'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'

    arquivos = "*tb_ponto*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    sql1  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    cnn = konnectDB(2)
    df = cnn.select(sql1)
    cnn.quit
    dt_mx = df.iat[0,0]
    print('\n',dt_mx)
    dt = rngDatas(dt_mx, dt_limite)
    print('\n',dt)
    df_data = pd.DataFrame(data=dt)
    lista_datas = f'lista_datas_{tabela}.csv'
    lista_datas = f"{diretorio}{lista_datas}"
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2))
    sql2 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ({not_in})
    """
    cnn = konnectDB(1)
    df = cnn.select(sql2)
    cnn.quit
    db = df['banco']
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data_entrada' # REPLICAR
    where = f'WHERE {campo_chave_dt} = '+'${vDataRef}'
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_produtividade_tempo(): # OK

    dt_limite = (date.today() - timedelta(days=1))
    tabela = 'tb_produtividade_tempo'
    proc = '-- N/A'
    destino = 'dm_db_tempos'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    arquivos = "*tb_produtividade_tempo*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    sql1  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"

    cnn = konnectDB(2)
    df = cnn.select(sql1)
    cnn.quit
    dt_mx = df.iat[0,0]

    print('\n',dt_mx)
    dt = rngDatas(dt_mx, dt_limite)
    print('\n',dt)
    df_data = pd.DataFrame(data=dt)
    lista_datas = f'lista_datas_{tabela}.csv'
    lista_datas = f"{diretorio}{lista_datas}"
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2))
    sql2 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ({not_in})
    """
    cnn = konnectDB(1)
    df = cnn.select(sql2)
    cnn.quit
    db = df['banco']

    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data' # REPLICAR
    where = f'WHERE {campo_chave_dt} = '+'${vDataRef}'

    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')
    
    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_ptoespelho_marcacoes(): # OK

    dt_vigente = (date.today() + timedelta(days=31))
    dt_anterior= (date.today() - timedelta(days=31))

    mes_folha_ini = dt_anterior.strftime("%Y%m") # mes passado
    mes_folha_fim = dt_vigente.strftime("%Y%m") # esse mês

    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    tabela = 'tb_ptoespelho_marcacoes'
    proc = 'CALL dm_db_tempos.sp_master_tempos;'

    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_SCHEMA = 'bd_bi_call_center' AND TABLE_NAME LIKE '{tabela}%'
    """

    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit

    destino = 'dm_db_tempos'
    arquivos = "*tb_ptoespelho_marcacoes*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR

    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"

    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'anomes_folha' # REPLICAR
    where = f"WHERE {campo_chave_dt} BETWEEN {mes_folha_ini} AND {mes_folha_fim}"
    # where = f"WHERE {campo_chave_dt} >= '202012'"
    sql2  = f"DELETE FROM {destino}.{tabela}_stg WHERE {campo_chave_dt} BETWEEN {mes_folha_ini} AND {mes_folha_fim}"
    cnn = konnectDB(2)
    df = cnn.exec(sql2)
    cnn.quit

    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']

    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    df = {'data': [dt]} # REPLICAR
    # df = [mes_folha_ini,mes_folha_fim] # REPLICAR
    print(type(df)) # REPLICAR
    df_data = pd.DataFrame(data=df) # REPLICAR
    lista_datas = f'lista_datas_{tabela}.csv' # REPLICAR
    lista_datas = f"{diretorio}{lista_datas}" # REPLICAR
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2)) # REPLICAR

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_ptoespelho_dsr():  # OK

    dt_limite = (date.today() - timedelta(days=1))
    tabela = 'tb_ptoespelho_dsr'
    proc = '-- N/A'
    destino = 'dm_db_tempos'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    arquivos = "*tb_ptoespelho_dsr*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    sql1  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    cnn = konnectDB(2)
    df = cnn.select(sql1)
    cnn.quit
    dt_mx = df.iat[0,0]

    print('\n',dt_mx)
    dt = rngDatas(dt_mx, dt_limite)
    print('\n',dt)
    df_data = pd.DataFrame(data=dt)
    lista_datas = f'lista_datas_{tabela}.csv'
    lista_datas = f"{diretorio}{lista_datas}"
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2))
    sql2 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_SCHEMA = 'bd_bi_call_center' AND TABLE_NAME = '{tabela}'
    """
    print(sql2)
    cnn = konnectDB(1)
    df = cnn.select(sql2)
    cnn.quit
    db = df['banco']

    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"

    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data_entrada' # REPLICAR
    where = f'WHERE {campo_chave_dt} = '+'${vDataRef}' # REPLICAR
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_escala_intergrall(): # OK

    dt_limite = (date.today() - timedelta(days=1))
    tabela = 'tb_escala_intergrall'
    proc = '-- N/A'
    destino = 'dm_db_tempos'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    arquivos = "*tb_escala_intergrall*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    sql1  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    cnn = konnectDB(2)
    df = cnn.select(sql1)
    cnn.quit
    dt_mx = df.iat[0,0]

    print('\n',dt_mx)
    dt = rngDatas(dt_mx, dt_limite)
    print('\n',dt)
    df_data = pd.DataFrame(data=dt)
    lista_datas = f'lista_datas_{tabela}.csv'
    lista_datas = f"{diretorio}{lista_datas}"
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2))
    sql2 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_SCHEMA = 'bd_bi_call_center' AND TABLE_NAME = '{tabela}'
    """
    print(sql2)
    cnn = konnectDB(1)
    df = cnn.select(sql2)
    cnn.quit
    db = df['banco']

    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"

    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data_entrada' # REPLICAR
    where = f'WHERE {campo_chave_dt} = '+'${vDataRef}' # REPLICAR    
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_ptoespelho_dados(): # OK

    dt_limite = (date.today() - timedelta(days=1))
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    tabela = 'tb_ptoespelho_dados'
    proc = '-- N/A'

    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_SCHEMA = 'bd_bi_call_center' AND TABLE_NAME = '{tabela}'
    """

    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit

    destino = 'dm_db_tempos'
    arquivos = "*tb_ptoespelho_dados*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR

    
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"

    where = ''
    sql2  = f"TRUNCATE {destino}.{tabela}_stg"
    cnn = konnectDB(2)
    df = cnn.exec(sql2)
    cnn.quit

    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']

    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data_cri' # REPLICAR

    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')
    
    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    df = {'data': [dt]} # REPLICAR
    print(type(df)) # REPLICAR
    df_data = pd.DataFrame(data=df) # REPLICAR
    lista_datas = f'lista_datas_{tabela}.csv' # REPLICAR
    lista_datas = f"{diretorio}{lista_datas}" # REPLICAR
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2)) # REPLICAR

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_ptoespelho_afastamentos(): # OK

    dt_limite = (date.today() - timedelta(days=1))
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_TEMPOS/'
    tabela = 'tb_ptoespelho_afastamentos'
    proc = '-- N/A'

    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_SCHEMA = 'bd_bi_call_center' AND TABLE_NAME = '{tabela}'
    """

    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit

    destino = 'dm_db_tempos'
    arquivos = "*tb_ptoespelho_afastamentos*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR

    
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"

    where = ''
    sql2  = f"TRUNCATE {destino}.{tabela}_stg"
    cnn = konnectDB(2)
    df = cnn.exec(sql2)
    cnn.quit

    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']

    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'data_cri' # REPLICAR

    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')
    
    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    df = {'data': [dt]} # REPLICAR
    print(type(df)) # REPLICAR
    df_data = pd.DataFrame(data=df) # REPLICAR
    lista_datas = f'lista_datas_{tabela}.csv' # REPLICAR
    lista_datas = f"{diretorio}{lista_datas}" # REPLICAR
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2)) # REPLICAR

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return