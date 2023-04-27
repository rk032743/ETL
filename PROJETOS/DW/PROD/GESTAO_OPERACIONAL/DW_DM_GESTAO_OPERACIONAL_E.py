import os
from datetime import date, timedelta, datetime
from time import gmtime, strftime
import pandas as pd
from CONFIG.ConectorMySQL import konnectDB
from FUNC.DW_FUNC_T import *

dt_default = '2022-07-01'


dirJob = "C:\\ETL\\DATASCIENCE\\PENTAHO\\WORKFLOW_DM_001\\"
# dirJob = "\\\\\SRVUSUARIOS\\BI_Adm\\Data_Science_Team\\Ewerton\\CODE\\DATASCIENCE\\PENTAHO\\WORKFLOW_DM_001\\"

not_in = "'test', 'bd_wr_analise'"


def tb_srh():

    dt_limite = (date.today() - timedelta(days=0))
    tabela = 'tb_srh'
    proc = '-- N/A'
    destino = 'dm_db_gestao_operacional'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_GESTAO_OPERACIONAL/'
    arquivos = "*tb_srh*.*"
    checa_pasta(diretorio)
    remove_arquivos(diretorio, arquivos)
    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ({not_in})
    """
    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit
    sql2  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    cnn = konnectDB(2)
    df = cnn.select(sql2)
    cnn.quit
    dt_mx = df.iat[0,0]
    print('\n',dt_mx)
    # dt = rngDatas(dt_mx, dt_limite)
    # print('\n',dt)
    # df_data = pd.DataFrame(data=dt)
    dt = date.today() # REPLICAR
    dt = dt.strftime("%Y-%m-%d") # REPLICAR
    df = {'data': [dt]} # REPLICAR
    print(type(df)) # REPLICAR
    df_data = pd.DataFrame(data=df) # REPLICAR
    lista_datas = f'lista_datas_{tabela}.csv'
    lista_datas = f"{diretorio}{lista_datas}"
    print('\n', "Lista de datas:", lista_datas, file=df_data.to_csv(lista_datas, header=None, index=None, sep=';'
    , line_terminator='\r\n', quotechar="'", quoting=2))
    sql2  = f"TRUNCATE {destino}.{tabela}_stg"
    cnn = konnectDB(2)
    df = cnn.exec(sql2)
    cnn.quit
    # db_list = [f'bd_bi_qsaude.{tabela}', f'bd_bi_bmg.{tabela}']
    # db = pd.DataFrame(data=db_list)
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    #dirJob = "C:\\Pentaho\\ETL\\WORKFLOW_DM_01\\"
    #pentahoJob = "JB_LOOP_CONTROL_0"
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* "
    campo_chave_dt = 'data_solicitacao'
    where = f"WHERE {campo_chave_dt} >= '2021-01-01' AND tipo_solicitacao_rh IN ('DM', 'PE', 'TR')"
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')
    dt = date.today()
    dt =dt.strftime("%Y-%m-%d")
    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_descricao():

    tabela = 'tb_descricao'
    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise')
    """
    print(f'SQL ---> {sql1}')
    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit
    diretorio = "C:/DS_STAGEDIR_TEMP/DM_GESTAO_OPERACIONAL/"
    destino = "dm_db_gestao_operacional"
    proc = f'CALL {destino}.sp_rank_descricao;'
    arquivos = [f'{tabela}.txt', f'bd_bi_{tabela}.csv']
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'etl_data' # REPLICAR
    where = ''
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'

    arquivos = "*tb_descricao*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
 
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

    sql  = f"TRUNCATE {destino}.{tabela}_stg"
    cnn = konnectDB(2)
    df = cnn.exec(sql)
    cnn.quit

    cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}" # REPLICAR
    cmd2 = f"""  "/param:pTabela={tabela}" "/param:pData='{dt}'" "/param:pArquivo={arquivo}" "/param:pWhere={where}" "/param:pCols={select_cols}" "/param:pCampoDT={campo_chave_dt}" """ # REPLICAR
    cmd3 = f""" "/param:pDump={diretorio}" "/param:pDestino={destino}" "/param:pProc={proc}" "/param:pTipo=2" "/param:pArquivoDatas={lista_datas}" "/param:pMesAlvo='%{tabela}'" """ # REPLICAR
    comando = f"{cmd1}{cmd2}{cmd3}"
    os.system(comando)

    return


def tb_hierarquia_chefe():

    start_process = datetime.now()
    dt_limite = (date.today() - timedelta(days=1))
    tabela = 'tb_hierarquia_chefe'
    proc = '-- N/A'
    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise')
    """
    print(f'SQL ---> {sql1}')
    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit
    destino = 'dm_db_gestao_operacional'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_GESTAO_OPERACIONAL/'
    arquivos = "*tb_hierarquia_chefe*.*"
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* "
    campo_chave_dt = 'data_atualiza_1' # REPLICAR
    where = '' # CARGA FULL
    #where = f'WHERE {campo_chave_dt} >= '+'${vDataRef}'
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    sql2  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    sql3 = f"TRUNCATE {destino}.{tabela}_stg'"
    cnn = konnectDB(2)
    df = cnn.select(sql2)
    cnn.exec(sql3)
    cnn.quit

    dt = df.iat[0,0]
    # dt = date.today() # CARGA FULL
    # dt = dt.strftime("%Y-%m-%d") # CARGA FULL
    df = {'data': [dt]} # CARGA FULL
    print(type(df)) # CARGA FULL
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

    end_process = datetime.now()

    # ETL METADATA
    metadata = dict()
    metadata['Processo'] = "Forecast"
    metadata['Registros'] = int(df_data.shape[0])

    if metadata['Registros'] == 0:
        status = "Erro"
    else:
        status = "Completo"
    metadata['Status'] = status
    metadata['Início'] = start_process.strftime("%Y-%m-%d %H:%M:%S")
    metadata['Fim'] = end_process.strftime("%Y-%m-%d %H:%M:%S")
    duration = end_process - start_process
    tempo = time.gmtime(duration.total_seconds())
    metadata['Duração'] = time.strftime("%H:%M:%S", tempo)
    print(metadata)

    return metadata


def tbadmin():

    dt_limite = (date.today() - timedelta(days=0))
    tabela = 'tbadmin'
    proc = 'CALL dm_db_gestao_operacional.sp_master_gestao_operacional;'
    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise')
    """
    print(f'SQL ---> {sql1}')
    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit
    destino = 'dm_db_gestao_operacional'
    diretorio = 'C:/DS_STAGEDIR_TEMP/DM_GESTAO_OPERACIONAL/'
    arquivos = "*tbadmin*.*"
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    select_cols = "NULL AS etl_origem,NULL AS etl_empresa,NULL AS etl_data,tb.data_geracao,tb.login,tb.login_ant,tb.tp_user,tb.tp_user_ant,tb.nome,tb.hora_trabalho,tb.empresa,tb.cpf,tb.data_admissao,tb.data_atualiza,tb.data_demissao,tb.hora_trabalho_sabado,tb.rh_cargo,tb.sexo,tb.data_nasc,tb.hora_trabalho_domingo,tb.centro_custo,tb.contrato,tb.ilha,tb.carga_horaria,tb.data_ferias,tb.data_ferias_ant,tb.data_promocao,tb.data_promocao_ant,tb.cep,tb.end_tip_log,tb.end_nom_log,tb.end_num,tb.end_compl,tb.bairro,tb.cidade,tb.uf,tb.microsiga_ferias_data_ini,tb.microsiga_ferias_data_fim,tb.dt_ini_afastamento,tb.dt_fim_afastamento,tb.rg,tb.rg_orgao,tb.rg_data,tb.rg_uf,tb.marca_ponto,tb.email,tb.celula,tb.email_uranet,tb.cod_filial_microsiga,tb.login_sistema_cliente,tb.equipe,tb.grupo_atende,tb.atende,tb.sub_niveis,tb.telefone,tb.escala_horario,tb.lanche_entrada,tb.lanche_saida,tb.lanche_entrada_sabado,tb.lanche_saida_sabado,tb.lanche_entrada_domingo,tb.lanche_saida_domingo,tb.ramal"
    # select_cols = "NULL AS etl_origem,NULL AS etl_empresa,NULL AS etl_data, tb.*"

    campo_chave_dt = "data_geracao"
    # DEFINIDO A CARGA COMPLETA DA TABELA COM O DÁRIO EM 13/12/2022
    where = f"WHERE tb.rh_cargo NOT IN ('EXT') AND LEFT(tb.login, 1) = 'r' " # CARGA FULL
    # where = f"WHERE {campo_chave_dt} = CURRENT_DATE() AND tb.rh_cargo NOT IN ('EXT') AND LEFT(tb.login, 1) = 'r' "
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'
    db.to_csv(arquivo, header=None, index=None, sep=';', line_terminator='\n')

    # DEFINIDO O TRUNCATE DA TABELA COM O DÁRIO EM 13/12/20202
    sql  = f"TRUNCATE {destino}.{tabela}_stg"
    cnn = konnectDB(2)
    df = cnn.exec(sql)
    cnn.quit

    sql2  = f"SELECT COALESCE(MIN(ult_data), '{dt_default}') as dt_mx FROM dba_db_adm.etl_carga_controle WHERE etl_origem LIKE '%{tabela}' AND etl_destino = '{destino}.{tabela}_stg' AND flag_ativa = 1"
    cnn = konnectDB(2)
    df = cnn.select(sql2)
    cnn.quit


    # dt_mx = df.iat[0,0]
    # print('\n',dt_mx)
    # dt = rngDatas(dt_mx, dt_limite)
    # print('\n',dt)
    # df = dt
    dt = df.iat[0,0]
    # dt = date.today() # CARGA FULL
    # dt = dt.strftime("%Y-%m-%d") # CARGA FULL
    df = {'data': [dt]} # CARGA FULL
    print(type(df)) # CARGA FULL
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


def tb_usuario():

    tabela = 'tb_usuario'
    sql1 = f"""
    SELECT CONCAT(TABLE_SCHEMA, '.' ,TABLE_NAME) as banco
    FROM information_schema.`TABLES`
    WHERE TABLE_NAME = '{tabela}' AND TABLE_SCHEMA NOT IN ('test', 'bd_wr_analise', 'bd_bi_dimep')
    """
    print(f'SQL ---> {sql1}')
    cnn = konnectDB(1)
    df = cnn.select(sql1)
    db = df['banco']
    cnn.quit
    diretorio = "C:/DS_STAGEDIR_TEMP/DM_FUNCIONARIOS/"
    destino = "dm_db_funcionarios"
    proc = f'CALL {destino}.sp_rank_usuario;'
    arquivos = [f'{tabela}.txt', f'bd_bi_{tabela}.csv']
    pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
    select_cols = "NULL AS etl_origem, NULL AS etl_empresa, NULL AS etl_data, tb.* " # REPLICAR
    campo_chave_dt = 'etl_data' # REPLICAR
    where = "WHERE LEFT(login, 1) = 'r'"
    pentahoJob = "JB_MASTER"
    pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
    arquivo = f'{diretorio}bd_bi_{tabela}.csv'

    arquivos = "*tb_usuario*.*" # REPLICAR
    checa_pasta(diretorio) # REPLICAR
    remove_arquivos(diretorio, arquivos) # REPLICAR
 
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